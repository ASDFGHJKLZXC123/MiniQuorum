package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"
)

const (
	dataBlockTarget = 4 << 10
	footerMagic     = "MQSSTB01"
	footerTrailer   = 16 // CRC32C, metadata length, magic.
)

var (
	// ErrCorrupt means the SSTable's bytes are malformed, out of bounds, or
	// fail their CRC32C checksum.
	ErrCorrupt = errors.New("lsm: corrupt sstable")
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
)

// TableEntry is one sorted SSTable record. Tombstone records have no value.
// Seq is the Raft applied index which wrote the value.
type TableEntry struct {
	Key       []byte
	Seq       uint64
	Value     []byte
	Tombstone bool
}

// SSTableWriter validates sorted entries and writes a complete table to w on
// Finish. It intentionally knows nothing about files or durability; callers
// may use any io.Writer, including an injected filesystem implementation.
type SSTableWriter struct {
	w       io.Writer
	entries []TableEntry
	lastKey []byte
	size    int64
	closed  bool
}

// NewSSTableWriter creates a writer for w.
func NewSSTableWriter(w io.Writer) *SSTableWriter { return &SSTableWriter{w: w} }

// Add appends an entry. Keys must be in ascending byte order; versions of the
// same key must be in strictly descending sequence order so a same-key run has
// at most one record per Raft sequence and readers can stop at the first match.
func (w *SSTableWriter) Add(entry TableEntry) error {
	if w == nil || w.w == nil {
		return fmt.Errorf("lsm: nil sstable writer")
	}
	if w.closed {
		return fmt.Errorf("lsm: Add after Finish")
	}
	if len(w.entries) != 0 {
		if !validSSTableRecordOrder(w.lastKey, w.entries[len(w.entries)-1].Seq, entry.Key, entry.Seq) {
			return fmt.Errorf("lsm: entries are not sorted by key then strictly descending sequence")
		}
	}
	copyEntry := TableEntry{Key: append([]byte(nil), entry.Key...), Seq: entry.Seq, Tombstone: entry.Tombstone}
	if !entry.Tombstone {
		copyEntry.Value = append([]byte(nil), entry.Value...)
	}
	w.entries = append(w.entries, copyEntry)
	w.lastKey = copyEntry.Key
	return nil
}

// Finish writes data blocks, index, Bloom filter, and footer. It may be called
// exactly once. A record (or a same-key run) larger than 4 KiB occupies an
// oversized data block rather than being rejected.
func (w *SSTableWriter) Finish() error {
	if w == nil || w.w == nil {
		return fmt.Errorf("lsm: nil sstable writer")
	}
	if w.closed {
		return fmt.Errorf("lsm: Finish called twice")
	}
	w.closed = true
	var table bytes.Buffer
	blocks, err := buildDataBlocks(w.entries)
	if err != nil {
		return err
	}
	index := make([]indexEntry, 0, len(blocks))
	for _, block := range blocks {
		offset := uint64(table.Len())
		if err := appendBlock(&table, block.data); err != nil {
			return err
		}
		index = append(index, indexEntry{firstKey: block.firstKey, offset: offset, length: uint64(table.Len()) - offset})
	}
	indexOffset := uint64(table.Len())
	indexData, err := encodeIndex(index)
	if err != nil {
		return err
	}
	if err := appendBlock(&table, indexData); err != nil {
		return err
	}
	indexLength := uint64(table.Len()) - indexOffset
	filter := NewBloomFilter(len(w.entries))
	for _, entry := range w.entries {
		filter.Add(entry.Key)
	}
	bloomData, err := filter.MarshalBinary()
	if err != nil {
		return err
	}
	bloomOffset := uint64(table.Len())
	if err := appendBlock(&table, bloomData); err != nil {
		return err
	}
	bloomLength := uint64(table.Len()) - bloomOffset
	var minKey, maxKey []byte
	if len(w.entries) != 0 {
		minKey = w.entries[0].Key
		maxKey = w.entries[len(w.entries)-1].Key
	}
	footer, err := encodeFooter(indexOffset, indexLength, bloomOffset, bloomLength, uint64(len(w.entries)), minKey, maxKey)
	if err != nil {
		return err
	}
	table.Write(footer)
	n, err := w.w.Write(table.Bytes())
	if err != nil {
		return err
	}
	if n != table.Len() {
		return io.ErrShortWrite
	}
	w.size = int64(n)
	return nil
}

type builtBlock struct {
	firstKey []byte
	data     []byte
}

func buildDataBlocks(entries []TableEntry) ([]builtBlock, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	blocks := make([]builtBlock, 0, len(entries)/8+1)
	start := 0
	for start < len(entries) {
		var data []byte
		end := start
		for end < len(entries) {
			record, err := encodeRecord(entries[end])
			if err != nil {
				return nil, err
			}
			// Keep all versions of a key in one block. This ensures the index
			// maps a key to every candidate version even at block boundaries.
			if end > start && len(data)+len(record) > dataBlockTarget && !bytes.Equal(entries[end].Key, entries[end-1].Key) {
				break
			}
			data = append(data, record...)
			end++
		}
		blocks = append(blocks, builtBlock{firstKey: append([]byte(nil), entries[start].Key...), data: data})
		start = end
	}
	return blocks, nil
}

func encodeRecord(entry TableEntry) ([]byte, error) {
	if uint64(len(entry.Key)) > math.MaxUint32 || uint64(len(entry.Value)) > math.MaxUint32 {
		return nil, fmt.Errorf("lsm: record field too large")
	}
	var out []byte
	out = appendUvarint(out, uint64(len(entry.Key)))
	out = append(out, entry.Key...)
	var seq [8]byte
	binary.LittleEndian.PutUint64(seq[:], entry.Seq)
	out = append(out, seq[:]...)
	if entry.Tombstone {
		out = append(out, 1)
		out = appendUvarint(out, 0)
		return out, nil
	}
	out = append(out, 0)
	out = appendUvarint(out, uint64(len(entry.Value)))
	out = append(out, entry.Value...)
	return out, nil
}

func decodeRecords(data []byte) ([]TableEntry, error) {
	var entries []TableEntry
	for len(data) != 0 {
		keyLen, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, corruptf("invalid key length")
		}
		data = data[n:]
		if keyLen > uint64(len(data)) || len(data)-int(keyLen) < 9 {
			return nil, corruptf("truncated record")
		}
		key := append([]byte(nil), data[:int(keyLen)]...)
		data = data[int(keyLen):]
		seq := binary.LittleEndian.Uint64(data[:8])
		tombstone := data[8]
		if tombstone > 1 {
			return nil, corruptf("invalid tombstone marker")
		}
		data = data[9:]
		valueLen, n := binary.Uvarint(data)
		if n <= 0 || valueLen > uint64(len(data)-max(n, 0)) {
			return nil, corruptf("invalid value length")
		}
		data = data[n:]
		if valueLen > uint64(len(data)) || (tombstone == 1 && valueLen != 0) {
			return nil, corruptf("truncated or invalid tombstone value")
		}
		entry := TableEntry{Key: key, Seq: seq, Tombstone: tombstone == 1}
		if !entry.Tombstone {
			entry.Value = append([]byte(nil), data[:int(valueLen)]...)
		}
		data = data[int(valueLen):]
		if len(entries) != 0 {
			previous := entries[len(entries)-1]
			if !validSSTableRecordOrder(previous.Key, previous.Seq, entry.Key, entry.Seq) {
				return nil, corruptf("records are not ordered by key then strictly descending sequence")
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// validSSTableRecordOrder is the shared writer/reopen invariant. Equal
// sequences are rejected even for byte-identical records: accepting them would
// make a same-key run contain more than one record for one Raft log position.
func validSSTableRecordOrder(previousKey []byte, previousSeq uint64, key []byte, seq uint64) bool {
	cmp := bytes.Compare(key, previousKey)
	return cmp > 0 || (cmp == 0 && seq < previousSeq)
}

type indexEntry struct {
	firstKey []byte
	offset   uint64
	length   uint64
}

func encodeIndex(index []indexEntry) ([]byte, error) {
	var out []byte
	out = appendUvarint(out, uint64(len(index)))
	for _, entry := range index {
		if uint64(len(entry.firstKey)) > math.MaxUint32 {
			return nil, fmt.Errorf("lsm: index key too large")
		}
		out = appendUvarint(out, uint64(len(entry.firstKey)))
		out = append(out, entry.firstKey...)
		var fixed [16]byte
		binary.LittleEndian.PutUint64(fixed[:8], entry.offset)
		binary.LittleEndian.PutUint64(fixed[8:], entry.length)
		out = append(out, fixed[:]...)
	}
	return out, nil
}

func decodeIndex(data []byte) ([]indexEntry, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 || count > uint64(len(data)) {
		return nil, corruptf("invalid index count")
	}
	data = data[n:]
	index := make([]indexEntry, 0, count)
	for range count {
		keyLen, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, corruptf("invalid index key length")
		}
		data = data[n:]
		if keyLen > uint64(len(data)) || len(data)-int(keyLen) < 16 {
			return nil, corruptf("truncated index entry")
		}
		entry := indexEntry{firstKey: append([]byte(nil), data[:int(keyLen)]...)}
		data = data[int(keyLen):]
		entry.offset = binary.LittleEndian.Uint64(data[:8])
		entry.length = binary.LittleEndian.Uint64(data[8:16])
		data = data[16:]
		if entry.length < 4 || (len(index) != 0 && bytes.Compare(entry.firstKey, index[len(index)-1].firstKey) <= 0) {
			return nil, corruptf("invalid index ordering")
		}
		index = append(index, entry)
	}
	if len(data) != 0 {
		return nil, corruptf("trailing index bytes")
	}
	return index, nil
}

func appendBlock(dst *bytes.Buffer, payload []byte) error {
	if uint64(len(payload)) > math.MaxUint32 {
		return fmt.Errorf("lsm: block too large")
	}
	dst.Write(payload)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.Checksum(payload, crcTable))
	dst.Write(crc[:])
	return nil
}

func encodeFooter(indexOffset, indexLength, bloomOffset, bloomLength, count uint64, minKey, maxKey []byte) ([]byte, error) {
	if uint64(len(minKey)) > math.MaxUint32 || uint64(len(maxKey)) > math.MaxUint32 {
		return nil, fmt.Errorf("lsm: footer key too large")
	}
	metadata := make([]byte, 48)
	binary.LittleEndian.PutUint64(metadata[0:8], indexOffset)
	binary.LittleEndian.PutUint64(metadata[8:16], indexLength)
	binary.LittleEndian.PutUint64(metadata[16:24], bloomOffset)
	binary.LittleEndian.PutUint64(metadata[24:32], bloomLength)
	binary.LittleEndian.PutUint64(metadata[32:40], count)
	binary.LittleEndian.PutUint32(metadata[40:44], uint32(len(minKey)))
	binary.LittleEndian.PutUint32(metadata[44:48], uint32(len(maxKey)))
	metadata = append(metadata, minKey...)
	metadata = append(metadata, maxKey...)
	if uint64(len(metadata)) > math.MaxUint32 {
		return nil, fmt.Errorf("lsm: footer too large")
	}
	trailer := make([]byte, footerTrailer)
	binary.LittleEndian.PutUint32(trailer[0:4], crc32.Checksum(metadata, crcTable))
	binary.LittleEndian.PutUint32(trailer[4:8], uint32(len(metadata)))
	copy(trailer[8:], footerMagic)
	return append(metadata, trailer...), nil
}

// SSTableReader provides validated random reads over an SSTable. It owns no
// file descriptor and performs all reads through the supplied io.ReaderAt.
type SSTableReader struct {
	r          io.ReaderAt
	size       int64
	index      []indexEntry
	bloom      *BloomFilter
	entryCount uint64
	minKey     []byte
	maxKey     []byte
}

// OpenSSTable validates the footer, index, and Bloom block. Individual data
// block CRCs are checked when the block is read.
func OpenSSTable(r io.ReaderAt, size int64) (*SSTableReader, error) {
	if r == nil || size < footerTrailer {
		return nil, corruptf("missing footer")
	}
	trailer := make([]byte, footerTrailer)
	if err := readAtFull(r, trailer, size-footerTrailer); err != nil {
		return nil, corruptf("read footer trailer: %v", err)
	}
	if string(trailer[8:]) != footerMagic {
		return nil, corruptf("bad footer magic")
	}
	metadataLen := binary.LittleEndian.Uint32(trailer[4:8])
	if metadataLen < 48 || int64(metadataLen) > size-footerTrailer {
		return nil, corruptf("invalid footer length")
	}
	metadataLength, ok := uint64ToInt(uint64(metadataLen))
	if !ok {
		return nil, corruptf("footer too large")
	}
	metadata := make([]byte, metadataLength)
	if err := readAtFull(r, metadata, size-footerTrailer-int64(metadataLen)); err != nil {
		return nil, corruptf("read footer: %v", err)
	}
	if want, got := binary.LittleEndian.Uint32(trailer[:4]), crc32.Checksum(metadata, crcTable); want != got {
		return nil, corruptf("footer crc32c mismatch")
	}
	indexOffset := binary.LittleEndian.Uint64(metadata[0:8])
	indexLength := binary.LittleEndian.Uint64(metadata[8:16])
	bloomOffset := binary.LittleEndian.Uint64(metadata[16:24])
	bloomLength := binary.LittleEndian.Uint64(metadata[24:32])
	entryCount := binary.LittleEndian.Uint64(metadata[32:40])
	minLen := binary.LittleEndian.Uint32(metadata[40:44])
	maxLen := binary.LittleEndian.Uint32(metadata[44:48])
	if uint64(len(metadata)-48) != uint64(minLen)+uint64(maxLen) || indexLength < 4 || bloomLength < 4 {
		return nil, corruptf("invalid footer metadata")
	}
	footerStart := uint64(size - footerTrailer - int64(metadataLen))
	if indexOffset > footerStart || indexLength > footerStart-indexOffset || bloomOffset > footerStart || bloomLength > footerStart-bloomOffset || indexOffset+indexLength > bloomOffset {
		return nil, corruptf("footer block bounds")
	}
	indexData, err := readBlock(r, indexOffset, indexLength)
	if err != nil {
		return nil, err
	}
	index, err := decodeIndex(indexData)
	if err != nil {
		return nil, err
	}
	if (entryCount == 0) != (len(index) == 0) {
		return nil, corruptf("empty table index mismatch")
	}
	for i, entry := range index {
		if entry.offset >= indexOffset || entry.length > indexOffset-entry.offset || (i > 0 && entry.offset < index[i-1].offset+index[i-1].length) {
			return nil, corruptf("data block bounds")
		}
	}
	bloomData, err := readBlock(r, bloomOffset, bloomLength)
	if err != nil {
		return nil, err
	}
	bloom, err := UnmarshalBloomFilter(bloomData)
	if err != nil {
		return nil, corruptf("bloom: %v", err)
	}
	minStart := 48
	minKey := append([]byte(nil), metadata[minStart:minStart+int(minLen)]...)
	maxKey := append([]byte(nil), metadata[minStart+int(minLen):]...)
	if entryCount > 0 && bytes.Compare(minKey, maxKey) > 0 {
		return nil, corruptf("footer key order")
	}
	return &SSTableReader{r: r, size: size, index: index, bloom: bloom, entryCount: entryCount, minKey: minKey, maxKey: maxKey}, nil
}

// Get returns the highest-sequence record for key. A returned tombstone is a
// found record; its caller decides how to represent deletion.
func (r *SSTableReader) Get(key []byte) (TableEntry, bool, error) {
	if r == nil {
		return TableEntry{}, false, fmt.Errorf("lsm: nil sstable reader")
	}
	if r.entryCount == 0 || !r.bloom.MayContain(key) || bytes.Compare(key, r.minKey) < 0 || bytes.Compare(key, r.maxKey) > 0 {
		return TableEntry{}, false, nil
	}
	i := sort.Search(len(r.index), func(i int) bool { return bytes.Compare(r.index[i].firstKey, key) > 0 }) - 1
	if i < 0 {
		return TableEntry{}, false, nil
	}
	data, err := readBlock(r.r, r.index[i].offset, r.index[i].length)
	if err != nil {
		return TableEntry{}, false, err
	}
	entries, err := decodeRecords(data)
	if err != nil {
		return TableEntry{}, false, err
	}
	if len(entries) == 0 || !bytes.Equal(entries[0].Key, r.index[i].firstKey) {
		return TableEntry{}, false, corruptf("index first key does not match data block")
	}
	for _, entry := range entries {
		cmp := bytes.Compare(entry.Key, key)
		if cmp == 0 {
			return entry, true, nil
		}
		if cmp > 0 {
			break
		}
	}
	return TableEntry{}, false, nil
}

// AllEntries validates and returns every data record in key/strictly-
// descending-seq order. It is used during manifest replay so a referenced
// table's data-block CRCs and metadata are checked before the engine publishes
// the version set.
func (r *SSTableReader) AllEntries() ([]TableEntry, error) {
	if r == nil {
		return nil, fmt.Errorf("lsm: nil sstable reader")
	}
	entries := make([]TableEntry, 0)
	for _, block := range r.index {
		data, err := readBlock(r.r, block.offset, block.length)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeRecords(data)
		if err != nil {
			return nil, err
		}
		if len(decoded) == 0 || !bytes.Equal(decoded[0].Key, block.firstKey) {
			return nil, corruptf("index first key does not match data block")
		}
		if len(entries) != 0 {
			cmp := bytes.Compare(decoded[0].Key, entries[len(entries)-1].Key)
			if cmp <= 0 {
				return nil, corruptf("records out of order or same key spans data blocks")
			}
		}
		for _, entry := range decoded {
			if !r.bloom.MayContain(entry.Key) {
				return nil, corruptf("bloom false negative for stored key")
			}
			entries = append(entries, TableEntry{
				Key:       cloneBytes(entry.Key),
				Seq:       entry.Seq,
				Value:     cloneBytes(entry.Value),
				Tombstone: entry.Tombstone,
			})
		}
	}
	if uint64(len(entries)) != r.entryCount {
		return nil, corruptf("entry count is %d, footer says %d", len(entries), r.entryCount)
	}
	if len(entries) != 0 && (!bytes.Equal(entries[0].Key, r.minKey) || !bytes.Equal(entries[len(entries)-1].Key, r.maxKey)) {
		return nil, corruptf("footer key bounds do not match records")
	}
	return entries, nil
}

// EntryCount reports the exact number of records, including tombstones.
func (r *SSTableReader) EntryCount() uint64 { return r.entryCount }

func readBlock(r io.ReaderAt, offset, length uint64) ([]byte, error) {
	if length < 4 || offset > math.MaxInt64 || length > math.MaxInt64 || offset+length < offset || offset+length > math.MaxInt64 {
		return nil, corruptf("invalid block bounds")
	}
	blockLength, ok := uint64ToInt(length)
	if !ok {
		return nil, corruptf("block too large")
	}
	block := make([]byte, blockLength)
	if err := readAtFull(r, block, int64(offset)); err != nil {
		return nil, corruptf("read block: %v", err)
	}
	payload := block[:len(block)-4]
	stored := binary.LittleEndian.Uint32(block[len(block)-4:])
	if got := crc32.Checksum(payload, crcTable); got != stored {
		return nil, corruptf("block crc32c mismatch")
	}
	return payload, nil
}

func uint64ToInt(value uint64) (int, bool) {
	if value > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(value), true
}

func readAtFull(r io.ReaderAt, data []byte, offset int64) error {
	n, err := r.ReadAt(data, offset)
	if n != len(data) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func appendUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
}
