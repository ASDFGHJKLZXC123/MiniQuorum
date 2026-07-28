package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestBloomFilterNoFalseNegativesAndFalsePositiveRate(t *testing.T) {
	const n = 20_000
	f := NewBloomFilter(n)
	for i := range n {
		f.Add([]byte(fmt.Sprintf("present-%08d", i)))
	}
	encoded, err := f.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	f, err = UnmarshalBloomFilter(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if !f.MayContain([]byte(fmt.Sprintf("present-%08d", i))) {
			t.Fatalf("false negative for %d", i)
		}
	}
	rng := rand.New(rand.NewPCG(17, 29))
	falsePositives := 0
	for range n {
		if f.MayContain([]byte(fmt.Sprintf("absent-%016x", rng.Uint64()))) {
			falsePositives++
		}
	}
	if rate := float64(falsePositives) / n; rate > .016 {
		t.Fatalf("false-positive rate %0.4f, want <= 0.016", rate)
	}
}

func TestBloomSizeBoundaryChecksDoNotAllocate(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, test := range []struct {
		name     string
		keys     int
		wantBits uint64
		wantOK   bool
	}{
		{name: "zero", keys: 0, wantBits: 8, wantOK: true},
		{name: "negative", keys: -1, wantBits: 8, wantOK: true},
		{name: "ordinary", keys: 3, wantBits: 30, wantOK: true},
		{name: "largest-safe-product", keys: maxInt / bloomBitsPerKey, wantBits: uint64(maxInt/bloomBitsPerKey) * bloomBitsPerKey, wantOK: true},
		{name: "max-int", keys: maxInt, wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			bits, _, ok := bloomSize(test.keys)
			if ok != test.wantOK || ok && bits != test.wantBits {
				t.Fatalf("bloomSize(%d) = (%d, ok=%v), want (%d, ok=%v)", test.keys, bits, ok, test.wantBits, test.wantOK)
			}
		})
	}

	if bytes, ok := bloomByteLen(math.MaxUint64); !ok || bytes <= 0 {
		t.Fatalf("bloomByteLen(MaxUint64) = (%d, ok=%v), want a positive addressable length", bytes, ok)
	}
	if f := NewBloomFilter(maxInt); f != nil {
		t.Fatal("NewBloomFilter accepted a size that cannot be represented safely")
	}

	malformed := make([]byte, 10)
	malformed[0] = bloomVersion
	malformed[1] = bloomHashCount
	binary.LittleEndian.PutUint64(malformed[2:10], math.MaxUint64-6)
	if _, err := UnmarshalBloomFilter(malformed); err == nil {
		t.Fatal("UnmarshalBloomFilter accepted an overflowing bit count")
	}
}

func TestSSTableRoundTripBoundaryAndCRC(t *testing.T) {
	maxValue := bytes.Repeat([]byte{'x'}, 1<<20)
	entries := []TableEntry{
		{Key: nil, Seq: 9, Value: []byte("empty-key")},
		{Key: []byte("a"), Seq: 10, Value: []byte("new")},
		{Key: []byte("a"), Seq: 8, Value: []byte("old")},
		{Key: []byte("gone"), Seq: 11, Tombstone: true},
		{Key: []byte("z"), Seq: 12, Value: maxValue},
	}
	var out bytes.Buffer
	w := NewSSTableWriter(&out)
	for _, entry := range entries {
		if err := w.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSSTable(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal(err)
	}
	assertSSTableEntry(t, r, nil, TableEntry{Key: nil, Seq: 9, Value: []byte("empty-key")})
	assertSSTableEntry(t, r, []byte("a"), TableEntry{Key: []byte("a"), Seq: 10, Value: []byte("new")})
	assertSSTableEntry(t, r, []byte("gone"), TableEntry{Key: []byte("gone"), Seq: 11, Tombstone: true})
	assertSSTableEntry(t, r, []byte("z"), TableEntry{Key: []byte("z"), Seq: 12, Value: maxValue})
	got, ok, err := r.Get([]byte("missing"))
	if err != nil || ok || got.Key != nil {
		t.Fatalf("missing = %#v, %v, %v", got, ok, err)
	}
	corrupt := append([]byte(nil), out.Bytes()...)
	corrupt[0] ^= 1
	r, err = OpenSSTable(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Get(nil)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupted data block error = %v, want ErrCorrupt", err)
	}
}

func assertSSTableEntry(t *testing.T, r *SSTableReader, key []byte, want TableEntry) {
	t.Helper()
	got, ok, err := r.Get(key)
	if err != nil || !ok {
		t.Fatalf("Get(%q) = %#v, %v, %v", key, got, ok, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Get(%q) = %#v, want %#v", key, got, want)
	}
}

func TestSSTableEmptyAndSingleEntry(t *testing.T) {
	for _, entries := range [][]TableEntry{nil, {{Key: []byte("only"), Seq: 1, Value: []byte("v")}}} {
		var out bytes.Buffer
		w := NewSSTableWriter(&out)
		for _, entry := range entries {
			if err := w.Add(entry); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		r, err := OpenSSTable(bytes.NewReader(out.Bytes()), int64(out.Len()))
		if err != nil {
			t.Fatal(err)
		}
		_, ok, err := r.Get([]byte("only"))
		if err != nil || ok != (len(entries) == 1) {
			t.Fatalf("entries=%d Get = %v, %v", len(entries), ok, err)
		}
	}
}

func TestSSTableRejectsUnsortedEntries(t *testing.T) {
	w := NewSSTableWriter(&bytes.Buffer{})
	if err := w.Add(TableEntry{Key: []byte("b"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(TableEntry{Key: []byte("a"), Seq: 1}); err == nil {
		t.Fatal("Add accepted unsorted key")
	}
}

func TestSSTableOversizedRecordGetsItsOwnReadableBlock(t *testing.T) {
	large := bytes.Repeat([]byte{'v'}, dataBlockTarget+1)
	var out bytes.Buffer
	w := NewSSTableWriter(&out)
	for _, entry := range []TableEntry{
		{Key: []byte("after"), Seq: 1, Value: []byte("a")},
		{Key: []byte("large"), Seq: 2, Value: large},
		{Key: []byte("z"), Seq: 3, Value: []byte("z")},
	} {
		if err := w.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSSTable(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := r.Get([]byte("large"))
	if err != nil || !ok || !bytes.Equal(got.Value, large) {
		t.Fatalf("large record = %#v, %v, %v", got, ok, err)
	}
	if len(r.index) != 3 {
		t.Fatalf("oversized record index blocks = %d, want 3", len(r.index))
	}
}

func TestSSTableDetectsCorruptMetadataAndShortReads(t *testing.T) {
	table := buildTestSSTable(t)
	layout := sstableLayout(t, table)

	tests := []struct {
		name    string
		corrupt func([]byte)
	}{
		{
			name: "index-block-crc",
			corrupt: func(data []byte) {
				data[layout.indexOffset] ^= 1
			},
		},
		{
			name: "bloom-block-crc",
			corrupt: func(data []byte) {
				data[layout.bloomOffset] ^= 1
			},
		},
		{
			name: "footer-crc",
			corrupt: func(data []byte) {
				data[layout.footerStart] ^= 1
			},
		},
		{
			name: "footer-block-bounds",
			corrupt: func(data []byte) {
				binary.LittleEndian.PutUint64(data[layout.footerStart:layout.footerStart+8], uint64(len(data)))
				rewriteFooterCRC(data, layout.footerStart)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), table...)
			test.corrupt(corrupt)
			if _, err := OpenSSTable(bytes.NewReader(corrupt), int64(len(corrupt))); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenSSTable() error = %v, want ErrCorrupt", err)
			}
		})
	}

	for size := 0; size < len(table); size++ {
		if _, err := OpenSSTable(bytes.NewReader(table[:size]), int64(size)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncation to %d bytes error = %v, want ErrCorrupt", size, err)
		}
	}

	if _, err := OpenSSTable(shortReaderAt{data: table}, int64(len(table))); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short ReaderAt error = %v, want ErrCorrupt", err)
	}
}

func buildTestSSTable(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	w := NewSSTableWriter(&out)
	for _, entry := range []TableEntry{{Key: []byte("a"), Seq: 1, Value: []byte("one")}, {Key: []byte("z"), Seq: 2, Value: []byte("two")}} {
		if err := w.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

type sstableFileLayout struct {
	indexOffset uint64
	bloomOffset uint64
	footerStart int
}

func sstableLayout(t *testing.T, data []byte) sstableFileLayout {
	t.Helper()
	trailer := data[len(data)-footerTrailer:]
	metadataLen := int(binary.LittleEndian.Uint32(trailer[4:8]))
	footerStart := len(data) - footerTrailer - metadataLen
	metadata := data[footerStart : len(data)-footerTrailer]
	return sstableFileLayout{
		indexOffset: binary.LittleEndian.Uint64(metadata[0:8]),
		bloomOffset: binary.LittleEndian.Uint64(metadata[16:24]),
		footerStart: footerStart,
	}
}

func rewriteFooterCRC(data []byte, footerStart int) {
	trailer := data[len(data)-footerTrailer:]
	binary.LittleEndian.PutUint32(trailer[:4], crc32.Checksum(data[footerStart:len(data)-footerTrailer], crcTable))
}

type shortReaderAt struct{ data []byte }

func (r shortReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[offset:])
	if n > 0 {
		n--
	}
	return n, nil
}
