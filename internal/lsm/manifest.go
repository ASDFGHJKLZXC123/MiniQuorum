package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	raftpb "miniquorum/proto"
)

const (
	manifestFilename       = "MANIFEST"
	manifestFrameStart     = byte(0xFA)
	manifestFrameEnd       = byte(0xFB)
	manifestSymbolBase     = byte(0x40)
	manifestSymbolLimit    = byte(0x4F)
	manifestHeaderSize     = uint64(8)
	manifestMarkerBytes    = uint64(2)
	manifestEncodedWidth   = uint64(2)
	sstableFilenameSuffix  = ".sst"
	defaultFileNumberWidth = 6
)

// ErrManifestCorrupt identifies a complete malformed record or a non-tail
// framing violation. Only an unterminated final record is classified as a
// removable torn tail.
var ErrManifestCorrupt = errors.New("lsm: corrupt manifest")

type fileMetadata struct {
	tier   uint32
	file   string
	minKey []byte
	maxKey []byte
	count  uint64
}

func (metadata fileMetadata) proto() *raftpb.AddedFile {
	return &raftpb.AddedFile{
		Tier:   metadata.tier,
		File:   metadata.file,
		MinKey: cloneBytes(metadata.minKey),
		MaxKey: cloneBytes(metadata.maxKey),
		Count:  metadata.count,
	}
}

type manifestState struct {
	files        map[string]fileMetadata
	flushedIndex uint64
	records      uint64
}

func newManifestState() manifestState {
	return manifestState{files: make(map[string]fileMetadata)}
}

func (state *manifestState) apply(edit *raftpb.VersionEdit) error {
	if edit == nil {
		return errors.New("nil VersionEdit")
	}
	if edit.GetFlushedIndex() < state.flushedIndex {
		return fmt.Errorf("flushed_index regresses from %d to %d", state.flushedIndex, edit.GetFlushedIndex())
	}
	if len(edit.GetAddedFiles()) == 0 && len(edit.GetRemovedFiles()) == 0 && edit.GetFlushedIndex() == state.flushedIndex {
		return errors.New("empty VersionEdit does not advance flushed_index")
	}

	removed := make(map[string]struct{}, len(edit.GetRemovedFiles()))
	for _, name := range edit.GetRemovedFiles() {
		if err := validateSSTableFilename(name); err != nil {
			return fmt.Errorf("removed file %q: %w", name, err)
		}
		if _, duplicate := removed[name]; duplicate {
			return fmt.Errorf("removed file %q appears twice", name)
		}
		if _, exists := state.files[name]; !exists {
			return fmt.Errorf("removed file %q is not referenced", name)
		}
		removed[name] = struct{}{}
	}

	added := make(map[string]fileMetadata, len(edit.GetAddedFiles()))
	for _, file := range edit.GetAddedFiles() {
		if file == nil {
			return errors.New("nil added file")
		}
		name := file.GetFile()
		if err := validateSSTableFilename(name); err != nil {
			return fmt.Errorf("added file %q: %w", name, err)
		}
		if _, duplicate := added[name]; duplicate {
			return fmt.Errorf("added file %q appears twice", name)
		}
		if _, alsoRemoved := removed[name]; alsoRemoved {
			return fmt.Errorf("file %q is both added and removed", name)
		}
		if _, exists := state.files[name]; exists {
			return fmt.Errorf("added file %q is already referenced", name)
		}
		if file.GetCount() == 0 {
			return fmt.Errorf("added file %q has zero entries", name)
		}
		if bytes.Compare(file.GetMinKey(), file.GetMaxKey()) > 0 {
			return fmt.Errorf("added file %q has min_key greater than max_key", name)
		}
		added[name] = fileMetadata{
			tier:   file.GetTier(),
			file:   name,
			minKey: cloneBytes(file.GetMinKey()),
			maxKey: cloneBytes(file.GetMaxKey()),
			count:  file.GetCount(),
		}
	}

	for name := range removed {
		delete(state.files, name)
	}
	for name, file := range added {
		state.files[name] = file
	}
	state.flushedIndex = edit.GetFlushedIndex()
	state.records++
	return nil
}

func validateSSTableFilename(name string) error {
	if name == "" || name == "." || strings.ContainsAny(name, "/\\\x00") || filepath.Base(name) != name || filepath.Clean(name) != name || !strings.HasSuffix(name, sstableFilenameSuffix) {
		return errors.New("invalid SSTable filename")
	}
	return nil
}

// encodeManifestFrame emits decision #16's exact physical record:
// 0xFA || E(LE32(len)||LE32(CRC32C)||protobuf VersionEdit) || 0xFB.
func encodeManifestFrame(edit *raftpb.VersionEdit) ([]byte, error) {
	if edit == nil {
		return nil, errors.New("lsm: encode nil VersionEdit")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(edit)
	if err != nil {
		return nil, fmt.Errorf("lsm: marshal VersionEdit: %w", err)
	}
	if uint64(len(payload)) > math.MaxUint32 {
		return nil, fmt.Errorf("lsm: manifest payload %d overflows uint32", len(payload))
	}
	frameSize, ok := checkedManifestFrameSize(uint64(len(payload)))
	if !ok {
		return nil, fmt.Errorf("lsm: manifest payload %d overflows frame size", len(payload))
	}
	frameSizeInt, err := checkedManifestInt(frameSize)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, frameSizeInt)
	frame[0] = manifestFrameStart
	frame[len(frame)-1] = manifestFrameEnd

	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], crc32.Checksum(payload, crcTable))
	symbols := frame[1 : len(frame)-1]
	if err := encodeManifestSymbols(symbols[:len(header)*2], header[:]); err != nil {
		return nil, err
	}
	if err := encodeManifestSymbols(symbols[len(header)*2:], payload); err != nil {
		return nil, err
	}
	return frame, nil
}

func decodeManifestFrame(symbols []byte) (*raftpb.VersionEdit, error) {
	symbolCount := uint64(len(symbols))
	if symbolCount%manifestEncodedWidth != 0 {
		return nil, fmt.Errorf("odd interior symbol count %d", symbolCount)
	}
	bodySize := symbolCount / manifestEncodedWidth
	if bodySize < manifestHeaderSize {
		return nil, fmt.Errorf("short decoded body: %d bytes", bodySize)
	}
	headerSymbols, ok := checkedManifestMul(manifestHeaderSize, manifestEncodedWidth)
	if !ok {
		return nil, errors.New("header symbol count overflow")
	}
	headerSymbolsInt, err := checkedManifestInt(headerSymbols)
	if err != nil {
		return nil, err
	}
	var header [8]byte
	if err := decodeManifestSymbols(header[:], symbols[:headerSymbolsInt]); err != nil {
		return nil, err
	}
	payloadSize := uint64(binary.LittleEndian.Uint32(header[:4]))
	wantBody, ok := checkedManifestAdd(manifestHeaderSize, payloadSize)
	if !ok {
		return nil, errors.New("decoded body size overflow")
	}
	if bodySize != wantBody {
		return nil, fmt.Errorf("decoded body size %d does not match header plus payload %d", bodySize, wantBody)
	}
	payloadSizeInt, err := checkedManifestInt(payloadSize)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, payloadSizeInt)
	if err := decodeManifestSymbols(payload, symbols[headerSymbolsInt:]); err != nil {
		return nil, err
	}
	if stored, computed := binary.LittleEndian.Uint32(header[4:]), crc32.Checksum(payload, crcTable); stored != computed {
		return nil, fmt.Errorf("crc32c mismatch: stored %08x, computed %08x", stored, computed)
	}
	edit := &raftpb.VersionEdit{}
	if err := proto.Unmarshal(payload, edit); err != nil {
		return nil, fmt.Errorf("unmarshal VersionEdit: %w", err)
	}
	return edit, nil
}

func replayManifestBytes(data []byte) (manifestState, int, bool, error) {
	state := newManifestState()
	offset := 0
	for offset < len(data) {
		if data[offset] != manifestFrameStart {
			return manifestState{}, 0, false, fmt.Errorf("%w: offset %d: byte 0x%02x at record boundary", ErrManifestCorrupt, offset, data[offset])
		}
		terminator := -1
		for position := offset + 1; position < len(data); position++ {
			symbol := data[position]
			switch {
			case symbol == manifestFrameEnd:
				terminator = position
			case symbol == manifestFrameStart:
				return manifestState{}, 0, false, fmt.Errorf("%w: offset %d: start marker before terminator", ErrManifestCorrupt, position)
			case !isManifestSymbol(symbol):
				return manifestState{}, 0, false, fmt.Errorf("%w: offset %d: invalid interior byte 0x%02x", ErrManifestCorrupt, position, symbol)
			}
			if terminator >= 0 {
				break
			}
		}
		if terminator < 0 {
			return state, offset, true, nil
		}
		edit, err := decodeManifestFrame(data[offset+1 : terminator])
		if err != nil {
			return manifestState{}, 0, false, fmt.Errorf("%w: offset %d: %v", ErrManifestCorrupt, offset, err)
		}
		if err := state.apply(edit); err != nil {
			return manifestState{}, 0, false, fmt.Errorf("%w: offset %d: invalid VersionEdit: %v", ErrManifestCorrupt, offset, err)
		}
		offset = terminator + 1
	}
	return state, offset, false, nil
}

func encodeManifestSymbols(dst, decoded []byte) error {
	want, ok := checkedManifestMul(uint64(len(decoded)), manifestEncodedWidth)
	if !ok || want != uint64(len(dst)) {
		return fmt.Errorf("lsm: symbol buffer has %d bytes, want %d", len(dst), want)
	}
	for index, value := range decoded {
		dst[2*index] = manifestSymbolBase + value>>4
		dst[2*index+1] = manifestSymbolBase + value&0x0f
	}
	return nil
}

func decodeManifestSymbols(dst, symbols []byte) error {
	want, ok := checkedManifestMul(uint64(len(dst)), manifestEncodedWidth)
	if !ok || want != uint64(len(symbols)) {
		return fmt.Errorf("interior has %d symbols, want %d", len(symbols), want)
	}
	for index := range dst {
		high, low := symbols[2*index], symbols[2*index+1]
		if !isManifestSymbol(high) {
			return fmt.Errorf("invalid interior byte 0x%02x at symbol %d", high, 2*index)
		}
		if !isManifestSymbol(low) {
			return fmt.Errorf("invalid interior byte 0x%02x at symbol %d", low, 2*index+1)
		}
		dst[index] = (high-manifestSymbolBase)<<4 | (low - manifestSymbolBase)
	}
	return nil
}

func isManifestSymbol(value byte) bool {
	return value >= manifestSymbolBase && value <= manifestSymbolLimit
}

func checkedManifestFrameSize(payloadSize uint64) (uint64, bool) {
	body, ok := checkedManifestAdd(manifestHeaderSize, payloadSize)
	if !ok {
		return 0, false
	}
	encoded, ok := checkedManifestMul(body, manifestEncodedWidth)
	if !ok {
		return 0, false
	}
	return checkedManifestAdd(manifestMarkerBytes, encoded)
}

func checkedManifestAdd(left, right uint64) (uint64, bool) {
	if math.MaxUint64-left < right {
		return 0, false
	}
	return left + right, true
}

func checkedManifestMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func checkedManifestInt(size uint64) (int, error) {
	if strconv.IntSize == 32 && size > math.MaxInt32 || strconv.IntSize == 64 && size > math.MaxInt64 {
		return 0, fmt.Errorf("lsm: size %d exceeds %d-bit int", size, strconv.IntSize)
	}
	return int(size), nil
}

func loadManifest(fs FS, dir string) (manifestState, error) {
	path := filepath.Join(dir, manifestFilename)
	info, err := fs.Stat(path)
	if errors.Is(err, ErrNotExist) {
		if err := createManifest(fs, dir, path); err != nil {
			return manifestState{}, err
		}
		return newManifestState(), nil
	}
	if err != nil {
		return manifestState{}, fmt.Errorf("lsm: stat manifest: %w", err)
	}
	if info.IsDir || info.Size < 0 {
		return manifestState{}, fmt.Errorf("%w: invalid manifest stat", ErrManifestCorrupt)
	}
	size, err := checkedManifestInt(uint64(info.Size))
	if err != nil {
		return manifestState{}, fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	file, err := fs.Open(path)
	if err != nil {
		return manifestState{}, fmt.Errorf("lsm: open manifest: %w", err)
	}
	data := make([]byte, size)
	readErr := readAtFull(file, data, 0)
	closeErr := file.Close()
	if readErr != nil {
		return manifestState{}, fmt.Errorf("lsm: read manifest: %w", errors.Join(readErr, closeErr))
	}
	if closeErr != nil {
		return manifestState{}, fmt.Errorf("lsm: close manifest: %w", closeErr)
	}

	state, complete, torn, err := replayManifestBytes(data)
	if err != nil {
		return manifestState{}, err
	}
	if torn {
		if err := truncateManifest(fs, path, int64(complete)); err != nil {
			return manifestState{}, err
		}
	}
	return state, nil
}

func createManifest(fs FS, dir, path string) error {
	file, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("lsm: create manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("lsm: sync new manifest: %w", errors.Join(err, file.Close()))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("lsm: close new manifest: %w", err)
	}
	if err := fs.SyncDir(dir); err != nil {
		return fmt.Errorf("lsm: sync manifest directory: %w", err)
	}
	return nil
}

func truncateManifest(fs FS, path string, size int64) error {
	file, err := fs.OpenAppend(path)
	if err != nil {
		return fmt.Errorf("lsm: open manifest to trim torn tail: %w", err)
	}
	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("lsm: truncate torn manifest tail: %w", errors.Join(err, file.Close()))
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("lsm: sync trimmed manifest: %w", errors.Join(err, file.Close()))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("lsm: close trimmed manifest: %w", err)
	}
	return nil
}

func syncManifest(fs FS, path string) error {
	file, err := fs.OpenAppend(path)
	if err != nil {
		return fmt.Errorf("lsm: open recovered manifest for sync: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("lsm: recovery sync manifest: %w", errors.Join(err, file.Close()))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("lsm: close recovery-synced manifest: %w", err)
	}
	return nil
}

// appendManifestEdit reports committed=true only after the complete frame's
// file sync succeeds. A close failure after that point returns both true and
// the close error so the caller can publish the genuinely durable edit while
// still fail-stopping.
func appendManifestEdit(fs FS, dir string, edit *raftpb.VersionEdit) (committed bool, err error) {
	frame, err := encodeManifestFrame(edit)
	if err != nil {
		return false, err
	}
	path := filepath.Join(dir, manifestFilename)
	file, err := fs.OpenAppend(path)
	if err != nil {
		return false, fmt.Errorf("lsm: open manifest for append: %w", err)
	}
	written, writeErr := file.Write(frame)
	if writeErr != nil || written != len(frame) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		return false, fmt.Errorf("lsm: append manifest: %w", errors.Join(writeErr, file.Close()))
	}
	if syncErr := file.Sync(); syncErr != nil {
		return false, fmt.Errorf("lsm: sync manifest: %w", errors.Join(syncErr, file.Close()))
	}
	if closeErr := file.Close(); closeErr != nil {
		return true, fmt.Errorf("lsm: close synced manifest: %w", closeErr)
	}
	return true, nil
}

func sortedManifestFiles(files map[string]fileMetadata) []fileMetadata {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]fileMetadata, 0, len(names))
	for _, name := range names {
		out = append(out, files[name])
	}
	return out
}
