package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	raftpb "miniquorum/proto"
)

func TestManifestDecision16ExactFramingAndCompleteReplay(t *testing.T) {
	first := &raftpb.VersionEdit{
		AddedFiles: []*raftpb.AddedFile{{
			Tier: 0, File: "000001.sst", MinKey: []byte{}, MaxKey: []byte("m"), Count: 3,
		}},
		FlushedIndex: 10,
	}
	second := &raftpb.VersionEdit{
		AddedFiles: []*raftpb.AddedFile{
			{Tier: 1, File: "000002.sst", MinKey: []byte("a"), MaxKey: []byte("z"), Count: 9},
			{Tier: 0, File: "000003.sst", MinKey: []byte("x"), MaxKey: []byte("zz"), Count: 2},
		},
		RemovedFiles: []string{"000001.sst"},
		FlushedIndex: 22,
	}

	firstFrame, err := encodeManifestFrame(first)
	if err != nil {
		t.Fatal(err)
	}
	if firstFrame[0] != 0xFA || firstFrame[len(firstFrame)-1] != 0xFB {
		t.Fatalf("frame markers = %02x/%02x, want fa/fb", firstFrame[0], firstFrame[len(firstFrame)-1])
	}
	for index, symbol := range firstFrame[1 : len(firstFrame)-1] {
		if symbol < 0x40 || symbol > 0x4F {
			t.Fatalf("interior symbol %d = %02x, want 40..4f", index, symbol)
		}
	}
	body := make([]byte, (len(firstFrame)-2)/2)
	if err := decodeManifestSymbols(body, firstFrame[1:len(firstFrame)-1]); err != nil {
		t.Fatal(err)
	}
	payloadLength := binary.LittleEndian.Uint32(body[:4])
	if int(payloadLength) != len(body)-8 {
		t.Fatalf("header payload length = %d, body payload = %d", payloadLength, len(body)-8)
	}
	payload := body[8:]
	if got, want := binary.LittleEndian.Uint32(body[4:8]), crc32.Checksum(payload, crcTable); got != want {
		t.Fatalf("header crc32c = %08x, want %08x", got, want)
	}
	decoded := &raftpb.VersionEdit{}
	if err := proto.Unmarshal(payload, decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded, first) {
		t.Fatalf("decoded VersionEdit = %v, want %v", decoded, first)
	}

	secondFrame, err := encodeManifestFrame(second)
	if err != nil {
		t.Fatal(err)
	}
	manifest := append(append([]byte(nil), firstFrame...), secondFrame...)
	state, complete, torn, err := replayManifestBytes(manifest)
	if err != nil || torn || complete != len(manifest) {
		t.Fatalf("replay = complete:%d torn:%v err:%v", complete, torn, err)
	}
	if state.flushedIndex != 22 || state.records != 2 {
		t.Fatalf("state = watermark:%d records:%d, want 22/2", state.flushedIndex, state.records)
	}
	wantFiles := []string{"000002.sst", "000003.sst"}
	var gotFiles []string
	for _, file := range sortedManifestFiles(state.files) {
		gotFiles = append(gotFiles, file.file)
	}
	if !reflect.DeepEqual(gotFiles, wantFiles) {
		t.Fatalf("files = %v, want %v", gotFiles, wantFiles)
	}
}

func TestManifestTornFinalPrefixesNeverPartiallyApplyAtomicEdit(t *testing.T) {
	initial := &raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{{File: "base.sst", MinKey: []byte("a"), MaxKey: []byte("z"), Count: 5}},
		FlushedIndex: 7,
	}
	atomic := &raftpb.VersionEdit{
		AddedFiles: []*raftpb.AddedFile{
			{Tier: 1, File: "left.sst", MinKey: []byte{}, MaxKey: []byte("m"), Count: 4},
			{Tier: 1, File: "right.sst", MinKey: []byte("n"), MaxKey: []byte("z"), Count: 6},
		},
		RemovedFiles: []string{"base.sst"},
		FlushedIndex: 19,
	}
	initialFrame, err := encodeManifestFrame(initial)
	if err != nil {
		t.Fatal(err)
	}
	atomicFrame, err := encodeManifestFrame(atomic)
	if err != nil {
		t.Fatal(err)
	}
	for retained := 0; retained < len(atomicFrame); retained++ {
		data := append(append([]byte(nil), initialFrame...), atomicFrame[:retained]...)
		state, complete, torn, err := replayManifestBytes(data)
		if err != nil {
			t.Fatalf("retained prefix %d: replay error = %v", retained, err)
		}
		if retained == 0 {
			if torn {
				t.Fatalf("empty retained prefix classified torn")
			}
		} else if !torn {
			t.Fatalf("retained prefix %d classified complete", retained)
		}
		if complete != len(initialFrame) || state.flushedIndex != 7 || len(state.files) != 1 {
			t.Fatalf("retained prefix %d partially applied: complete=%d watermark=%d files=%v", retained, complete, state.flushedIndex, state.files)
		}
		if _, ok := state.files["base.sst"]; !ok {
			t.Fatalf("retained prefix %d removed base file before complete terminator", retained)
		}
	}

	full := append(append([]byte(nil), initialFrame...), atomicFrame...)
	state, _, torn, err := replayManifestBytes(full)
	if err != nil || torn {
		t.Fatalf("full atomic edit replay = torn:%v err:%v", torn, err)
	}
	if state.flushedIndex != 19 || len(state.files) != 2 {
		t.Fatalf("full atomic edit state = watermark:%d files:%v", state.flushedIndex, state.files)
	}
	if _, ok := state.files["base.sst"]; ok {
		t.Fatal("full atomic edit retained removed base file")
	}
}

func TestManifestCompleteCorruptionIsNeverClassifiedAsTornTail(t *testing.T) {
	first := mustManifestFrame(t, &raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{{File: "one.sst", MinKey: []byte("a"), MaxKey: []byte("a"), Count: 1}},
		FlushedIndex: 1,
	})
	second := mustManifestFrame(t, &raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{{File: "two.sst", MinKey: []byte("b"), MaxKey: []byte("b"), Count: 1}},
		FlushedIndex: 2,
	})
	third := mustManifestFrame(t, &raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{{File: "three.sst", MinKey: []byte("c"), MaxKey: []byte("c"), Count: 1}},
		FlushedIndex: 3,
	})

	tests := []struct {
		name string
		data func() []byte
	}{
		{
			name: "middle-record crc mismatch",
			data: func() []byte {
				corrupt := append([]byte(nil), second...)
				corrupt[1+16] = manifestSymbolBase + (corrupt[1+16]-manifestSymbolBase+1)%16
				return append(append(append([]byte(nil), first...), corrupt...), third...)
			},
		},
		{
			name: "malformed nibble",
			data: func() []byte {
				corrupt := append([]byte(nil), second...)
				corrupt[5] = 0x3F
				return append(append([]byte(nil), first...), corrupt...)
			},
		},
		{
			name: "odd encoded symbol count",
			data: func() []byte {
				corrupt := append([]byte(nil), second[:len(second)-1]...)
				corrupt = append(corrupt, manifestSymbolBase, manifestFrameEnd)
				return append(append([]byte(nil), first...), corrupt...)
			},
		},
		{
			name: "declared length mismatch",
			data: func() []byte {
				corrupt := append([]byte(nil), second...)
				corrupt[1] = manifestSymbolBase + (corrupt[1]-manifestSymbolBase+1)%16
				return append(append([]byte(nil), first...), corrupt...)
			},
		},
		{
			name: "unterminated middle record before later start",
			data: func() []byte {
				return append(append(append([]byte(nil), first...), second[:len(second)-1]...), third...)
			},
		},
		{
			name: "junk at record boundary",
			data: func() []byte {
				return append(append([]byte(nil), first...), 0)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, torn, err := replayManifestBytes(test.data())
			if !errors.Is(err, ErrManifestCorrupt) || torn {
				t.Fatalf("replay error = %v, torn=%v; want ErrManifestCorrupt and not torn", err, torn)
			}
		})
	}
}

func TestManifestCheckedSizesAndMalformedPayloads(t *testing.T) {
	if _, ok := checkedManifestFrameSize(math.MaxUint64); ok {
		t.Fatal("checkedManifestFrameSize(MaxUint64) succeeded")
	}
	if _, ok := checkedManifestAdd(math.MaxUint64, 1); ok {
		t.Fatal("checkedManifestAdd overflow succeeded")
	}
	if _, ok := checkedManifestMul(math.MaxUint64, 2); ok {
		t.Fatal("checkedManifestMul overflow succeeded")
	}
	if got, ok := checkedManifestFrameSize(math.MaxUint32); !ok || got != 2*(8+uint64(math.MaxUint32))+2 {
		t.Fatalf("checkedManifestFrameSize(MaxUint32) = %d,%v", got, ok)
	}

	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], math.MaxUint32)
	symbols := make([]byte, len(header)*2)
	if err := encodeManifestSymbols(symbols, header[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeManifestFrame(symbols); err == nil {
		t.Fatal("decodeManifestFrame allocated/accepted impossible declared payload length")
	}

	malformedProto := rawManifestFrame(t, []byte{0x0A, 0xFF})
	if _, _, _, err := replayManifestBytes(malformedProto); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("malformed protobuf error = %v, want ErrManifestCorrupt", err)
	}
}

func TestManifestVersionEditValidation(t *testing.T) {
	base := newManifestState()
	if err := base.apply(&raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{{File: "base.sst", MinKey: []byte("a"), MaxKey: []byte("z"), Count: 1}},
		FlushedIndex: 5,
	}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		edit *raftpb.VersionEdit
	}{
		{name: "regression", edit: &raftpb.VersionEdit{FlushedIndex: 4}},
		{name: "empty no-op", edit: &raftpb.VersionEdit{FlushedIndex: 5}},
		{name: "remove missing", edit: &raftpb.VersionEdit{RemovedFiles: []string{"missing.sst"}, FlushedIndex: 5}},
		{name: "duplicate remove", edit: &raftpb.VersionEdit{RemovedFiles: []string{"base.sst", "base.sst"}, FlushedIndex: 5}},
		{name: "path traversal", edit: &raftpb.VersionEdit{AddedFiles: []*raftpb.AddedFile{{File: "../bad.sst", Count: 1}}, FlushedIndex: 6}},
		{name: "wrong suffix", edit: &raftpb.VersionEdit{AddedFiles: []*raftpb.AddedFile{{File: "bad.tmp", Count: 1}}, FlushedIndex: 6}},
		{name: "zero count", edit: &raftpb.VersionEdit{AddedFiles: []*raftpb.AddedFile{{File: "empty.sst"}}, FlushedIndex: 6}},
		{name: "reversed bounds", edit: &raftpb.VersionEdit{AddedFiles: []*raftpb.AddedFile{{File: "bad.sst", MinKey: []byte("z"), MaxKey: []byte("a"), Count: 1}}, FlushedIndex: 6}},
		{name: "already referenced", edit: &raftpb.VersionEdit{AddedFiles: []*raftpb.AddedFile{{File: "base.sst", Count: 1}}, FlushedIndex: 6}},
		{name: "add and remove same", edit: &raftpb.VersionEdit{AddedFiles: []*raftpb.AddedFile{{File: "base.sst", Count: 1}}, RemovedFiles: []string{"base.sst"}, FlushedIndex: 6}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			copyState := manifestState{files: make(map[string]fileMetadata), flushedIndex: base.flushedIndex, records: base.records}
			for name, file := range base.files {
				copyState.files[name] = file
			}
			if err := copyState.apply(test.edit); err == nil {
				t.Fatalf("apply(%v) succeeded", test.edit)
			}
			if copyState.flushedIndex != base.flushedIndex || !reflect.DeepEqual(copyState.files, base.files) {
				t.Fatal("rejected edit partially mutated manifest state")
			}
		})
	}
	if err := base.apply(&raftpb.VersionEdit{FlushedIndex: 6}); err != nil {
		t.Fatalf("watermark-only advancing edit rejected: %v", err)
	}
}

func TestManifestReopenRejectsCorruptionAndMissingReferences(t *testing.T) {
	t.Run("corrupt complete frame", func(t *testing.T) {
		const dir = "/db"
		fs := NewSimFS()
		engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(1)})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		appendAndSyncTestFile(t, fs, filepath.Join(dir, manifestFilename), []byte{0x00})
		if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(2)}); !errors.Is(err, ErrManifestCorrupt) {
			t.Fatalf("Open() error = %v, want ErrManifestCorrupt", err)
		}
	})

	t.Run("missing referenced SSTable", func(t *testing.T) {
		const dir = "/db"
		fs := NewSimFS()
		engine, err := Open(dir, Options{FS: fs, Rand: newSeededRand(3)})
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
		frame := mustManifestFrame(t, &raftpb.VersionEdit{
			AddedFiles:   []*raftpb.AddedFile{{File: "missing.sst", MinKey: []byte("k"), MaxKey: []byte("k"), Count: 1}},
			FlushedIndex: 1,
		})
		appendAndSyncTestFile(t, fs, filepath.Join(dir, manifestFilename), frame)
		if _, err := Open(dir, Options{FS: fs, Rand: newSeededRand(4)}); !errors.Is(err, ErrManifestCorrupt) {
			t.Fatalf("Open() error = %v, want ErrManifestCorrupt", err)
		}
	})
}

func mustManifestFrame(t *testing.T, edit *raftpb.VersionEdit) []byte {
	t.Helper()
	frame, err := encodeManifestFrame(edit)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func rawManifestFrame(t *testing.T, payload []byte) []byte {
	t.Helper()
	body := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(body[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(body[4:8], crc32.Checksum(payload, crcTable))
	copy(body[8:], payload)
	frame := make([]byte, 2+2*len(body))
	frame[0], frame[len(frame)-1] = manifestFrameStart, manifestFrameEnd
	if err := encodeManifestSymbols(frame[1:len(frame)-1], body); err != nil {
		t.Fatal(err)
	}
	return frame
}

func appendAndSyncTestFile(t *testing.T, fs FS, path string, data []byte) {
	t.Helper()
	file, err := fs.OpenAppend(path)
	if err != nil {
		t.Fatal(err)
	}
	if written, err := file.Write(data); err != nil || written != len(data) {
		t.Fatalf("Write() = %d,%v", written, err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManifestDeterministicEncoding(t *testing.T) {
	edit := &raftpb.VersionEdit{
		AddedFiles: []*raftpb.AddedFile{
			{Tier: 2, File: "a.sst", MinKey: []byte{}, MaxKey: bytes.Repeat([]byte("z"), 16), Count: 99},
		},
		RemovedFiles: []string{"old.sst"},
		FlushedIndex: 123,
	}
	first := mustManifestFrame(t, edit)
	second := mustManifestFrame(t, proto.Clone(edit).(*raftpb.VersionEdit))
	if !bytes.Equal(first, second) {
		t.Fatal("identical VersionEdits produced different physical bytes")
	}
}
