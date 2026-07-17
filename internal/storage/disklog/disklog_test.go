package disklog

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

func mustOpen(t *testing.T, dir string, opts Options) *DiskLog {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open(%s) error: %v", dir, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func mustSave(t *testing.T, l *DiskLog, hs *raft.HardState, entries []raftpb.Entry) {
	t.Helper()
	if err := l.Save(hs, entries); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
}

// makeEntries builds NORMAL entries at consecutive indexes from first; an
// empty payload string yields nil Data, matching what reads return.
func makeEntries(first, term uint64, payloads ...string) []raftpb.Entry {
	entries := make([]raftpb.Entry, len(payloads))
	for i, payload := range payloads {
		var data []byte
		if payload != "" {
			data = []byte(payload)
		}
		entries[i] = raftpb.Entry{Index: first + uint64(i), Term: term, Type: raftpb.EntryType_NORMAL, Data: data}
	}
	return entries
}

func assertState(t *testing.T, l *DiskLog, wantHard raft.HardState, wantEntries []raftpb.Entry) {
	t.Helper()
	hard, err := l.HardState()
	if err != nil {
		t.Fatalf("HardState() error: %v", err)
	}
	if hard != wantHard {
		t.Fatalf("HardState() = %+v, want %+v", hard, wantHard)
	}
	if len(wantEntries) == 0 {
		if first, last := l.FirstIndex(), l.LastIndex(); first != 1 || last != 0 {
			t.Fatalf("empty log indexes = (%d, %d), want (1, 0)", first, last)
		}
		return
	}
	first := wantEntries[0].Index
	last := wantEntries[len(wantEntries)-1].Index
	if gotFirst, gotLast := l.FirstIndex(), l.LastIndex(); gotFirst != first || gotLast != last {
		t.Fatalf("indexes = (%d, %d), want (%d, %d)", gotFirst, gotLast, first, last)
	}
	got, err := l.Entries(first, last+1)
	if err != nil {
		t.Fatalf("Entries(%d, %d) error: %v", first, last+1, err)
	}
	if !reflect.DeepEqual(got, wantEntries) {
		t.Fatalf("Entries(%d, %d) = %#v, want %#v", first, last+1, got, wantEntries)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%s) error: %v", path, err)
	}
	return info.Size()
}

func flipByte(t *testing.T, path string, offset int) {
	t.Helper()
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error: %v", path, err)
	}
	buf[offset] ^= 0xff
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
}

// patchUint32 overwrites the little-endian uint32 at offset — pointed at a
// frame's first header field, it corrupts that record's length in place.
func patchUint32(t *testing.T, path string, offset int, value uint32) {
	t.Helper()
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error: %v", path, err)
	}
	binary.LittleEndian.PutUint32(buf[offset:], value)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error: %v", path, err)
	}
}

func appendBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile(%s) error: %v", path, err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write(%s) error: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close(%s) error: %v", path, err)
	}
}

type frameInfo struct {
	offset int
	length int // full frame length including the 8-byte header
	record *raftpb.LogRecord
}

func scanFrames(t *testing.T, path string) []frameInfo {
	t.Helper()
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error: %v", path, err)
	}
	var frames []frameInfo
	offset := 0
	for offset < len(buf) {
		if len(buf)-offset < frameHeaderSize {
			t.Fatalf("scanFrames(%s): short header at offset %d", path, offset)
		}
		payloadLen := int(binary.LittleEndian.Uint32(buf[offset:]))
		end := offset + frameHeaderSize + payloadLen
		if end > len(buf) {
			t.Fatalf("scanFrames(%s): short payload at offset %d", path, offset)
		}
		record := &raftpb.LogRecord{}
		if err := proto.Unmarshal(buf[offset+frameHeaderSize:end], record); err != nil {
			t.Fatalf("scanFrames(%s): unmarshal at offset %d: %v", path, offset, err)
		}
		frames = append(frames, frameInfo{offset: offset, length: end - offset, record: record})
		offset = end
	}
	return frames
}

func recordKind(record *raftpb.LogRecord) string {
	switch {
	case record.GetHardState() != nil:
		return "hard"
	case record.GetTruncate() != nil:
		return "trunc"
	case record.GetEntries() != nil:
		return "entries"
	}
	return "unknown"
}

func frameKinds(t *testing.T, path string) []string {
	t.Helper()
	var kinds []string
	for _, f := range scanFrames(t, path) {
		kinds = append(kinds, recordKind(f.record))
	}
	return kinds
}

func listSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error: %v", dir, err)
	}
	var names []string
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), segmentSuffix) {
			names = append(names, ent.Name())
		}
	}
	return names
}

func TestOpenEmptyLog(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	assertState(t, l, raft.HardState{}, nil)
	got, err := l.Entries(1, 1)
	if err != nil || got != nil {
		t.Fatalf("Entries(1, 1) = %v, %v, want nil, nil", got, err)
	}
	if _, err := l.Entries(1, 2); !errors.Is(err, storage.ErrOutOfBounds) {
		t.Fatalf("Entries(1, 2) error = %v, want ErrOutOfBounds", err)
	}
	if names := listSegmentFiles(t, dir); !reflect.DeepEqual(names, []string{"000001.seg"}) {
		t.Fatalf("segments = %v, want [000001.seg]", names)
	}
	if size := fileSize(t, filepath.Join(dir, "000001.seg")); size != 0 {
		t.Fatalf("fresh segment size = %d, want 0", size)
	}
}

func TestOpenErrors(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing"), Options{}); err == nil || !strings.Contains(err.Error(), "open dir") {
		t.Fatalf("Open(missing dir) error = %v, want open dir failure", err)
	}
	if _, err := Open(t.TempDir(), Options{RotateSize: -1}); err == nil || !strings.Contains(err.Error(), "negative rotate size") {
		t.Fatalf("Open(rotate -1) error = %v, want negative rotate size", err)
	}
}

func TestSaveAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	hard := raft.HardState{Term: 7, VotedFor: 3}
	entries := makeEntries(1, 5, "one", "two")
	entries = append(entries, raftpb.Entry{Index: 3, Term: 7, Type: raftpb.EntryType_NOOP})
	mustSave(t, l, &hard, entries)

	want := makeEntries(1, 5, "one", "two")
	want = append(want, raftpb.Entry{Index: 3, Term: 7, Type: raftpb.EntryType_NOOP})
	assertState(t, l, hard, want)

	// The caller keeps ownership of what it passed in and what it got back.
	entries[0].Data[0] = 'X'
	got, err := l.Entries(1, 2)
	if err != nil {
		t.Fatalf("Entries(1, 2) error: %v", err)
	}
	if string(got[0].Data) != "one" {
		t.Fatalf("Save retained caller data: %q", got[0].Data)
	}
	got[0].Data[0] = 'Y'
	again, err := l.Entries(1, 2)
	if err != nil {
		t.Fatalf("Entries(1, 2) error: %v", err)
	}
	if string(again[0].Data) != "one" {
		t.Fatalf("Entries leaked internal data: %q", again[0].Data)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	reopened := mustOpen(t, dir, Options{})
	assertState(t, reopened, hard, want)
}

func TestSaveBatchingOneSyncPerSave(t *testing.T) {
	dir := t.TempDir()
	var fileSyncs, dirSyncs int
	l := mustOpen(t, dir, Options{
		syncFile: func(f *os.File) error { fileSyncs++; return f.Sync() },
		syncDir:  func(f *os.File) error { dirSyncs++; return f.Sync() },
	})
	if dirSyncs != 1 || fileSyncs != 0 {
		t.Fatalf("after Open: dirSyncs=%d fileSyncs=%d, want 1, 0", dirSyncs, fileSyncs)
	}
	seg := filepath.Join(dir, "000001.seg")

	hardOnly := raft.HardState{Term: 2, VotedFor: 1}
	mustSave(t, l, &hardOnly, nil)
	if fileSyncs != 1 {
		t.Fatalf("hard-state-only Save: fileSyncs=%d, want 1", fileSyncs)
	}

	mustSave(t, l, nil, makeEntries(1, 2, "a", "b"))
	if fileSyncs != 2 {
		t.Fatalf("entries-only Save: fileSyncs=%d, want 2", fileSyncs)
	}
	hard, err := l.HardState()
	if err != nil || hard != hardOnly {
		t.Fatalf("entries-only Save changed hard state: %+v, %v", hard, err)
	}

	combined := raft.HardState{Term: 3, VotedFor: 2}
	mustSave(t, l, &combined, makeEntries(3, 3, "c"))
	if fileSyncs != 3 {
		t.Fatalf("combined Save: fileSyncs=%d, want 3", fileSyncs)
	}

	sizeBefore := fileSize(t, seg)
	mustSave(t, l, nil, nil) // empty batch: nothing durable to add
	if fileSyncs != 3 || dirSyncs != 1 {
		t.Fatalf("empty Save synced: fileSyncs=%d dirSyncs=%d, want 3, 1", fileSyncs, dirSyncs)
	}
	if size := fileSize(t, seg); size != sizeBefore {
		t.Fatalf("empty Save wrote %d bytes", size-sizeBefore)
	}

	wantKinds := []string{"hard", "entries", "hard", "entries"}
	if kinds := frameKinds(t, seg); !reflect.DeepEqual(kinds, wantKinds) {
		t.Fatalf("frame kinds = %v, want %v", kinds, wantKinds)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	want := append(makeEntries(1, 2, "a", "b"), makeEntries(3, 3, "c")...)
	assertState(t, mustOpen(t, dir, Options{}), combined, want)
}

func TestSuffixTruncationOnOverlap(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	seg := filepath.Join(dir, "000001.seg")
	mustSave(t, l, nil, makeEntries(1, 1, "a", "b", "c", "d", "e"))
	mustSave(t, l, nil, makeEntries(3, 2, "C", "D"))

	want := append(makeEntries(1, 1, "a", "b"), makeEntries(3, 2, "C", "D")...)
	assertState(t, l, raft.HardState{}, want)
	if _, err := l.Entries(1, 7); !errors.Is(err, storage.ErrOutOfBounds) {
		t.Fatalf("Entries(1, 7) after truncation error = %v, want ErrOutOfBounds", err)
	}

	// The overwrite appended a TruncateRecord before its entries.
	kinds := frameKinds(t, seg)
	if want := []string{"entries", "trunc", "entries"}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("frame kinds = %v, want %v", kinds, want)
	}
	frames := scanFrames(t, seg)
	if from := frames[1].record.GetTruncate().GetFromIndex(); from != 3 {
		t.Fatalf("TruncateRecord.from_index = %d, want 3", from)
	}

	// A pure tail append writes no truncate record.
	mustSave(t, l, nil, makeEntries(5, 2, "E"))
	kinds = frameKinds(t, seg)
	if want := []string{"entries", "trunc", "entries", "entries"}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("frame kinds after tail append = %v, want %v", kinds, want)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	want = append(want, makeEntries(5, 2, "E")...)
	reopened := mustOpen(t, dir, Options{})
	assertState(t, reopened, raft.HardState{}, want)

	// Overwrite from index 1: the whole log is replaced, including on replay.
	mustSave(t, reopened, nil, makeEntries(1, 3, "z"))
	assertState(t, reopened, raft.HardState{}, makeEntries(1, 3, "z"))
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	assertState(t, mustOpen(t, dir, Options{}), raft.HardState{}, makeEntries(1, 3, "z"))
}

func TestRotationAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{RotateSize: 1}) // every Save after the first rotates
	mustSave(t, l, nil, makeEntries(1, 1, "a"))
	mustSave(t, l, nil, makeEntries(2, 1, "b"))
	mustSave(t, l, nil, makeEntries(3, 1, "c"))
	wantNames := []string{"000001.seg", "000002.seg", "000003.seg"}
	if names := listSegmentFiles(t, dir); !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("segments = %v, want %v", names, wantNames)
	}
	want := append(append(makeEntries(1, 1, "a"), makeEntries(2, 1, "b")...), makeEntries(3, 1, "c")...)
	assertState(t, l, raft.HardState{}, want)

	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	reopened := mustOpen(t, dir, Options{RotateSize: 1})
	assertState(t, reopened, raft.HardState{}, want)
	mustSave(t, reopened, nil, makeEntries(4, 1, "d"))
	if names := listSegmentFiles(t, dir); len(names) != 4 {
		t.Fatalf("segments after post-recovery Save = %v, want 4 files", names)
	}
	assertState(t, reopened, raft.HardState{}, append(want, makeEntries(4, 1, "d")...))
}

func TestRotationExactBoundary(t *testing.T) {
	// Measure the exact on-disk size of one batch shape.
	probeDir := t.TempDir()
	probe := mustOpen(t, probeDir, Options{})
	mustSave(t, probe, nil, makeEntries(1, 1, "aa", "bb"))
	batchSize := fileSize(t, filepath.Join(probeDir, "000001.seg"))

	// A batch landing exactly at RotateSize fills its segment completely;
	// rotation happens on the next Save, never mid-batch.
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{RotateSize: batchSize})
	mustSave(t, l, nil, makeEntries(1, 1, "aa", "bb"))
	if names := listSegmentFiles(t, dir); len(names) != 1 {
		t.Fatalf("segments after exactly-full write = %v, want just 000001.seg", names)
	}
	mustSave(t, l, nil, makeEntries(3, 1, "cc", "dd"))
	mustSave(t, l, nil, makeEntries(5, 1, "ee", "ff"))
	wantNames := []string{"000001.seg", "000002.seg", "000003.seg"}
	if names := listSegmentFiles(t, dir); !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("segments = %v, want %v", names, wantNames)
	}
	for _, name := range wantNames {
		if size := fileSize(t, filepath.Join(dir, name)); size != batchSize {
			t.Fatalf("%s size = %d, want exactly %d", name, size, batchSize)
		}
	}
	want := append(append(makeEntries(1, 1, "aa", "bb"), makeEntries(3, 1, "cc", "dd")...), makeEntries(5, 1, "ee", "ff")...)
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	assertState(t, mustOpen(t, dir, Options{}), raft.HardState{}, want)
}

func TestOversizedBatchStaysWhole(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{RotateSize: 1})
	mustSave(t, l, nil, makeEntries(1, 1, "one", "two", "three"))
	if names := listSegmentFiles(t, dir); len(names) != 1 {
		t.Fatalf("oversized batch split across segments: %v", names)
	}
	if size := fileSize(t, filepath.Join(dir, "000001.seg")); size <= 1 {
		t.Fatalf("segment size = %d, want > RotateSize", size)
	}
	mustSave(t, l, nil, makeEntries(4, 1, "four"))
	if names := listSegmentFiles(t, dir); len(names) != 2 {
		t.Fatalf("next Save should rotate: %v", names)
	}
}

func TestCloseReopenRecovery(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{RotateSize: 1})
	hard1 := raft.HardState{Term: 1, VotedFor: 2}
	mustSave(t, l, &hard1, makeEntries(1, 1, "a", "b"))
	mustSave(t, l, nil, makeEntries(3, 1, "c", "d"))
	hard2 := raft.HardState{Term: 2, VotedFor: 3}
	mustSave(t, l, &hard2, nil)
	mustSave(t, l, nil, makeEntries(3, 2, "C"))
	mustSave(t, l, nil, makeEntries(4, 2, "D", "E"))
	want := append(append(makeEntries(1, 1, "a", "b"), makeEntries(3, 2, "C")...), makeEntries(4, 2, "D", "E")...)
	assertState(t, l, hard2, want)
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	reopened := mustOpen(t, dir, Options{RotateSize: 1})
	assertState(t, reopened, hard2, want)
	mustSave(t, reopened, nil, makeEntries(6, 2, "f"))
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	assertState(t, mustOpen(t, dir, Options{}), hard2, append(want, makeEntries(6, 2, "f")...))
}

func TestHardStateLastWins(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	for term := uint64(1); term <= 4; term++ {
		hard := raft.HardState{Term: term, VotedFor: raft.NodeID(term % 3)}
		mustSave(t, l, &hard, nil)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	assertState(t, mustOpen(t, dir, Options{}), raft.HardState{Term: 4, VotedFor: 1}, nil)
}

func TestTornOrBadFinalRecordDiscarded(t *testing.T) {
	// Each subtest builds a log with batch1 (entries 1..2), then batch2
	// (hard state + entry 3), then damages batch2's final frame. Recovery
	// must drop the damaged record, keep everything before it, and
	// physically truncate the file at the damaged frame's offset.
	hard1 := raft.HardState{Term: 1, VotedFor: 1}
	hard2 := raft.HardState{Term: 2, VotedFor: 2}
	build := func(t *testing.T) (dir, seg string, frames []frameInfo) {
		t.Helper()
		dir = t.TempDir()
		l := mustOpen(t, dir, Options{})
		mustSave(t, l, &hard1, makeEntries(1, 1, "one", "two"))
		mustSave(t, l, &hard2, makeEntries(3, 2, "three"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		seg = filepath.Join(dir, "000001.seg")
		frames = scanFrames(t, seg)
		// batch1 = [hard, entries], batch2 = [hard, entries]
		if kinds := frameKinds(t, seg); !reflect.DeepEqual(kinds, []string{"hard", "entries", "hard", "entries"}) {
			t.Fatalf("unexpected layout: %v", kinds)
		}
		return dir, seg, frames
	}
	keptEntries := makeEntries(1, 1, "one", "two")

	check := func(t *testing.T, dir, seg string, wantHard raft.HardState, wantSize int64) {
		t.Helper()
		l := mustOpen(t, dir, Options{})
		assertState(t, l, wantHard, keptEntries)
		if size := fileSize(t, seg); size != wantSize {
			t.Fatalf("segment size after recovery = %d, want %d", size, wantSize)
		}
		// The log accepts appends again and the replacement survives.
		mustSave(t, l, nil, makeEntries(3, 3, "replacement"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		assertState(t, mustOpen(t, dir, Options{}), wantHard, append(makeEntries(1, 1, "one", "two"), makeEntries(3, 3, "replacement")...))
	}

	t.Run("payload cut mid-record", func(t *testing.T) {
		dir, seg, frames := build(t)
		last := frames[len(frames)-1]
		if err := os.Truncate(seg, int64(last.offset+last.length-2)); err != nil {
			t.Fatal(err)
		}
		// batch2's hard-state frame was written before the torn entries
		// frame; a retained prefix of the batch is a legal recovered state.
		check(t, dir, seg, hard2, int64(last.offset))
	})
	t.Run("header cut mid-record", func(t *testing.T) {
		dir, seg, frames := build(t)
		last := frames[len(frames)-1]
		if err := os.Truncate(seg, int64(last.offset+frameHeaderSize-3)); err != nil {
			t.Fatal(err)
		}
		check(t, dir, seg, hard2, int64(last.offset))
	})
	t.Run("crc-damaged final record", func(t *testing.T) {
		dir, seg, frames := build(t)
		last := frames[len(frames)-1]
		flipByte(t, seg, last.offset+frameHeaderSize)
		check(t, dir, seg, hard2, int64(last.offset))
	})
	t.Run("whole final batch torn away", func(t *testing.T) {
		dir, seg, frames := build(t)
		batch2Hard := frames[2]
		if err := os.Truncate(seg, int64(batch2Hard.offset+3)); err != nil {
			t.Fatal(err)
		}
		check(t, dir, seg, hard1, int64(batch2Hard.offset))
	})
	t.Run("first-ever record torn", func(t *testing.T) {
		dir := t.TempDir()
		l := mustOpen(t, dir, Options{})
		mustSave(t, l, nil, makeEntries(1, 1, "a"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		seg := filepath.Join(dir, "000001.seg")
		if err := os.Truncate(seg, 5); err != nil {
			t.Fatal(err)
		}
		reopened := mustOpen(t, dir, Options{})
		assertState(t, reopened, raft.HardState{}, nil)
		if size := fileSize(t, seg); size != 0 {
			t.Fatalf("segment size = %d, want 0", size)
		}
		mustSave(t, reopened, nil, makeEntries(1, 2, "fresh"))
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		assertState(t, mustOpen(t, dir, Options{}), raft.HardState{}, makeEntries(1, 2, "fresh"))
	})
}

func TestMidLogCorruptionRefusesStartup(t *testing.T) {
	t.Run("corrupt record with records after it in the final segment", func(t *testing.T) {
		dir := t.TempDir()
		l := mustOpen(t, dir, Options{})
		mustSave(t, l, nil, makeEntries(1, 1, "a"))
		mustSave(t, l, nil, makeEntries(2, 1, "b"))
		mustSave(t, l, nil, makeEntries(3, 1, "c"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		seg := filepath.Join(dir, "000001.seg")
		frames := scanFrames(t, seg)
		flipByte(t, seg, frames[1].offset+frameHeaderSize)
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open() error = %v, want ErrCorrupt", err)
		}
		if !strings.Contains(err.Error(), "000001.seg") || !strings.Contains(err.Error(), "crc32c mismatch") {
			t.Fatalf("corruption error lacks location or cause: %v", err)
		}
	})
	t.Run("corrupt record in a non-final segment", func(t *testing.T) {
		dir := t.TempDir()
		l := mustOpen(t, dir, Options{RotateSize: 1})
		mustSave(t, l, nil, makeEntries(1, 1, "a"))
		mustSave(t, l, nil, makeEntries(2, 1, "b"))
		mustSave(t, l, nil, makeEntries(3, 1, "c"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		flipByte(t, filepath.Join(dir, "000002.seg"), frameHeaderSize)
		_, err := Open(dir, Options{RotateSize: 1})
		if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "000002.seg") {
			t.Fatalf("Open() error = %v, want ErrCorrupt in 000002.seg", err)
		}
	})
	t.Run("short record in a non-final segment", func(t *testing.T) {
		dir := t.TempDir()
		l := mustOpen(t, dir, Options{RotateSize: 1})
		mustSave(t, l, nil, makeEntries(1, 1, "a"))
		mustSave(t, l, nil, makeEntries(2, 1, "b"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		seg := filepath.Join(dir, "000001.seg")
		if err := os.Truncate(seg, fileSize(t, seg)-2); err != nil {
			t.Fatal(err)
		}
		_, err := Open(dir, Options{RotateSize: 1})
		if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "000001.seg") {
			t.Fatalf("Open() error = %v, want ErrCorrupt in 000001.seg", err)
		}
	})
}

func TestLengthHeaderCorruption(t *testing.T) {
	// Three single-entry batches in one segment, one entries frame each. The
	// length header under test belongs to frames[1] (a middle record, with an
	// intact record after it) or frames[2] (the true final record). A length
	// header can lie in either direction, and neither lie may be believed:
	// an upward-corrupted middle length claims bytes through the end of the
	// file and must not masquerade as a torn tail (that would silently
	// discard the intact records after it), while a downward-corrupted final
	// length leaves trailing bytes that must not read as mid-log corruption
	// (nothing intact follows, so it is the log's damaged tail).
	build := func(t *testing.T) (dir, seg string, frames []frameInfo) {
		t.Helper()
		dir = t.TempDir()
		l := mustOpen(t, dir, Options{})
		mustSave(t, l, nil, makeEntries(1, 1, "alpha"))
		mustSave(t, l, nil, makeEntries(2, 1, "beta"))
		mustSave(t, l, nil, makeEntries(3, 1, "gamma"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		seg = filepath.Join(dir, "000001.seg")
		frames = scanFrames(t, seg)
		if len(frames) != 3 {
			t.Fatalf("frames = %d, want 3", len(frames))
		}
		return dir, seg, frames
	}

	t.Run("middle length corrupted past end of file", func(t *testing.T) {
		dir, seg, frames := build(t)
		patchUint32(t, seg, frames[1].offset, uint32(fileSize(t, seg)))
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "intact record") {
			t.Fatalf("Open() error = %v, want ErrCorrupt naming the intact record after the damage", err)
		}
	})
	t.Run("middle length corrupted to claim exactly the rest of the file", func(t *testing.T) {
		dir, seg, frames := build(t)
		rest := int(fileSize(t, seg)) - frames[1].offset - frameHeaderSize
		patchUint32(t, seg, frames[1].offset, uint32(rest))
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open() error = %v, want ErrCorrupt", err)
		}
	})
	t.Run("middle length corrupted within the file", func(t *testing.T) {
		dir, seg, frames := build(t)
		payloadLen := frames[1].length - frameHeaderSize
		patchUint32(t, seg, frames[1].offset, uint32(payloadLen+3)) // swallows part of frame 2's header
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open() error = %v, want ErrCorrupt", err)
		}
	})
	t.Run("length corrupted in a non-final segment", func(t *testing.T) {
		dir := t.TempDir()
		l := mustOpen(t, dir, Options{RotateSize: 1})
		mustSave(t, l, nil, makeEntries(1, 1, "a"))
		mustSave(t, l, nil, makeEntries(2, 1, "b"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		seg := filepath.Join(dir, "000001.seg")
		patchUint32(t, seg, 0, uint32(fileSize(t, seg)+50))
		_, err := Open(dir, Options{RotateSize: 1})
		if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "000001.seg") {
			t.Fatalf("Open() error = %v, want ErrCorrupt in 000001.seg", err)
		}
	})

	// The final record's length corrupted: whatever the direction, nothing
	// intact follows it, so it is dropped as the log's damaged tail — the
	// file is truncated at its offset, earlier records survive, and the log
	// accepts appends again.
	checkTailDropped := func(t *testing.T, dir, seg string, frames []frameInfo) {
		t.Helper()
		l := mustOpen(t, dir, Options{})
		want := append(makeEntries(1, 1, "alpha"), makeEntries(2, 1, "beta")...)
		assertState(t, l, raft.HardState{}, want)
		if size := fileSize(t, seg); size != int64(frames[2].offset) {
			t.Fatalf("segment size after recovery = %d, want %d", size, frames[2].offset)
		}
		mustSave(t, l, nil, makeEntries(3, 2, "replacement"))
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		assertState(t, mustOpen(t, dir, Options{}), raft.HardState{}, append(want, makeEntries(3, 2, "replacement")...))
	}
	t.Run("final length corrupted downward", func(t *testing.T) {
		dir, seg, frames := build(t)
		payloadLen := frames[2].length - frameHeaderSize
		patchUint32(t, seg, frames[2].offset, uint32(payloadLen-2))
		checkTailDropped(t, dir, seg, frames)
	})
	t.Run("final length corrupted to zero", func(t *testing.T) {
		dir, seg, frames := build(t)
		patchUint32(t, seg, frames[2].offset, 0)
		checkTailDropped(t, dir, seg, frames)
	})
	t.Run("final length corrupted past end of file", func(t *testing.T) {
		dir, seg, frames := build(t)
		patchUint32(t, seg, frames[2].offset, uint32(frames[2].length+100))
		checkTailDropped(t, dir, seg, frames)
	})
}

func TestSemanticallyInvalidRecordRefused(t *testing.T) {
	cases := []struct {
		name    string
		record  *raftpb.LogRecord
		message string
	}{
		{"unknown record type", &raftpb.LogRecord{}, "unknown record type"},
		{"empty entries record", &raftpb.LogRecord{Body: &raftpb.LogRecord_Entries{Entries: &raftpb.EntriesRecord{}}}, "empty entries record"},
		{"truncate from index zero", &raftpb.LogRecord{Body: &raftpb.LogRecord_Truncate{Truncate: &raftpb.TruncateRecord{}}}, "truncate record from index 0"},
		{"entries record leaving a gap", &raftpb.LogRecord{Body: &raftpb.LogRecord_Entries{Entries: &raftpb.EntriesRecord{
			Entries: []*raftpb.Entry{{Index: 9, Term: 1}},
		}}}, "does not extend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			l := mustOpen(t, dir, Options{})
			mustSave(t, l, nil, makeEntries(1, 1, "a"))
			if err := l.Close(); err != nil {
				t.Fatalf("Close() error: %v", err)
			}
			frame, err := appendFrame(nil, tc.record)
			if err != nil {
				t.Fatalf("appendFrame() error: %v", err)
			}
			appendBytes(t, filepath.Join(dir, "000001.seg"), frame)
			_, err = Open(dir, Options{})
			if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("Open() error = %v, want ErrCorrupt with %q", err, tc.message)
			}
		})
	}
}

func TestSegmentSequenceGapRefused(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000002.seg"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{})
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("Open() error = %v, want ErrCorrupt sequence gap", err)
	}
}

func TestForeignFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"junk.txt", "12345.seg", "00000a.seg", "segments"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	l := mustOpen(t, dir, Options{})
	mustSave(t, l, nil, makeEntries(1, 1, "a"))
	if names := listSegmentFiles(t, dir); !reflect.DeepEqual(names, []string{"000001.seg", "00000a.seg", "12345.seg"}) {
		t.Fatalf("segment-suffixed files = %v", names)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	assertState(t, mustOpen(t, dir, Options{}), raft.HardState{}, makeEntries(1, 1, "a"))
}

func TestDirectorySyncFailpoint(t *testing.T) {
	t.Run("directory sync runs on create and every rotation", func(t *testing.T) {
		dir := t.TempDir()
		var dirSyncs int
		l := mustOpen(t, dir, Options{
			RotateSize: 1,
			syncDir:    func(f *os.File) error { dirSyncs++; return f.Sync() },
		})
		if dirSyncs != 1 {
			t.Fatalf("dirSyncs after Open = %d, want 1", dirSyncs)
		}
		mustSave(t, l, nil, makeEntries(1, 1, "a")) // first batch: no rotation
		if dirSyncs != 1 {
			t.Fatalf("dirSyncs after non-rotating Save = %d, want 1", dirSyncs)
		}
		mustSave(t, l, nil, makeEntries(2, 1, "b")) // rotates to 000002.seg
		mustSave(t, l, nil, makeEntries(3, 1, "c")) // rotates to 000003.seg
		if dirSyncs != 3 {
			t.Fatalf("dirSyncs after two rotations = %d, want 3", dirSyncs)
		}
	})
	t.Run("directory sync failure at Open surfaces", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := Open(t.TempDir(), Options{syncDir: func(*os.File) error { return boom }})
		if !errors.Is(err, boom) || !strings.Contains(err.Error(), "sync dir") {
			t.Fatalf("Open() error = %v, want wrapped boom from dir sync", err)
		}
	})
	t.Run("directory sync failure at rotation fails the Save and poisons the log", func(t *testing.T) {
		dir := t.TempDir()
		boom := errors.New("boom")
		var failDirSync bool
		l := mustOpen(t, dir, Options{
			RotateSize: 1,
			syncDir: func(f *os.File) error {
				if failDirSync {
					return boom
				}
				return f.Sync()
			},
		})
		mustSave(t, l, nil, makeEntries(1, 1, "a"))
		failDirSync = true
		if err := l.Save(nil, makeEntries(2, 1, "b")); !errors.Is(err, boom) {
			t.Fatalf("rotating Save error = %v, want boom", err)
		}
		if last := l.LastIndex(); last != 1 {
			t.Fatalf("LastIndex after failed Save = %d, want 1", last)
		}
		failDirSync = false
		err := l.Save(nil, makeEntries(2, 1, "b"))
		if !errors.Is(err, boom) || !strings.Contains(err.Error(), "poisoned") {
			t.Fatalf("Save after failure error = %v, want poisoned boom", err)
		}
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		// The half-rotated dir (000002.seg created, sync unreported) is a
		// crash-equivalent state, but the segment's name was never made
		// durable and later Saves sync only the file. Recovery may accept
		// the half-rotation only by retrying the directory sync itself —
		// and must refuse to open while it still cannot.
		if _, err := Open(dir, Options{syncDir: func(*os.File) error { return boom }}); !errors.Is(err, boom) || !strings.Contains(err.Error(), "recovery sync dir") {
			t.Fatalf("Open() with still-failing dir sync error = %v, want recovery dir-sync boom", err)
		}
		var dirSyncs int
		reopened, err := Open(dir, Options{syncDir: func(f *os.File) error { dirSyncs++; return f.Sync() }})
		if err != nil {
			t.Fatalf("Open() error: %v", err)
		}
		defer func() { _ = reopened.Close() }()
		if dirSyncs != 1 {
			t.Fatalf("dirSyncs during recovery = %d, want 1", dirSyncs)
		}
		assertState(t, reopened, raft.HardState{}, makeEntries(1, 1, "a"))
		// The recovered half-rotation is durable end to end; the empty
		// final segment accepts appends as usual.
		mustSave(t, reopened, nil, makeEntries(2, 1, "b"))
		assertState(t, reopened, raft.HardState{}, makeEntries(1, 1, "a", "b"))
	})
}

func TestFileSyncFailurePoisons(t *testing.T) {
	dir := t.TempDir()
	boom := errors.New("boom")
	var failFileSync bool
	l := mustOpen(t, dir, Options{
		syncFile: func(f *os.File) error {
			if failFileSync {
				return boom
			}
			return f.Sync()
		},
	})
	mustSave(t, l, nil, makeEntries(1, 1, "a"))
	failFileSync = true
	if err := l.Save(nil, makeEntries(2, 1, "b")); !errors.Is(err, boom) {
		t.Fatalf("Save with failing sync error = %v, want boom", err)
	}
	assertState(t, l, raft.HardState{}, makeEntries(1, 1, "a")) // mirror unchanged
	failFileSync = false
	err := l.Save(nil, makeEntries(2, 1, "b"))
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("Save after sync failure error = %v, want poisoned boom", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// The poison dies with the process, and the failed batch's bytes —
	// written whole, sync unreported — may sit only in the page cache:
	// readable, but not durable. Recovery may serve them solely because it
	// syncs them first; recovered state drives responses (a re-granted vote
	// carries no new HardState to Save), so accepting surviving frames
	// without establishing their durability would let a later crash forget
	// an already-acknowledged vote or entry.
	t.Run("reopen establishes durability before serving", func(t *testing.T) {
		var fileSyncs, dirSyncs int
		reopened, err := Open(dir, Options{
			syncFile: func(f *os.File) error { fileSyncs++; return f.Sync() },
			syncDir:  func(f *os.File) error { dirSyncs++; return f.Sync() },
		})
		if err != nil {
			t.Fatalf("Open() error: %v", err)
		}
		defer func() { _ = reopened.Close() }()
		if fileSyncs != 1 || dirSyncs != 1 {
			t.Fatalf("recovery syncs = %d file, %d dir, want 1 file (the lone segment), 1 dir", fileSyncs, dirSyncs)
		}
		assertState(t, reopened, raft.HardState{}, append(makeEntries(1, 1, "a"), makeEntries(2, 1, "b")...))
	})
	t.Run("reopen fails when recovery cannot sync the segment", func(t *testing.T) {
		_, err := Open(dir, Options{syncFile: func(*os.File) error { return boom }})
		if !errors.Is(err, boom) || !strings.Contains(err.Error(), "recovery sync") {
			t.Fatalf("Open() error = %v, want recovery-sync boom", err)
		}
	})
	t.Run("reopen fails when recovery cannot sync the directory", func(t *testing.T) {
		_, err := Open(dir, Options{syncDir: func(*os.File) error { return boom }})
		if !errors.Is(err, boom) || !strings.Contains(err.Error(), "recovery sync dir") {
			t.Fatalf("Open() error = %v, want recovery dir-sync boom", err)
		}
	})
}

func TestRecoverySyncsEverySegmentAndDirectory(t *testing.T) {
	// Open after a restart cannot know which surviving bytes and names were
	// ever synced, so before serving it must sync every segment file and
	// then the directory — after which ordinary Saves resume their
	// one-sync-per-Save contract.
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{RotateSize: 1})
	mustSave(t, l, nil, makeEntries(1, 1, "a"))
	mustSave(t, l, nil, makeEntries(2, 1, "b"))
	mustSave(t, l, nil, makeEntries(3, 1, "c")) // three segments
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	var fileSyncs, dirSyncs int
	reopened, err := Open(dir, Options{
		RotateSize: 1,
		syncFile:   func(f *os.File) error { fileSyncs++; return f.Sync() },
		syncDir:    func(f *os.File) error { dirSyncs++; return f.Sync() },
	})
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if fileSyncs != 3 || dirSyncs != 1 {
		t.Fatalf("recovery syncs = %d file, %d dir, want 3 file (one per segment), 1 dir", fileSyncs, dirSyncs)
	}
	mustSave(t, reopened, nil, makeEntries(4, 1, "d")) // rotates, then one batch sync
	if fileSyncs != 4 {
		t.Fatalf("fileSyncs after one post-recovery Save = %d, want 4", fileSyncs)
	}
}

func TestNonContiguousAppendRejected(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	mustSave(t, l, nil, makeEntries(1, 1, "a", "b", "c"))

	if err := l.Save(nil, makeEntries(5, 1, "gap")); err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("Save with index gap error = %v, want non-contiguous", err)
	}
	gapped := make([]raftpb.Entry, 2)
	gapped[0] = raftpb.Entry{Index: 4, Term: 1}
	gapped[1] = raftpb.Entry{Index: 6, Term: 1}
	if err := l.Save(nil, gapped); err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("Save with intra-batch gap error = %v, want non-contiguous", err)
	}
	zero := make([]raftpb.Entry, 1)
	zero[0] = raftpb.Entry{Index: 0, Term: 1}
	if err := l.Save(nil, zero); err == nil || !strings.Contains(err.Error(), "non-contiguous") {
		t.Fatalf("Save with index 0 error = %v, want non-contiguous", err)
	}

	// Rejection is a validation error, not a poisoning disk failure.
	mustSave(t, l, nil, makeEntries(4, 1, "d"))
	assertState(t, l, raft.HardState{}, append(makeEntries(1, 1, "a", "b", "c"), makeEntries(4, 1, "d")...))
}

func TestEntriesBounds(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	mustSave(t, l, nil, makeEntries(1, 1, "a", "b", "c", "d", "e", "f"))
	got, err := l.Entries(5, 7)
	if err != nil {
		t.Fatalf("Entries(5, 7) error: %v", err)
	}
	if want := makeEntries(5, 1, "e", "f"); !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries(5, 7) = %#v, want %#v", got, want)
	}
	for _, bounds := range [][2]uint64{{0, 3}, {4, 8}, {6, 5}} {
		if _, err := l.Entries(bounds[0], bounds[1]); !errors.Is(err, storage.ErrOutOfBounds) {
			t.Errorf("Entries(%d, %d) error = %v, want ErrOutOfBounds", bounds[0], bounds[1], err)
		}
	}
	if got, err := l.Entries(7, 7); err != nil || got != nil {
		t.Fatalf("Entries(7, 7) = %v, %v, want nil, nil", got, err)
	}
}

func TestSaveAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	mustSave(t, l, nil, makeEntries(1, 1, "a"))
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close() error: %v", err)
	}
	if err := l.Save(nil, makeEntries(2, 1, "b")); !errors.Is(err, errClosed) {
		t.Fatalf("Save after Close error = %v, want errClosed", err)
	}
	// Reads keep serving the in-memory mirror.
	assertState(t, l, raft.HardState{}, makeEntries(1, 1, "a"))
}

func TestConcurrentReadersDuringSave(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{RotateSize: 256})
	done := make(chan struct{})
	var wg sync.WaitGroup
	for reader := 0; reader < 2; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				last := l.LastIndex()
				if _, err := l.HardState(); err != nil {
					t.Errorf("HardState() error: %v", err)
					return
				}
				if last == 0 {
					continue
				}
				got, err := l.Entries(1, last+1)
				if err != nil {
					t.Errorf("Entries(1, %d) error: %v", last+1, err)
					return
				}
				for i := range got {
					if got[i].Index != uint64(i)+1 {
						t.Errorf("entry %d has index %d", i, got[i].Index)
						return
					}
				}
			}
		}()
	}
	for i := uint64(1); i <= 60; i++ {
		hard := raft.HardState{Term: i, VotedFor: 1}
		mustSave(t, l, &hard, makeEntries(i, i, "payload"))
	}
	close(done)
	wg.Wait()
	if last := l.LastIndex(); last != 60 {
		t.Fatalf("LastIndex = %d, want 60", last)
	}
}
