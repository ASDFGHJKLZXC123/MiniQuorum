package disklog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

func TestSnapshotOverlapCompactionPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	log := mustOpen(t, dir, Options{RotateSize: 1})
	hard := raft.HardState{Term: 2, VotedFor: 1}
	for index := uint64(1); index <= 8; index++ {
		mustSave(t, log, &hard, []raftpb.Entry{{Index: index, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte{byte(index)}}})
		hard = raft.HardState{Term: 2, VotedFor: 1}
	}
	meta := raft.SnapshotMeta{Index: 8, Term: 2}
	if err := log.SaveSnapshot(meta); err != nil {
		t.Fatalf("SaveSnapshot() error: %v", err)
	}
	if err := log.Compact(5); err != nil {
		t.Fatalf("Compact() error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpen(t, dir, Options{RotateSize: 1})
	if got, err := reopened.Snapshot(); err != nil || got != meta {
		t.Fatalf("Snapshot() = %+v,%v, want %+v", got, err, meta)
	}
	if got, err := reopened.Compacted(); err != nil || got != (raft.SnapshotMeta{Index: 5, Term: 2}) {
		t.Fatalf("Compacted() = %+v,%v, want 5/2", got, err)
	}
	if reopened.FirstIndex() != 6 || reopened.LastIndex() != 8 {
		t.Fatalf("indexes = %d..%d, want 6..8", reopened.FirstIndex(), reopened.LastIndex())
	}
	got, err := reopened.Entries(6, 9)
	if err != nil {
		t.Fatal(err)
	}
	want := []raftpb.Entry{
		{Index: 6, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte{6}},
		{Index: 7, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte{7}},
		{Index: 8, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte{8}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retained overlap = %#v, want %#v", got, want)
	}
	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) > 4 {
		t.Fatalf("segment count = %d (%v), want bounded by overlap plus checkpoint", len(seqs), seqs)
	}
}

func TestRepeatedSnapshotCompactionKeepsSegmentCountBounded(t *testing.T) {
	const (
		cycles    = 12
		perCycle  = 25
		tail      = 7
		maxSteady = tail + 3 // retained entry segments, checkpoint, and one conservative boundary segment
	)
	dir := t.TempDir()
	log := mustOpen(t, dir, Options{RotateSize: 1})
	var peakAfterCompact int
	for cycle := uint64(1); cycle <= cycles; cycle++ {
		first := (cycle-1)*perCycle + 1
		last := cycle * perCycle
		for index := first; index <= last; index++ {
			mustSave(t, log, nil, []raftpb.Entry{{Index: index, Term: cycle, Type: raftpb.EntryType_NORMAL, Data: []byte{byte(index)}}})
		}
		meta := raft.SnapshotMeta{Index: last, Term: cycle}
		if err := log.SaveSnapshot(meta); err != nil {
			t.Fatalf("cycle %d SaveSnapshot(): %v", cycle, err)
		}
		if err := log.Compact(last - tail); err != nil {
			t.Fatalf("cycle %d Compact(): %v", cycle, err)
		}
		seqs, err := listSegments(dir)
		if err != nil {
			t.Fatal(err)
		}
		peakAfterCompact = max(peakAfterCompact, len(seqs))
		if len(seqs) > maxSteady {
			t.Fatalf("cycle %d retained %d segments (%v), want at most %d", cycle, len(seqs), seqs, maxSteady)
		}
		if got := log.FirstIndex(); got != last-tail+1 {
			t.Fatalf("cycle %d FirstIndex() = %d, want %d", cycle, got, last-tail+1)
		}
	}
	if peakAfterCompact < tail {
		t.Fatalf("peak retained segment count = %d, want evidence of a nontrivial oscillating tail", peakAfterCompact)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := mustOpen(t, dir, Options{RotateSize: 1})
	wantSnapshot := raft.SnapshotMeta{Index: cycles * perCycle, Term: cycles}
	if got, err := reopened.Snapshot(); err != nil || got != wantSnapshot {
		t.Fatalf("reopened Snapshot() = %+v,%v, want %+v", got, err, wantSnapshot)
	}
	if got, err := reopened.Compacted(); err != nil || got.Index != wantSnapshot.Index-tail {
		t.Fatalf("reopened Compacted() = %+v,%v, want index %d", got, err, wantSnapshot.Index-tail)
	}
}

func TestInstalledSnapshotDiscardsConflictingSuffixDurably(t *testing.T) {
	dir := t.TempDir()
	log := mustOpen(t, dir, Options{RotateSize: 1})
	for index := uint64(1); index <= 6; index++ {
		mustSave(t, log, nil, []raftpb.Entry{{Index: index, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("old")}})
	}
	installed := raft.SnapshotMeta{Index: 3, Term: 2}
	if err := log.SaveSnapshot(installed); err != nil {
		t.Fatalf("SaveSnapshot(installed) error: %v", err)
	}
	if log.FirstIndex() != 4 || log.LastIndex() != 3 {
		t.Fatalf("installed empty tail indexes = %d..%d, want 4..3", log.FirstIndex(), log.LastIndex())
	}
	if err := log.Save(nil, []raftpb.Entry{{Index: 4, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("new")}}); err != nil {
		t.Fatalf("append new suffix: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := mustOpen(t, dir, Options{RotateSize: 1})
	got, err := reopened.Entries(4, 5)
	if err != nil {
		t.Fatal(err)
	}
	want := []raftpb.Entry{{Index: 4, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("new")}}
	if !reflect.DeepEqual(got, want) || reopened.LastIndex() != 4 {
		t.Fatalf("recovered suffix = %#v last=%d, want only %#v", got, reopened.LastIndex(), want)
	}
}

func TestInstalledSnapshotCompactionReclaimsConflictingSameSegmentBytes(t *testing.T) {
	dir := t.TempDir()
	log := mustOpen(t, dir, Options{RotateSize: 1 << 20})
	entries := make([]raftpb.Entry, 64)
	for i := range entries {
		entries[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1, Type: raftpb.EntryType_NORMAL, Data: make([]byte, 1024)}
	}
	mustSave(t, log, &raft.HardState{Term: 2, VotedFor: 1}, entries)
	before := fileSize(t, filepath.Join(dir, segmentName(1)))
	installed := raft.SnapshotMeta{Index: 8, Term: 2}
	if err := log.SaveSnapshot(installed); err != nil {
		t.Fatal(err)
	}
	if err := log.Compact(installed.Index); err != nil {
		t.Fatal(err)
	}
	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 1 || seqs[0] == 1 {
		t.Fatalf("segments after installed compaction = %v, want only fresh checkpoint survivor", seqs)
	}
	after := fileSize(t, filepath.Join(dir, segmentName(seqs[0])))
	if after >= before/4 {
		t.Fatalf("checkpoint segment bytes = %d, old conflicting segment = %d; stale suffix was not reclaimed", after, before)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := mustOpen(t, dir, Options{})
	if reopened.FirstIndex() != installed.Index+1 || reopened.LastIndex() != installed.Index {
		t.Fatalf("reopened indexes = %d..%d, want empty installed tail %d..%d", reopened.FirstIndex(), reopened.LastIndex(), installed.Index+1, installed.Index)
	}
}

func TestCompactionSegmentFloorCompletesInterruptedPrefixCleanup(t *testing.T) {
	dir := t.TempDir()
	log := mustOpen(t, dir, Options{RotateSize: 1})
	for index := uint64(1); index <= 5; index++ {
		mustSave(t, log, nil, []raftpb.Entry{{Index: index, Term: 1, Type: raftpb.EntryType_NORMAL}})
	}
	backups := make(map[uint64][]byte)
	for seq := uint64(1); seq <= 3; seq++ {
		data, err := os.ReadFile(filepath.Join(dir, segmentName(seq)))
		if err != nil {
			t.Fatal(err)
		}
		backups[seq] = data
	}
	if err := log.SaveSnapshot(raft.SnapshotMeta{Index: 5, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := log.Compact(3); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	// Recreate the already-retired physical prefix, the namespace shape a
	// crash can leave after metadata publication but before all unlinks sync.
	for seq, data := range backups {
		if err := os.WriteFile(filepath.Join(dir, segmentName(seq)), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	reopened := mustOpen(t, dir, Options{RotateSize: 1})
	if reopened.FirstIndex() != 4 || reopened.LastIndex() != 5 {
		t.Fatalf("indexes after cleanup = %d..%d, want 4..5", reopened.FirstIndex(), reopened.LastIndex())
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := os.Stat(filepath.Join(dir, segmentName(seq))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired segment %s survived recovery: %v", segmentName(seq), err)
		}
	}
}

func TestMissingDeclaredSegmentFloorIsCorruption(t *testing.T) {
	dir := t.TempDir()
	log := mustOpen(t, dir, Options{RotateSize: 1})
	for index := uint64(1); index <= 5; index++ {
		mustSave(t, log, nil, []raftpb.Entry{{Index: index, Term: 1, Type: raftpb.EntryType_NORMAL}})
	}
	if err := log.SaveSnapshot(raft.SnapshotMeta{Index: 5, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := log.Compact(3); err != nil {
		t.Fatal(err)
	}
	floor := log.segmentFloor
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, segmentName(floor))); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{RotateSize: 1})
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "segment sequence gap") {
		t.Fatalf("Open() error = %v, want missing floor corruption", err)
	}
}

func TestCompactRefusesBeyondSnapshot(t *testing.T) {
	log := mustOpen(t, t.TempDir(), Options{})
	mustSave(t, log, nil, []raftpb.Entry{{Index: 1, Term: 1}})
	if err := log.Compact(1); !errors.Is(err, storage.ErrCompactionBeyondSnapshot) {
		t.Fatalf("Compact(1) error = %v, want ErrCompactionBeyondSnapshot", err)
	}
}
