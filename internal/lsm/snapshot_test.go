package lsm

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestLSMSnapshotRoundTripPreservesHashDedupAndImmutableManifest(t *testing.T) {
	fs := NewSimFS()
	source, err := OpenStateMachine("/source", Options{FS: fs, Rand: newSeededRand(60_001), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	entries := []*raftpb.Entry{
		stateMachineEntry(t, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("old")}),
		stateMachineEntry(t, 2, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")}),
		stateMachineEntry(t, 3, &raftpb.Command{ClientId: 3, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("new")}),
	}
	for _, entry := range entries {
		if _, err := source.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	meta := raft.SnapshotMeta{Index: 3, Term: 7}
	wantHash := source.Hash()
	const snapshotDir = "/snapshots/snapshot-3"
	if err := source.CreateSnapshot(snapshotDir, meta); err != nil {
		t.Fatalf("CreateSnapshot(): %v", err)
	}
	if got := source.DurableIndex(); got != meta.Index {
		t.Fatalf("DurableIndex() = %d, want %d", got, meta.Index)
	}
	snapshotManifest, err := fs.LiveBytes(filepath.Join(snapshotDir, manifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	// Mutating and flushing the live engine must not mutate the copied
	// snapshot MANIFEST (SSTables themselves are immutable hard links).
	if _, err := source.Apply(stateMachineEntry(t, 4, &raftpb.Command{ClientId: 4, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("other"), Value: []byte("later")})); err != nil {
		t.Fatal(err)
	}
	if err := source.Engine().ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if got, err := fs.LiveBytes(filepath.Join(snapshotDir, manifestFilename)); err != nil || !bytes.Equal(got, snapshotManifest) {
		t.Fatalf("snapshot MANIFEST changed after live flush: equal=%v err=%v", bytes.Equal(got, snapshotManifest), err)
	}

	target, err := OpenStateMachine("/target", Options{FS: fs, Rand: newSeededRand(60_002), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	restoredMeta, err := target.RestoreSnapshot(snapshotDir)
	if err != nil || restoredMeta != meta {
		t.Fatalf("RestoreSnapshot() = %+v,%v, want %+v", restoredMeta, err, meta)
	}
	if got := target.Hash(); got != wantHash {
		t.Fatalf("restored Hash() = %x, want %x", got, wantHash)
	}
	retry := stateMachineEntry(t, 4, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")})
	result, err := target.Apply(retry)
	if err != nil || !result.Found || string(result.Value) != "old" {
		t.Fatalf("dedup retry = %#v,%v, want cached old", result, err)
	}
	current, err := target.Read([]byte("key"))
	if err != nil || !current.Found || string(current.Value) != "new" {
		t.Fatalf("current value = %#v,%v, want new", current, err)
	}

	// A second generation proves adopted filenames remain bounded instead of
	// recursively embedding prior names.
	if err := target.CreateSnapshot("/snapshots/snapshot-4", raft.SnapshotMeta{Index: 4, Term: 7}); err != nil {
		t.Fatal(err)
	}
	target2, err := OpenStateMachine("/target2", Options{FS: fs, Rand: newSeededRand(60_003)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target2.RestoreSnapshot("/snapshots/snapshot-4"); err != nil {
		t.Fatal(err)
	}
	for _, name := range target2.Engine().manifestFileNamesForTest() {
		if strings.HasSuffix(name, sstableFilenameSuffix) && len(name) > 96 {
			t.Fatalf("adopted SSTable filename grew unbounded: %q (%d bytes)", name, len(name))
		}
	}
	if err := target2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotValidationCloseFailureRetainsHandleForCloseRetry(t *testing.T) {
	fs := NewSimFS()
	source, snapshotDir := buildOneTableSnapshot(t, fs, "/close-source", "/snapshots/close")
	defer func() { _ = source.Close() }()
	state, err := loadManifest(fs, snapshotDir)
	if err != nil {
		t.Fatal(err)
	}
	target, err := OpenStateMachine("/close-target", Options{FS: fs, Rand: newSeededRand(60_011)})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected validation close failure")
	fs.ResetEvents()
	if err := fs.SetErrorSchedule([]SimErrorDirective{{Op: FSOpClose, Occurrence: 1, Point: SimBeforeOperation, Err: boom}}); err != nil {
		t.Fatal(err)
	}
	target.engine.mu.Lock()
	err = validateSnapshotSSTables(target.engine, snapshotDir, state)
	target.engine.mu.Unlock()
	if !errors.Is(err, boom) {
		t.Fatalf("validateSnapshotSSTables() error = %v, want %v", err, boom)
	}
	if got := len(target.engine.unresolved); got != 1 {
		t.Fatalf("unresolved handles = %d, want validation handle retained", got)
	}
	if err := fs.SetErrorSchedule(nil); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("Close() retry: %v", err)
	}
	if got := len(target.engine.unresolved); got != 0 {
		t.Fatalf("Close() retained %d unresolved handles", got)
	}
}

func TestSnapshotMetadataReadCloseFailureRetainsHandleForCloseRetry(t *testing.T) {
	fs := NewSimFS()
	source, snapshotDir := buildOneTableSnapshot(t, fs, "/meta-close-source", "/snapshots/meta-close")
	defer func() { _ = source.Close() }()
	target, err := OpenStateMachine("/meta-close-target", Options{FS: fs, Rand: newSeededRand(60_016)})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected metadata close failure")
	fs.ResetEvents()
	if err := fs.SetErrorSchedule([]SimErrorDirective{{Op: FSOpClose, Occurrence: 1, Point: SimBeforeOperation, Err: boom}}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.RestoreSnapshot(snapshotDir); !errors.Is(err, boom) {
		t.Fatalf("RestoreSnapshot() error = %v, want %v", err, boom)
	}
	if got := len(target.engine.unresolved); got != 1 {
		t.Fatalf("unresolved handles = %d, want metadata handle retained", got)
	}
	if err := fs.SetErrorSchedule(nil); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("Close() retry: %v", err)
	}
	if got := len(target.engine.unresolved); got != 0 {
		t.Fatalf("Close() retained %d unresolved handles", got)
	}
}

func TestSnapshotPostPublicationFailureFailStopsReadsAndRetainsOwnership(t *testing.T) {
	fs := NewSimFS()
	source, snapshotDir := buildOneTableSnapshot(t, fs, "/poison-source", "/snapshots/poison")
	defer func() { _ = source.Close() }()
	target, err := OpenStateMachine("/poison-target", Options{FS: fs, Rand: newSeededRand(60_021)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Apply(stateMachineEntry(t, 1, &raftpb.Command{ClientId: 9, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("old"), Value: []byte("state")})); err != nil {
		t.Fatal(err)
	}
	if err := target.Engine().ForceFlush(); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected post-publish directory sync failure")
	fs.ResetEvents()
	if err := fs.SetErrorSchedule([]SimErrorDirective{{Op: FSOpDirSync, Occurrence: 2, Point: SimBeforeOperation, Err: boom}}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.RestoreSnapshot(snapshotDir); !errors.Is(err, boom) {
		t.Fatalf("RestoreSnapshot() error = %v, want %v", err, boom)
	}
	if _, err := target.Read([]byte("key")); err == nil || !strings.Contains(err.Error(), "snapshot adoption") {
		t.Fatalf("Read() error = %v, want snapshot-adoption fail-stop", err)
	}
	if got := len(target.engine.tables) + len(target.engine.unresolved); got == 0 {
		t.Fatal("post-publication failure lost old table-handle ownership")
	}
	if err := fs.SetErrorSchedule(nil); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestRestoreSnapshotRejectsChecksumCorruptionBeforeAdoption(t *testing.T) {
	fs := NewSimFS()
	source, snapshotDir := buildOneTableSnapshot(t, fs, "/checksum-source", "/snapshots/checksum")
	defer func() { _ = source.Close() }()
	names, err := fs.List(snapshotDir)
	if err != nil {
		t.Fatal(err)
	}
	var tablePath string
	for _, name := range names {
		if strings.HasSuffix(name, sstableFilenameSuffix) {
			tablePath = filepath.Join(snapshotDir, name)
			break
		}
	}
	if tablePath == "" {
		t.Fatal("snapshot contains no SSTable")
	}
	file, err := fs.Open(tablePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	target, err := OpenStateMachine("/checksum-target", Options{FS: fs, Rand: newSeededRand(60_031)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if _, err := target.RestoreSnapshot(snapshotDir); err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Fatalf("RestoreSnapshot() error = %v, want checksum refusal", err)
	}
	if got := target.AppliedIndex(); got != 0 {
		t.Fatalf("target applied index = %d after rejected snapshot, want 0", got)
	}
}

func TestSimFSParentSyncMakesRecursiveSnapshotRemovalDurable(t *testing.T) {
	fs := NewSimFS()
	const (
		root = "/snapshots"
		dir  = "/snapshots/snapshot-1"
		file = "/snapshots/snapshot-1/META.pb"
	)
	if err := fs.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	handle, err := fs.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	if err := handle.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(root); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := fs.SyncDir(root); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(RetainAllUnsynced); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.LiveBytes(file); !errors.Is(err, ErrNotExist) {
		t.Fatalf("removed nested file resurrected after crash: %v", err)
	}
	if names, err := fs.List(root); err != nil || len(names) != 0 {
		t.Fatalf("snapshot root after recover = %v,%v, want empty", names, err)
	}
}

func buildOneTableSnapshot(t *testing.T, fs *SimFS, engineDir, snapshotDir string) (*StateMachine, string) {
	t.Helper()
	sm, err := OpenStateMachine(engineDir, Options{FS: fs, Rand: newSeededRand(60_100), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply(stateMachineEntry(t, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("value")})); err != nil {
		t.Fatal(err)
	}
	if err := sm.CreateSnapshot(snapshotDir, raft.SnapshotMeta{Index: 1, Term: 1}); err != nil {
		t.Fatal(err)
	}
	return sm, snapshotDir
}

func (engine *Engine) manifestFileNamesForTest() []string {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	names := make([]string, 0, len(engine.manifest.files))
	for name := range engine.manifest.files {
		names = append(names, name)
	}
	return names
}
