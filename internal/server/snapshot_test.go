package server

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/lsm"
	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine/mapsm"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

func TestSnapshotManagerPostApplyTriggerRetainsTailAndRestoresDedup(t *testing.T) {
	root := filepath.Join(t.TempDir(), "snapshots")
	store := storage.NewMemStorage()
	sm := mapsm.New()
	core := &recordingRaftCompactor{}
	manager, err := NewSnapshotManager(SnapshotConfig{
		Dir: root, EntryThreshold: 2, ByteThreshold: math.MaxUint64, TailEntries: 1,
	}, store, sm, core)
	if err != nil {
		t.Fatal(err)
	}
	entries := []raftpb.Entry{
		snapshotCommandEntry(t, 1, 2, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("old")}),
		snapshotCommandEntry(t, 2, 2, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")}),
		snapshotCommandEntry(t, 3, 2, &raftpb.Command{ClientId: 3, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("new")}),
	}
	if err := store.Save(&raft.HardState{Term: 2}, entries); err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		if _, err := sm.Apply(&entries[i]); err != nil {
			t.Fatal(err)
		}
		if err := manager.ObserveApplied(entries[i : i+1]); err != nil {
			t.Fatalf("ObserveApplied(index=%d): %v", entries[i].Index, err)
		}
		if i == 1 {
			if got, _ := store.Snapshot(); got != (raft.SnapshotMeta{}) {
				t.Fatalf("entry trigger fired at threshold equality: %+v", got)
			}
		}
	}
	meta := raft.SnapshotMeta{Index: 3, Term: 2}
	if got, err := store.Snapshot(); err != nil || got != meta {
		t.Fatalf("Snapshot() = %+v,%v, want %+v", got, err, meta)
	}
	if got, err := store.Compacted(); err != nil || got != (raft.SnapshotMeta{Index: 2, Term: 2}) {
		t.Fatalf("Compacted() = %+v,%v, want 2/2", got, err)
	}
	if store.FirstIndex() != 3 || store.LastIndex() != 3 {
		t.Fatalf("retained tail = %d..%d, want index 3", store.FirstIndex(), store.LastIndex())
	}
	if !reflect.DeepEqual(core.metas, []raft.SnapshotMeta{{Index: 2, Term: 2}}) {
		t.Fatalf("live core compactions = %+v, want 2/2", core.metas)
	}

	wantHash := sm.Hash()
	restored := mapsm.New()
	if got, err := RecoverSnapshot(root, store, restored); err != nil || got != meta {
		t.Fatalf("RecoverSnapshot() = %+v,%v, want %+v", got, err, meta)
	}
	if got := restored.Hash(); got != wantHash {
		t.Fatalf("restored Hash() = %x, want %x", got, wantHash)
	}
	// Client 2's cached GET observed "old" at index 2. Retrying it after the
	// newer PUT proves the full dedup result survived the snapshot.
	retry := snapshotCommandEntry(t, 4, 2, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")})
	result, err := restored.Apply(&retry)
	if err != nil || !result.Found || string(result.Value) != "old" {
		t.Fatalf("dedup retry = %#v,%v, want cached old", result, err)
	}
	current, err := restored.Read([]byte("key"))
	if err != nil || !current.Found || string(current.Value) != "new" {
		t.Fatalf("current Read = %#v,%v, want new", current, err)
	}
}

func TestSnapshotManagerByteTriggerIsStrictlyGreaterThanThreshold(t *testing.T) {
	store := storage.NewMemStorage()
	sm := mapsm.New()
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryType_NOOP},
		{Index: 2, Term: 1, Type: raftpb.EntryType_NOOP},
	}
	if proto.Size(&entries[0]) != proto.Size(&entries[1]) {
		t.Fatal("test requires equal encoded entry sizes")
	}
	if err := store.Save(&raft.HardState{Term: 1}, entries); err != nil {
		t.Fatal(err)
	}
	manager, err := NewSnapshotManager(SnapshotConfig{
		Dir: filepath.Join(t.TempDir(), "snapshots"), EntryThreshold: math.MaxUint64,
		ByteThreshold: uint64(proto.Size(&entries[0])), TailEntries: 1,
	}, store, sm, &recordingRaftCompactor{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply(&entries[0]); err != nil {
		t.Fatal(err)
	}
	if err := manager.ObserveApplied(entries[:1]); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Snapshot(); got != (raft.SnapshotMeta{}) {
		t.Fatalf("byte trigger fired at threshold equality: %+v", got)
	}
	if _, err := sm.Apply(&entries[1]); err != nil {
		t.Fatal(err)
	}
	if err := manager.ObserveApplied(entries[1:]); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Snapshot(); got != (raft.SnapshotMeta{Index: 2, Term: 1}) {
		t.Fatalf("byte trigger after threshold = %+v, want 2/1", got)
	}
}

func TestSnapshotManagerCrashOrderRecoveryMatrix(t *testing.T) {
	crash := errors.New("simulated process crash")
	stages := []struct {
		name          string
		installHook   func(*SnapshotManager)
		wantSnapshot  raft.SnapshotMeta
		wantCompacted raft.SnapshotMeta
		wantRestore   bool
	}{
		{
			name: "after directory before metadata",
			installHook: func(m *SnapshotManager) {
				m.hooks.afterDirectory = func() error { return crash }
			},
		},
		{
			name: "after metadata before compact",
			installHook: func(m *SnapshotManager) {
				m.hooks.afterMetadata = func() error { return crash }
			},
			wantSnapshot: raft.SnapshotMeta{Index: 2, Term: 1}, wantRestore: true,
		},
		{
			name: "after compact",
			installHook: func(m *SnapshotManager) {
				m.hooks.afterCompact = func() error { return crash }
			},
			wantSnapshot:  raft.SnapshotMeta{Index: 2, Term: 1},
			wantCompacted: raft.SnapshotMeta{Index: 1, Term: 1}, wantRestore: true,
		},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "snapshots")
			store := storage.NewMemStorage()
			sm := mapsm.New()
			manager, err := NewSnapshotManager(SnapshotConfig{
				Dir: root, EntryThreshold: 1, ByteThreshold: math.MaxUint64, TailEntries: 1,
			}, store, sm, &recordingRaftCompactor{})
			if err != nil {
				t.Fatal(err)
			}
			stage.installHook(manager)
			entries := []raftpb.Entry{
				snapshotCommandEntry(t, 1, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("one")}),
				snapshotCommandEntry(t, 2, 1, &raftpb.Command{ClientId: 1, Seq: 2, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("two")}),
			}
			if err := store.Save(&raft.HardState{Term: 1}, entries); err != nil {
				t.Fatal(err)
			}
			for i := range entries {
				if _, err := sm.Apply(&entries[i]); err != nil {
					t.Fatal(err)
				}
				observeErr := manager.ObserveApplied(entries[i : i+1])
				if i == len(entries)-1 {
					if !errors.Is(observeErr, crash) {
						t.Fatalf("trigger error = %v, want simulated crash", observeErr)
					}
				} else if observeErr != nil {
					t.Fatalf("pre-trigger ObserveApplied() error: %v", observeErr)
				}
			}
			if got, _ := store.Snapshot(); got != stage.wantSnapshot {
				t.Fatalf("Snapshot() = %+v, want %+v", got, stage.wantSnapshot)
			}
			if got, _ := store.Compacted(); got != stage.wantCompacted {
				t.Fatalf("Compacted() = %+v, want %+v", got, stage.wantCompacted)
			}
			recovered := mapsm.New()
			gotMeta, recoverErr := RecoverSnapshot(root, store, recovered)
			if recoverErr != nil || gotMeta != stage.wantSnapshot {
				t.Fatalf("RecoverSnapshot() = %+v,%v, want %+v", gotMeta, recoverErr, stage.wantSnapshot)
			}
			if stage.wantRestore {
				value, err := recovered.Read([]byte("key"))
				if err != nil || !value.Found || string(value.Value) != "two" {
					t.Fatalf("restored value = %#v,%v, want two", value, err)
				}
			} else {
				names, err := os.ReadDir(root)
				if err != nil || len(names) != 0 {
					t.Fatalf("orphan cleanup entries = %v,%v, want empty", names, err)
				}
			}
		})
	}
}

func TestSnapshotManagerDefersReplayBehindOrphanLSMWatermark(t *testing.T) {
	root := t.TempDir()
	engineDir := filepath.Join(root, "lsm")
	snapshotRoot := filepath.Join(root, "snapshots")
	options := lsm.Options{
		Rand: snapshotLSMRand{}, FlushThreshold: 1 << 30, DisableAutoCompaction: true,
	}
	if err := os.MkdirAll(engineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store := storage.NewMemStorage()
	entries := []raftpb.Entry{
		snapshotCommandEntry(t, 1, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("old")}),
		snapshotCommandEntry(t, 2, 1, &raftpb.Command{ClientId: 2, Seq: 7, Op: raftpb.Op_GET, Key: []byte("key")}),
		snapshotCommandEntry(t, 3, 1, &raftpb.Command{ClientId: 1, Seq: 2, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("new")}),
		{Index: 4, Term: 1, Type: raftpb.EntryType_NOOP},
	}
	if err := store.Save(&raft.HardState{Term: 1}, entries); err != nil {
		t.Fatal(err)
	}

	beforeCrash, err := lsm.OpenStateMachine(engineDir, options)
	if err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		if _, err := beforeCrash.Apply(&entries[i]); err != nil {
			t.Fatal(err)
		}
	}
	wantHash := beforeCrash.Hash()
	newer := raft.SnapshotMeta{Index: 4, Term: 1}
	orphan := filepath.Join(snapshotRoot, snapshotDirectoryName(newer))
	if err := beforeCrash.CreateSnapshot(orphan, newer); err != nil {
		t.Fatal(err)
	}
	if got := beforeCrash.DurableIndex(); got != newer.Index {
		t.Fatalf("orphan force-flush watermark = %d, want %d", got, newer.Index)
	}
	// Model the crash window before store.SaveSnapshot: the LSM MANIFEST is
	// through F=4, while Raft still names the old zero snapshot.
	if err := beforeCrash.Close(); err != nil {
		t.Fatal(err)
	}

	afterCrash, err := lsm.OpenStateMachine(engineDir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := afterCrash.Close(); err != nil {
			t.Errorf("close replayed LSM: %v", err)
		}
	}()
	if meta, err := RecoverSnapshot(snapshotRoot, store, afterCrash); err != nil || meta != (raft.SnapshotMeta{}) {
		t.Fatalf("RecoverSnapshot() = %+v,%v, want old zero snapshot", meta, err)
	}
	if names, err := os.ReadDir(snapshotRoot); err != nil || len(names) != 0 {
		t.Fatalf("orphan snapshot cleanup = %v,%v, want empty root", names, err)
	}
	if err := afterCrash.BeginReplay(1, newer.Index); err != nil {
		t.Fatal(err)
	}
	core := &recordingRaftCompactor{}
	manager, err := NewSnapshotManager(SnapshotConfig{
		Dir: snapshotRoot, EntryThreshold: 1, ByteThreshold: math.MaxUint64, TailEntries: 1,
	}, store, afterCrash, core)
	if err != nil {
		t.Fatal(err)
	}

	for i := range entries {
		if _, err := afterCrash.Apply(&entries[i]); err != nil {
			t.Fatalf("replay index %d: %v", entries[i].Index, err)
		}
		if err := manager.ObserveApplied(entries[i : i+1]); err != nil {
			t.Fatalf("observe replay index %d: %v", entries[i].Index, err)
		}
		if entries[i].Index == 2 {
			if got, _ := store.Snapshot(); got != (raft.SnapshotMeta{}) {
				t.Fatalf("snapshot published at replay threshold below durable watermark: %+v", got)
			}
		}
	}
	if got, err := store.Snapshot(); err != nil || got != newer {
		t.Fatalf("Snapshot() after replay catch-up = %+v,%v, want %+v", got, err, newer)
	}
	if got, err := store.Compacted(); err != nil || got != (raft.SnapshotMeta{Index: 3, Term: 1}) {
		t.Fatalf("Compacted() after replay catch-up = %+v,%v, want 3/1", got, err)
	}
	if !reflect.DeepEqual(core.metas, []raft.SnapshotMeta{{Index: 3, Term: 1}}) {
		t.Fatalf("live core compactions = %+v, want 3/1", core.metas)
	}
	if got := afterCrash.Hash(); got != wantHash {
		t.Fatalf("replayed/snapshotted Hash() = %x, want %x", got, wantHash)
	}

	// Client 2's replayed GET cached "old" before the later PUT. Its exact
	// retry proves dedup state stayed intact across the orphan cleanup, replay,
	// and eventual replacement snapshot.
	retry := snapshotCommandEntry(t, 5, 1, &raftpb.Command{ClientId: 2, Seq: 7, Op: raftpb.Op_GET, Key: []byte("key")})
	result, err := afterCrash.Apply(&retry)
	if err != nil || !result.Found || string(result.Value) != "old" {
		t.Fatalf("dedup retry after replay snapshot = %#v,%v, want cached old", result, err)
	}
	current, err := afterCrash.Read([]byte("key"))
	if err != nil || !current.Found || string(current.Value) != "new" {
		t.Fatalf("current value after dedup retry = %#v,%v, want new", current, err)
	}
}

func TestSnapshotManagerRejectsBoundaryTermMismatchBeforePublication(t *testing.T) {
	store := storage.NewMemStorage()
	if err := store.Save(nil, []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("durable")}}); err != nil {
		t.Fatal(err)
	}
	sm := mapsm.New()
	manager, err := NewSnapshotManager(SnapshotConfig{
		Dir: filepath.Join(t.TempDir(), "snapshots"), EntryThreshold: math.MaxUint64, ByteThreshold: 1, TailEntries: 1,
	}, store, sm, &recordingRaftCompactor{})
	if err != nil {
		t.Fatal(err)
	}
	entries := []raftpb.Entry{snapshotCommandEntry(t, 1, 2, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("value")})}
	if _, err := sm.Apply(&entries[0]); err != nil {
		t.Fatal(err)
	}
	if err := manager.ObserveApplied(entries); err == nil {
		t.Fatal("ObserveApplied() error = nil, want boundary mismatch")
	}
	if got, _ := store.Snapshot(); got != (raft.SnapshotMeta{}) {
		t.Fatalf("Snapshot() = %+v, want unpublished zero metadata", got)
	}
}

type fixedWatermark uint64

func (w fixedWatermark) DurableIndex() uint64 { return uint64(w) }

type snapshotLSMRand struct{}

func (snapshotLSMRand) IntN(int) int { return 0 }

func TestCompactThroughRefusesBeyondDurableWatermark(t *testing.T) {
	store := storage.NewMemStorage()
	entries := []raftpb.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	if err := store.Save(nil, entries); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(raft.SnapshotMeta{Index: 5, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := CompactThrough(store, fixedWatermark(4), 5); err == nil || !strings.Contains(err.Error(), "engine durable watermark 4") {
		t.Fatalf("CompactThrough() error = %v, want engine watermark refusal", err)
	}
	if got, _ := store.Compacted(); got != (raft.SnapshotMeta{}) {
		t.Fatalf("storage mutated on refusal: %+v", got)
	}
}

func snapshotCommandEntry(t *testing.T, index, term uint64, command *raftpb.Command) raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return raftpb.Entry{Index: index, Term: term, Type: raftpb.EntryType_NORMAL, Data: data}
}

type recordingRaftCompactor struct {
	metas []raft.SnapshotMeta
	err   error
}

func (c *recordingRaftCompactor) Compact(meta raft.SnapshotMeta) error {
	c.metas = append(c.metas, meta)
	return c.err
}
