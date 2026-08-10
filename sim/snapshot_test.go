package sim

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/lsm"
	"miniquorum/internal/raft"
	"miniquorum/internal/server"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	raftpb "miniquorum/proto"
)

var errScriptedSnapshotCrash = errors.New("sim: scripted snapshot crash")

type snapshotCrashStage uint8

const (
	crashAfterSnapshotDirectory snapshotCrashStage = iota + 1
	crashAfterSnapshotMetadata
	crashAfterSnapshotCompaction
)

type scriptedSnapshotStorage struct {
	*CrashStorage
	stage snapshotCrashStage
}

func (s *scriptedSnapshotStorage) SaveSnapshot(meta raft.SnapshotMeta) error {
	if s.stage == crashAfterSnapshotDirectory {
		return errScriptedSnapshotCrash
	}
	if err := s.CrashStorage.SaveSnapshot(meta); err != nil {
		return err
	}
	if s.stage == crashAfterSnapshotMetadata {
		return errScriptedSnapshotCrash
	}
	return nil
}

func (s *scriptedSnapshotStorage) Compact(index uint64) error {
	if err := s.CrashStorage.Compact(index); err != nil {
		return err
	}
	if s.stage == crashAfterSnapshotCompaction {
		return errScriptedSnapshotCrash
	}
	return nil
}

type snapshotMatrixCompactor struct {
	calls []raft.SnapshotMeta
}

func (c *snapshotMatrixCompactor) Compact(meta raft.SnapshotMeta) error {
	c.calls = append(c.calls, meta)
	return nil
}

type snapshotMatrixEngine struct {
	stateMachine statemachine.StateMachine
	snapshotRoot string
	crash        func()
	reopen       func() statemachine.StateMachine
	listRoot     func() []string
}

func TestSnapshotTruncateRestartMatrixBothEnginesWithScriptedCrashStorage(t *testing.T) {
	stages := []struct {
		name          string
		stage         snapshotCrashStage
		wantSnapshot  raft.SnapshotMeta
		wantCompacted raft.SnapshotMeta
	}{
		{name: "after snapshot before metadata", stage: crashAfterSnapshotDirectory},
		{name: "after metadata before truncate", stage: crashAfterSnapshotMetadata, wantSnapshot: raft.SnapshotMeta{Index: 3, Term: 1}},
		{name: "after truncate", stage: crashAfterSnapshotCompaction, wantSnapshot: raft.SnapshotMeta{Index: 3, Term: 1}, wantCompacted: raft.SnapshotMeta{Index: 2, Term: 1}},
	}
	for _, engineName := range []string{"map", "lsm"} {
		for _, stage := range stages {
			t.Run(engineName+"/"+stage.name, func(t *testing.T) {
				engine := newSnapshotMatrixEngine(t, engineName)
				store, err := NewCrashStorage(1, FaultSchedule{})
				if err != nil {
					t.Fatal(err)
				}
				wrapped := &scriptedSnapshotStorage{CrashStorage: store, stage: stage.stage}
				compactor := &snapshotMatrixCompactor{}
				manager, err := server.NewSnapshotManager(server.SnapshotConfig{
					Dir: engine.snapshotRoot, EntryThreshold: 2, ByteThreshold: math.MaxUint64, TailEntries: 1,
				}, wrapped, engine.stateMachine, compactor)
				if err != nil {
					t.Fatal(err)
				}
				entries := snapshotMatrixEntries(t)
				if err := store.Save(&raft.HardState{Term: 1}, entries); err != nil {
					t.Fatal(err)
				}
				for i := range entries {
					if _, err := engine.stateMachine.Apply(&entries[i]); err != nil {
						t.Fatalf("apply index %d: %v", entries[i].Index, err)
					}
				}
				wantHash := engine.stateMachine.Hash()
				if err := manager.ObserveApplied(entries); !errors.Is(err, errScriptedSnapshotCrash) {
					t.Fatalf("ObserveApplied() error = %v, want scripted crash", err)
				}
				if len(compactor.calls) != 0 {
					t.Fatalf("live Raft compactor ran after crash stage: %+v", compactor.calls)
				}

				engine.crash()
				if err := store.Crash(); err != nil {
					t.Fatal(err)
				}
				if err := store.Recover(); err != nil {
					t.Fatal(err)
				}
				if got, err := store.Snapshot(); err != nil || got != stage.wantSnapshot {
					t.Fatalf("recovered Snapshot() = %+v,%v, want %+v", got, err, stage.wantSnapshot)
				}
				if got, err := store.Compacted(); err != nil || got != stage.wantCompacted {
					t.Fatalf("recovered Compacted() = %+v,%v, want %+v", got, err, stage.wantCompacted)
				}

				restored := engine.reopen()
				defer closeSnapshotMatrixStateMachine(t, restored)
				meta, err := server.RecoverSnapshot(engine.snapshotRoot, store, restored)
				if err != nil || meta != stage.wantSnapshot {
					t.Fatalf("RecoverSnapshot() = %+v,%v, want %+v", meta, err, stage.wantSnapshot)
				}
				replaySnapshotSuffix(t, restored, store, meta.Index+1)
				if got := restored.Hash(); got != wantHash {
					t.Fatalf("restored Hash() = %x, want %x", got, wantHash)
				}
				value, err := restored.Read([]byte("key"))
				if err != nil || !value.Found || string(value.Value) != "new" {
					t.Fatalf("restored Read(key) = %#v,%v, want new", value, err)
				}
				names := engine.listRoot()
				if stage.wantSnapshot == (raft.SnapshotMeta{}) {
					if len(names) != 0 {
						t.Fatalf("orphan snapshot directory survived recovery: %v", names)
					}
				} else if want := snapshotMatrixDirectoryName(stage.wantSnapshot); len(names) != 1 || names[0] != want {
					t.Fatalf("snapshot root = %v, want only %s", names, want)
				}
			})
		}
	}
}

func TestLSMScriptedCrashDuringSnapshotWriteReplaysWithoutLossOrOrphan(t *testing.T) {
	fs := lsm.NewSimFS()
	const (
		engineDir   = "/phase6-write-crash-engine"
		snapshotDir = "/phase6-write-crash-snapshots"
	)
	open := func() *lsm.StateMachine {
		sm, err := lsm.OpenStateMachine(engineDir, lsm.Options{
			FS: fs, Rand: newSeededLSMRand(66_001), FlushThreshold: 1 << 30, DisableAutoCompaction: true,
		})
		if err != nil {
			t.Fatalf("open LSM state machine: %v", err)
		}
		return sm
	}
	sm := open()
	store, err := NewCrashStorage(1, FaultSchedule{})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := server.NewSnapshotManager(server.SnapshotConfig{
		Dir: snapshotDir, EntryThreshold: 2, ByteThreshold: math.MaxUint64, TailEntries: 1,
	}, store, sm, &snapshotMatrixCompactor{})
	if err != nil {
		t.Fatal(err)
	}
	entries := snapshotMatrixEntries(t)
	if err := store.Save(&raft.HardState{Term: 1}, entries); err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		if _, err := sm.Apply(&entries[i]); err != nil {
			t.Fatal(err)
		}
	}
	wantHash := sm.Hash()
	if err := fs.SetCrashSchedule([]lsm.SimCrashDirective{{
		Op: lsm.FSOpLink, Occurrence: 1, Point: lsm.SimAfterOperation, RetainUnsynced: 0,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ObserveApplied(entries); !errors.Is(err, lsm.ErrSimulatedCrash) {
		t.Fatalf("ObserveApplied() error = %v, want SimFS crash during snapshot write", err)
	}
	if !fs.Crashed() {
		t.Fatal("SimFS did not enter crashed state")
	}
	events := fs.Events()
	if len(events) == 0 || events[len(events)-1].Op != lsm.FSOpLink || !events[len(events)-1].Completed {
		t.Fatalf("last SimFS event = %+v, want completed snapshot hard-link crash point", events)
	}
	if got, err := store.Snapshot(); err != nil || got != (raft.SnapshotMeta{}) {
		t.Fatalf("Snapshot() after interrupted write = %+v,%v, want unpublished zero", got, err)
	}
	if err := sm.Crash(); err != nil {
		t.Fatal(err)
	}
	if err := store.Crash(); err != nil {
		t.Fatal(err)
	}
	if err := store.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}
	restored := open()
	defer closeSnapshotMatrixStateMachine(t, restored)
	meta, err := server.RecoverSnapshot(snapshotDir, store, restored)
	if err != nil || meta != (raft.SnapshotMeta{}) {
		t.Fatalf("RecoverSnapshot() = %+v,%v, want zero", meta, err)
	}
	replaySnapshotSuffix(t, restored, store, 1)
	if got := restored.Hash(); got != wantHash {
		t.Fatalf("replayed Hash() = %x, want %x", got, wantHash)
	}
	if names, err := fs.List(snapshotDir); err != nil || len(names) != 0 {
		t.Fatalf("snapshot root after interrupted-write recovery = %v,%v, want empty", names, err)
	}
}

func TestSustainedSnapshottingBoundsSimLogBothEngines(t *testing.T) {
	const (
		snapshotEvery = uint64(6)
		tail          = uint64(3)
		entryCount    = uint64(60)
	)
	for _, engineName := range []string{"map", "lsm"} {
		t.Run(engineName, func(t *testing.T) {
			engine := newSnapshotMatrixEngine(t, engineName)
			store, err := NewCrashStorage(1, FaultSchedule{})
			if err != nil {
				t.Fatal(err)
			}
			compactor := &snapshotMatrixCompactor{}
			manager, err := server.NewSnapshotManager(server.SnapshotConfig{
				Dir: engine.snapshotRoot, EntryThreshold: snapshotEvery - 1,
				ByteThreshold: math.MaxUint64, TailEntries: tail,
			}, store, engine.stateMachine, compactor)
			if err != nil {
				t.Fatal(err)
			}
			for index := uint64(1); index <= entryCount; index++ {
				batch := snapshotMatrixPutBatch(t, index)
				if err := store.Save(&raft.HardState{Term: 1}, batch); err != nil {
					t.Fatalf("save index %d: %v", index, err)
				}
				if _, err := engine.stateMachine.Apply(&batch[0]); err != nil {
					t.Fatalf("apply index %d: %v", index, err)
				}
				if err := manager.ObserveApplied(batch); err != nil {
					t.Fatalf("observe index %d: %v", index, err)
				}
				if index%snapshotEvery == 0 {
					if got := store.LastIndex() - store.FirstIndex() + 1; got > tail {
						t.Fatalf("index %d retained logical tail %d, want at most %d", index, got, tail)
					}
				}
			}
			wantMeta := raft.SnapshotMeta{Index: entryCount, Term: 1}
			if got, err := store.Snapshot(); err != nil || got != wantMeta {
				t.Fatalf("Snapshot() = %+v,%v, want %+v", got, err, wantMeta)
			}
			wantBase := raft.SnapshotMeta{Index: entryCount - tail, Term: 1}
			if got, err := store.Compacted(); err != nil || got != wantBase {
				t.Fatalf("Compacted() = %+v,%v, want %+v", got, err, wantBase)
			}
			if got, want := len(compactor.calls), int(entryCount/snapshotEvery); got != want {
				t.Fatalf("live compaction calls = %d, want %d", got, want)
			}
			wantHash := engine.stateMachine.Hash()
			engine.crash()
			if err := store.Crash(); err != nil {
				t.Fatal(err)
			}
			if err := store.Recover(); err != nil {
				t.Fatal(err)
			}
			restored := engine.reopen()
			defer closeSnapshotMatrixStateMachine(t, restored)
			meta, err := server.RecoverSnapshot(engine.snapshotRoot, store, restored)
			if err != nil || meta != wantMeta {
				t.Fatalf("RecoverSnapshot() = %+v,%v, want %+v", meta, err, wantMeta)
			}
			if got := restored.Hash(); got != wantHash {
				t.Fatalf("restored Hash() = %x, want %x", got, wantHash)
			}
			if names := engine.listRoot(); len(names) != 1 || names[0] != snapshotMatrixDirectoryName(wantMeta) {
				t.Fatalf("snapshot root = %v, want only latest snapshot", names)
			}
		})
	}
}

func newSnapshotMatrixEngine(t *testing.T, name string) snapshotMatrixEngine {
	t.Helper()
	switch name {
	case "map":
		root := filepath.Join(t.TempDir(), "snapshots")
		return snapshotMatrixEngine{
			stateMachine: mapsm.New(),
			snapshotRoot: root,
			crash:        func() {},
			reopen:       func() statemachine.StateMachine { return mapsm.New() },
			listRoot: func() []string {
				entries, err := os.ReadDir(root)
				if err != nil {
					t.Fatal(err)
				}
				names := make([]string, 0, len(entries))
				for _, entry := range entries {
					names = append(names, entry.Name())
				}
				return names
			},
		}
	case "lsm":
		fs := lsm.NewSimFS()
		const (
			engineDir = "/phase6-matrix-engine"
			root      = "/phase6-matrix-snapshots"
		)
		open := func() *lsm.StateMachine {
			sm, err := lsm.OpenStateMachine(engineDir, lsm.Options{
				FS: fs, Rand: newSeededLSMRand(66_002), FlushThreshold: 1 << 30, DisableAutoCompaction: true,
			})
			if err != nil {
				t.Fatalf("open LSM state machine: %v", err)
			}
			return sm
		}
		sm := open()
		return snapshotMatrixEngine{
			stateMachine: sm,
			snapshotRoot: root,
			crash: func() {
				if err := sm.Crash(); err != nil {
					t.Fatal(err)
				}
			},
			reopen: func() statemachine.StateMachine {
				if err := fs.Recover(); err != nil {
					t.Fatal(err)
				}
				return open()
			},
			listRoot: func() []string {
				names, err := fs.List(root)
				if err != nil {
					t.Fatal(err)
				}
				return names
			},
		}
	default:
		t.Fatalf("unknown snapshot matrix engine %q", name)
		return snapshotMatrixEngine{}
	}
}

func snapshotMatrixEntries(t *testing.T) []raftpb.Entry {
	t.Helper()
	commands := []*raftpb.Command{
		{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("old")},
		{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")},
		{ClientId: 3, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("new")},
	}
	entries := make([]raftpb.Entry, len(commands))
	for i, command := range commands {
		data, err := proto.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = raftpb.Entry{Index: uint64(i + 1), Term: 1, Type: raftpb.EntryType_NORMAL, Data: data}
	}
	return entries
}

func snapshotMatrixPutBatch(t *testing.T, index uint64) []raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(&raftpb.Command{
		ClientId: index, Seq: 1, Op: raftpb.Op_PUT,
		Key: []byte(fmt.Sprintf("key-%03d", index%9)), Value: []byte(fmt.Sprintf("value-%03d", index)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return []raftpb.Entry{{Index: index, Term: 1, Type: raftpb.EntryType_NORMAL, Data: data}}
}

func replaySnapshotSuffix(t *testing.T, sm statemachine.StateMachine, store *CrashStorage, first uint64) {
	t.Helper()
	last := store.LastIndex()
	if replay, ok := sm.(interface{ BeginReplay(uint64, uint64) error }); ok {
		if err := replay.BeginReplay(first, last); err != nil {
			t.Fatalf("BeginReplay(%d,%d): %v", first, last, err)
		}
	}
	if last < first {
		return
	}
	entries, err := store.Entries(first, last+1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		if _, err := sm.Apply(&entries[i]); err != nil {
			t.Fatalf("replay index %d: %v", entries[i].Index, err)
		}
	}
}

func closeSnapshotMatrixStateMachine(t *testing.T, sm statemachine.StateMachine) {
	t.Helper()
	if closer, ok := sm.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			t.Errorf("close restored state machine: %v", err)
		}
	}
}

func snapshotMatrixDirectoryName(meta raft.SnapshotMeta) string {
	return fmt.Sprintf("snapshot-%020d-%020d", meta.Index, meta.Term)
}
