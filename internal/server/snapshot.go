package server

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

const (
	DefaultSnapshotEntryThreshold = uint64(10_000)
	DefaultSnapshotByteThreshold  = uint64(16 << 20)
	DefaultSnapshotTailEntries    = uint64(100)
	snapshotDirectoryPrefix       = "snapshot-"
)

// SnapshotConfig controls the server-layer post-apply trigger. Zero values
// select the Phase 6 defaults; a nil SnapshotManager disables snapshotting.
type SnapshotConfig struct {
	Dir            string
	EntryThreshold uint64
	ByteThreshold  uint64
	TailEntries    uint64
}

type durableStateMachine interface {
	statemachine.StateMachine
	DurableIndex() uint64
}

// RaftLogCompactor advances the live core's in-memory log base only after the
// corresponding storage compaction is durable.
type RaftLogCompactor interface {
	Compact(raft.SnapshotMeta) error
}

// SnapshotManager observes whole committed apply batches. When either bound
// is exceeded it snapshots the last entry in the batch, publishes Raft
// metadata only after the snapshot directory is durable, then compacts while
// retaining TailEntries of overlap.
type SnapshotManager struct {
	config SnapshotConfig
	store  storage.Storage
	sm     durableStateMachine
	fs     statemachine.SnapshotDirectoryFS
	raft   RaftLogCompactor

	lastSnapshot raft.SnapshotMeta
	observed     uint64
	entryBytes   uint64
	entryCount   uint64

	// hooks are deterministic crash seams used only by in-package tests.
	hooks snapshotHooks
}

type snapshotHooks struct {
	afterDirectory func() error
	afterMetadata  func() error
	afterCompact   func() error
}

// NewSnapshotManager validates that the restored state machine and Raft
// snapshot metadata describe the same applied index.
func NewSnapshotManager(config SnapshotConfig, store storage.Storage, sm statemachine.StateMachine, raftLog RaftLogCompactor) (*SnapshotManager, error) {
	if store == nil || sm == nil || raftLog == nil {
		return nil, errors.New("server: snapshot manager requires storage, state machine, and Raft log compactor")
	}
	if config.Dir == "" {
		return nil, errors.New("server: snapshot directory is empty")
	}
	if config.EntryThreshold == 0 {
		config.EntryThreshold = DefaultSnapshotEntryThreshold
	}
	if config.ByteThreshold == 0 {
		config.ByteThreshold = DefaultSnapshotByteThreshold
	}
	if config.TailEntries == 0 {
		config.TailEntries = DefaultSnapshotTailEntries
	}
	durable, ok := sm.(durableStateMachine)
	if !ok {
		return nil, errors.New("server: state machine does not expose a durable snapshot watermark")
	}
	meta, err := store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("server: read snapshot metadata for manager: %w", err)
	}
	if sm.AppliedIndex() != meta.Index {
		return nil, fmt.Errorf("server: restored state machine applied index %d differs from Raft snapshot index %d", sm.AppliedIndex(), meta.Index)
	}
	fs := statemachine.SnapshotDirectoryFS(osSnapshotDirectoryFS{})
	if provider, ok := sm.(statemachine.SnapshotDirectoryFSProvider); ok {
		fs = provider.SnapshotDirectoryFS()
		if fs == nil {
			return nil, errors.New("server: state machine returned a nil snapshot filesystem")
		}
	}
	if err := prepareSnapshotRoot(fs, config.Dir); err != nil {
		return nil, err
	}
	return &SnapshotManager{config: config, store: store, sm: durable, fs: fs, raft: raftLog, lastSnapshot: meta, observed: meta.Index}, nil
}

// ObserveApplied is called once after an entire Ready apply batch and before
// Advance. It therefore snapshots only quiescent, fully-applied state.
func (m *SnapshotManager) ObserveApplied(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	for i := range entries {
		entry := &entries[i]
		if entry.Index <= m.lastSnapshot.Index {
			continue
		}
		if m.observed == math.MaxUint64 || entry.Index != m.observed+1 {
			return fmt.Errorf("server: snapshot observer saw index %d after %d", entry.Index, m.observed)
		}
		if entry.Term == 0 {
			return fmt.Errorf("server: snapshot observer saw zero term at index %d", entry.Index)
		}
		m.observed = entry.Index
		m.entryCount = saturatingAdd(m.entryCount, 1)
		m.entryBytes = saturatingAdd(m.entryBytes, uint64(proto.Size(entry)))
	}
	if m.sm.AppliedIndex() != m.observed {
		return fmt.Errorf("server: snapshot observer applied index %d, last committed entry %d", m.sm.AppliedIndex(), m.observed)
	}
	if m.entryCount <= m.config.EntryThreshold && m.entryBytes <= m.config.ByteThreshold {
		return nil
	}
	if durable := m.sm.DurableIndex(); durable > m.observed {
		// A crash after an LSM snapshot force-flush but before SaveSnapshot can
		// leave the live engine durably ahead of the older Raft snapshot. During
		// replay, retain the crossed-trigger counters and wait until applied
		// state catches that watermark; attempting a snapshot below it would be
		// a forbidden MANIFEST watermark regression and would livelock restart.
		return nil
	}
	last := &entries[len(entries)-1]
	if last.Index != m.observed {
		return fmt.Errorf("server: snapshot batch ends at %d, observed index is %d", last.Index, m.observed)
	}
	return m.create(raft.SnapshotMeta{Index: last.Index, Term: last.Term})
}

func (m *SnapshotManager) create(meta raft.SnapshotMeta) error {
	if meta.Index == math.MaxUint64 {
		return errors.New("server: snapshot index overflows boundary lookup")
	}
	boundary, err := m.store.Entries(meta.Index, meta.Index+1)
	if err != nil {
		return fmt.Errorf("server: read local snapshot boundary entry %d: %w", meta.Index, err)
	}
	if len(boundary) != 1 || boundary[0].Index != meta.Index || boundary[0].Term != meta.Term {
		return fmt.Errorf("server: local snapshot boundary %d/%d does not match durable Raft entry %#v", meta.Index, meta.Term, boundary)
	}
	path := filepath.Join(m.config.Dir, snapshotDirectoryName(meta))
	names, err := m.fs.List(m.config.Dir)
	if err != nil {
		return fmt.Errorf("server: list snapshot root before create: %w", err)
	}
	for _, name := range names {
		if name == filepath.Base(path) {
			return fmt.Errorf("server: snapshot directory already exists: %s", path)
		}
	}
	if err := m.sm.CreateSnapshot(path, meta); err != nil {
		return fmt.Errorf("server: create state-machine snapshot at %d/%d: %w", meta.Index, meta.Term, err)
	}
	if durable := m.sm.DurableIndex(); durable < meta.Index {
		return fmt.Errorf("server: snapshot index %d exceeds engine durable watermark %d", meta.Index, durable)
	}
	if err := m.fs.SyncDir(m.config.Dir); err != nil {
		return fmt.Errorf("server: sync snapshot root after directory publication: %w", err)
	}
	if m.hooks.afterDirectory != nil {
		if err := m.hooks.afterDirectory(); err != nil {
			return err
		}
	}
	if err := m.store.SaveSnapshot(meta); err != nil {
		return fmt.Errorf("server: publish Raft snapshot metadata: %w", err)
	}
	if m.hooks.afterMetadata != nil {
		if err := m.hooks.afterMetadata(); err != nil {
			return err
		}
	}
	compactThrough := uint64(0)
	if meta.Index > m.config.TailEntries {
		compactThrough = meta.Index - m.config.TailEntries
	}
	if err := CompactThrough(m.store, m.sm, compactThrough); err != nil {
		return err
	}
	compacted, err := m.store.Compacted()
	if err != nil {
		return fmt.Errorf("server: read compacted metadata for live Raft core: %w", err)
	}
	if err := m.raft.Compact(compacted); err != nil {
		return fmt.Errorf("server: compact live Raft log through %d/%d: %w", compacted.Index, compacted.Term, err)
	}
	if m.hooks.afterCompact != nil {
		if err := m.hooks.afterCompact(); err != nil {
			return err
		}
	}
	if err := cleanupSnapshotDirectories(m.fs, m.config.Dir, filepath.Base(path)); err != nil {
		return err
	}
	m.lastSnapshot = meta
	m.entryCount = 0
	m.entryBytes = 0
	return nil
}

// CompactThrough is the belt-and-suspenders boundary between engine and Raft
// durability. No storage implementation is asked to discard an index beyond
// the engine's durable snapshot watermark.
func CompactThrough(store storage.Storage, sm interface{ DurableIndex() uint64 }, uptoIndex uint64) error {
	if store == nil || sm == nil {
		return errors.New("server: compact requires storage and durable state machine")
	}
	if durable := sm.DurableIndex(); uptoIndex > durable {
		return fmt.Errorf("server: refuse compact through %d beyond engine durable watermark %d", uptoIndex, durable)
	}
	if err := store.Compact(uptoIndex); err != nil {
		return fmt.Errorf("server: compact Raft log through %d: %w", uptoIndex, err)
	}
	return nil
}

// RecoverSnapshot restores exactly the snapshot named by durable Raft
// metadata and removes incomplete/unreferenced snapshot directories. A crash
// after directory publication but before SaveSnapshot leaves an orphan and
// replays the old log; a crash after SaveSnapshot restores the new state and
// tolerates either the old or advanced compaction base.
func RecoverSnapshot(dir string, store storage.Storage, sm statemachine.StateMachine) (raft.SnapshotMeta, error) {
	if dir == "" || store == nil || sm == nil {
		return raft.SnapshotMeta{}, errors.New("server: snapshot recovery requires directory, storage, and state machine")
	}
	fs := statemachine.SnapshotDirectoryFS(osSnapshotDirectoryFS{})
	if provider, ok := sm.(statemachine.SnapshotDirectoryFSProvider); ok {
		fs = provider.SnapshotDirectoryFS()
		if fs == nil {
			return raft.SnapshotMeta{}, errors.New("server: state machine returned a nil snapshot filesystem")
		}
	}
	if err := prepareSnapshotRoot(fs, dir); err != nil {
		return raft.SnapshotMeta{}, err
	}
	meta, err := store.Snapshot()
	if err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("server: read snapshot metadata during recovery: %w", err)
	}
	keep := ""
	if meta.Index != 0 {
		keep = snapshotDirectoryName(meta)
	}
	if err := cleanupSnapshotDirectories(fs, dir, keep); err != nil {
		return raft.SnapshotMeta{}, err
	}
	if keep == "" {
		return raft.SnapshotMeta{}, nil
	}
	names, err := fs.List(dir)
	if err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("server: list snapshot root during recovery: %w", err)
	}
	found := false
	for _, name := range names {
		if name == keep {
			found = true
			break
		}
	}
	if !found {
		return raft.SnapshotMeta{}, fmt.Errorf("server: durable snapshot metadata %d/%d has no directory %s", meta.Index, meta.Term, keep)
	}
	restored, err := sm.RestoreSnapshot(filepath.Join(dir, keep))
	if err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("server: restore snapshot %d/%d: %w", meta.Index, meta.Term, err)
	}
	if restored != meta {
		return raft.SnapshotMeta{}, fmt.Errorf("server: restored snapshot metadata %d/%d differs from Raft metadata %d/%d", restored.Index, restored.Term, meta.Index, meta.Term)
	}
	return meta, nil
}

func prepareSnapshotRoot(fs statemachine.SnapshotDirectoryFS, dir string) error {
	if err := fs.MkdirAll(dir); err != nil {
		return fmt.Errorf("server: create snapshot root: %w", err)
	}
	if err := fs.SyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("server: sync snapshot root parent: %w", err)
	}
	return nil
}

func cleanupSnapshotDirectories(fs statemachine.SnapshotDirectoryFS, root, keep string) error {
	names, err := fs.List(root)
	if err != nil {
		return fmt.Errorf("server: list snapshot root for cleanup: %w", err)
	}
	sort.Strings(names)
	dirty := false
	for _, name := range names {
		if !strings.HasPrefix(name, snapshotDirectoryPrefix) || name == keep {
			continue
		}
		if err := fs.RemoveAll(filepath.Join(root, name)); err != nil {
			return fmt.Errorf("server: remove orphan snapshot %s: %w", name, err)
		}
		dirty = true
	}
	if dirty {
		if err := fs.SyncDir(root); err != nil {
			return fmt.Errorf("server: sync snapshot root after orphan cleanup: %w", err)
		}
	}
	return nil
}

func snapshotDirectoryName(meta raft.SnapshotMeta) string {
	return fmt.Sprintf("%s%020d-%020d", snapshotDirectoryPrefix, meta.Index, meta.Term)
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

type osSnapshotDirectoryFS struct{}

func (osSnapshotDirectoryFS) MkdirAll(name string) error  { return os.MkdirAll(name, 0o700) }
func (osSnapshotDirectoryFS) RemoveAll(name string) error { return os.RemoveAll(name) }
func (osSnapshotDirectoryFS) List(name string) ([]string, error) {
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}
func (osSnapshotDirectoryFS) SyncDir(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
