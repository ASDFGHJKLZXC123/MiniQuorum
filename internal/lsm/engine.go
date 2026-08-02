// Package lsm implements MiniQuorum's synchronous, deterministic LSM storage
// engine. All persistence crosses the injected FS boundary.
package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	raftpb "miniquorum/proto"
)

// DefaultFlushThreshold is the Phase 5 default active-memtable threshold.
const DefaultFlushThreshold int64 = 4 << 20

// Entry is one ordered key/value record returned by Engine.Entries.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Seq       uint64
}

// Options configures a persistent Engine opened with Open.
type Options struct {
	FS             FS
	Rand           Rand
	FlushThreshold int64
	// SkipBloom bypasses Bloom filters in SSTable reads. Production usage keeps
	// Bloom enabled by default.
	SkipBloom bool
	// DisableAutoCompaction suppresses automatic compaction after every flush.
	// This is benchmark-only.
	DisableAutoCompaction bool
}

type immutableMemtable struct {
	list      *skipList
	count     int
	maxSeq    uint64
	watermark uint64
}

type tableHandle struct {
	metadata fileMetadata
	file     File
	reader   *SSTableReader
	size     int64
}

// Engine owns the one engine-level RWMutex. Writes, freezes, manifest
// publication, and immutable drops take the write lock. Reads retain the read
// lock through all memtable and SSTable block work.
type Engine struct {
	mu sync.RWMutex

	rnd                   Rand
	list                  *skipList
	activeSize            int64
	activeCount           int
	activeMaxSeq          uint64
	immutables            []*immutableMemtable
	flushThreshold        int64
	appliedIndex          uint64
	manifest              manifestState
	tables                []*tableHandle
	nextFileNumber        uint64
	fs                    FS
	dir                   string
	poisoned              error
	unresolved            []File
	obsolete              []obsoleteFile
	obsoleteDirDirty      bool
	compactionHook        func(compactionStage)
	disableAutoCompaction bool
	readWithBloom         bool
	completedFlushes      uint64
	completedCompactions  uint64
	closed                bool
}

// EngineBenchmarkCounters reports additive, lock-protected benchmark counters.
type EngineBenchmarkCounters struct {
	CompletedFlushes     uint64
	CompletedCompactions uint64
}

// SnapshotBenchmarkCounters returns the current benchmark counters.
func (engine *Engine) SnapshotBenchmarkCounters() EngineBenchmarkCounters {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return EngineBenchmarkCounters{
		CompletedFlushes:     engine.completedFlushes,
		CompletedCompactions: engine.completedCompactions,
	}
}

// NewEngine preserves Packet 5A's in-memory constructor. Persistence and
// automatic flushing are disabled; callers that need Packet 5C use Open.
func NewEngine(rnd Rand) *Engine {
	if rnd == nil {
		panic("lsm: NewEngine: rnd must not be nil")
	}
	return &Engine{
		rnd:            rnd,
		list:           newSkipList(rnd),
		manifest:       newManifestState(),
		nextFileNumber: 1,
		readWithBloom:  true,
	}
}

// Open replays MANIFEST, validates every referenced SSTable, removes all
// unreferenced .sst files durably, and returns a persistent engine.
// AppliedIndex intentionally starts at zero: FlushedIndex is durability
// evidence, never a pre-snapshot full-log recovery start point.
func Open(dir string, options Options) (*Engine, error) {
	if options.Rand == nil {
		return nil, errors.New("lsm: Open: Rand must not be nil")
	}
	fs := options.FS
	if fs == nil {
		fs = RealFS{}
	}
	threshold := options.FlushThreshold
	if threshold == 0 {
		threshold = DefaultFlushThreshold
	}
	if threshold < 0 {
		return nil, fmt.Errorf("lsm: negative flush threshold %d", threshold)
	}

	state, err := loadManifest(fs, dir)
	if err != nil {
		return nil, err
	}
	engine := &Engine{
		rnd:                   options.Rand,
		list:                  newSkipList(options.Rand),
		flushThreshold:        threshold,
		disableAutoCompaction: options.DisableAutoCompaction,
		readWithBloom:         !options.SkipBloom,
		manifest:              state,
		nextFileNumber:        1,
		fs:                    fs,
		dir:                   dir,
	}
	if err := engine.openReferencedTables(); err != nil {
		return nil, errors.Join(err, engine.closeTables(), engine.closeUnresolvedLocked())
	}
	if err := engine.syncRecoveredVersion(); err != nil {
		return nil, errors.Join(err, engine.closeTables(), engine.closeUnresolvedLocked())
	}
	if err := engine.removeOrphans(); err != nil {
		return nil, errors.Join(err, engine.closeTables(), engine.closeUnresolvedLocked())
	}
	engine.nextFileNumber = nextFileNumber(state.files)
	return engine, nil
}

// Put inserts or updates key at seq. The accepted bytes are visible before a
// threshold-triggered flush starts, so any flush failure still leaves the
// already-applied write readable from an immutable memtable.
func (engine *Engine) Put(key, value []byte, seq uint64) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.putLocked(key, value, seq)
}

func (engine *Engine) putLocked(key, value []byte, seq uint64) error {
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	engine.advanceAppliedForWriteLocked(seq)
	if engine.putActiveLocked(key, value, false, seq) && engine.shouldFlushLocked() {
		return engine.flushThroughLocked(engine.automaticFlushTargetLocked())
	}
	return nil
}

// Delete records a tombstone at seq and obeys Put's synchronous flush/error
// contract.
func (engine *Engine) Delete(key []byte, seq uint64) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.deleteLocked(key, seq)
}

func (engine *Engine) deleteLocked(key []byte, seq uint64) error {
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	engine.advanceAppliedForWriteLocked(seq)
	if engine.putActiveLocked(key, nil, true, seq) && engine.shouldFlushLocked() {
		return engine.flushThroughLocked(engine.automaticFlushTargetLocked())
	}
	return nil
}

func (engine *Engine) advanceAppliedForWriteLocked(seq uint64) {
	if seq > engine.appliedIndex {
		engine.appliedIndex = seq
	}
}

func (engine *Engine) putActiveLocked(key, value []byte, tombstone bool, seq uint64) bool {
	oldValue, oldTombstone, oldSeq, found := engine.list.get(key)
	if found && seq <= oldSeq {
		return false
	}
	if found {
		engine.activeSize -= memtableEntrySize(key, oldValue, oldTombstone)
	} else {
		engine.activeCount++
	}
	keyCopy := cloneBytes(key)
	valueCopy := cloneBytes(value)
	if !engine.list.put(keyCopy, valueCopy, tombstone, seq) {
		panic("lsm: accepted active write was rejected by skip list")
	}
	engine.activeSize += memtableEntrySize(key, value, tombstone)
	if seq > engine.activeMaxSeq {
		engine.activeMaxSeq = seq
	}
	return true
}

func memtableEntrySize(key, value []byte, tombstone bool) int64 {
	size := uint64(len(key)) + 8 + 1
	if !tombstone {
		size += uint64(len(value))
	}
	if size > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(size)
}

func (engine *Engine) shouldFlushLocked() bool {
	return engine.fs != nil && engine.flushThreshold > 0 && engine.activeSize >= engine.flushThreshold
}

func (engine *Engine) automaticFlushTargetLocked() uint64 {
	return max(engine.appliedIndex, engine.manifest.flushedIndex)
}

// AdvanceAppliedIndex is the explicit Packet 5E seam for GET/NOOP entries
// which advance Raft application without adding KV bytes.
func (engine *Engine) AdvanceAppliedIndex(index uint64) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.advanceAppliedIndexLocked(index)
}

func (engine *Engine) advanceAppliedIndexLocked(index uint64) error {
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	if index < engine.appliedIndex {
		return fmt.Errorf("lsm: applied index regresses from %d to %d", engine.appliedIndex, index)
	}
	engine.appliedIndex = index
	return nil
}

// AppliedIndex reports the highest index supplied by writes or
// AdvanceAppliedIndex. It is distinct from the durable FlushedIndex.
func (engine *Engine) AppliedIndex() uint64 {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.appliedIndex
}

// FlushedIndex is the manifest-persisted truncation-safety watermark.
func (engine *Engine) FlushedIndex() uint64 {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.manifest.flushedIndex
}

// ForceFlush synchronously makes every KV effect through AppliedIndex durable.
// When there are no new KV bytes but AppliedIndex advanced, it appends a
// no-file VersionEdit carrying the new watermark.
func (engine *Engine) ForceFlush() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	return engine.flushThroughLocked(engine.appliedIndex)
}

// FlushThrough is the checked internal watermark seam. The target must
// already have been applied; callers cannot manufacture a future or regressing
// watermark.
func (engine *Engine) FlushThrough(index uint64) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	return engine.flushThroughLocked(index)
}

func (engine *Engine) flushThroughLocked(index uint64) error {
	if engine.fs == nil {
		if engine.activeCount == 0 && len(engine.immutables) == 0 && index == engine.manifest.flushedIndex {
			return nil
		}
		return errors.New("lsm: persistent filesystem is not configured")
	}
	if index < engine.manifest.flushedIndex {
		return fmt.Errorf("lsm: flushed index regresses from %d to %d", engine.manifest.flushedIndex, index)
	}
	if index > engine.appliedIndex && index > engine.manifest.flushedIndex {
		return fmt.Errorf("lsm: cannot flush unapplied index %d beyond applied index %d", index, engine.appliedIndex)
	}
	if engine.activeMaxSeq > index {
		return fmt.Errorf("lsm: active memtable contains seq %d beyond flush target %d", engine.activeMaxSeq, index)
	}
	for _, immutable := range engine.immutables {
		if immutable.watermark > index {
			return fmt.Errorf("lsm: immutable watermark %d is beyond flush target %d", immutable.watermark, index)
		}
	}
	if engine.activeCount != 0 {
		engine.freezeLocked(index)
	}
	for len(engine.immutables) != 0 {
		if err := engine.flushOldestLocked(); err != nil {
			return err
		}
	}
	if index > engine.manifest.flushedIndex {
		edit := &raftpb.VersionEdit{FlushedIndex: index}
		_, err := engine.commitEditLocked(edit, nil)
		return err
	}
	return nil
}

func (engine *Engine) freezeLocked(watermark uint64) {
	immutable := &immutableMemtable{
		list:      engine.list,
		count:     engine.activeCount,
		maxSeq:    engine.activeMaxSeq,
		watermark: watermark,
	}
	engine.immutables = append(engine.immutables, immutable)
	engine.list = newSkipList(engine.rnd)
	engine.activeSize = 0
	engine.activeCount = 0
	engine.activeMaxSeq = 0
}

func (engine *Engine) flushOldestLocked() error {
	immutable := engine.immutables[0]
	if immutable.count == 0 {
		return errors.New("lsm: internal empty immutable")
	}
	if immutable.maxSeq > immutable.watermark {
		return fmt.Errorf("lsm: immutable max seq %d exceeds watermark %d", immutable.maxSeq, immutable.watermark)
	}
	filename, err := engine.allocateFilenameLocked()
	if err != nil {
		return err
	}
	path := filepath.Join(engine.dir, filename)
	file, err := engine.fs.Create(path)
	if err != nil {
		return fmt.Errorf("lsm: create SSTable %s: %w", filename, err)
	}

	writer := NewSSTableWriter(file)
	var firstKey, lastKey []byte
	count := uint64(0)
	var addErr error
	immutable.list.forEach(func(key, value []byte, tombstone bool, seq uint64) bool {
		if count == 0 {
			firstKey = cloneBytes(key)
		}
		lastKey = cloneBytes(key)
		count++
		addErr = writer.Add(TableEntry{Key: key, Value: value, Tombstone: tombstone, Seq: seq})
		return addErr == nil
	})
	if addErr != nil {
		return fmt.Errorf("lsm: build SSTable %s: %w", filename, errors.Join(addErr, engine.closeFileRetainingLocked(file)))
	}
	if err := writer.Finish(); err != nil {
		return fmt.Errorf("lsm: finish SSTable %s: %w", filename, errors.Join(err, engine.closeFileRetainingLocked(file)))
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("lsm: sync SSTable %s: %w", filename, errors.Join(err, engine.closeFileRetainingLocked(file)))
	}
	if err := engine.closeFileRetainingLocked(file); err != nil {
		return fmt.Errorf("lsm: close SSTable %s: %w", filename, err)
	}
	if err := engine.fs.SyncDir(engine.dir); err != nil {
		return fmt.Errorf("lsm: sync SSTable directory for %s: %w", filename, err)
	}

	metadata := fileMetadata{
		tier:   tableSizeClass(writer.size),
		file:   filename,
		minKey: firstKey,
		maxKey: lastKey,
		count:  count,
	}
	table, err := engine.openTable(metadata, immutable.watermark)
	if err != nil {
		return err
	}
	edit := &raftpb.VersionEdit{
		AddedFiles:   []*raftpb.AddedFile{metadata.proto()},
		FlushedIndex: immutable.watermark,
	}
	committed, err := engine.commitEditLocked(edit, table)
	if committed {
		engine.completedFlushes++
		engine.immutables = engine.immutables[1:]
		if err == nil {
			if !engine.disableAutoCompaction {
				err = engine.compactAllLocked()
			}
		}
	}
	return err
}

func (engine *Engine) commitEditLocked(edit *raftpb.VersionEdit, table *tableHandle) (bool, error) {
	next := manifestState{
		files:        make(map[string]fileMetadata, len(engine.manifest.files)+len(edit.GetAddedFiles())),
		flushedIndex: engine.manifest.flushedIndex,
		records:      engine.manifest.records,
	}
	for name, metadata := range engine.manifest.files {
		next.files[name] = metadata
	}
	if err := next.apply(edit); err != nil {
		var closeErr error
		if table != nil {
			closeErr = engine.closeFileRetainingLocked(table.file)
		}
		return false, errors.Join(fmt.Errorf("lsm: invalid manifest edit: %w", err), closeErr)
	}
	committed, unresolved, appendErr := appendManifestEdit(engine.fs, engine.dir, edit)
	if unresolved != nil {
		engine.unresolved = append(engine.unresolved, unresolved)
	}
	if !committed {
		if isUncertainManifestAppend(appendErr) {
			engine.poisonLocked(appendErr)
		}
		if table != nil {
			closeErr := engine.closeFileRetainingLocked(table.file)
			appendErr = errors.Join(appendErr, closeErr)
		}
		return false, appendErr
	}
	engine.manifest = next
	if table != nil {
		engine.tables = append(engine.tables, table)
	}
	return true, appendErr
}

func (engine *Engine) allocateFilenameLocked() (string, error) {
	if engine.nextFileNumber == 0 || engine.nextFileNumber == math.MaxUint64 {
		return "", errors.New("lsm: SSTable file number exhausted")
	}
	number := engine.nextFileNumber
	engine.nextFileNumber++
	return fmt.Sprintf("%0*d%s", defaultFileNumberWidth, number, sstableFilenameSuffix), nil
}

func nextFileNumber(files map[string]fileMetadata) uint64 {
	next := uint64(1)
	for name := range files {
		stem := strings.TrimSuffix(name, sstableFilenameSuffix)
		number, err := strconv.ParseUint(stem, 10, 64)
		if err == nil && number >= next && number != math.MaxUint64 {
			next = number + 1
		}
	}
	return next
}

func (engine *Engine) openReferencedTables() error {
	for _, metadata := range sortedManifestFiles(engine.manifest.files) {
		table, err := engine.openTable(metadata, engine.manifest.flushedIndex)
		if err != nil {
			return err
		}
		engine.tables = append(engine.tables, table)
	}
	return nil
}

// syncRecoveredVersion mirrors Phase 3's recovery durability rule: bytes and
// names that merely read back after a prior process died are synced before
// the recovered manifest state can be served. Referenced files, their
// directory entries, and finally MANIFEST are established in dependency
// order.
func (engine *Engine) syncRecoveredVersion() error {
	for _, table := range engine.tables {
		if err := table.file.Sync(); err != nil {
			return fmt.Errorf("lsm: recovery sync SSTable %s: %w", table.metadata.file, err)
		}
	}
	if err := engine.fs.SyncDir(engine.dir); err != nil {
		return fmt.Errorf("lsm: recovery sync SSTable directory: %w", err)
	}
	if err := syncManifest(engine.fs, filepath.Join(engine.dir, manifestFilename)); err != nil {
		return err
	}
	return nil
}

func (engine *Engine) openTable(metadata fileMetadata, maxSequence uint64) (*tableHandle, error) {
	table, err := engine.openTableOwned(metadata, maxSequence)
	if err == nil {
		return table, nil
	}
	if table == nil || table.file == nil {
		return nil, err
	}
	closeErr := engine.closeFileRetainingLocked(table.file)
	return nil, errors.Join(err, closeErr)
}

// openTableOwned returns ownership of an opened handle even when validation
// fails. Packet 5D's compaction path uses this form so a failed validation and
// failed Close cannot silently lose the handle.
func (engine *Engine) openTableOwned(metadata fileMetadata, maxSequence uint64) (*tableHandle, error) {
	path := filepath.Join(engine.dir, metadata.file)
	info, err := engine.fs.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%w: stat referenced SSTable %s: %w", ErrManifestCorrupt, metadata.file, err)
	}
	if info.IsDir || info.Size < 0 {
		return nil, fmt.Errorf("%w: invalid stat for referenced SSTable %s", ErrManifestCorrupt, metadata.file)
	}
	file, err := engine.fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open referenced SSTable %s: %w", ErrManifestCorrupt, metadata.file, err)
	}
	reader, err := OpenSSTable(file, info.Size)
	if err != nil {
		return &tableHandle{metadata: metadata, file: file, size: info.Size}, fmt.Errorf("%w: open referenced SSTable %s: %w", ErrManifestCorrupt, metadata.file, err)
	}
	if reader.EntryCount() != metadata.count || !bytes.Equal(reader.minKey, metadata.minKey) || !bytes.Equal(reader.maxKey, metadata.maxKey) {
		return &tableHandle{metadata: metadata, file: file, reader: reader, size: info.Size}, fmt.Errorf("%w: metadata mismatch for referenced SSTable %s", ErrManifestCorrupt, metadata.file)
	}
	entries, err := reader.AllEntries()
	if err != nil {
		return &tableHandle{metadata: metadata, file: file, reader: reader, size: info.Size}, fmt.Errorf("%w: validate referenced SSTable %s: %w", ErrManifestCorrupt, metadata.file, err)
	}
	for _, entry := range entries {
		if entry.Seq > maxSequence {
			return &tableHandle{metadata: metadata, file: file, reader: reader, size: info.Size}, fmt.Errorf("%w: SSTable %s seq %d exceeds flushed_index %d", ErrManifestCorrupt, metadata.file, entry.Seq, maxSequence)
		}
	}
	return &tableHandle{metadata: metadata, file: file, reader: reader, size: info.Size}, nil
}

func (engine *Engine) removeOrphans() error {
	names, err := engine.fs.List(engine.dir)
	if err != nil {
		return fmt.Errorf("lsm: list SSTable directory: %w", err)
	}
	removed := false
	for _, name := range names {
		if !strings.HasSuffix(name, sstableFilenameSuffix) {
			continue
		}
		if _, referenced := engine.manifest.files[name]; referenced {
			continue
		}
		path := filepath.Join(engine.dir, name)
		info, err := engine.fs.Stat(path)
		if err != nil {
			return fmt.Errorf("lsm: stat orphan %s: %w", name, err)
		}
		if info.IsDir {
			continue
		}
		if err := engine.fs.Remove(path); err != nil {
			return fmt.Errorf("lsm: remove orphan %s: %w", name, err)
		}
		removed = true
	}
	if removed {
		if err := engine.fs.SyncDir(engine.dir); err != nil {
			return fmt.Errorf("lsm: sync orphan removals: %w", err)
		}
	}
	return nil
}

// Lookup resolves the active memtable, every retained immutable, and every
// manifest-referenced SSTable by highest Raft seq. The engine read lock stays
// held through Bloom checks and block reads.
func (engine *Engine) Lookup(key []byte) (value []byte, tombstone bool, seq uint64, found bool, err error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.lookupLocked(key)
}

func (engine *Engine) lookupLocked(key []byte) (value []byte, tombstone bool, seq uint64, found bool, err error) {
	if engine.closed {
		return nil, false, 0, false, errors.New("lsm: engine is closed")
	}

	consider := func(candidateValue []byte, candidateTombstone bool, candidateSeq uint64, candidateFound bool) error {
		if !candidateFound {
			return nil
		}
		if !found || candidateSeq > seq {
			value, tombstone, seq, found = candidateValue, candidateTombstone, candidateSeq, true
			return nil
		}
		if candidateSeq == seq && (candidateTombstone != tombstone || !bytes.Equal(candidateValue, value)) {
			return fmt.Errorf("%w: conflicting versions for key at seq %d", ErrCorrupt, seq)
		}
		return nil
	}

	activeValue, activeTombstone, activeSeq, activeFound := engine.list.get(key)
	if err := consider(activeValue, activeTombstone, activeSeq, activeFound); err != nil {
		return nil, false, 0, false, err
	}
	for _, immutable := range engine.immutables {
		candidateValue, candidateTombstone, candidateSeq, candidateFound := immutable.list.get(key)
		if err := consider(candidateValue, candidateTombstone, candidateSeq, candidateFound); err != nil {
			return nil, false, 0, false, err
		}
	}
	for _, table := range engine.tables {
		entry, candidateFound, readErr := table.reader.get(key, engine.readWithBloom)
		if readErr != nil {
			return nil, false, 0, false, fmt.Errorf("lsm: read SSTable %s: %w", table.metadata.file, readErr)
		}
		if err := consider(entry.Value, entry.Tombstone, entry.Seq, candidateFound); err != nil {
			return nil, false, 0, false, err
		}
	}
	return cloneBytes(value), tombstone, seq, found, nil
}

// Read is the normal logical point-read surface. A highest-seq tombstone is
// represented as not found.
func (engine *Engine) Read(key []byte) ([]byte, bool, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return engine.readLocked(key)
}

func (engine *Engine) readLocked(key []byte) ([]byte, bool, error) {
	value, tombstone, _, found, err := engine.lookupLocked(key)
	if err != nil || !found || tombstone {
		return nil, false, err
	}
	return value, true, nil
}

// Get preserves Packet 5A's tuple API while using the complete Packet 5C read
// path. Persistent read errors map to not-found here; new integration code
// uses Read or Lookup so errors remain explicit.
func (engine *Engine) Get(key []byte) (value []byte, tombstone bool, seq uint64, found bool) {
	value, tombstone, seq, found, err := engine.Lookup(key)
	if err != nil {
		return nil, false, 0, false
	}
	return value, tombstone, seq, found
}

// Entries preserves Packet 5A's active-memtable snapshot API.
func (engine *Engine) Entries() []Entry {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	var out []Entry
	engine.list.forEach(func(key, value []byte, tombstone bool, seq uint64) bool {
		out = append(out, Entry{Key: cloneBytes(key), Value: cloneBytes(value), Tombstone: tombstone, Seq: seq})
		return true
	})
	return out
}

// PendingImmutables reports the number of frozen memtables retained for
// readability. It is an internal integration/test seam.
func (engine *Engine) PendingImmutables() int {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return len(engine.immutables)
}

// ReferencedSSTables returns manifest filenames in deterministic order.
func (engine *Engine) ReferencedSSTables() []string {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	files := sortedManifestFiles(engine.manifest.files)
	names := make([]string, len(files))
	for index := range files {
		names[index] = files[index].file
	}
	return names
}

// Close retries any pending obsolete-file removal and directory sync, then
// releases retained MANIFEST/output handles and referenced SSTable handles.
// Published VersionEdits themselves are already durable.
func (engine *Engine) Close() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if !engine.closed {
		engine.closed = true
	}
	cleanupErr := engine.reconcileObsoleteLocked()
	unresolvedErr := engine.closeUnresolvedLocked()
	return errors.Join(cleanupErr, unresolvedErr, engine.closeTables())
}

// Abandon marks the engine unusable without reconciling obsolete files,
// syncing directories, or closing handles. It models abrupt process loss;
// only normal shutdown calls Close.
func (engine *Engine) Abandon() {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.closed = true
}

func (engine *Engine) closeTables() error {
	var err error
	unresolved := make([]*tableHandle, 0, len(engine.tables))
	for _, table := range engine.tables {
		closeErr := table.file.Close()
		if closeErr == nil || errors.Is(closeErr, ErrFSClosed) {
			continue
		}
		err = errors.Join(err, closeErr)
		unresolved = append(unresolved, table)
	}
	engine.tables = unresolved
	return err
}

func (engine *Engine) checkOpenLocked() error {
	if engine.closed {
		return errors.New("lsm: engine is closed")
	}
	if engine.poisoned != nil {
		return fmt.Errorf("lsm: engine is fail-stopped after uncertain MANIFEST append: %w", engine.poisoned)
	}
	return nil
}

func (engine *Engine) poisonLocked(err error) {
	if engine.poisoned == nil {
		engine.poisoned = err
	}
}

func (engine *Engine) closeUnresolvedLocked() error {
	var err error
	unresolved := make([]File, 0, len(engine.unresolved))
	for _, file := range engine.unresolved {
		closeErr := file.Close()
		if closeErr == nil || errors.Is(closeErr, ErrFSClosed) {
			continue
		}
		err = errors.Join(err, closeErr)
		unresolved = append(unresolved, file)
	}
	engine.unresolved = unresolved
	return err
}

func (engine *Engine) closeFileRetainingLocked(file File) error {
	if file == nil {
		return nil
	}
	err := file.Close()
	if err != nil && !errors.Is(err, ErrFSClosed) {
		engine.unresolved = append(engine.unresolved, file)
	}
	return err
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
