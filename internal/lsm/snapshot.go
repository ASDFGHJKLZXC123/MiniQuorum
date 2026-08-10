package lsm

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

const (
	lsmSnapshotFormatVersion = uint32(1)
	lsmSnapshotMetaFilename  = "META.pb"
	lsmSnapshotTempFilename  = "META.pb.tmp"
)

// CreateSnapshot force-flushes exactly through meta.Index while holding the
// engine write lock, which also excludes synchronous compaction. It hard-links
// immutable SSTables, copies the mutable MANIFEST, then writes the complete
// dedup/meta envelope last. Every file and the snapshot directory are synced
// before this method returns.
func (m *StateMachine) CreateSnapshot(dir string, meta raft.SnapshotMeta) error {
	engine := m.engine
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.checkOpenLocked(); err != nil {
		return err
	}
	if meta.Index != m.applied || meta.Index != engine.appliedIndex {
		return fmt.Errorf("lsm: snapshot index %d does not equal state/engine applied indexes %d/%d", meta.Index, m.applied, engine.appliedIndex)
	}
	if meta.Index == 0 && meta.Term != 0 || meta.Index != 0 && meta.Term == 0 {
		return fmt.Errorf("lsm: invalid snapshot meta index=%d term=%d", meta.Index, meta.Term)
	}
	if filepath.Clean(dir) == filepath.Clean(engine.dir) {
		return errors.New("lsm: snapshot directory equals live engine directory")
	}
	if err := engine.fs.MkdirAll(dir); err != nil {
		return fmt.Errorf("lsm: create snapshot directory: %w", err)
	}
	if err := engine.fs.SyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("lsm: sync snapshot parent directory: %w", err)
	}
	names, err := engine.fs.List(dir)
	if err != nil {
		return fmt.Errorf("lsm: list snapshot directory: %w", err)
	}
	for _, name := range names {
		if name == lsmSnapshotTempFilename {
			if err := engine.fs.Remove(filepath.Join(dir, name)); err != nil {
				return fmt.Errorf("lsm: remove stale snapshot meta temp: %w", err)
			}
			continue
		}
		return fmt.Errorf("lsm: snapshot directory is not empty: %s", name)
	}

	if err := engine.flushThroughLocked(meta.Index); err != nil {
		return fmt.Errorf("lsm: force flush for snapshot: %w", err)
	}
	if engine.manifest.flushedIndex != meta.Index {
		return fmt.Errorf("lsm: snapshot durable watermark %d does not exactly equal snapshot index %d", engine.manifest.flushedIndex, meta.Index)
	}

	files := make([]*raftpb.SnapshotFile, 0, len(engine.manifest.files)+1)
	for _, metadata := range sortedManifestFiles(engine.manifest.files) {
		source := filepath.Join(engine.dir, metadata.file)
		destination := filepath.Join(dir, metadata.file)
		if err := engine.fs.Link(source, destination); err != nil {
			return fmt.Errorf("lsm: hard-link snapshot SSTable %s: %w", metadata.file, err)
		}
		if err := syncInjectedFile(engine, destination); err != nil {
			return fmt.Errorf("lsm: sync snapshot SSTable %s: %w", metadata.file, err)
		}
		file, err := describeSnapshotFile(engine, destination, metadata.file)
		if err != nil {
			return err
		}
		files = append(files, file)
	}

	manifestBytes, err := readInjectedFile(engine, filepath.Join(engine.dir, manifestFilename))
	if err != nil {
		return fmt.Errorf("lsm: read live MANIFEST for snapshot: %w", err)
	}
	manifestPath := filepath.Join(dir, manifestFilename)
	if err := writeInjectedFile(engine, manifestPath, manifestBytes); err != nil {
		return fmt.Errorf("lsm: copy snapshot MANIFEST: %w", err)
	}
	manifestFile, err := describeSnapshotFile(engine, manifestPath, manifestFilename)
	if err != nil {
		return err
	}
	files = append(files, manifestFile)
	sort.Slice(files, func(i, j int) bool { return files[i].GetName() < files[j].GetName() })

	snapshot := &raftpb.StateMachineSnapshot{
		FormatVersion: lsmSnapshotFormatVersion,
		Engine:        raftpb.SnapshotEngine_SNAPSHOT_ENGINE_LSM,
		Meta:          &raftpb.SnapshotMetadata{Index: meta.Index, Term: meta.Term},
		Files:         files,
	}
	clientIDs := make([]uint64, 0, len(m.dedup))
	for clientID := range m.dedup {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Slice(clientIDs, func(i, j int) bool { return clientIDs[i] < clientIDs[j] })
	for _, clientID := range clientIDs {
		record := m.dedup[clientID]
		snapshot.Dedup = append(snapshot.Dedup, &raftpb.SnapshotDedupRecord{
			ClientId: clientID,
			LastSeq:  record.lastSeq,
			Value:    cloneBytes(record.lastResult.Value),
			Found:    record.lastResult.Found,
		})
	}
	metaBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("lsm: marshal snapshot metadata: %w", err)
	}
	tempPath := filepath.Join(dir, lsmSnapshotTempFilename)
	if err := writeInjectedFile(engine, tempPath, metaBytes); err != nil {
		return fmt.Errorf("lsm: write snapshot metadata: %w", err)
	}
	if err := engine.fs.Rename(tempPath, filepath.Join(dir, lsmSnapshotMetaFilename)); err != nil {
		return fmt.Errorf("lsm: publish snapshot metadata: %w", err)
	}
	if err := engine.fs.SyncDir(dir); err != nil {
		return fmt.Errorf("lsm: sync snapshot directory: %w", err)
	}
	return nil
}

// RestoreSnapshot verifies the checksummed immutable file set and copied
// MANIFEST, atomically adopts it through the live directory's MANIFEST, and
// rebuilds the logical Hash view from SSTables.
func (m *StateMachine) RestoreSnapshot(dir string) (raft.SnapshotMeta, error) {
	if filepath.Clean(dir) == filepath.Clean(m.engine.dir) {
		return raft.SnapshotMeta{}, errors.New("lsm: snapshot directory equals live engine directory")
	}
	engine := m.engine
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if err := engine.checkOpenLocked(); err != nil {
		return raft.SnapshotMeta{}, err
	}
	metaBytes, err := readInjectedFile(engine, filepath.Join(dir, lsmSnapshotMetaFilename))
	if err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("lsm: read snapshot metadata: %w", err)
	}
	snapshot := &raftpb.StateMachineSnapshot{}
	if err := proto.Unmarshal(metaBytes, snapshot); err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("lsm: decode snapshot metadata: %w", err)
	}
	meta, state, dedup, err := validateLSMSnapshot(engine, dir, snapshot)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}

	values, err := m.adoptSnapshotLocked(dir, meta, state)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	m.values = values
	m.dedup = dedup
	m.applied = meta.Index
	m.replaying = false
	m.replayCeiling = 0
	return meta, nil
}

// DurableIndex exposes the exact MANIFEST-persisted truncation watermark to
// the server without extending the frozen StateMachine interface.
func (m *StateMachine) DurableIndex() uint64 { return m.engine.FlushedIndex() }

func validateLSMSnapshot(owner *Engine, dir string, snapshot *raftpb.StateMachineSnapshot) (raft.SnapshotMeta, manifestState, map[uint64]stateDedupRecord, error) {
	if snapshot.GetFormatVersion() != lsmSnapshotFormatVersion {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot format version %d, want %d", snapshot.GetFormatVersion(), lsmSnapshotFormatVersion)
	}
	if snapshot.GetEngine() != raftpb.SnapshotEngine_SNAPSHOT_ENGINE_LSM {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot engine %s, want lsm", snapshot.GetEngine())
	}
	if snapshot.GetMeta() == nil {
		return raft.SnapshotMeta{}, manifestState{}, nil, errors.New("lsm: snapshot metadata is missing")
	}
	if len(snapshot.GetKeyValues()) != 0 {
		return raft.SnapshotMeta{}, manifestState{}, nil, errors.New("lsm: LSM snapshot contains serialized key/value records")
	}
	meta := raft.SnapshotMeta{Index: snapshot.GetMeta().GetIndex(), Term: snapshot.GetMeta().GetTerm()}
	if meta.Index == 0 && meta.Term != 0 || meta.Index != 0 && meta.Term == 0 {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: invalid snapshot meta index=%d term=%d", meta.Index, meta.Term)
	}

	dedup := make(map[uint64]stateDedupRecord, len(snapshot.GetDedup()))
	var previousClient uint64
	for index, record := range snapshot.GetDedup() {
		if record == nil {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: nil snapshot dedup record %d", index)
		}
		if index != 0 && record.GetClientId() <= previousClient {
			return raft.SnapshotMeta{}, manifestState{}, nil, errors.New("lsm: snapshot dedup records are not strictly sorted")
		}
		dedup[record.GetClientId()] = stateDedupRecord{
			lastSeq: record.GetLastSeq(),
			lastResult: statemachine.Result{
				Value: cloneBytes(record.GetValue()),
				Found: record.GetFound(),
			},
		}
		previousClient = record.GetClientId()
	}

	listed := make(map[string]struct{}, len(snapshot.GetFiles()))
	var previousName string
	for index, file := range snapshot.GetFiles() {
		if file == nil {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: nil snapshot file record %d", index)
		}
		name := file.GetName()
		if index != 0 && name <= previousName {
			return raft.SnapshotMeta{}, manifestState{}, nil, errors.New("lsm: snapshot files are not strictly sorted")
		}
		if name != manifestFilename {
			if err := validateSSTableFilename(name); err != nil {
				return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot file %q: %w", name, err)
			}
		}
		if len(file.GetChecksumSha256()) != sha256.Size {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot file %s has %d-byte checksum, want %d", name, len(file.GetChecksumSha256()), sha256.Size)
		}
		data, err := readInjectedFile(owner, filepath.Join(dir, name))
		if err != nil {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: read snapshot file %s: %w", name, err)
		}
		if uint64(len(data)) != file.GetSize() {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot file %s size %d, metadata says %d", name, len(data), file.GetSize())
		}
		digest := sha256.Sum256(data)
		if !bytes.Equal(digest[:], file.GetChecksumSha256()) {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot file %s sha256 mismatch", name)
		}
		listed[name] = struct{}{}
		previousName = name
	}
	manifestBytes, err := readInjectedFile(owner, filepath.Join(dir, manifestFilename))
	if err != nil {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: read snapshot MANIFEST: %w", err)
	}
	state, complete, torn, err := replayManifestBytes(manifestBytes)
	if err != nil {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: validate snapshot MANIFEST: %w", err)
	}
	if torn || complete != len(manifestBytes) {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("%w: snapshot MANIFEST has a torn tail", ErrManifestCorrupt)
	}
	if state.flushedIndex != meta.Index {
		return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: snapshot MANIFEST flushed index %d does not exactly equal metadata index %d", state.flushedIndex, meta.Index)
	}
	if _, ok := listed[manifestFilename]; !ok || len(listed) != len(state.files)+1 {
		return raft.SnapshotMeta{}, manifestState{}, nil, errors.New("lsm: snapshot metadata file set does not match MANIFEST")
	}
	for name := range state.files {
		if _, ok := listed[name]; !ok {
			return raft.SnapshotMeta{}, manifestState{}, nil, fmt.Errorf("lsm: MANIFEST-referenced SSTable %s is absent from snapshot metadata", name)
		}
	}
	if err := validateSnapshotSSTables(owner, dir, state); err != nil {
		return raft.SnapshotMeta{}, manifestState{}, nil, err
	}
	return meta, state, dedup, nil
}

func validateSnapshotSSTables(owner *Engine, dir string, state manifestState) error {
	validator := &Engine{fs: owner.fs, dir: dir}
	for _, metadata := range sortedManifestFiles(state.files) {
		table, err := validator.openTableOwned(metadata, state.flushedIndex)
		if table != nil && table.file != nil {
			if closeErr := table.file.Close(); closeErr != nil && !errors.Is(closeErr, ErrFSClosed) {
				owner.unresolved = append(owner.unresolved, table.file)
				err = errors.Join(err, closeErr)
			}
		}
		if err != nil {
			return fmt.Errorf("lsm: validate snapshot SSTable %s: %w", metadata.file, err)
		}
	}
	return nil
}

func (m *StateMachine) adoptSnapshotLocked(snapshotDir string, meta raft.SnapshotMeta, state manifestState) (map[string][]byte, error) {
	engine := m.engine
	adopted := manifestState{files: make(map[string]fileMetadata, len(state.files)), flushedIndex: meta.Index, records: 1}
	for ordinal, metadata := range sortedManifestFiles(state.files) {
		source := filepath.Join(snapshotDir, metadata.file)
		sourceBytes, err := readInjectedFile(engine, source)
		if err != nil {
			return nil, fmt.Errorf("lsm: read snapshot SSTable %s for adoption: %w", metadata.file, err)
		}
		digest := sha256.Sum256(sourceBytes)
		// Content-addressing accepts different physical layouts for snapshots
		// at the same Raft index. The fixed-width digest+ordinal stays bounded
		// across arbitrary snapshot/restore generations.
		adoptedName := fmt.Sprintf("snapshot-%x-%06d.sst", digest, ordinal+1)
		if err := validateSSTableFilename(adoptedName); err != nil {
			return nil, fmt.Errorf("lsm: adopted SSTable name %q: %w", adoptedName, err)
		}
		destination := filepath.Join(engine.dir, adoptedName)
		if _, err := engine.fs.Stat(destination); errors.Is(err, ErrNotExist) {
			if err := engine.fs.Link(source, destination); err != nil {
				return nil, fmt.Errorf("lsm: stage snapshot SSTable %s: %w", metadata.file, err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("lsm: stat staged snapshot SSTable %s: %w", adoptedName, err)
		} else {
			destinationBytes, readDestinationErr := readInjectedFile(engine, destination)
			if readDestinationErr != nil {
				return nil, fmt.Errorf("lsm: read existing adopted SSTable %s: %w", adoptedName, readDestinationErr)
			}
			if !bytes.Equal(sourceBytes, destinationBytes) {
				return nil, fmt.Errorf("lsm: existing adopted SSTable %s does not match snapshot bytes", adoptedName)
			}
		}
		if err := syncInjectedFile(engine, destination); err != nil {
			return nil, fmt.Errorf("lsm: sync staged SSTable %s: %w", adoptedName, err)
		}
		metadata.file = adoptedName
		adopted.files[adoptedName] = metadata
	}
	values, err := logicalValuesFromSnapshotTables(engine, engine.dir, adopted)
	if err != nil {
		return nil, err
	}
	added := make([]*raftpb.AddedFile, 0, len(adopted.files))
	for _, metadata := range sortedManifestFiles(adopted.files) {
		added = append(added, metadata.proto())
	}
	frame, err := encodeManifestFrame(&raftpb.VersionEdit{AddedFiles: added, FlushedIndex: meta.Index})
	if err != nil {
		return nil, fmt.Errorf("lsm: encode adopted snapshot MANIFEST: %w", err)
	}
	manifestTemp := filepath.Join(engine.dir, "MANIFEST.snapshot.tmp")
	if err := engine.fs.Remove(manifestTemp); err != nil && !errors.Is(err, ErrNotExist) {
		return nil, fmt.Errorf("lsm: remove stale adopted MANIFEST temp: %w", err)
	}
	if err := writeInjectedFile(engine, manifestTemp, frame); err != nil {
		return nil, fmt.Errorf("lsm: stage adopted snapshot MANIFEST: %w", err)
	}
	if err := engine.fs.SyncDir(engine.dir); err != nil {
		return nil, fmt.Errorf("lsm: sync staged snapshot file set: %w", err)
	}
	if err := engine.reconcileObsoleteLocked(); err != nil {
		return nil, fmt.Errorf("lsm: reconcile old engine before restore: %w", err)
	}
	if err := engine.closeUnresolvedLocked(); err != nil {
		return nil, fmt.Errorf("lsm: close unresolved old files before restore: %w", err)
	}
	oldTables := append([]*tableHandle(nil), engine.tables...)
	if err := engine.fs.Rename(manifestTemp, filepath.Join(engine.dir, manifestFilename)); err != nil {
		engine.poisonReadsLocked(err)
		return nil, fmt.Errorf("lsm: atomically publish adopted snapshot MANIFEST: %w", err)
	}
	if err := engine.fs.SyncDir(engine.dir); err != nil {
		engine.poisonReadsLocked(err)
		return nil, fmt.Errorf("lsm: sync adopted snapshot MANIFEST: %w", err)
	}
	// MANIFEST now names the snapshot tables. Keep every replaced handle in
	// Close's retry set until the new version is fully opened and synced; a
	// post-publication failure must not leak ownership or serve partial state.
	engine.unresolved = make([]File, 0, len(oldTables))
	for _, table := range oldTables {
		engine.unresolved = append(engine.unresolved, table.file)
	}

	engine.list = newSkipList(engine.rnd)
	engine.activeSize = 0
	engine.activeCount = 0
	engine.activeMaxSeq = 0
	engine.immutables = nil
	engine.appliedIndex = meta.Index
	engine.manifest = adopted
	engine.tables = nil
	engine.nextFileNumber = nextFileNumber(adopted.files)
	engine.poisoned = nil
	engine.readPoisoned = nil
	engine.obsolete = nil
	engine.obsoleteDirDirty = false
	if err := engine.openReferencedTables(); err != nil {
		engine.poisonReadsLocked(err)
		return nil, fmt.Errorf("lsm: open adopted snapshot tables: %w", err)
	}
	if err := engine.syncRecoveredVersion(); err != nil {
		engine.poisonReadsLocked(err)
		return nil, fmt.Errorf("lsm: sync adopted snapshot version: %w", err)
	}
	var closeOldErr error
	unresolvedOld := make([]File, 0, len(oldTables))
	for _, table := range oldTables {
		if err := table.file.Close(); err != nil && !errors.Is(err, ErrFSClosed) {
			closeOldErr = errors.Join(closeOldErr, err)
			unresolvedOld = append(unresolvedOld, table.file)
		}
	}
	engine.unresolved = unresolvedOld
	if closeOldErr != nil {
		engine.poisonReadsLocked(closeOldErr)
		return nil, fmt.Errorf("lsm: close replaced SSTables after snapshot publication: %w", closeOldErr)
	}
	if err := engine.removeOrphans(); err != nil {
		engine.poisonReadsLocked(err)
		return nil, fmt.Errorf("lsm: clean replaced snapshot files: %w", err)
	}
	return values, nil
}

func logicalValuesFromSnapshotTables(owner *Engine, dir string, state manifestState) (map[string][]byte, error) {
	validator := &Engine{fs: owner.fs, dir: dir}
	tables := make([]*tableHandle, 0, len(state.files))
	for _, metadata := range sortedManifestFiles(state.files) {
		table, err := validator.openTableOwned(metadata, state.flushedIndex)
		if err != nil {
			if table != nil && table.file != nil {
				if closeErr := table.file.Close(); closeErr != nil && !errors.Is(closeErr, ErrFSClosed) {
					owner.unresolved = append(owner.unresolved, table.file)
					err = errors.Join(err, closeErr)
				}
			}
			for _, opened := range tables {
				if closeErr := opened.file.Close(); closeErr != nil && !errors.Is(closeErr, ErrFSClosed) {
					owner.unresolved = append(owner.unresolved, opened.file)
					err = errors.Join(err, closeErr)
				}
			}
			return nil, fmt.Errorf("lsm: open staged snapshot SSTable %s: %w", metadata.file, err)
		}
		tables = append(tables, table)
	}
	temporary := &Engine{tables: tables}
	values, valuesErr := logicalValuesFromTablesLocked(temporary)
	var closeErr error
	for _, table := range tables {
		if err := table.file.Close(); err != nil && !errors.Is(err, ErrFSClosed) {
			owner.unresolved = append(owner.unresolved, table.file)
			closeErr = errors.Join(closeErr, err)
		}
	}
	if err := errors.Join(valuesErr, closeErr); err != nil {
		return nil, fmt.Errorf("lsm: read staged snapshot values: %w", err)
	}
	return values, nil
}

func logicalValuesFromTablesLocked(engine *Engine) (map[string][]byte, error) {
	winners := make(map[string]TableEntry)
	for _, table := range engine.tables {
		entries, err := table.reader.AllEntries()
		if err != nil {
			return nil, fmt.Errorf("lsm: rebuild logical snapshot state from %s: %w", table.metadata.file, err)
		}
		for _, entry := range entries {
			key := string(entry.Key)
			prior, exists := winners[key]
			switch {
			case !exists || entry.Seq > prior.Seq:
				winners[key] = cloneTableEntry(entry)
			case entry.Seq == prior.Seq && !sameTableRecord(entry, prior):
				return nil, fmt.Errorf("%w: conflicting snapshot versions for key %x at seq %d", ErrCorrupt, entry.Key, entry.Seq)
			}
		}
	}
	values := make(map[string][]byte, len(winners))
	for key, entry := range winners {
		if !entry.Tombstone {
			values[key] = cloneBytes(entry.Value)
		}
	}
	return values, nil
}

func describeSnapshotFile(owner *Engine, path, name string) (*raftpb.SnapshotFile, error) {
	data, err := readInjectedFile(owner, path)
	if err != nil {
		return nil, fmt.Errorf("lsm: read snapshot file %s for checksum: %w", name, err)
	}
	digest := sha256.Sum256(data)
	return &raftpb.SnapshotFile{Name: name, Size: uint64(len(data)), ChecksumSha256: digest[:]}, nil
}

// These helpers run with owner.mu held. Every opened handle crosses the
// engine's normal close-and-retain boundary so a failed Close remains owned
// and can be retried by Engine.Close on the fail-stop path.
func readInjectedFile(owner *Engine, path string) ([]byte, error) {
	info, err := owner.fs.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir || info.Size < 0 || uint64(info.Size) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("invalid file size %d", info.Size)
	}
	file, err := owner.fs.Open(path)
	if err != nil {
		return nil, err
	}
	data := make([]byte, int(info.Size))
	readErr := readAtFull(file, data, 0)
	closeErr := owner.closeFileRetainingLocked(file)
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	return data, nil
}

func writeInjectedFile(owner *Engine, path string, data []byte) error {
	file, err := owner.fs.Create(path)
	if err != nil {
		return err
	}
	if n, err := file.Write(data); err != nil {
		return errors.Join(err, owner.closeFileRetainingLocked(file))
	} else if n != len(data) {
		return errors.Join(io.ErrShortWrite, owner.closeFileRetainingLocked(file))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, owner.closeFileRetainingLocked(file))
	}
	return owner.closeFileRetainingLocked(file)
}

func syncInjectedFile(owner *Engine, path string) error {
	file, err := owner.fs.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, owner.closeFileRetainingLocked(file))
	}
	return owner.closeFileRetainingLocked(file)
}
