// Package mapsm implements the Phase 2 in-memory map state machine.
package mapsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

// MapStateMachine is an in-memory key/value state machine with per-client
// sequence deduplication. Its single RWMutex protects all logical state.
type MapStateMachine struct {
	mu sync.RWMutex

	values  map[string][]byte
	dedup   map[uint64]dedupRecord
	applied uint64
	// snapshotDurable advances only after STATE.pb and its directory have
	// crossed their fsync boundary (or after a successful restore).
	snapshotDurable uint64
}

type dedupRecord struct {
	lastSeq    uint64
	lastResult statemachine.Result
}

// New constructs an empty map state machine.
func New() *MapStateMachine {
	return &MapStateMachine{
		values: make(map[string][]byte),
		dedup:  make(map[uint64]dedupRecord),
	}
}

var _ statemachine.StateMachine = (*MapStateMachine)(nil)

// Apply synchronously processes entry without retaining or mutating it.
func (m *MapStateMachine) Apply(entry *raftpb.Entry) (statemachine.Result, error) {
	if entry == nil {
		return statemachine.Result{}, fmt.Errorf("apply nil entry")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	switch entry.GetType() {
	case raftpb.EntryType_NOOP:
		m.advanceAppliedIndex(entry.GetIndex())
		return statemachine.Result{}, nil
	case raftpb.EntryType_NORMAL:
		var command raftpb.Command
		if err := proto.Unmarshal(entry.GetData(), &command); err != nil {
			return statemachine.Result{}, fmt.Errorf("decode command: %w", err)
		}

		if record, ok := m.dedup[command.GetClientId()]; ok && command.GetSeq() <= record.lastSeq {
			m.advanceAppliedIndex(entry.GetIndex())
			return cloneResult(record.lastResult), nil
		}

		result, err := m.execute(&command)
		if err != nil {
			return statemachine.Result{}, err
		}
		m.dedup[command.GetClientId()] = dedupRecord{
			lastSeq:    command.GetSeq(),
			lastResult: cloneResult(result),
		}
		m.advanceAppliedIndex(entry.GetIndex())
		return cloneResult(result), nil
	default:
		return statemachine.Result{}, fmt.Errorf("unsupported entry type %s", entry.GetType())
	}
}

func (m *MapStateMachine) execute(command *raftpb.Command) (statemachine.Result, error) {
	key := string(command.GetKey())
	switch command.GetOp() {
	case raftpb.Op_PUT:
		m.values[key] = cloneBytes(command.GetValue())
		return statemachine.Result{}, nil
	case raftpb.Op_DELETE:
		delete(m.values, key)
		return statemachine.Result{}, nil
	case raftpb.Op_GET:
		value, found := m.values[key]
		return statemachine.Result{Value: cloneBytes(value), Found: found}, nil
	default:
		return statemachine.Result{}, fmt.Errorf("unsupported command op %s", command.GetOp())
	}
}

// Read returns the current value for key without going through the Raft log.
func (m *MapStateMachine) Read(key []byte) (statemachine.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, found := m.values[string(key)]
	return statemachine.Result{Value: cloneBytes(value), Found: found}, nil
}

// AppliedIndex returns the greatest successfully applied log index.
func (m *MapStateMachine) AppliedIndex() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applied
}

// Hash returns a deterministic digest of all logical state, including KV
// values, deduplication records, and the applied index.
func (m *MapStateMachine) Hash() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h := fnv.New64a()
	hashUint64(h, m.applied)

	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hashUint64(h, uint64(len(keys)))
	for _, key := range keys {
		hashBytes(h, []byte(key))
		hashBytes(h, m.values[key])
	}

	clientIDs := make([]uint64, 0, len(m.dedup))
	for clientID := range m.dedup {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Slice(clientIDs, func(i, j int) bool { return clientIDs[i] < clientIDs[j] })
	hashUint64(h, uint64(len(clientIDs)))
	for _, clientID := range clientIDs {
		record := m.dedup[clientID]
		hashUint64(h, clientID)
		hashUint64(h, record.lastSeq)
		hashBytes(h, record.lastResult.Value)
		if record.lastResult.Found {
			hashUint64(h, 1)
		} else {
			hashUint64(h, 0)
		}
	}

	return h.Sum64()
}

const (
	mapSnapshotFormatVersion = uint32(1)
	mapSnapshotFilename      = "STATE.pb"
	mapSnapshotTempFilename  = "STATE.pb.tmp"
)

// CreateSnapshot writes one deterministic protobuf containing sorted KV
// pairs, the complete client dedup table, and the server-supplied Raft meta.
// The state lock remains held through file and directory fsync, matching the
// apply-loop quiescence contract.
func (m *MapStateMachine) CreateSnapshot(dir string, meta raft.SnapshotMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if meta.Index != m.applied {
		return fmt.Errorf("mapsm: snapshot index %d does not equal applied index %d", meta.Index, m.applied)
	}
	if meta.Index == 0 && meta.Term != 0 || meta.Index != 0 && meta.Term == 0 {
		return fmt.Errorf("mapsm: invalid snapshot meta index=%d term=%d", meta.Index, meta.Term)
	}

	snapshot := &raftpb.StateMachineSnapshot{
		FormatVersion: mapSnapshotFormatVersion,
		Engine:        raftpb.SnapshotEngine_SNAPSHOT_ENGINE_MAP,
		Meta:          &raftpb.SnapshotMetadata{Index: meta.Index, Term: meta.Term},
	}
	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		snapshot.KeyValues = append(snapshot.KeyValues, &raftpb.SnapshotKeyValue{
			Key:   []byte(key),
			Value: cloneBytes(m.values[key]),
		})
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
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("mapsm: marshal snapshot: %w", err)
	}
	if err := writeMapSnapshot(dir, data); err != nil {
		return err
	}
	m.snapshotDurable = meta.Index
	return nil
}

// RestoreSnapshot validates a complete snapshot before atomically replacing
// the in-memory maps under the state lock.
func (m *MapStateMachine) RestoreSnapshot(dir string) (raft.SnapshotMeta, error) {
	data, err := os.ReadFile(filepath.Join(dir, mapSnapshotFilename))
	if err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("mapsm: read snapshot: %w", err)
	}
	snapshot := &raftpb.StateMachineSnapshot{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		return raft.SnapshotMeta{}, fmt.Errorf("mapsm: decode snapshot: %w", err)
	}
	meta, values, dedup, err := decodeMapSnapshot(snapshot)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}

	m.mu.Lock()
	m.values = values
	m.dedup = dedup
	m.applied = meta.Index
	m.snapshotDurable = meta.Index
	m.mu.Unlock()
	return meta, nil
}

// DurableIndex is the server's belt-and-suspenders truncation watermark.
func (m *MapStateMachine) DurableIndex() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotDurable
}

func decodeMapSnapshot(snapshot *raftpb.StateMachineSnapshot) (raft.SnapshotMeta, map[string][]byte, map[uint64]dedupRecord, error) {
	if snapshot.GetFormatVersion() != mapSnapshotFormatVersion {
		return raft.SnapshotMeta{}, nil, nil, fmt.Errorf("mapsm: snapshot format version %d, want %d", snapshot.GetFormatVersion(), mapSnapshotFormatVersion)
	}
	if snapshot.GetEngine() != raftpb.SnapshotEngine_SNAPSHOT_ENGINE_MAP {
		return raft.SnapshotMeta{}, nil, nil, fmt.Errorf("mapsm: snapshot engine %s, want map", snapshot.GetEngine())
	}
	if snapshot.GetMeta() == nil {
		return raft.SnapshotMeta{}, nil, nil, errors.New("mapsm: snapshot metadata is missing")
	}
	if len(snapshot.GetFiles()) != 0 {
		return raft.SnapshotMeta{}, nil, nil, errors.New("mapsm: map snapshot unexpectedly lists LSM files")
	}
	meta := raft.SnapshotMeta{Index: snapshot.GetMeta().GetIndex(), Term: snapshot.GetMeta().GetTerm()}
	if meta.Index == 0 && meta.Term != 0 || meta.Index != 0 && meta.Term == 0 {
		return raft.SnapshotMeta{}, nil, nil, fmt.Errorf("mapsm: invalid snapshot meta index=%d term=%d", meta.Index, meta.Term)
	}
	values := make(map[string][]byte, len(snapshot.GetKeyValues()))
	var previousKey []byte
	for index, pair := range snapshot.GetKeyValues() {
		if pair == nil {
			return raft.SnapshotMeta{}, nil, nil, fmt.Errorf("mapsm: nil key/value record %d", index)
		}
		if index != 0 && bytes.Compare(previousKey, pair.GetKey()) >= 0 {
			return raft.SnapshotMeta{}, nil, nil, errors.New("mapsm: snapshot keys are not strictly sorted")
		}
		key := cloneBytes(pair.GetKey())
		values[string(key)] = cloneBytes(pair.GetValue())
		previousKey = key
	}
	dedup := make(map[uint64]dedupRecord, len(snapshot.GetDedup()))
	var previousClient uint64
	for index, record := range snapshot.GetDedup() {
		if record == nil {
			return raft.SnapshotMeta{}, nil, nil, fmt.Errorf("mapsm: nil dedup record %d", index)
		}
		if index != 0 && record.GetClientId() <= previousClient {
			return raft.SnapshotMeta{}, nil, nil, errors.New("mapsm: snapshot dedup records are not strictly sorted")
		}
		dedup[record.GetClientId()] = dedupRecord{
			lastSeq: record.GetLastSeq(),
			lastResult: statemachine.Result{
				Value: cloneBytes(record.GetValue()),
				Found: record.GetFound(),
			},
		}
		previousClient = record.GetClientId()
	}
	return meta, values, dedup, nil
}

func writeMapSnapshot(dir string, data []byte) error {
	if dir == "" {
		return errors.New("mapsm: snapshot directory is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mapsm: create snapshot directory: %w", err)
	}
	if err := syncMapDirectory(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("mapsm: sync snapshot parent directory: %w", err)
	}
	tempPath := filepath.Join(dir, mapSnapshotTempFilename)
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mapsm: remove stale snapshot temp: %w", err)
	}
	finalPath := filepath.Join(dir, mapSnapshotFilename)
	if _, err := os.Stat(finalPath); err == nil {
		return fmt.Errorf("mapsm: snapshot file already exists: %s", finalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("mapsm: stat snapshot file: %w", err)
	}
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("mapsm: create snapshot temp: %w", err)
	}
	if n, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("mapsm: write snapshot: %w", err)
	} else if n != len(data) {
		_ = file.Close()
		return fmt.Errorf("mapsm: write snapshot: wrote %d of %d bytes: %w", n, len(data), io.ErrShortWrite)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("mapsm: sync snapshot file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("mapsm: close snapshot file: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("mapsm: publish snapshot file: %w", err)
	}
	if err := syncMapDirectory(dir); err != nil {
		return fmt.Errorf("mapsm: sync snapshot directory: %w", err)
	}
	return nil
}

func syncMapDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func (m *MapStateMachine) advanceAppliedIndex(index uint64) {
	if index > m.applied {
		m.applied = index
	}
}

func cloneResult(result statemachine.Result) statemachine.Result {
	return statemachine.Result{Value: cloneBytes(result.Value), Found: result.Found}
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func hashUint64(h interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = h.Write(encoded[:])
}

func hashBytes(h interface{ Write([]byte) (int, error) }, value []byte) {
	hashUint64(h, uint64(len(value)))
	_, _ = h.Write(value)
}
