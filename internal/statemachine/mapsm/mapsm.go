// Package mapsm implements the Phase 2 in-memory map state machine.
package mapsm

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
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

// CreateSnapshot is a Phase 6 stub.
func (*MapStateMachine) CreateSnapshot(string, raft.SnapshotMeta) error {
	return statemachine.ErrSnapshotUnsupported
}

// RestoreSnapshot is a Phase 6 stub.
func (*MapStateMachine) RestoreSnapshot(string) (raft.SnapshotMeta, error) {
	return raft.SnapshotMeta{}, statemachine.ErrSnapshotUnsupported
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
