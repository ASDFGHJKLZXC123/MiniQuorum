package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

// StateMachine adapts Engine to the frozen replicated-state-machine
// interface. Engine owns durable LSM state; StateMachine owns the ephemeral
// deduplication records and the logical replay view used to rebuild them.
//
// StateMachine intentionally owns no second lock: the Engine's one RWMutex
// protects durable state and this logical/dedup/replay state together.
type StateMachine struct {
	engine *Engine

	// values is the logical key/value state reconstructed from the retained
	// Raft log. It is intentionally retained after recovery: Hash must include
	// exactly the state represented by the complete replay without depending
	// on physical LSM layout.
	values  map[string][]byte
	dedup   map[uint64]stateDedupRecord
	applied uint64

	// A reopened pre-Phase-6 engine has durable tables which can contain
	// entries later than an early retained-log GET. While applying the retained
	// prefix through replayCeiling, GET therefore reads values rather than the
	// normal LSM lookup path. replayCeiling is the last retained Raft-log index
	// captured by the host at process start, never FlushedIndex.
	replaying     bool
	replayCeiling uint64
}

type stateDedupRecord struct {
	lastSeq    uint64
	lastResult statemachine.Result
}

var _ statemachine.StateMachine = (*StateMachine)(nil)

// OpenStateMachine opens one persistent LSM state machine. dir is the
// engine-only directory; callers keep their Raft log in its existing data
// directory and must replay the complete retained log from index zero after a
// process restart. FlushedIndex is deliberately not used as a replay start.
func OpenStateMachine(dir string, options Options) (*StateMachine, error) {
	engine, err := Open(dir, options)
	if err != nil {
		return nil, err
	}
	return &StateMachine{
		engine: engine,
		values: make(map[string][]byte),
		dedup:  make(map[uint64]stateDedupRecord),
	}, nil
}

// BeginReplay records the exact retained-log interval for a pre-snapshot
// process restart. It never applies entries itself: Raft still establishes
// commitment and emits them through the ordinary Ready lifecycle. Retained
// entries must begin at index 1 until Phase 6 introduces snapshots.
func (m *StateMachine) BeginReplay(firstIndex, lastIndex uint64) error {
	m.engine.mu.Lock()
	defer m.engine.mu.Unlock()
	if lastIndex < firstIndex {
		m.replaying = false
		m.replayCeiling = 0
		return nil
	}
	if firstIndex != 1 {
		return fmt.Errorf("lsm: pre-snapshot replay starts at %d, want 1", firstIndex)
	}
	m.replaying = true
	m.replayCeiling = lastIndex
	return nil
}

// Engine exposes the underlying engine for force-flush and storage lifecycle
// seams. Callers must not bypass StateMachine.Apply for replicated writes.
func (m *StateMachine) Engine() *Engine { return m.engine }

// Close releases engine file handles. It is deliberately outside the frozen
// StateMachine interface because ordinary MapStateMachine instances have no
// durable resources to close.
func (m *StateMachine) Close() error {
	return m.engine.Close()
}

// Crash discards this process's volatile engine state. SimFS implements the
// same crash boundary as the Phase 3 storage model; real files are left to
// the operating-system process crash and are merely closed here for explicit
// simulator lifecycle ownership.
func (m *StateMachine) Crash() error {
	if crashFS, ok := m.engine.fs.(*SimFS); ok {
		if crashFS.Crashed() {
			m.engine.Abandon()
			return nil
		}
		// Never gracefully close before a simulated crash: Close may remove
		// obsolete files or SyncDir and would erase the crash window under
		// test. Crash first, then abandon this process-local engine.
		err := crashFS.Crash(RetainAllUnsynced)
		m.engine.Abandon()
		return err
	}
	return m.engine.Close()
}

// FinishReplay is an optional host lifecycle seam for a host that knows the
// exact retained-log boundary. The normal recovery path also leaves replay
// mode automatically once it applies that captured boundary entry. The
// reconstructed logical state remains in memory for Hash and deduplication.
func (m *StateMachine) FinishReplay() {
	m.engine.mu.Lock()
	defer m.engine.mu.Unlock()
	m.replaying = false
}

// Apply synchronously processes one committed entry without retaining or
// mutating it. All filesystem/engine errors are returned to the host so the
// existing Ready lifecycle fail-stops before Node.Advance.
func (m *StateMachine) Apply(entry *raftpb.Entry) (statemachine.Result, error) {
	if entry == nil {
		return statemachine.Result{}, fmt.Errorf("apply nil entry")
	}

	m.engine.mu.Lock()
	defer m.engine.mu.Unlock()

	if m.replaying && entry.GetIndex() > m.replayCeiling {
		m.replaying = false
	}

	switch entry.GetType() {
	case raftpb.EntryType_NOOP:
		if err := m.advanceAppliedLocked(entry.GetIndex()); err != nil {
			return statemachine.Result{}, err
		}
		m.finishReplayLocked(entry.GetIndex())
		return statemachine.Result{}, nil
	case raftpb.EntryType_NORMAL:
		var command raftpb.Command
		if err := proto.Unmarshal(entry.GetData(), &command); err != nil {
			return statemachine.Result{}, fmt.Errorf("decode command: %w", err)
		}

		if record, ok := m.dedup[command.GetClientId()]; ok && command.GetSeq() <= record.lastSeq {
			if err := m.advanceAppliedLocked(entry.GetIndex()); err != nil {
				return statemachine.Result{}, err
			}
			m.finishReplayLocked(entry.GetIndex())
			return cloneStateResult(record.lastResult), nil
		}

		result, err := m.executeLocked(&command, entry.GetIndex())
		if err != nil {
			return statemachine.Result{}, err
		}
		m.dedup[command.GetClientId()] = stateDedupRecord{
			lastSeq:    command.GetSeq(),
			lastResult: cloneStateResult(result),
		}
		if entry.GetIndex() > m.applied {
			// PUT and DELETE advance Engine's applied index as part of their
			// accepted write; GET advances it in executeLocked.
			m.applied = entry.GetIndex()
		}
		m.finishReplayLocked(entry.GetIndex())
		return cloneStateResult(result), nil
	default:
		return statemachine.Result{}, fmt.Errorf("unsupported entry type %s", entry.GetType())
	}
}

func (m *StateMachine) finishReplayLocked(index uint64) {
	if m.replaying && index >= m.replayCeiling {
		m.replaying = false
	}
}

func (m *StateMachine) executeLocked(command *raftpb.Command, index uint64) (statemachine.Result, error) {
	key := command.GetKey()
	switch command.GetOp() {
	case raftpb.Op_PUT:
		if err := m.engine.putLocked(key, command.GetValue(), index); err != nil {
			return statemachine.Result{}, err
		}
		m.values[string(key)] = cloneBytes(command.GetValue())
		return statemachine.Result{}, nil
	case raftpb.Op_DELETE:
		if err := m.engine.deleteLocked(key, index); err != nil {
			return statemachine.Result{}, err
		}
		delete(m.values, string(key))
		return statemachine.Result{}, nil
	case raftpb.Op_GET:
		var result statemachine.Result
		if m.replaying {
			value, found := m.values[string(key)]
			result = statemachine.Result{Value: cloneBytes(value), Found: found}
		} else {
			value, found, err := m.engine.readLocked(key)
			if err != nil {
				return statemachine.Result{}, err
			}
			result = statemachine.Result{Value: value, Found: found}
		}
		if err := m.advanceAppliedLocked(index); err != nil {
			return statemachine.Result{}, err
		}
		return result, nil
	default:
		return statemachine.Result{}, fmt.Errorf("unsupported command op %s", command.GetOp())
	}
}

func (m *StateMachine) advanceAppliedLocked(index uint64) error {
	if index <= m.applied {
		return nil
	}
	if err := m.engine.advanceAppliedIndexLocked(index); err != nil {
		return err
	}
	m.applied = index
	return nil
}

// Read uses the normal complete Engine read path and is safe concurrently
// with Apply. Historical replay is intentionally never used for this Phase 6
// direct-read surface.
func (m *StateMachine) Read(key []byte) (statemachine.Result, error) {
	m.engine.mu.RLock()
	defer m.engine.mu.RUnlock()
	value, found, err := m.engine.readLocked(key)
	if err != nil {
		return statemachine.Result{}, err
	}
	return statemachine.Result{Value: value, Found: found}, nil
}

// AppliedIndex reports the greatest successfully applied Raft log index.
func (m *StateMachine) AppliedIndex() uint64 {
	m.engine.mu.RLock()
	defer m.engine.mu.RUnlock()
	return m.applied
}

// Hash implements exactly the mapsm deterministic full-state digest: applied
// index, ordered logical key/value state, then ordered dedup records including
// the cached result bytes and Found bit.
func (m *StateMachine) Hash() uint64 {
	m.engine.mu.RLock()
	defer m.engine.mu.RUnlock()

	h := fnv.New64a()
	hashStateUint64(h, m.applied)

	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hashStateUint64(h, uint64(len(keys)))
	for _, key := range keys {
		hashStateBytes(h, []byte(key))
		hashStateBytes(h, m.values[key])
	}

	clientIDs := make([]uint64, 0, len(m.dedup))
	for clientID := range m.dedup {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Slice(clientIDs, func(i, j int) bool { return clientIDs[i] < clientIDs[j] })
	hashStateUint64(h, uint64(len(clientIDs)))
	for _, clientID := range clientIDs {
		record := m.dedup[clientID]
		hashStateUint64(h, clientID)
		hashStateUint64(h, record.lastSeq)
		hashStateBytes(h, record.lastResult.Value)
		if record.lastResult.Found {
			hashStateUint64(h, 1)
		} else {
			hashStateUint64(h, 0)
		}
	}

	return h.Sum64()
}

// CreateSnapshot is a Phase 6 stub.
func (*StateMachine) CreateSnapshot(string, raft.SnapshotMeta) error {
	return statemachine.ErrSnapshotUnsupported
}

// RestoreSnapshot is a Phase 6 stub.
func (*StateMachine) RestoreSnapshot(string) (raft.SnapshotMeta, error) {
	return raft.SnapshotMeta{}, statemachine.ErrSnapshotUnsupported
}

func cloneStateResult(result statemachine.Result) statemachine.Result {
	return statemachine.Result{Value: cloneBytes(result.Value), Found: result.Found}
}

func hashStateUint64(h interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = h.Write(encoded[:])
}

func hashStateBytes(h interface{ Write([]byte) (int, error) }, value []byte) {
	hashStateUint64(h, uint64(len(value)))
	_, _ = h.Write(value)
}
