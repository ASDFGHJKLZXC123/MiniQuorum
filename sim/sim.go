// Package sim is the Phase 1 sim v0: a single-threaded, deterministic
// virtual-time simulator that drives internal/raft nodes through Tick,
// Step, and the Ready/Advance lifecycle exactly as a production host would.
//
// The scheduler is the only driver. Nothing here spawns a goroutine, reads
// wall time, or lets Go map iteration influence event order; every ordering
// decision is either a fixed slice order or a (virtual time, sequence)
// tie-break on the event heap.
package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"math/rand"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

const (
	defaultTickIntervalMS = VirtualTime(50)
	defaultMinDelayMS     = VirtualTime(1)
	defaultMaxDelayMS     = VirtualTime(10)
)

// Config configures one Sim run. NodeIDs fixes both cluster membership and
// the deterministic tie-break order for same-time events; it must never be
// derived from map iteration by the caller.
type Config struct {
	Seed            int64
	NodeIDs         []raft.NodeID
	ElectionTickMin int
	ElectionTickMax int
	HeartbeatTicks  int
	TickIntervalMS  VirtualTime // default 50
	MinDelayMS      VirtualTime // default 1
	MaxDelayMS      VirtualTime // default 10
}

// simNode is the sim's per-node bookkeeping. storage outlives crashes; it
// models the disk. node is nil while the node is crashed.
//
// generation is bumped on every crash. Tick events carry the generation they
// were scheduled under (see scheduleTick); an event whose generation no
// longer matches sn.generation is stale and is dropped without rescheduling.
// This closes the window where a tick queued before a crash is still
// in-flight when Restart runs: without the check, that stale tick would
// later fire against the freshly restarted node and reschedule itself,
// running a second tick stream alongside the one Restart started.
type simNode struct {
	id         raft.NodeID
	cfg        raft.Config
	storage    storage.Storage
	rnd        *nodeRand
	node       *raft.Node
	halted     bool
	generation uint64

	applied         []raftpb.Entry
	results         []statemachine.Result
	lastApplied     uint64
	sm              statemachine.StateMachine
	newStateMachine func() statemachine.StateMachine
}

type partitionKey struct{ from, to raft.NodeID }

// Sim is a single-threaded deterministic multi-node Raft simulator.
type Sim struct {
	now   VirtualTime
	seq   uint64
	queue eventQueue

	order []raft.NodeID
	nodes map[raft.NodeID]*simNode

	partition map[partitionKey]struct{}

	delayRand    *rand.Rand
	tickInterval VirtualTime
	minDelay     VirtualTime
	maxDelay     VirtualTime

	invariants []InvariantFunc
	leaders    *leaderTracker

	trace []string
}

// NewSim builds a Sim with cfg.NodeIDs as a fully connected cluster, each
// node starting cold (empty InitialState) with its first tick scheduled one
// tick interval out. All randomness (message delay, per-node election
// jitter) derives from cfg.Seed.
func NewSim(cfg Config) (*Sim, error) {
	return newSim(cfg, FaultSchedule{}, false)
}

// newCrashSim builds the same scheduler as NewSim but gives every node a
// CrashStorage driven by schedule. It is intentionally package-private: the
// Phase 3 matrix and later in-package fault harnesses share it without
// changing the frozen public Config surface.
func newCrashSim(cfg Config, schedule FaultSchedule) (*Sim, error) {
	return newSim(cfg, schedule, true)
}

func newSim(cfg Config, schedule FaultSchedule, crashStorage bool) (*Sim, error) {
	if len(cfg.NodeIDs) == 0 {
		return nil, errors.New("sim: Config.NodeIDs must not be empty")
	}
	seen := make(map[raft.NodeID]struct{}, len(cfg.NodeIDs))
	for _, id := range cfg.NodeIDs {
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("sim: duplicate NodeID %d", id)
		}
		seen[id] = struct{}{}
	}

	tickInterval := cfg.TickIntervalMS
	if tickInterval == 0 {
		tickInterval = defaultTickIntervalMS
	}
	minDelay := cfg.MinDelayMS
	maxDelay := cfg.MaxDelayMS
	if minDelay == 0 && maxDelay == 0 {
		minDelay, maxDelay = defaultMinDelayMS, defaultMaxDelayMS
	}
	if maxDelay < minDelay {
		return nil, fmt.Errorf("sim: MaxDelayMS %d < MinDelayMS %d", maxDelay, minDelay)
	}

	order := append([]raft.NodeID(nil), cfg.NodeIDs...)
	delayRand, perNode := newRandStreams(cfg.Seed, order)

	s := &Sim{
		order:        order,
		nodes:        make(map[raft.NodeID]*simNode, len(order)),
		partition:    make(map[partitionKey]struct{}),
		delayRand:    delayRand,
		tickInterval: tickInterval,
		minDelay:     minDelay,
		maxDelay:     maxDelay,
		leaders:      newLeaderTracker(),
		// Log matching is a universal Phase 2 safety property, not an
		// opt-in scenario assertion. Run checks it after every event.
		invariants: []InvariantFunc{LogMatching},
	}

	for _, id := range order {
		// The frozen raft.Config.Peers contract is full ordered cluster
		// membership, including the local node itself (see raft_test.go and
		// Node.hasMajority, which divides by len(peers)). Each node gets its
		// own copy so no two nodes ever alias the same backing array.
		peers := append([]raft.NodeID(nil), order...)
		rc := raft.Config{
			ID:              id,
			Peers:           peers,
			ElectionTickMin: cfg.ElectionTickMin,
			ElectionTickMax: cfg.ElectionTickMax,
			HeartbeatTicks:  cfg.HeartbeatTicks,
		}
		rnd := perNode[id]
		var store storage.Storage = storage.NewMemStorage()
		if crashStorage {
			var err error
			store, err = NewCrashStorage(id, schedule)
			if err != nil {
				return nil, err
			}
		}
		sn := &simNode{
			id:      id,
			cfg:     rc,
			storage: store,
			rnd:     rnd,
			node:    raft.NewNode(rc, raft.InitialState{}, rnd),
		}
		s.nodes[id] = sn
	}
	for _, id := range order {
		s.scheduleTick(id, s.tickInterval)
	}
	return s, nil
}

// setStateMachineFactory installs a fresh deterministic state machine on
// every node and remembers how to replace it after a simulated process
// restart. The factory runs synchronously on the simulator thread.
func (s *Sim) setStateMachineFactory(factory func() statemachine.StateMachine) {
	for _, id := range s.order {
		sn := s.nodes[id]
		sn.newStateMachine = factory
		if factory == nil {
			sn.sm = nil
			continue
		}
		sn.sm = factory()
	}
}

// RegisterInvariant adds an invariant check run, in registration order,
// after every processed event.
func (s *Sim) RegisterInvariant(fn InvariantFunc) { s.invariants = append(s.invariants, fn) }

// Partition blocks messages sent from -> to. The mask is an ordered pair:
// blocking from->to does not block to->from unless added separately.
func (s *Sim) Partition(from, to raft.NodeID) {
	s.partition[partitionKey{from, to}] = struct{}{}
}

// Heal removes a previously installed from->to block.
func (s *Sim) Heal(from, to raft.NodeID) {
	delete(s.partition, partitionKey{from, to})
}

func (s *Sim) blocked(from, to raft.NodeID) bool {
	_, ok := s.partition[partitionKey{from, to}]
	return ok
}

// ScheduleCrash discards node id's live *raft.Node at virtual time at. Its
// sim storage survives, per the sim crash model.
func (s *Sim) ScheduleCrash(id raft.NodeID, at VirtualTime) {
	s.pushAt(at, &event{kind: eventCrash, node: id})
}

// ScheduleRestart rebuilds node id from its surviving sim storage at virtual
// time at, and resumes its tick schedule.
func (s *Sim) ScheduleRestart(id raft.NodeID, at VirtualTime) {
	s.pushAt(at, &event{kind: eventRestart, node: id})
}

// Now returns the sim's current virtual time.
func (s *Sim) Now() VirtualTime { return s.now }

// Trace returns the ordered, deterministic event log recorded so far. Two
// Sim runs built from identical Config (same seed) produce identical traces.
func (s *Sim) Trace() []string { return append([]string(nil), s.trace...) }

// HighestTerm returns the greatest term durably recorded by any simulated
// node. It is an inspection helper for scenario assertions; it does not
// expose or alter Raft's internal state.
func (s *Sim) HighestTerm() uint64 {
	var highest uint64
	for _, id := range s.order {
		hs, err := s.nodes[id].storage.HardState()
		if err == nil && hs.Term > highest {
			highest = hs.Term
		}
	}
	return highest
}

// Leaderships returns a stable snapshot of the leaders observed from emitted
// AppendEntries messages, grouped by term. The returned slices and map are
// copies, so callers cannot affect invariant tracking.
func (s *Sim) Leaderships() map[uint64][]raft.NodeID {
	observed := make(map[uint64][]raft.NodeID, len(s.leaders.leaders))
	for term, set := range s.leaders.leaders {
		ids := make([]raft.NodeID, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		observed[term] = sortedIDs(ids)
	}
	return observed
}

// Propose synchronously supplies a client proposal to one simulated node and
// processes the resulting Ready through the same persistence/send/apply/
// Advance path as ticks and inbound messages. A down or fail-stopped node is
// reported as not leader.
func (s *Sim) Propose(id raft.NodeID, data []byte) (index, term uint64, isLeader bool) {
	sn, ok := s.nodes[id]
	if !ok || sn.node == nil || sn.halted {
		s.record("propose node=%d reject(unavailable)", id)
		return 0, 0, false
	}
	index, term, isLeader = sn.node.Propose(data)
	s.record("propose node=%d index=%d term=%d leader=%t", id, index, term, isLeader)
	s.processReady(sn, sn.node.Ready())
	return index, term, isLeader
}

// Log returns a deep copy of the entries durably saved for a simulated node,
// in index order. It is an inspection helper for deterministic scenarios.
func (s *Sim) Log(id raft.NodeID) []raftpb.Entry {
	sn, ok := s.nodes[id]
	if !ok {
		return nil
	}
	first, last := sn.storage.FirstIndex(), sn.storage.LastIndex()
	if last < first {
		return nil
	}
	entries, err := sn.storage.Entries(first, last+1)
	if err != nil {
		return nil
	}
	return cloneSimEntries(entries)
}

// AppliedEntries returns a deep copy of every committed entry handed to the
// simulator's synchronous apply step, in application order.
func (s *Sim) AppliedEntries(id raft.NodeID) []raftpb.Entry {
	sn, ok := s.nodes[id]
	if !ok {
		return nil
	}
	return cloneSimEntries(sn.applied)
}

// LastApplied returns the greatest committed index processed by the
// simulator's synchronous apply step for id.
func (s *Sim) LastApplied(id raft.NodeID) uint64 {
	sn, ok := s.nodes[id]
	if !ok {
		return 0
	}
	return sn.lastApplied
}

// Run drains the event queue through virtual time `until` (inclusive),
// running every registered invariant after each processed event. It returns
// the first invariant error encountered, if any.
func (s *Sim) Run(until VirtualTime) error {
	for s.queue.Len() > 0 {
		if s.queue[0].time > until {
			break
		}
		ev := heap.Pop(&s.queue).(*event)
		s.now = ev.time
		s.handleEvent(ev)
		for _, inv := range s.invariants {
			if err := inv(s); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Sim) pushAt(at VirtualTime, e *event) {
	e.time = at
	e.seq = s.seq
	s.seq++
	heap.Push(&s.queue, e)
}

func (s *Sim) scheduleTick(id raft.NodeID, at VirtualTime) {
	s.pushAt(at, &event{kind: eventTick, node: id, generation: s.nodes[id].generation})
}

// tickIsStale reports whether ev (an eventTick) was scheduled under a
// generation of its node that a subsequent crash has since invalidated.
func (s *Sim) tickIsStale(ev *event) bool {
	sn := s.nodes[ev.node]
	return sn == nil || ev.generation != sn.generation
}

func (s *Sim) scheduleMessage(m *raftpb.Message) {
	span := int64(s.maxDelay - s.minDelay + 1)
	delay := s.minDelay + VirtualTime(s.delayRand.Int63n(span))
	s.pushAt(s.now+delay, &event{kind: eventMessage, msg: m})
}

func (s *Sim) record(format string, args ...any) {
	s.trace = append(s.trace, fmt.Sprintf("t=%d %s", s.now, fmt.Sprintf(format, args...)))
}

func cloneSimEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]raftpb.Entry, 0, len(entries))
	for i := range entries {
		cloned = append(cloned, raftpb.Entry{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  entries[i].Type,
			Data:  append([]byte(nil), entries[i].Data...),
		})
	}
	return cloned
}
