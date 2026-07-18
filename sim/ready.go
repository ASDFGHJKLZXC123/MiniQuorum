package sim

import (
	"errors"
	"fmt"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

func (s *Sim) handleEvent(ev *event) {
	switch ev.kind {
	case eventTick:
		if s.tickIsStale(ev) {
			s.record("tick node=%d skip(stale gen=%d current=%d)", ev.node, ev.generation, s.nodes[ev.node].generation)
			return
		}
		s.handleTick(ev.node)
	case eventMessage:
		s.handleMessage(ev.msg)
	case eventCrash:
		s.handleCrash(ev.node)
	case eventRestart:
		s.handleRestart(ev.node)
	case eventFault:
		s.applyFault(ev.fault)
	}
}

func (s *Sim) handleTick(id raft.NodeID) {
	sn := s.nodes[id]
	if sn.node == nil || sn.halted {
		s.record("tick node=%d skip(down)", id)
		return
	}
	if sn.paused {
		// A GC pause / VM freeze does not stop the wall clock: the node's
		// tick schedule keeps advancing underneath it so ticking resumes at
		// the right cadence on FaultResume, but the tick itself has no
		// effect while frozen.
		s.record("tick node=%d skip(paused)", id)
		s.scheduleTick(id, s.now+s.nextTickDelay(sn))
		return
	}
	sn.node.Tick()
	s.record("tick node=%d", id)
	s.processReady(sn, sn.node.Ready())
	if sn.node != nil && !sn.halted {
		s.scheduleTick(id, s.now+s.nextTickDelay(sn))
	}
}

func (s *Sim) handleMessage(m *raftpb.Message) {
	from, to := raft.NodeID(m.GetFrom()), raft.NodeID(m.GetTo())
	if s.blocked(from, to) {
		s.record("msg %d->%d drop(partition)", from, to)
		return
	}
	sn, ok := s.nodes[to]
	if !ok || sn.node == nil || sn.halted {
		s.record("msg %d->%d drop(unreachable)", from, to)
		return
	}
	if sn.paused {
		s.record("msg %d->%d drop(paused)", from, to)
		return
	}
	sn.node.Step(m)
	s.record("msg %d->%d deliver", from, to)
	s.processReady(sn, sn.node.Ready())
}

func (s *Sim) handleCrash(id raft.NodeID) {
	sn := s.nodes[id]
	if store, ok := sn.storage.(*CrashStorage); ok && !store.Crashed() {
		if err := store.Crash(); err != nil {
			s.record("crash node=%d storage_error=%v", id, err)
		}
	}
	s.markProcessCrashed(sn)
	s.record("crash node=%d", id)
}

func (s *Sim) handleRestart(id raft.NodeID) {
	sn := s.nodes[id]
	if sn.node != nil && !sn.halted {
		// Restart only ever follows a crash (sn.node == nil) or a fail-stop
		// (sn.halted, whose tick stream already died out with no reschedule
		// in handleTick's skip(down) path). A still-ticking live node has
		// exactly one pending tick event outstanding under its current
		// generation; rebuilding it here without discarding that node would
		// schedule a second, independent tick stream at the bottom of this
		// function (generation is only bumped by a real crash), so a
		// restart racing a live node is rejected instead of silently
		// running two tick streams for one node.
		s.record("restart node=%d skip(live)", id)
		return
	}
	if store, ok := sn.storage.(*CrashStorage); ok && store.Crashed() {
		if err := store.Recover(); err != nil {
			sn.halted = true
			s.record("restart node=%d recovery_error=%v fail-stop", id, err)
			return
		}
	}
	init, err := recoveredInitialState(sn.storage)
	if err != nil {
		sn.halted = true
		s.record("restart node=%d recovery_error=%v fail-stop", id, err)
		return
	}
	sn.node = raft.NewNode(sn.cfg, init, sn.rnd)
	sn.halted = false
	if sn.newStateMachine != nil {
		sn.sm = sn.newStateMachine()
		sn.applied = nil
		sn.results = nil
		sn.lastApplied = 0
	}
	s.record("restart node=%d", id)
	s.scheduleTick(id, s.now+s.nextTickDelay(sn))
}

func (s *Sim) markProcessCrashed(sn *simNode) {
	sn.node = nil
	sn.halted = false
	sn.generation++
	// A crashed process cannot also be "paused" (frozen but alive); clear it
	// so a later restart starts fresh rather than inheriting a stale freeze
	// from before the crash.
	sn.paused = false
}

// processReady applies the frozen Ready contract in the mandated order:
// Storage.Save, then send messages, then apply committed entries in order,
// then Node.Advance. A Save error is fail-stop for this node's batch: no
// send, no apply, no Advance.
func (s *Sim) processReady(sn *simNode, rd raft.Ready) {
	if err := sn.storage.Save(rd.HardState, rd.Entries); err != nil {
		if errors.Is(err, ErrCrashed) {
			s.markProcessCrashed(sn)
			s.record("node=%d save_crash %v", sn.id, err)
			return
		}
		sn.halted = true
		s.record("node=%d save_error %v fail-stop", sn.id, err)
		return
	}
	for _, m := range rd.Messages {
		s.observeLeader(m)
		s.scheduleMessage(m)
	}
	if store, ok := sn.storage.(*CrashStorage); ok && store.CrashIfArmed() {
		info, _ := store.LastCrash()
		s.markProcessCrashed(sn)
		s.record("node=%d after_send_crash save=%d", sn.id, info.Save)
		return
	}
	for i := range rd.CommittedEntries {
		var result statemachine.Result
		if sn.sm != nil {
			var err error
			result, err = sn.sm.Apply(&rd.CommittedEntries[i])
			if err != nil {
				sn.halted = true
				s.record("node=%d apply_error index=%d %v fail-stop", sn.id, rd.CommittedEntries[i].Index, err)
				return
			}
		}
		sn.applied = append(sn.applied, raftpb.Entry{
			Index: rd.CommittedEntries[i].Index,
			Term:  rd.CommittedEntries[i].Term,
			Type:  rd.CommittedEntries[i].Type,
			Data:  append([]byte(nil), rd.CommittedEntries[i].Data...),
		})
		sn.results = append(sn.results, statemachine.Result{
			Value: append([]byte(nil), result.Value...),
			Found: result.Found,
		})
		sn.lastApplied = rd.CommittedEntries[i].Index
	}
	sn.node.Advance()
	s.record("node=%d ready hardstate=%v msgs=%d committed=%d", sn.id, rd.HardState != nil, len(rd.Messages), len(rd.CommittedEntries))
}

func recoveredInitialState(store storageReader) (raft.InitialState, error) {
	hard, err := store.HardState()
	if err != nil {
		return raft.InitialState{}, err
	}
	first, last := store.FirstIndex(), store.LastIndex()
	var entries []raftpb.Entry
	if last >= first {
		if last == ^uint64(0) {
			return raft.InitialState{}, fmt.Errorf("sim: last index overflows half-open range")
		}
		entries, err = store.Entries(first, last+1)
		if err != nil {
			return raft.InitialState{}, err
		}
	}
	return raft.InitialState{HardState: hard, Entries: entries}, nil
}

type storageReader interface {
	HardState() (raft.HardState, error)
	Entries(lo, hi uint64) ([]raftpb.Entry, error)
	FirstIndex() uint64
	LastIndex() uint64
}

// observeLeader records term/leader observations strictly from a
// deterministic Raft output: only a Leader ever emits AppendEntriesReq
// (heartbeats), per the phase-1 spec, so seeing one from a node is proof of
// that node's leadership for the message's term. No raft-internal role
// field is read.
func (s *Sim) observeLeader(m *raftpb.Message) {
	if m.GetAppendEntries() != nil {
		s.leaders.observe(m.GetTerm(), raft.NodeID(m.GetFrom()))
	}
}
