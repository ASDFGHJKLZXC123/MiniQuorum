package sim

import (
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func (s *Sim) handleEvent(ev *event) {
	switch ev.kind {
	case eventTick:
		s.handleTick(ev.node)
	case eventMessage:
		s.handleMessage(ev.msg)
	case eventCrash:
		s.handleCrash(ev.node)
	case eventRestart:
		s.handleRestart(ev.node)
	}
}

func (s *Sim) handleTick(id raft.NodeID) {
	sn := s.nodes[id]
	if sn.node == nil || sn.halted {
		s.record("tick node=%d skip(down)", id)
		return
	}
	sn.node.Tick()
	s.record("tick node=%d", id)
	s.processReady(sn, sn.node.Ready())
	s.scheduleTick(id, s.now+s.tickInterval)
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
	sn.node.Step(m)
	s.record("msg %d->%d deliver", from, to)
	s.processReady(sn, sn.node.Ready())
}

func (s *Sim) handleCrash(id raft.NodeID) {
	sn := s.nodes[id]
	sn.node = nil
	sn.halted = false
	s.record("crash node=%d", id)
}

func (s *Sim) handleRestart(id raft.NodeID) {
	sn := s.nodes[id]
	hs, _ := sn.storage.HardState()
	var entries []raftpb.Entry
	if first, last := sn.storage.FirstIndex(), sn.storage.LastIndex(); last >= first {
		entries, _ = sn.storage.Entries(first, last+1)
	}
	init := raft.InitialState{HardState: hs, Entries: entries}
	sn.node = raft.NewNode(sn.cfg, init, sn.rnd)
	sn.halted = false
	s.record("restart node=%d", id)
	s.scheduleTick(id, s.now+s.tickInterval)
}

// processReady applies the frozen Ready contract in the mandated order:
// Storage.Save, then send messages, then apply committed entries in order,
// then Node.Advance. A Save error is fail-stop for this node's batch: no
// send, no apply, no Advance.
func (s *Sim) processReady(sn *simNode, rd raft.Ready) {
	if err := sn.storage.Save(rd.HardState, rd.Entries); err != nil {
		sn.halted = true
		s.record("node=%d save_error %v fail-stop", sn.id, err)
		return
	}
	for _, m := range rd.Messages {
		s.observeLeader(m)
		s.scheduleMessage(m)
	}
	// Phase 1 sim v0 has no state machine to apply into (Phase 2); the loop
	// exists to preserve the mandated Save -> send -> apply -> Advance order.
	for range rd.CommittedEntries {
	}
	sn.node.Advance()
	s.record("node=%d ready hardstate=%v msgs=%d committed=%d", sn.id, rd.HardState != nil, len(rd.Messages), len(rd.CommittedEntries))
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
