package sim

import (
	"errors"
	"testing"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

// spyStorage records Save calls and can be told to fail, so tests can
// observe the Save -> send -> apply -> Advance order and the fail-stop path
// without depending on internal/raft doing anything (it is still the
// Phase 0 stub on this branch).
type spyStorage struct {
	*storage.MemStorage
	saveCalls int
	failSave  bool
}

func newSpyStorage() *spyStorage { return &spyStorage{MemStorage: storage.NewMemStorage()} }

func (s *spyStorage) Save(hs *raft.HardState, entries []raftpb.Entry) error {
	s.saveCalls++
	if s.failSave {
		return errors.New("spy: forced save failure")
	}
	return s.MemStorage.Save(hs, entries)
}

func newTestSim(t *testing.T, ids ...raft.NodeID) *Sim {
	t.Helper()
	s, err := NewSim(Config{Seed: 1, NodeIDs: ids})
	if err != nil {
		t.Fatalf("NewSim() error = %v", err)
	}
	return s
}

func heartbeatMessage(from, to raft.NodeID, term uint64) *raftpb.Message {
	return &raftpb.Message{
		From: uint64(from),
		To:   uint64(to),
		Term: term,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{}},
	}
}

// TestProcessReadySendsOnlyAfterSuccessfulSave verifies the mandated order:
// a successful Storage.Save is followed by message scheduling and Advance.
func TestProcessReadySendsOnlyAfterSuccessfulSave(t *testing.T) {
	s := newTestSim(t, 1, 2)
	sn := s.nodes[1]
	spy := newSpyStorage()
	sn.storage = spy

	before := s.queue.Len()
	rd := raft.Ready{
		HardState: &raft.HardState{Term: 1, VotedFor: 1},
		Messages:  []*raftpb.Message{heartbeatMessage(1, 2, 1)},
	}
	s.processReady(sn, rd)

	if spy.saveCalls != 1 {
		t.Fatalf("saveCalls = %d, want 1", spy.saveCalls)
	}
	if sn.halted {
		t.Fatal("node halted after a successful Save, want not halted")
	}
	if got := s.queue.Len(); got != before+1 {
		t.Fatalf("queue len = %d, want %d (one message scheduled)", got, before+1)
	}
	if got, err := spy.HardState(); err != nil || got.Term != 1 {
		t.Fatalf("persisted HardState = %+v, err = %v, want Term 1", got, err)
	}
}

// TestProcessReadyFailStopsOnSaveError verifies a Save error is fail-stop:
// no message is scheduled and the node is marked halted, per CLAUDE.md.
func TestProcessReadyFailStopsOnSaveError(t *testing.T) {
	s := newTestSim(t, 1, 2)
	sn := s.nodes[1]
	spy := newSpyStorage()
	spy.failSave = true
	sn.storage = spy

	before := s.queue.Len()
	rd := raft.Ready{
		HardState: &raft.HardState{Term: 1, VotedFor: 1},
		Messages:  []*raftpb.Message{heartbeatMessage(1, 2, 1)},
	}
	s.processReady(sn, rd)

	if !sn.halted {
		t.Fatal("node not halted after a failed Save, want halted (fail-stop)")
	}
	if got := s.queue.Len(); got != before {
		t.Fatalf("queue len = %d, want %d (no message scheduled after Save error)", got, before)
	}
}

// TestObserveLeaderFromAppendEntries verifies leadership is recorded purely
// from a deterministic Raft output (an emitted AppendEntriesReq), never
// from a raft-internal role field.
func TestObserveLeaderFromAppendEntries(t *testing.T) {
	s := newTestSim(t, 1, 2, 3)
	s.observeLeader(heartbeatMessage(1, 2, 7))
	if n := s.leaders.leaders[7][1]; !n {
		t.Fatal("leader 1 not observed at term 7")
	}

	// A RequestVote message must never be mistaken for a leadership signal.
	vote := &raftpb.Message{
		From: 2, To: 1, Term: 7,
		Body: &raftpb.Message_RequestVote{RequestVote: &raftpb.RequestVoteReq{}},
	}
	s.observeLeader(vote)
	if len(s.leaders.leaders[7]) != 1 {
		t.Fatalf("leaders[7] = %v, want only node 1 (RequestVote must not count)", s.leaders.leaders[7])
	}
}

// TestSingleLeaderPerTermFiresOnSecondLeader exercises the invariant hook
// end to end: two distinct nodes observed as leader in the same term must
// trip SingleLeaderPerTerm.
func TestSingleLeaderPerTermFiresOnSecondLeader(t *testing.T) {
	s := newTestSim(t, 1, 2, 3)
	s.observeLeader(heartbeatMessage(1, 2, 3))
	if err := SingleLeaderPerTerm(s); err != nil {
		t.Fatalf("SingleLeaderPerTerm() = %v, want nil after one leader", err)
	}
	s.observeLeader(heartbeatMessage(2, 3, 3))
	if err := SingleLeaderPerTerm(s); err == nil {
		t.Fatal("SingleLeaderPerTerm() = nil, want a violation after two leaders in one term")
	}
}
