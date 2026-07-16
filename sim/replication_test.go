package sim

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestSimulatorProposeReplicatesAndAppliesSynchronously(t *testing.T) {
	s := phase1Sim(t, 208)
	term, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	var follower raft.NodeID
	for _, id := range s.order {
		if id != leader {
			follower = id
			break
		}
	}
	if index, gotTerm, ok := s.Propose(follower, []byte("must-reject")); ok || index != 0 || gotTerm != term {
		t.Fatalf("follower Propose = (%d,%d,%v), want (0,%d,false)", index, gotTerm, ok, term)
	}

	data := []byte("replicated-command")
	index, gotTerm, ok := s.Propose(leader, data)
	if !ok || gotTerm != term || index == 0 {
		t.Fatalf("leader Propose = (%d,%d,%v), want nonzero index in term %d", index, gotTerm, ok, term)
	}
	data[0] = 'X'
	runFor(t, s, 3*phase1Window)

	for _, id := range s.order {
		if !simContainsEntry(s.Log(id), index, term, raftpb.EntryType_NORMAL, []byte("replicated-command")) {
			t.Fatalf("node %d log = %s, want proposal at (%d,%d)", id, simEntries(s.Log(id)), index, term)
		}
		if !simContainsEntry(s.AppliedEntries(id), index, term, raftpb.EntryType_NORMAL, []byte("replicated-command")) {
			t.Fatalf("node %d applied = %s, want committed proposal at (%d,%d)", id, simEntries(s.AppliedEntries(id)), index, term)
		}
		if got := s.LastApplied(id); got < index {
			t.Fatalf("node %d LastApplied = %d, want at least %d", id, got, index)
		}
	}
}

func TestSimulatorNOOPVisibleAtEveryObservedTermStart(t *testing.T) {
	s := phase1Sim(t, 218)
	firstTerm, firstLeader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	runFor(t, s, 2*phase1Window)
	s.ScheduleCrash(firstLeader, s.Now())
	runFor(t, s, 5*phase1Window)

	leaderships := s.Leaderships()
	if len(leaderships) < 2 {
		t.Fatalf("leaderships = %v, want at least terms %d and a successor", leaderships, firstTerm)
	}
	terms := make([]uint64, 0, len(leaderships))
	for term := range leaderships {
		terms = append(terms, term)
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i] < terms[j] })
	for _, term := range terms {
		leaders := leaderships[term]
		for _, leader := range leaders {
			if !simContainsNOOP(s.Log(leader), term) {
				t.Fatalf("leader %d log = %s, want term-start NOOP for observed term %d", leader, simEntries(s.Log(leader)), term)
			}
		}
	}
}

func TestSimulatorProposalPathIsSameSeedDeterministic(t *testing.T) {
	run := func() []string {
		s, err := NewSim(Config{Seed: 228, NodeIDs: []raft.NodeID{1, 2, 3}})
		if err != nil {
			t.Fatalf("NewSim() error: %v", err)
		}
		s.RegisterInvariant(SingleLeaderPerTerm)
		if err := s.Run(3 * phase1Window); err != nil {
			t.Fatalf("initial Run() error: %v", err)
		}
		term := s.HighestTerm()
		leaders := s.Leaderships()[term]
		if len(leaders) != 1 {
			t.Fatalf("leaders at highest term %d = %v, want one", term, leaders)
		}
		if _, _, ok := s.Propose(leaders[0], []byte("deterministic")); !ok {
			t.Fatalf("node %d rejected proposal in observed leader term %d", leaders[0], term)
		}
		if err := s.Run(5 * phase1Window); err != nil {
			t.Fatalf("replication Run() error: %v", err)
		}
		return s.Trace()
	}

	first, second := run(), run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same-seed proposal traces differ:\nfirst: %v\nsecond: %v", first, second)
	}
}

func TestSimulatorBurstProposalsCommitOnlyAfterMajorityPersistence(t *testing.T) {
	s := phase1Sim(t, 238)
	s.RegisterInvariant(func(s *Sim) error {
		majority := len(s.order)/2 + 1
		for _, id := range s.order {
			applied := s.AppliedEntries(id)
			for i := range applied {
				replicas := 0
				for _, peer := range s.order {
					if simHasExactEntry(s.Log(peer), &applied[i]) {
						replicas++
					}
				}
				if replicas < majority {
					return fmt.Errorf("node %d applied (%d,%d) with %d replicas, want %d", id, applied[i].Index, applied[i].Term, replicas, majority)
				}
			}
		}
		return nil
	})
	term, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	var lastIndex uint64
	for i := 0; i < 10; i++ {
		index, gotTerm, ok := s.Propose(leader, []byte{byte(i)})
		if !ok || gotTerm != term {
			t.Fatalf("burst proposal %d = (%d,%d,%v), want leader term %d", i, index, gotTerm, ok, term)
		}
		lastIndex = index
	}
	runFor(t, s, 3*phase1Window)
	for _, id := range s.order {
		if got := s.LastApplied(id); got < lastIndex {
			t.Fatalf("node %d LastApplied = %d, want at least burst tail %d", id, got, lastIndex)
		}
	}
}

func simContainsEntry(entries []raftpb.Entry, index, term uint64, typ raftpb.EntryType, data []byte) bool {
	for i := range entries {
		if entries[i].Index == index && entries[i].Term == term && entries[i].Type == typ && bytes.Equal(entries[i].Data, data) {
			return true
		}
	}
	return false
}

func simContainsNOOP(entries []raftpb.Entry, term uint64) bool {
	for i := range entries {
		if entries[i].Term == term && entries[i].Type == raftpb.EntryType_NOOP {
			return true
		}
	}
	return false
}

func simHasExactEntry(entries []raftpb.Entry, want *raftpb.Entry) bool {
	if want == nil {
		return false
	}
	for i := range entries {
		if entries[i].Index == want.Index && entries[i].Term == want.Term && entries[i].Type == want.Type && bytes.Equal(entries[i].Data, want.Data) {
			return true
		}
	}
	return false
}

func simEntries(entries []raftpb.Entry) string {
	if len(entries) == 0 {
		return "[]"
	}
	result := "["
	for i := range entries {
		if i > 0 {
			result += ", "
		}
		result += entries[i].String()
	}
	return result + "]"
}
