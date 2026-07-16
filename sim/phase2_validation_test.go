package sim

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	raftpb "miniquorum/proto"
)

func TestLogMatchingInvariantDetectsPrefixAndCommitViolations(t *testing.T) {
	t.Run("shared index and term require identical prefix", func(t *testing.T) {
		s := newTestSim(t, 1, 2)
		saveSimLog(t, s, 1, []raftpb.Entry{
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("left")},
			{Index: 2, Term: 3, Type: raftpb.EntryType_NOOP},
		})
		saveSimLog(t, s, 2, []raftpb.Entry{
			{Index: 1, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("right")},
			{Index: 2, Term: 3, Type: raftpb.EntryType_NOOP},
		})
		if err := LogMatching(s); err == nil {
			t.Fatal("LogMatching() error = nil, want shared (2,3) with different prefixes to fail")
		}
	})

	t.Run("committed prefixes must match without a shared term", func(t *testing.T) {
		s := newTestSim(t, 1, 2)
		saveSimLog(t, s, 1, []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("left")}})
		saveSimLog(t, s, 2, []raftpb.Entry{{Index: 1, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("right")}})
		s.nodes[1].lastApplied = 1
		s.nodes[2].lastApplied = 1
		if err := LogMatching(s); err == nil {
			t.Fatal("LogMatching() error = nil, want conflicting committed prefixes to fail")
		}
	})

	t.Run("matching committed prefixes permit an uncommitted suffix", func(t *testing.T) {
		s := newTestSim(t, 1, 2)
		saveSimLog(t, s, 1, []raftpb.Entry{
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("same")},
			{Index: 2, Term: 2, Type: raftpb.EntryType_NOOP},
		})
		saveSimLog(t, s, 2, []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("same")}})
		s.nodes[1].lastApplied = 1
		s.nodes[2].lastApplied = 1
		if err := LogMatching(s); err != nil {
			t.Fatalf("LogMatching() error = %v, want valid matching prefixes", err)
		}
	})
}

// TestCrossLeaderDedupExactlyOneMutation retries the exact same logical PUT
// after a leader change. An intervening client first changes the value to v2;
// if the original PUT were executed a second time the state would revert to
// v1. Retaining v2 therefore proves that only the first logical mutation
// executed, while both leaders return the identical cached Result.
func TestCrossLeaderDedupExactlyOneMutation(t *testing.T) {
	s := phase2StateMachineSim(t, 2026071601)
	oldTerm, oldLeader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	original := &raftpb.Command{ClientId: 77, Seq: 9, Op: raftpb.Op_PUT, Key: []byte("dedup-key"), Value: []byte("v1")}
	firstIndex, firstTerm := proposeCommand(t, s, oldLeader, original)
	firstResult := awaitAppliedResult(t, s, oldLeader, firstIndex, firstTerm, 3*phase1Window)
	awaitAppliedEverywhere(t, s, firstIndex, firstTerm, 3*phase1Window)

	majority := without(s.order, oldLeader)
	partitionGroups(s, []raft.NodeID{oldLeader}, majority)
	newTerm, newLeader := requireSuccessorLeader(t, s, oldTerm, oldLeader, 10*phase1Window)

	interveningIndex, interveningTerm := proposeCommand(t, s, newLeader, &raftpb.Command{ClientId: 88, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("dedup-key"), Value: []byte("v2")})
	_ = awaitAppliedResult(t, s, newLeader, interveningIndex, interveningTerm, 3*phase1Window)

	retry := proto.Clone(original).(*raftpb.Command)
	retryIndex, retryTerm := proposeCommand(t, s, newLeader, retry)
	if retryTerm != newTerm {
		t.Fatalf("retry term = %d, want successor term %d", retryTerm, newTerm)
	}
	retryResult := awaitAppliedResult(t, s, newLeader, retryIndex, retryTerm, 3*phase1Window)
	if !reflect.DeepEqual(firstResult, retryResult) {
		t.Fatalf("cross-leader retry Result = %#v, first Result = %#v", retryResult, firstResult)
	}

	got, err := s.nodes[newLeader].sm.Read([]byte("dedup-key"))
	if err != nil {
		t.Fatalf("new leader state-machine Read: %v", err)
	}
	if !got.Found || !bytes.Equal(got.Value, []byte("v2")) {
		t.Fatalf("dedup-key after cross-leader retry = %#v, want intervening v2 (v1 would prove a second execution)", got)
	}

	var matchingApplications int
	applied := s.AppliedEntries(newLeader)
	for entryPos := range applied {
		entry := &applied[entryPos]
		if entry.Type != raftpb.EntryType_NORMAL {
			continue
		}
		var command raftpb.Command
		if err := proto.Unmarshal(entry.Data, &command); err == nil && command.ClientId == original.ClientId && command.Seq == original.Seq {
			matchingApplications++
		}
	}
	if matchingApplications != 2 {
		t.Fatalf("new leader applied-log occurrences for client=%d seq=%d = %d, want original plus retry", original.ClientId, original.Seq, matchingApplications)
	}
}

func TestGETLinearizabilityAcrossLeaderPartition(t *testing.T) {
	s := phase2StateMachineSim(t, 2026071602)
	oldTerm, oldLeader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	v1Index, v1Term := proposeCommand(t, s, oldLeader, &raftpb.Command{ClientId: 101, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v1")})
	awaitAppliedEverywhere(t, s, v1Index, v1Term, 3*phase1Window)

	majority := without(s.order, oldLeader)
	partitionGroups(s, []raft.NodeID{oldLeader}, majority)
	_, newLeader := requireSuccessorLeader(t, s, oldTerm, oldLeader, 10*phase1Window)

	v2Index, v2Term := proposeCommand(t, s, newLeader, &raftpb.Command{ClientId: 102, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v2")})
	_ = awaitAppliedResult(t, s, newLeader, v2Index, v2Term, 3*phase1Window)

	getIndex, getTerm := proposeCommand(t, s, newLeader, &raftpb.Command{ClientId: 103, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	getResult := awaitAppliedResult(t, s, newLeader, getIndex, getTerm, 3*phase1Window)
	if !getResult.Found || !bytes.Equal(getResult.Value, []byte("v2")) {
		t.Fatalf("GET through successor leader = %#v, want found v2", getResult)
	}
	assertAppliedCommand(t, s, newLeader, getIndex, getTerm, raftpb.Op_GET)

	// Without CheckQuorum the isolated old leader can still accept a proposal,
	// but it cannot commit it. A real Execute waits for Apply and times out;
	// the simulator proves the same condition by running well past an election
	// window with no applied Result at the proposed log position.
	oldGetIndex, oldGetTerm := proposeCommand(t, s, oldLeader, &raftpb.Command{ClientId: 104, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	runFor(t, s, 5*phase1Window)
	if _, ok := appliedResultAt(s.nodes[oldLeader], oldGetIndex, oldGetTerm); ok {
		t.Fatalf("minority old leader applied GET at (%d,%d); it must time out without quorum", oldGetIndex, oldGetTerm)
	}
	assertLoggedCommand(t, s, oldLeader, oldGetIndex, oldGetTerm, raftpb.Op_GET)
	staleState, err := s.nodes[oldLeader].sm.Read([]byte("k"))
	if err != nil {
		t.Fatalf("old leader state-machine Read: %v", err)
	}
	if !staleState.Found || !bytes.Equal(staleState.Value, []byte("v1")) {
		t.Fatalf("old leader local state = %#v, want stale v1 to make the no-return assertion meaningful", staleState)
	}
}

func TestPhase2FiveHundredSeedsAllInvariants(t *testing.T) {
	for run := 0; run < 500; run++ {
		seed := int64(202607160000 + run)
		s, err := NewSim(Config{Seed: seed, NodeIDs: []raft.NodeID{1, 2, 3}})
		if err != nil {
			t.Fatalf("seed %d NewSim: %v", seed, err)
		}
		// NewSim installs LogMatching; Phase 1's invariant is registered here.
		s.RegisterInvariant(SingleLeaderPerTerm)
		if err := s.Run(3 * phase1Window); err != nil {
			t.Fatalf("seed %d initial run: %v", seed, err)
		}
		term := s.HighestTerm()
		leaders := s.Leaderships()[term]
		if len(leaders) != 1 {
			t.Fatalf("seed %d leaders in term %d = %v, want one", seed, term, leaders)
		}
		leader := leaders[0]
		for proposal := 0; proposal < 3; proposal++ {
			if _, gotTerm, ok := s.Propose(leader, []byte(fmt.Sprintf("seed=%d proposal=%d", seed, proposal))); !ok || gotTerm != term {
				t.Fatalf("seed %d proposal %d rejected by leader %d in term %d", seed, proposal, leader, term)
			}
		}
		if err := s.Run(s.Now() + 2*phase1Window); err != nil {
			t.Fatalf("seed %d replication run: %v", seed, err)
		}

		majority := without(s.order, leader)
		partitionGroups(s, []raft.NodeID{leader}, majority)
		if err := s.Run(s.Now() + 7*phase1Window); err != nil {
			t.Fatalf("seed %d failover run: %v", seed, err)
		}
		newTerm := s.HighestTerm()
		newLeaders := s.Leaderships()[newTerm]
		if newTerm <= term || len(newLeaders) != 1 || newLeaders[0] == leader {
			t.Fatalf("seed %d successor after isolating %d in term %d: term=%d leaders=%v", seed, leader, term, newTerm, newLeaders)
		}
		if _, gotTerm, ok := s.Propose(newLeaders[0], []byte(fmt.Sprintf("seed=%d successor", seed))); !ok || gotTerm != newTerm {
			t.Fatalf("seed %d successor proposal rejected in term %d", seed, newTerm)
		}
		if err := s.Run(s.Now() + 2*phase1Window); err != nil {
			t.Fatalf("seed %d successor replication: %v", seed, err)
		}
		healGroups(s, []raft.NodeID{leader}, majority)
		if err := s.Run(s.Now() + 3*phase1Window); err != nil {
			t.Fatalf("seed %d heal run: %v", seed, err)
		}
	}
}

func phase2StateMachineSim(t *testing.T, seed int64) *Sim {
	t.Helper()
	s := phase1Sim(t, seed)
	for _, id := range s.order {
		s.nodes[id].sm = mapsm.New()
	}
	return s
}

func proposeCommand(t *testing.T, s *Sim, leader raft.NodeID, command *raftpb.Command) (uint64, uint64) {
	t.Helper()
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	index, term, ok := s.Propose(leader, data)
	if !ok {
		t.Fatalf("node %d rejected command proposal in highest term %d", leader, s.HighestTerm())
	}
	return index, term
}

func awaitAppliedResult(t *testing.T, s *Sim, id raft.NodeID, index, term uint64, within VirtualTime) statemachine.Result {
	t.Helper()
	deadline := s.Now() + within
	for {
		if result, ok := appliedResultAt(s.nodes[id], index, term); ok {
			return result
		}
		if s.Now() >= deadline {
			t.Fatalf("node %d did not apply (%d,%d) by t=%d; applied=%s", id, index, term, deadline, simEntries(s.AppliedEntries(id)))
		}
		runFor(t, s, phase1TickInterval)
	}
}

func awaitAppliedEverywhere(t *testing.T, s *Sim, index, term uint64, within VirtualTime) {
	t.Helper()
	deadline := s.Now() + within
	for {
		all := true
		for _, id := range s.order {
			if _, ok := appliedResultAt(s.nodes[id], index, term); !ok {
				all = false
				break
			}
		}
		if all {
			return
		}
		if s.Now() >= deadline {
			for _, id := range s.order {
				if _, ok := appliedResultAt(s.nodes[id], index, term); !ok {
					t.Fatalf("node %d did not apply (%d,%d) by t=%d; applied=%s", id, index, term, deadline, simEntries(s.AppliedEntries(id)))
				}
			}
		}
		runFor(t, s, phase1TickInterval)
	}
}

func appliedResultAt(node *simNode, index, term uint64) (statemachine.Result, bool) {
	for i := len(node.applied) - 1; i >= 0; i-- {
		if node.applied[i].Index == index && node.applied[i].Term == term {
			return node.results[i], true
		}
	}
	return statemachine.Result{}, false
}

func requireSuccessorLeader(t *testing.T, s *Sim, oldTerm uint64, oldLeader raft.NodeID, within VirtualTime) (uint64, raft.NodeID) {
	t.Helper()
	deadline := s.Now() + within
	for s.Now() < deadline {
		runFor(t, s, phase1TickInterval)
		term := s.HighestTerm()
		leaders := s.Leaderships()[term]
		if term > oldTerm && len(leaders) == 1 && leaders[0] != oldLeader {
			return term, leaders[0]
		}
	}
	t.Fatalf("no successor to leader %d term %d by t=%d; highest=%d leaderships=%v", oldLeader, oldTerm, deadline, s.HighestTerm(), s.Leaderships())
	return 0, 0
}

func assertAppliedCommand(t *testing.T, s *Sim, id raft.NodeID, index, term uint64, op raftpb.Op) {
	t.Helper()
	entries := s.AppliedEntries(id)
	for entryPos := range entries {
		entry := &entries[entryPos]
		if entry.Index == index && entry.Term == term {
			assertCommandOp(t, entry, op)
			return
		}
	}
	t.Fatalf("node %d has no applied entry at (%d,%d)", id, index, term)
}

func assertLoggedCommand(t *testing.T, s *Sim, id raft.NodeID, index, term uint64, op raftpb.Op) {
	t.Helper()
	entries := s.Log(id)
	for entryPos := range entries {
		entry := &entries[entryPos]
		if entry.Index == index && entry.Term == term {
			assertCommandOp(t, entry, op)
			return
		}
	}
	t.Fatalf("node %d has no log entry at (%d,%d)", id, index, term)
}

func assertCommandOp(t *testing.T, entry *raftpb.Entry, op raftpb.Op) {
	t.Helper()
	if entry.Type != raftpb.EntryType_NORMAL {
		t.Fatalf("entry (%d,%d) type = %s, want NORMAL command", entry.Index, entry.Term, entry.Type)
	}
	var command raftpb.Command
	if err := proto.Unmarshal(entry.Data, &command); err != nil {
		t.Fatalf("decode entry (%d,%d): %v", entry.Index, entry.Term, err)
	}
	if command.Op != op {
		t.Fatalf("entry (%d,%d) op = %s, want %s", entry.Index, entry.Term, command.Op, op)
	}
}

func saveSimLog(t *testing.T, s *Sim, id raft.NodeID, entries []raftpb.Entry) {
	t.Helper()
	if err := s.nodes[id].storage.Save(nil, entries); err != nil {
		t.Fatalf("node %d Save: %v", id, err)
	}
}
