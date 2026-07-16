package sim

import (
	"bytes"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// TestFigure8CurrentTermCommitRuleFiveNodes scripts the two continuations of
// Raft paper Figure 8. At the shared point, servers 1, 2, and 3 contain x from
// term 2 while server 5 contains the competing y from term 3. Server 1 then
// becomes leader in term 4 and learns that x is stored on a majority.
//
// The unsafe continuation shows that y can still overwrite x after server 1
// crashes, so counting replicas alone must not commit x. The safe continuation
// replicates server 1's term-4 NOOP; committing that current-term entry commits
// the preceding x as well and makes the competing term-3 log unable to win the
// next election. Removing the current-term clause makes the shared-point
// assertion fail before either continuation is followed.
func TestFigure8CurrentTermCommitRuleFiveNodes(t *testing.T) {
	t.Run("old-term majority is not committed and can be overwritten", func(t *testing.T) {
		s, stage := figure8ReachSharedPoint(t)

		if len(stage.committed) != 0 {
			t.Fatalf("term-4 leader committed %s after only learning that old-term x has a majority; want nothing committed", figure8Entries(stage.committed))
		}

		figure8CrashNode(s, 1)
		term5 := figure8Campaign(t, s, 5)
		term5 = figure8GrantVote(t, s, term5, 2)
		term5 = figure8GrantVote(t, s, term5, 3)
		if len(term5.entries) != 1 || !figure8IsEntry(&term5.entries[0], 3, 5, raftpb.EntryType_NOOP, nil) {
			t.Fatalf("term-5 leader Ready entries = %s, want index 3 term 5 NOOP", figure8Entries(term5.entries))
		}

		retry2 := figure8RejectProbe(t, s, term5.messages, 5, 2)
		retry3 := figure8RejectProbe(t, s, term5.messages, 5, 3)
		commit2 := figure8AcceptAppend(t, s, retry2, 5, 2)
		if len(commit2.committed) != 0 {
			t.Fatalf("term-5 leader committed with only two replicas: %s", figure8Entries(commit2.committed))
		}
		commit3 := figure8AcceptAppend(t, s, retry3, 5, 3)
		if !figure8Contains(commit3.committed, 2, 3, []byte("y")) || !figure8Contains(commit3.committed, 3, 5, nil) {
			t.Fatalf("term-5 committed entries = %s, want competing y followed by the term-5 NOOP", figure8Entries(commit3.committed))
		}

		for _, id := range []raft.NodeID{2, 3, 5} {
			log := figure8StoredLog(t, s, id)
			if figure8Contains(log, 2, 2, []byte("x")) {
				t.Fatalf("node %d retained x after Figure 8 overwrite: %s", id, figure8Entries(log))
			}
			if !figure8Contains(log, 2, 3, []byte("y")) {
				t.Fatalf("node %d log = %s, want competing term-3 y", id, figure8Entries(log))
			}
		}
	})

	t.Run("current-term NOOP commits the old entry and protects it", func(t *testing.T) {
		s, stage := figure8ReachSharedPoint(t)
		commit2 := figure8AcceptAppend(t, s, stage.noopTo2, 1, 2)
		if len(commit2.committed) != 0 {
			t.Fatalf("term-4 leader committed with only two replicas: %s", figure8Entries(commit2.committed))
		}
		commit3 := figure8AcceptAppend(t, s, stage.noopTo3, 1, 3)
		if got := commit3.committed; len(got) != 3 ||
			!figure8IsEntry(&got[0], 1, 1, raftpb.EntryType_NORMAL, []byte("base")) ||
			!figure8IsEntry(&got[1], 2, 2, raftpb.EntryType_NORMAL, []byte("x")) ||
			!figure8IsEntry(&got[2], 3, 4, raftpb.EntryType_NOOP, nil) {
			t.Fatalf("term-4 committed entries = %s, want ordered [base, x, NOOP]", figure8Entries(got))
		}

		term5 := figure8Campaign(t, s, 5)
		term5 = figure8GrantVote(t, s, term5, 4)
		voteRequests := term5.messages
		for _, voter := range []raft.NodeID{2, 3} {
			req := figure8Message(t, voteRequests, 5, voter, "RequestVote")
			response := figure8Step(t, s, voter, req)
			vote := figure8Message(t, response.messages, voter, 5, "RequestVoteResp").GetRequestVoteResp()
			if vote.VoteGranted {
				t.Fatalf("node %d granted term-5 vote to stale term-3 log after term-4 NOOP committed", voter)
			}
			term5 = figure8Step(t, s, 5, figure8Message(t, response.messages, voter, 5, "RequestVoteResp"))
			if len(term5.entries) != 0 || figure8HasAppendEntries(term5.messages) {
				t.Fatalf("server 5 became leader with stale log after protected commit: entries=%s messages=%d", figure8Entries(term5.entries), len(term5.messages))
			}
		}
	})
}

type figure8Stage struct {
	committed []raftpb.Entry
	noopTo2   *raftpb.Message
	noopTo3   *raftpb.Message
}

type figure8Batch struct {
	entries   []raftpb.Entry
	messages  []*raftpb.Message
	committed []raftpb.Entry
}

func figure8ReachSharedPoint(t *testing.T) (*Sim, figure8Stage) {
	t.Helper()
	s := figure8NewSim(t)
	campaign := figure8Campaign(t, s, 1)

	// Let server 5 observe term 4 while rejecting server 1's less up-to-date
	// log. It can therefore start the competing election in term 5 later.
	req5 := figure8Message(t, campaign.messages, 1, 5, "RequestVote")
	response5 := figure8Step(t, s, 5, req5)
	if figure8Message(t, response5.messages, 5, 1, "RequestVoteResp").GetRequestVoteResp().VoteGranted {
		t.Fatal("server 5 granted server 1 a vote despite its newer term-3 entry")
	}

	campaign = figure8GrantVote(t, s, campaign, 2)
	campaign = figure8GrantVote(t, s, campaign, 3)
	if len(campaign.entries) != 1 || !figure8IsEntry(&campaign.entries[0], 3, 4, raftpb.EntryType_NOOP, nil) {
		t.Fatalf("term-4 leader Ready entries = %s, want index 3 term 4 NOOP", figure8Entries(campaign.entries))
	}

	probe2 := figure8Message(t, campaign.messages, 1, 2, "AppendEntries")
	probe3 := figure8Message(t, campaign.messages, 1, 3, "AppendEntries")
	for _, probe := range []*raftpb.Message{probe2, probe3} {
		req := probe.GetAppendEntries()
		if req.PrevLogIndex != 2 || req.PrevLogTerm != 2 || len(req.Entries) != 0 {
			t.Fatalf("term-4 initial probe = prev (%d,%d), entries %d; want prev (2,2), empty", req.PrevLogIndex, req.PrevLogTerm, len(req.Entries))
		}
	}

	after2 := figure8AcceptAppend(t, s, probe2, 1, 2)
	if len(after2.committed) != 0 {
		t.Fatalf("term-4 leader committed before a majority response: %s", figure8Entries(after2.committed))
	}
	after3 := figure8AcceptAppend(t, s, probe3, 1, 3)
	stage := figure8Stage{
		committed: after3.committed,
		noopTo2:   figure8Message(t, after2.messages, 1, 2, "AppendEntries"),
		noopTo3:   figure8Message(t, after3.messages, 1, 3, "AppendEntries"),
	}
	for _, message := range []*raftpb.Message{stage.noopTo2, stage.noopTo3} {
		req := message.GetAppendEntries()
		if req.PrevLogIndex != 2 || req.PrevLogTerm != 2 || len(req.Entries) != 1 ||
			req.Entries[0].Index != 3 || req.Entries[0].Term != 4 || req.Entries[0].Type != raftpb.EntryType_NOOP {
			t.Fatalf("term-4 NOOP replication = prev (%d,%d), entries %v; want index 3 term 4 NOOP", req.PrevLogIndex, req.PrevLogTerm, req.Entries)
		}
	}
	return s, stage
}

func figure8NewSim(t *testing.T) *Sim {
	t.Helper()
	s, err := NewSim(Config{
		Seed:            8,
		NodeIDs:         []raft.NodeID{1, 2, 3, 4, 5},
		ElectionTickMin: 1,
		ElectionTickMax: 2,
		HeartbeatTicks:  10,
	})
	if err != nil {
		t.Fatalf("NewSim() error: %v", err)
	}
	logs := map[raft.NodeID][]raftpb.Entry{
		1: {
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("base")},
			{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("x")},
		},
		2: {
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("base")},
			{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("x")},
		},
		3: {
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("base")},
			{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("x")},
		},
		4: {
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("base")},
		},
		5: {
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("base")},
			{Index: 2, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("y")},
		},
	}
	for _, id := range s.order {
		sn := s.nodes[id]
		hs := raft.HardState{Term: 3}
		if err := sn.storage.Save(&hs, logs[id]); err != nil {
			t.Fatalf("seed node %d storage: %v", id, err)
		}
		entries := figure8StoredLog(t, s, id)
		sn.node = raft.NewNode(sn.cfg, raft.InitialState{HardState: hs, Entries: entries}, sn.rnd)
	}
	return s
}

func figure8Campaign(t *testing.T, s *Sim, id raft.NodeID) figure8Batch {
	t.Helper()
	sn := s.nodes[id]
	if sn.node == nil {
		t.Fatalf("campaign node %d is down", id)
	}
	sn.node.Tick()
	return figure8TakeReady(t, sn)
}

func figure8GrantVote(t *testing.T, s *Sim, campaign figure8Batch, voter raft.NodeID) figure8Batch {
	t.Helper()
	var candidate raft.NodeID
	for _, message := range campaign.messages {
		if message.GetRequestVote() != nil {
			candidate = raft.NodeID(message.From)
			break
		}
	}
	if candidate == 0 {
		t.Fatal("campaign batch has no RequestVote message")
	}
	req := figure8Message(t, campaign.messages, candidate, voter, "RequestVote")
	response := figure8Step(t, s, voter, req)
	resp := figure8Message(t, response.messages, voter, candidate, "RequestVoteResp")
	if !resp.GetRequestVoteResp().VoteGranted {
		t.Fatalf("node %d rejected expected vote for node %d", voter, candidate)
	}
	result := figure8Step(t, s, candidate, resp)
	result.messages = append(append([]*raftpb.Message(nil), campaign.messages...), result.messages...)
	return result
}

func figure8RejectProbe(t *testing.T, s *Sim, messages []*raftpb.Message, leader, follower raft.NodeID) *raftpb.Message {
	t.Helper()
	probe := figure8Message(t, messages, leader, follower, "AppendEntries")
	response := figure8Step(t, s, follower, probe)
	resp := figure8Message(t, response.messages, follower, leader, "AppendEntriesResp")
	if resp.GetAppendEntriesResp().Success {
		t.Fatalf("node %d accepted mismatched probe from node %d", follower, leader)
	}
	retry := figure8Step(t, s, leader, resp)
	message := figure8Message(t, retry.messages, leader, follower, "AppendEntries")
	req := message.GetAppendEntries()
	if req.PrevLogIndex != 1 || req.PrevLogTerm != 1 || len(req.Entries) != 2 ||
		req.Entries[0].Index != 2 || req.Entries[0].Term != 3 ||
		req.Entries[1].Index != 3 || req.Entries[1].Term != 5 || req.Entries[1].Type != raftpb.EntryType_NOOP {
		t.Fatalf("node %d retry = prev (%d,%d), entries %v; want [term-3 y, term-5 NOOP]", leader, req.PrevLogIndex, req.PrevLogTerm, req.Entries)
	}
	return message
}

func figure8AcceptAppend(t *testing.T, s *Sim, message *raftpb.Message, leader, follower raft.NodeID) figure8Batch {
	t.Helper()
	response := figure8Step(t, s, follower, message)
	resp := figure8Message(t, response.messages, follower, leader, "AppendEntriesResp")
	if !resp.GetAppendEntriesResp().Success {
		t.Fatalf("node %d rejected expected AppendEntries from node %d", follower, leader)
	}
	return figure8Step(t, s, leader, resp)
}

func figure8Step(t *testing.T, s *Sim, id raft.NodeID, message *raftpb.Message) figure8Batch {
	t.Helper()
	sn := s.nodes[id]
	if sn.node == nil {
		t.Fatalf("step node %d is down", id)
	}
	sn.node.Step(message)
	return figure8TakeReady(t, sn)
}

func figure8TakeReady(t *testing.T, sn *simNode) figure8Batch {
	t.Helper()
	rd := sn.node.Ready()
	if err := sn.storage.Save(rd.HardState, rd.Entries); err != nil {
		t.Fatalf("node %d Save: %v", sn.id, err)
	}
	// Capturing the messages is the test transport's synchronous Send step;
	// committed entries are observed in order before Advance acknowledges the
	// batch. This is the same Save -> send -> apply -> Advance lifecycle as Sim.
	batch := figure8Batch{entries: rd.Entries, messages: rd.Messages, committed: rd.CommittedEntries}
	for range rd.CommittedEntries {
	}
	sn.node.Advance()
	return batch
}

func figure8CrashNode(s *Sim, id raft.NodeID) {
	sn := s.nodes[id]
	sn.node = nil
	sn.generation++
}

func figure8StoredLog(t *testing.T, s *Sim, id raft.NodeID) []raftpb.Entry {
	t.Helper()
	store := s.nodes[id].storage
	first, last := store.FirstIndex(), store.LastIndex()
	if last < first {
		return nil
	}
	entries, err := store.Entries(first, last+1)
	if err != nil {
		t.Fatalf("node %d Entries(%d,%d): %v", id, first, last+1, err)
	}
	return entries
}

func figure8Message(t *testing.T, messages []*raftpb.Message, from, to raft.NodeID, kind string) *raftpb.Message {
	t.Helper()
	for _, message := range messages {
		if raft.NodeID(message.From) != from || raft.NodeID(message.To) != to {
			continue
		}
		matches := false
		switch kind {
		case "RequestVote":
			matches = message.GetRequestVote() != nil
		case "RequestVoteResp":
			matches = message.GetRequestVoteResp() != nil
		case "AppendEntries":
			matches = message.GetAppendEntries() != nil
		case "AppendEntriesResp":
			matches = message.GetAppendEntriesResp() != nil
		}
		if matches {
			return message
		}
	}
	t.Fatalf("messages have no %s from %d to %d: %v", kind, from, to, messages)
	return nil
}

func figure8HasAppendEntries(messages []*raftpb.Message) bool {
	for _, message := range messages {
		if message.GetAppendEntries() != nil {
			return true
		}
	}
	return false
}

func figure8Contains(entries []raftpb.Entry, index, term uint64, data []byte) bool {
	for i := range entries {
		if entries[i].Index == index && entries[i].Term == term && bytes.Equal(entries[i].Data, data) {
			return true
		}
	}
	return false
}

func figure8IsEntry(entry *raftpb.Entry, index, term uint64, typ raftpb.EntryType, data []byte) bool {
	return entry != nil && entry.Index == index && entry.Term == term && entry.Type == typ && bytes.Equal(entry.Data, data)
}

func figure8Entries(entries []raftpb.Entry) string {
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
