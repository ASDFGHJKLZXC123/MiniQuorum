package raft

import (
	"testing"

	raftpb "miniquorum/proto"
)

func TestNodeCompactBoundsLiveLogAndRetainsConfiguredOverlap(t *testing.T) {
	entries := []raftpb.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 2}, {Index: 5, Term: 2},
	}
	node := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, InitialState{
		HardState: HardState{Term: 2}, Entries: entries, Applied: 5,
	}, fixedTestRand{})
	if err := node.Compact(SnapshotMeta{Index: 3, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if node.log.snapshot != (SnapshotMeta{Index: 3, Term: 1}) || len(node.log.entries) != 2 {
		t.Fatalf("compacted log = base %+v entries %+v, want base 3/1 and indexes 4..5", node.log.snapshot, node.log.entries)
	}
	if _, ok := node.log.term(2); ok {
		t.Fatal("term(2) remains visible after live compaction")
	}
	if term, ok := node.log.term(3); !ok || term != 1 {
		t.Fatalf("term(3) = %d,%v, want compacted boundary term 1", term, ok)
	}
	if term, ok := node.log.term(4); !ok || term != 2 {
		t.Fatalf("term(4) = %d,%v, want retained overlap term 2", term, ok)
	}
	if err := node.Compact(SnapshotMeta{Index: 5, Term: 9}); err == nil {
		t.Fatal("Compact() accepted mismatched boundary term")
	}
	if err := node.Compact(SnapshotMeta{Index: 6, Term: 2}); err == nil {
		t.Fatal("Compact() accepted index beyond applied")
	}
}

func TestNodeCompactAcceptsAppliedPendingReadyBeforeAdvance(t *testing.T) {
	entries := []raftpb.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	node := NewNode(Config{ID: 1, Peers: []NodeID{1, 2}}, InitialState{
		HardState: HardState{Term: 1}, Entries: entries,
	}, fixedTestRand{})
	node.Step(&raftpb.Message{From: 2, To: 1, Term: 1, Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
		Term: 1, LeaderId: 2, PrevLogIndex: 5, PrevLogTerm: 1, LeaderCommit: 5,
	}}})
	rd := node.Ready()
	if len(rd.CommittedEntries) != 5 {
		t.Fatalf("CommittedEntries = %d, want pending applied batch through 5", len(rd.CommittedEntries))
	}
	// The host has applied rd.CommittedEntries but has not called Advance yet.
	if err := node.Compact(SnapshotMeta{Index: 3, Term: 1}); err != nil {
		t.Fatalf("Compact() before Advance: %v", err)
	}
	node.Advance()
	if node.lastApplied != 5 || node.log.snapshot.Index != 3 {
		t.Fatalf("after Advance applied=%d base=%+v, want applied 5 base 3/1", node.lastApplied, node.log.snapshot)
	}
}

func TestNodeCompactInvalidatesInflightAppendCrossingNewBase(t *testing.T) {
	entries := []raftpb.Entry{
		{Index: 1, Term: 2}, {Index: 2, Term: 2}, {Index: 3, Term: 2},
		{Index: 4, Term: 2}, {Index: 5, Term: 2},
	}
	leaderNode := NewNode(Config{
		ID: 1, Peers: []NodeID{1, 2}, HeartbeatTicks: 1,
	}, InitialState{
		HardState: HardState{Term: 2}, Entries: entries, Applied: 5,
	}, fixedTestRand{})
	leaderNode.role = leader
	leaderNode.nextIndex = map[NodeID]uint64{1: 6, 2: 2}
	leaderNode.matchIndex = map[NodeID]uint64{1: 5, 2: 1}
	leaderNode.inflight = make(map[NodeID]*appendInflight)

	// The original request spans indexes 2..5. Treat the Ready message as sent
	// but lose it in transport, leaving only its logical in-flight state.
	leaderNode.sendAppend(2, false)
	original := replicationMessageTo(t, leaderNode.Ready().Messages, 2).GetAppendEntries()
	if original.PrevLogIndex != 1 || len(original.Entries) != 4 {
		t.Fatalf("original request = %v, want prev 1 and entries 2..5", original)
	}
	leaderNode.Advance()

	if err := leaderNode.Compact(SnapshotMeta{Index: 3, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if leaderNode.inflight[2] != nil {
		t.Fatalf("inflight after compaction = %v, want invalidated request crossing base 3", leaderNode.inflight[2])
	}

	// A heartbeat must construct a new request solely from the retained suffix,
	// not re-emit the old logical request with an empty/truncated payload.
	leaderNode.Tick()
	retryMessage := replicationMessageTo(t, leaderNode.Ready().Messages, 2)
	retry := retryMessage.GetAppendEntries()
	if retry.PrevLogIndex != 3 || retry.PrevLogTerm != 2 || len(retry.Entries) != 2 ||
		retry.Entries[0].Index != 4 || retry.Entries[1].Index != 5 {
		t.Fatalf("post-compaction retry = %v, want prev 3 and retained entries 4..5", retry)
	}
	leaderNode.Advance()

	// A follower already through the retained boundary accepts the fresh retry,
	// and its response clears the new in-flight request normally.
	followerNode := NewNode(Config{ID: 2, Peers: []NodeID{1, 2}}, InitialState{
		HardState: HardState{Term: 2}, Entries: entries[:3], Applied: 3,
	}, fixedTestRand{})
	followerNode.Step(retryMessage)
	followerReady := followerNode.Ready()
	if len(followerReady.Messages) != 1 || followerReady.Messages[0].To != 1 {
		t.Fatalf("follower messages = %v, want one response to leader", followerReady.Messages)
	}
	response := followerReady.Messages[0]
	if got := response.GetAppendEntriesResp(); got == nil || !got.Success || got.MatchIndex != 5 {
		t.Fatalf("retry response = %v, want success through index 5", got)
	}
	leaderNode.Step(response)
	if leaderNode.inflight[2] != nil || leaderNode.matchIndex[2] != 5 || leaderNode.nextIndex[2] != 6 {
		t.Fatalf("leader progress after retry = inflight %v match %d next %d, want nil,5,6",
			leaderNode.inflight[2], leaderNode.matchIndex[2], leaderNode.nextIndex[2])
	}
}
