package raft

import (
	"testing"

	raftpb "miniquorum/proto"
)

func TestFollowerAheadCompactionRecoversAfterDroppedSuccessAcknowledgement(t *testing.T) {
	entries := compactionHintEntries()
	leaderNode := NewNode(Config{
		ID: 1, Peers: []NodeID{1, 2, 3}, HeartbeatTicks: 1,
	}, InitialState{
		HardState: HardState{Term: 2}, Entries: entries, Applied: 5,
	}, fixedTestRand{})
	leaderNode.role = leader
	// Node 3 supplied the quorum acknowledgement through index 5. Node 2 also
	// received those entries, but its success was lost, so only its progress is
	// stale while the committed/applied index remains legitimate.
	leaderNode.nextIndex = map[NodeID]uint64{1: 6, 2: 2, 3: 6}
	leaderNode.matchIndex = map[NodeID]uint64{1: 5, 2: 1, 3: 5}
	leaderNode.inflight = make(map[NodeID]*appendInflight)

	followerNode := NewNode(Config{ID: 2, Peers: []NodeID{1, 2, 3}}, InitialState{
		HardState: HardState{Term: 2}, Entries: entries, Applied: 5,
	}, fixedTestRand{})

	// The leader's stale progress sends entries 2..5. The follower accepts the
	// request, but transport drops its exact success acknowledgement.
	leaderNode.sendAppend(2, false)
	initial := replicationMessageTo(t, leaderNode.Ready().Messages, 2)
	if req := initial.GetAppendEntries(); req.PrevLogIndex != 1 || len(req.Entries) != 4 {
		t.Fatalf("initial append = %v, want prev 1 and entries 2..5", req)
	}
	leaderNode.Advance()
	followerNode.Step(initial)
	droppedSuccess := appendResponseMessage(t, followerNode.Ready())
	if resp := droppedSuccess.GetAppendEntriesResp(); !resp.Success || resp.MatchIndex != 5 {
		t.Fatalf("dropped response = %v, want success through index 5", resp)
	}
	followerNode.Advance()

	// The follower snapshots independently after applying through index 5. A
	// retry of the still-outstanding request now names discarded predecessor 1.
	if err := followerNode.Compact(SnapshotMeta{Index: 3, Term: 2}); err != nil {
		t.Fatal(err)
	}
	leaderNode.Tick()
	replayed := replicationMessageTo(t, leaderNode.Ready().Messages, 2)
	if req := replayed.GetAppendEntries(); req.PrevLogIndex != 1 || len(req.Entries) != 4 {
		t.Fatalf("replayed append = %v, want original prev 1 and entries 2..5", req)
	}
	leaderNode.Advance()

	followerNode.Step(replayed)
	rejection := appendResponseMessage(t, followerNode.Ready())
	if resp := rejection.GetAppendEntriesResp(); resp.Success {
		t.Fatalf("compacted-prefix response = %v, want rejection", resp)
	} else if resp.MatchIndex != 3 {
		t.Errorf("compacted-prefix rejection hint = %d, want boundary 3", resp.MatchIndex)
	}
	followerNode.Advance()

	// A failure hint may reposition nextIndex only. It cannot manufacture an
	// acknowledgement or move commit; the replacement request must prove the
	// retained suffix normally.
	leaderNode.Step(rejection)
	repositioned := leaderNode.Ready()
	if leaderNode.matchIndex[2] != 1 || leaderNode.commitIndex != 5 {
		t.Errorf("failure hint changed acknowledged state: match=%d commit=%d, want 1,5",
			leaderNode.matchIndex[2], leaderNode.commitIndex)
	}
	replacement := replicationMessageTo(t, repositioned.Messages, 2)
	if req := replacement.GetAppendEntries(); req.PrevLogIndex != 3 || req.PrevLogTerm != 2 ||
		len(req.Entries) != 2 || req.Entries[0].Index != 4 || req.Entries[1].Index != 5 {
		t.Fatalf("replacement append = %v, want prev 3 and entries 4..5", req)
	}
	leaderNode.Advance()

	// Let the follower prove the replacement suffix, but delay that success.
	followerNode.Step(replacement)
	replacementSuccess := appendResponseMessage(t, followerNode.Ready())
	if resp := replacementSuccess.GetAppendEntriesResp(); !resp.Success || resp.MatchIndex != 5 {
		t.Fatalf("replacement response = %v, want success through index 5", resp)
	}
	followerNode.Advance()

	// A new proposal arrives while the index-5 replacement remains in flight.
	// A delayed duplicate of the old compaction rejection must not clear or
	// rebuild that request, or its valid success would be mistaken as stale.
	index, term, ok := leaderNode.Propose([]byte("after-catch-up"))
	if !ok || index != 6 || term != 2 {
		t.Fatalf("Propose = (%d,%d,%v), want (6,2,true)", index, term, ok)
	}
	proposed := leaderNode.Ready()
	if len(proposed.Entries) != 1 || proposed.Entries[0].Index != 6 {
		t.Fatalf("proposal Ready entries = %+v, want only index 6", proposed.Entries)
	}
	leaderNode.Advance()
	pendingThroughFive := leaderNode.inflight[2]
	if pendingThroughFive == nil || pendingThroughFive.prevIndex != 3 || pendingThroughFive.lastIndex != 5 {
		t.Fatalf("pending replacement = %v, want prev 3 through index 5", pendingThroughFive)
	}

	leaderNode.Step(rejection)
	staleHint := leaderNode.Ready()
	if leaderNode.inflight[2] != pendingThroughFive || leaderNode.nextIndex[2] != 4 ||
		leaderNode.matchIndex[2] != 1 || leaderNode.commitIndex != 5 {
		t.Errorf("stale hint changed progress: inflight=%v next=%d match=%d commit=%d",
			leaderNode.inflight[2], leaderNode.nextIndex[2], leaderNode.matchIndex[2], leaderNode.commitIndex)
	}
	if len(staleHint.Messages) != 0 || len(staleHint.CommittedEntries) != 0 {
		t.Errorf("stale hint emitted messages=%d committed=%v, want none", len(staleHint.Messages), staleHint.CommittedEntries)
	}
	leaderNode.Advance()

	// The preserved success is still attributable to the exact index-5 request
	// and naturally schedules the new index-6 suffix.
	leaderNode.Step(replacementSuccess)
	catchUp := leaderNode.Ready()
	if leaderNode.matchIndex[2] != 5 || leaderNode.nextIndex[2] != 6 || leaderNode.commitIndex != 5 {
		t.Fatalf("catch-up progress = match %d next %d commit %d, want 5,6,5",
			leaderNode.matchIndex[2], leaderNode.nextIndex[2], leaderNode.commitIndex)
	}
	entrySix := replicationMessageTo(t, catchUp.Messages, 2)
	if req := entrySix.GetAppendEntries(); req.PrevLogIndex != 5 || len(req.Entries) != 1 || req.Entries[0].Index != 6 {
		t.Fatalf("continued replication = %v, want prev 5 and entry 6", req)
	}
	leaderNode.Advance()

	// The originally dropped success is now a delayed duplicate. It covers 5,
	// so it cannot acknowledge or replace the outstanding request through 6.
	pendingSix := leaderNode.inflight[2]
	leaderNode.Step(droppedSuccess)
	duplicate := leaderNode.Ready()
	if leaderNode.inflight[2] != pendingSix || leaderNode.matchIndex[2] != 5 ||
		leaderNode.nextIndex[2] != 6 || leaderNode.commitIndex != 5 {
		t.Errorf("duplicate success changed progress: inflight=%v match=%d next=%d commit=%d",
			leaderNode.inflight[2], leaderNode.matchIndex[2], leaderNode.nextIndex[2], leaderNode.commitIndex)
	}
	if len(duplicate.Messages) != 0 || len(duplicate.CommittedEntries) != 0 {
		t.Errorf("duplicate success emitted messages=%d committed=%v, want none", len(duplicate.Messages), duplicate.CommittedEntries)
	}
	leaderNode.Advance()

	followerNode.Step(entrySix)
	entrySixSuccess := appendResponseMessage(t, followerNode.Ready())
	if resp := entrySixSuccess.GetAppendEntriesResp(); !resp.Success || resp.MatchIndex != 6 {
		t.Fatalf("entry-6 response = %v, want success through 6", resp)
	}
	followerNode.Advance()
	leaderNode.Step(entrySixSuccess)
	committed := leaderNode.Ready()
	if leaderNode.matchIndex[2] != 6 || leaderNode.nextIndex[2] != 7 || leaderNode.commitIndex != 6 {
		t.Fatalf("continued progress = match %d next %d commit %d, want 6,7,6",
			leaderNode.matchIndex[2], leaderNode.nextIndex[2], leaderNode.commitIndex)
	}
	if len(committed.CommittedEntries) != 1 || committed.CommittedEntries[0].Index != 6 {
		t.Fatalf("continued committed entries = %+v, want only index 6", committed.CommittedEntries)
	}
}

func TestFollowerCompactionHintRequiresStrictlyDiscardedPredecessor(t *testing.T) {
	tests := []struct {
		name        string
		request     *raftpb.Message
		wantSuccess bool
		wantMatch   uint64
	}{
		{
			name:      "discarded predecessor hints compacted boundary",
			request:   replicationAppendRequest(1, 2, 2, 2, 2, 5),
			wantMatch: 3,
		},
		{
			name:      "mismatched retained boundary is an ordinary rejection",
			request:   replicationAppendRequest(1, 2, 2, 3, 99, 5),
			wantMatch: 0,
		},
		{
			name:        "matching retained boundary keeps exact success identity",
			request:     replicationAppendRequest(1, 2, 2, 3, 2, 5),
			wantSuccess: true,
			wantMatch:   3,
		},
		{
			name: "malformed suffix below boundary gets no hint",
			request: replicationAppendRequest(1, 2, 2, 2, 2, 5,
				&raftpb.Entry{Index: 4, Term: 2}),
			wantMatch: 0,
		},
		{
			name: "overflowing request gets no hint",
			request: replicationAppendRequest(1, 2, 2, ^uint64(0), 2, 5,
				&raftpb.Entry{Index: 0, Term: 2}),
			wantMatch: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			followerNode := newCompactedHintFollower(t)
			followerNode.Step(tt.request)
			response := appendResponseMessage(t, followerNode.Ready()).GetAppendEntriesResp()
			if response.Success != tt.wantSuccess || response.MatchIndex != tt.wantMatch {
				t.Fatalf("response = %v, want success=%v matchIndex=%d", response, tt.wantSuccess, tt.wantMatch)
			}
		})
	}
}

func TestLeaderIgnoresStaleAndInvalidNonzeroCompactionHints(t *testing.T) {
	tests := []struct {
		name string
		hint uint64
	}{
		{name: "duplicate at current predecessor", hint: 4},
		{name: "lower stale hint above retry floor", hint: 3},
		{name: "out of log", hint: 6},
		{name: "overflow", hint: ^uint64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaderNode := newHintLeader(5)
			leaderNode.sendAppend(2, false)
			request := replicationMessageTo(t, leaderNode.Ready().Messages, 2).GetAppendEntries()
			if request.PrevLogIndex != 4 || len(request.Entries) != 1 || request.Entries[0].Index != 5 {
				t.Fatalf("initial request = %v, want prev 4 and entry 5", request)
			}
			leaderNode.Advance()
			pending := leaderNode.inflight[2]

			leaderNode.Step(replicationAppendResponse(2, 1, 2, false, tt.hint))
			ready := leaderNode.Ready()
			if leaderNode.inflight[2] != pending || leaderNode.nextIndex[2] != 5 ||
				leaderNode.matchIndex[2] != 1 || leaderNode.commitIndex != 1 {
				t.Errorf("hint %d changed progress: inflight=%v next=%d match=%d commit=%d",
					tt.hint, leaderNode.inflight[2], leaderNode.nextIndex[2], leaderNode.matchIndex[2], leaderNode.commitIndex)
			}
			if len(ready.Messages) != 0 || len(ready.CommittedEntries) != 0 {
				t.Errorf("hint %d emitted messages=%d committed=%v, want none",
					tt.hint, len(ready.Messages), ready.CommittedEntries)
			}
		})
	}
}

func TestLeaderAcceptsHighestInLogForwardCompactionHint(t *testing.T) {
	leaderNode := newHintLeader(2)
	leaderNode.sendAppend(2, false)
	leaderNode.Advance()

	leaderNode.Step(replicationAppendResponse(2, 1, 2, false, 5))
	ready := leaderNode.Ready()
	if leaderNode.matchIndex[2] != 1 || leaderNode.commitIndex != 1 || leaderNode.nextIndex[2] != 6 {
		t.Fatalf("highest valid hint progress = match %d commit %d next %d, want 1,1,6",
			leaderNode.matchIndex[2], leaderNode.commitIndex, leaderNode.nextIndex[2])
	}
	probe := replicationMessageTo(t, ready.Messages, 2).GetAppendEntries()
	if probe.PrevLogIndex != 5 || probe.PrevLogTerm != 2 || len(probe.Entries) != 0 {
		t.Fatalf("highest valid hint probe = %v, want empty probe at 5/2", probe)
	}
}

func compactionHintEntries() []raftpb.Entry {
	return []raftpb.Entry{
		{Index: 1, Term: 2, Type: raftpb.EntryType_NOOP},
		{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("two")},
		{Index: 3, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("three")},
		{Index: 4, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("four")},
		{Index: 5, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("five")},
	}
}

func newCompactedHintFollower(t *testing.T) *Node {
	t.Helper()
	node := NewNode(Config{ID: 2, Peers: []NodeID{1, 2, 3}}, InitialState{
		HardState: HardState{Term: 2}, Entries: compactionHintEntries(), Applied: 5,
	}, fixedTestRand{})
	if err := node.Compact(SnapshotMeta{Index: 3, Term: 2}); err != nil {
		t.Fatal(err)
	}
	return node
}

func newHintLeader(next uint64) *Node {
	node := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, InitialState{
		HardState: HardState{Term: 2}, Entries: compactionHintEntries(), Applied: 1,
	}, fixedTestRand{})
	node.role = leader
	node.nextIndex = map[NodeID]uint64{1: 6, 2: next, 3: 2}
	node.matchIndex = map[NodeID]uint64{1: 5, 2: 1, 3: 1}
	node.inflight = make(map[NodeID]*appendInflight)
	return node
}

func appendResponseMessage(t *testing.T, ready Ready) *raftpb.Message {
	t.Helper()
	if len(ready.Messages) != 1 || ready.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("Ready messages = %v, want one AppendEntriesResp", ready.Messages)
	}
	return ready.Messages[0]
}
