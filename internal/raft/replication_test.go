package raft

import (
	"bytes"
	"testing"

	raftpb "miniquorum/proto"
)

func TestInMemoryLogTermAndIndexLookup(t *testing.T) {
	log := newRaftLog(SnapshotMeta{Index: 3, Term: 2}, []raftpb.Entry{
		{Index: 4, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("four")},
		{Index: 5, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("five")},
	})
	if got := log.firstIndex(); got != 4 {
		t.Fatalf("firstIndex = %d, want 4", got)
	}
	if got := log.lastIndex(); got != 5 {
		t.Fatalf("lastIndex = %d, want 5", got)
	}
	for index, wantTerm := range map[uint64]uint64{3: 2, 4: 3, 5: 3} {
		if term, ok := log.term(index); !ok || term != wantTerm {
			t.Fatalf("term(%d) = (%d,%v), want (%d,true)", index, term, ok, wantTerm)
		}
	}
	if _, ok := log.term(2); ok {
		t.Fatal("term(2) found compacted entry, want missing")
	}
	if _, ok := log.term(6); ok {
		t.Fatal("term(6) found future entry, want missing")
	}

	data := []byte("six")
	if index := log.appendLocal(4, raftpb.EntryType_NORMAL, data); index != 6 {
		t.Fatalf("appendLocal index = %d, want 6", index)
	}
	data[0] = 'X'
	if entry := log.entry(6); entry == nil || string(entry.data) != "six" {
		t.Fatalf("entry(6) = %v, want cloned data six", entry)
	}
}

func TestFollowerAppendConsistencyAndConflictReplacement(t *testing.T) {
	t.Run("missing previous entry rejects", func(t *testing.T) {
		node := replicationFollower([]raftpb.Entry{
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("one")},
		})
		node.Step(replicationAppendRequest(2, 1, 3, 2, 2, 0))
		ready := node.Ready()
		if replicationAppendSuccess(t, ready) {
			t.Fatal("AppendEntries success = true, want false for missing prevLogIndex")
		}
		if len(ready.Entries) != 0 || node.log.lastIndex() != 1 {
			t.Fatalf("missing-prev rejection changed log: Ready entries=%d lastIndex=%d", len(ready.Entries), node.log.lastIndex())
		}
	})

	t.Run("previous term mismatch rejects without truncating", func(t *testing.T) {
		node := replicationFollower([]raftpb.Entry{
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("one")},
			{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("two")},
		})
		node.Step(replicationAppendRequest(2, 1, 3, 2, 9, 0,
			&raftpb.Entry{Index: 3, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("new")},
		))
		ready := node.Ready()
		if replicationAppendSuccess(t, ready) {
			t.Fatal("AppendEntries success = true, want false for prevLogTerm mismatch")
		}
		if len(ready.Entries) != 0 || node.log.lastIndex() != 2 || node.log.entry(2).term != 2 {
			t.Fatalf("term-mismatch rejection changed log: Ready entries=%d lastIndex=%d entry2=%v", len(ready.Entries), node.log.lastIndex(), node.log.entry(2))
		}
	})

	t.Run("conflicting suffix truncates then appends", func(t *testing.T) {
		node := replicationFollower([]raftpb.Entry{
			{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("one")},
			{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("old-two")},
			{Index: 3, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("old-three")},
		})
		node.Step(replicationAppendRequest(2, 1, 3, 1, 1, 0,
			&raftpb.Entry{Index: 2, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("new-two")},
			&raftpb.Entry{Index: 3, Term: 3, Type: raftpb.EntryType_NORMAL, Data: []byte("new-three")},
		))
		ready := node.Ready()
		if !replicationAppendSuccess(t, ready) {
			t.Fatal("AppendEntries success = false, want true for matching prev entry")
		}
		if len(ready.Entries) != 2 || !replicationEntry(&ready.Entries[0], 2, 3, []byte("new-two")) || !replicationEntry(&ready.Entries[1], 3, 3, []byte("new-three")) {
			t.Fatalf("Ready entries = %s, want replacement suffix at indexes 2 and 3", replicationEntries(ready.Entries))
		}
		if node.log.lastIndex() != 3 || !bytes.Equal(node.log.entry(2).data, []byte("new-two")) || !bytes.Equal(node.log.entry(3).data, []byte("new-three")) {
			t.Fatalf("in-memory log after replacement = %s", replicationEntries(node.log.rangeEntries(1, node.log.lastIndex()+1)))
		}
		node.Advance()
		if got := node.Ready().Entries; len(got) != 0 {
			t.Fatalf("replacement entries re-emitted after Advance: %s", replicationEntries(got))
		}
	})
}

func TestFollowerSuccessReportsAndCommitsOnlyRequestCoverage(t *testing.T) {
	node := replicationFollower([]raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("one")},
		{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("two")},
		{Index: 3, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("three")},
	})

	// The follower already has later entries, but this empty request proves
	// agreement only through prevLogIndex=1.
	node.Step(replicationAppendRequest(2, 1, 3, 1, 1, 3))
	ready := node.Ready()
	if len(ready.Messages) != 1 || ready.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("Ready messages = %v, want one AppendEntriesResp", ready.Messages)
	}
	response := ready.Messages[0].GetAppendEntriesResp()
	if !response.Success || response.MatchIndex != 1 {
		t.Fatalf("AppendEntriesResp = %v, want success covering exactly index 1", response)
	}
	if node.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want request-covered index 1", node.commitIndex)
	}
	if len(ready.CommittedEntries) != 1 || !replicationEntry(&ready.CommittedEntries[0], 1, 1, []byte("one")) {
		t.Fatalf("committed entries = %s, want only request-covered entry 1", replicationEntries(ready.CommittedEntries))
	}
}

func TestLeaderProposeAppendsAndFansOut(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}},
		InitialState{
			HardState: HardState{Term: 7},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 7, Type: raftpb.EntryType_NOOP},
			},
		},
		fixedTestRand{},
	)
	node.role = leader
	node.commitIndex = 1
	node.lastApplied = 1
	node.nextIndex = map[NodeID]uint64{1: 2, 2: 2, 3: 2}
	node.matchIndex = map[NodeID]uint64{1: 1, 2: 1, 3: 1}
	node.inflight = make(map[NodeID]*appendInflight)

	data := []byte("command")
	index, term, ok := node.Propose(data)
	if !ok || index != 2 || term != 7 {
		t.Fatalf("Propose = (%d,%d,%v), want (2,7,true)", index, term, ok)
	}
	data[0] = 'X'
	ready := node.Ready()
	if len(ready.Entries) != 1 || !replicationEntry(&ready.Entries[0], 2, 7, []byte("command")) {
		t.Fatalf("Ready entries = %s, want cloned normal proposal", replicationEntries(ready.Entries))
	}
	if len(ready.Messages) != 2 {
		t.Fatalf("proposal fan-out messages = %d, want 2", len(ready.Messages))
	}
	for _, peer := range []NodeID{2, 3} {
		message := replicationMessageTo(t, ready.Messages, peer)
		req := message.GetAppendEntries()
		if req == nil || req.PrevLogIndex != 1 || req.PrevLogTerm != 7 || req.LeaderCommit != 1 || len(req.Entries) != 1 ||
			!replicationEntry(req.Entries[0], 2, 7, []byte("command")) {
			t.Fatalf("proposal AppendEntries to %d = %v", peer, req)
		}
	}
}

func TestLeaderRejectionDecrementsNextIndexAndRetries(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}},
		InitialState{
			HardState: HardState{Term: 2},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("old")},
				{Index: 2, Term: 2, Type: raftpb.EntryType_NOOP},
			},
		},
		fixedTestRand{},
	)
	node.role = leader
	node.nextIndex = map[NodeID]uint64{1: 3, 2: 3, 3: 3}
	node.matchIndex = map[NodeID]uint64{1: 2, 2: 0, 3: 0}
	node.inflight = make(map[NodeID]*appendInflight)
	node.sendAppend(2, true)
	probe := replicationMessageTo(t, node.Ready().Messages, 2).GetAppendEntries()
	if probe.PrevLogIndex != 2 || probe.PrevLogTerm != 2 || len(probe.Entries) != 0 {
		t.Fatalf("initial probe = %v, want empty after index 2 term 2", probe)
	}
	node.Advance()

	node.Step(replicationAppendResponse(2, 1, 2, false, 0))
	ready := node.Ready()
	if node.nextIndex[2] != 2 {
		t.Fatalf("nextIndex[2] = %d, want 2 after linear decrement", node.nextIndex[2])
	}
	retry := replicationMessageTo(t, ready.Messages, 2).GetAppendEntries()
	if retry.PrevLogIndex != 1 || retry.PrevLogTerm != 1 || len(retry.Entries) != 1 || !replicationEntry(retry.Entries[0], 2, 2, nil) {
		t.Fatalf("retry AppendEntries = %v, want prev (1,1) plus index-2 NOOP", retry)
	}
	node.Advance()

	node.Step(replicationAppendResponse(2, 1, 2, true, 2))
	if node.matchIndex[2] != 2 || node.nextIndex[2] != 3 {
		t.Fatalf("successful retry progress = match %d next %d, want match 2 next 3", node.matchIndex[2], node.nextIndex[2])
	}
}

func TestLeaderDelayedDuplicateProbeResponseDoesNotCreditInflightSuffix(t *testing.T) {
	leaderNode := NewNode(
		Config{
			ID:              1,
			Peers:           []NodeID{1, 2, 3},
			ElectionTickMin: 1,
			ElectionTickMax: 2,
			HeartbeatTicks:  1,
		},
		InitialState{},
		fixedTestRand{},
	)
	followerNode := NewNode(
		Config{ID: 2, Peers: []NodeID{1, 2, 3}},
		InitialState{},
		fixedTestRand{},
	)

	// Node 1 wins term 1 and sends the pre-NOOP empty probe to node 2.
	leaderNode.Tick()
	campaign := leaderNode.Ready()
	if len(campaign.Messages) != 2 {
		t.Fatalf("campaign messages = %d, want two RequestVote requests", len(campaign.Messages))
	}
	leaderNode.Advance()
	leaderNode.Step(requestVoteResponse(2, 1, 1, true))
	leadership := leaderNode.Ready()
	if len(leadership.Entries) != 1 || !replicationTypedEntry(&leadership.Entries[0], 1, 1, raftpb.EntryType_NOOP, nil) {
		t.Fatalf("leadership entries = %s, want NOOP@1", replicationEntries(leadership.Entries))
	}
	probe := replicationMessageTo(t, leadership.Messages, 2)
	probeRequest := probe.GetAppendEntries()
	if probeRequest.PrevLogIndex != 0 || len(probeRequest.Entries) != 0 {
		t.Fatalf("initial request to node 2 = %v, want empty probe at index 0", probeRequest)
	}
	leaderNode.Advance()

	// While that probe is in flight, append a normal proposal at index 2.
	index, term, ok := leaderNode.Propose([]byte("proposal"))
	if !ok || index != 2 || term != 1 {
		t.Fatalf("Propose = (%d,%d,%v), want (2,1,true)", index, term, ok)
	}
	proposal := leaderNode.Ready()
	if len(proposal.Entries) != 1 || !replicationEntry(&proposal.Entries[0], 2, 1, []byte("proposal")) {
		t.Fatalf("proposal entries = %s, want normal proposal@2", replicationEntries(proposal.Entries))
	}
	leaderNode.Advance()

	// Node 2's real probe response covers only index 0. Consuming it starts the
	// newer suffix request for NOOP@1 and the proposal@2.
	followerNode.Step(probe)
	probeResult := followerNode.Ready()
	if len(probeResult.Messages) != 1 || probeResult.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("probe result messages = %v, want one AppendEntriesResp", probeResult.Messages)
	}
	probeResponse := probeResult.Messages[0]
	if response := probeResponse.GetAppendEntriesResp(); !response.Success || response.MatchIndex != 0 {
		t.Fatalf("probe response = %v, want success acknowledging index 0", response)
	}
	followerNode.Advance()

	leaderNode.Step(probeResponse)
	suffixResult := leaderNode.Ready()
	suffix := replicationMessageTo(t, suffixResult.Messages, 2)
	suffixRequest := suffix.GetAppendEntries()
	if suffixRequest.PrevLogIndex != 0 || len(suffixRequest.Entries) != 2 ||
		!replicationTypedEntry(suffixRequest.Entries[0], 1, 1, raftpb.EntryType_NOOP, nil) ||
		!replicationEntry(suffixRequest.Entries[1], 2, 1, []byte("proposal")) {
		t.Fatalf("suffix request = %v, want entries 1..2", suffixRequest)
	}
	outstanding := leaderNode.inflight[2]
	if outstanding == nil || outstanding.lastIndex != 2 {
		t.Fatalf("inflight[2] = %v, want suffix through index 2", outstanding)
	}
	leaderNode.Advance()

	// Deliver a delayed duplicate of the old index-0 success before node 2
	// receives the suffix. It must be completely inert.
	leaderNode.Step(probeResponse)
	staleResult := leaderNode.Ready()
	if leaderNode.matchIndex[2] != 0 || leaderNode.nextIndex[2] != 1 || leaderNode.commitIndex != 0 {
		t.Fatalf("stale probe progress = match %d next %d commit %d, want 0,1,0", leaderNode.matchIndex[2], leaderNode.nextIndex[2], leaderNode.commitIndex)
	}
	if len(staleResult.CommittedEntries) != 0 {
		t.Fatalf("stale probe emitted committed entries: %s", replicationEntries(staleResult.CommittedEntries))
	}
	if len(staleResult.Messages) != 0 {
		t.Fatalf("stale probe replaced the suffix with %d messages", len(staleResult.Messages))
	}
	if leaderNode.inflight[2] != outstanding {
		t.Fatalf("stale probe changed inflight[2] from %p to %p", outstanding, leaderNode.inflight[2])
	}
	leaderNode.Advance()

	// A heartbeat tick retries the exact outstanding suffix, proving the stale
	// response did not strand it.
	leaderNode.Tick()
	retryResult := leaderNode.Ready()
	retry := replicationMessageTo(t, retryResult.Messages, 2).GetAppendEntries()
	if retry.PrevLogIndex != 0 || len(retry.Entries) != 2 || retry.Entries[1].Index != 2 {
		t.Fatalf("retried suffix = %v, want outstanding entries 1..2", retry)
	}
	leaderNode.Advance()

	// The real suffix acknowledgement still makes normal progress and commits
	// both current-term entries with the leader and node 2 as a majority.
	followerNode.Step(suffix)
	realSuffixResult := followerNode.Ready()
	if len(realSuffixResult.Messages) != 1 || realSuffixResult.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("suffix result messages = %v, want one AppendEntriesResp", realSuffixResult.Messages)
	}
	suffixResponse := realSuffixResult.Messages[0]
	if response := suffixResponse.GetAppendEntriesResp(); !response.Success || response.MatchIndex != 2 {
		t.Fatalf("suffix response = %v, want success acknowledging index 2", response)
	}
	followerNode.Advance()

	leaderNode.Step(suffixResponse)
	committed := leaderNode.Ready()
	if leaderNode.matchIndex[2] != 2 || leaderNode.nextIndex[2] != 3 || leaderNode.commitIndex != 2 {
		t.Fatalf("real suffix progress = match %d next %d commit %d, want 2,3,2", leaderNode.matchIndex[2], leaderNode.nextIndex[2], leaderNode.commitIndex)
	}
	if len(committed.CommittedEntries) != 2 ||
		!replicationTypedEntry(&committed.CommittedEntries[0], 1, 1, raftpb.EntryType_NOOP, nil) ||
		!replicationEntry(&committed.CommittedEntries[1], 2, 1, []byte("proposal")) {
		t.Fatalf("committed entries = %s, want [NOOP@1, proposal@2]", replicationEntries(committed.CommittedEntries))
	}
}

func TestLeaderIgnoresOutOfRangeSuccessAcknowledgement(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}},
		InitialState{
			HardState: HardState{Term: 2},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 2, Type: raftpb.EntryType_NOOP},
				{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL},
			},
		},
		fixedTestRand{},
	)
	node.role = leader
	node.nextIndex = map[NodeID]uint64{1: 3, 2: 1, 3: 1}
	node.matchIndex = map[NodeID]uint64{1: 2, 2: 0, 3: 0}
	node.inflight = make(map[NodeID]*appendInflight)
	node.sendAppend(2, false)
	outstanding := node.inflight[2]
	node.Advance()

	node.Step(replicationAppendResponse(2, 1, 2, true, 3))
	ready := node.Ready()
	if node.matchIndex[2] != 0 || node.nextIndex[2] != 1 || node.commitIndex != 0 {
		t.Fatalf("out-of-range progress = match %d next %d commit %d, want 0,1,0", node.matchIndex[2], node.nextIndex[2], node.commitIndex)
	}
	if node.inflight[2] != outstanding {
		t.Fatalf("out-of-range acknowledgement changed inflight[2] from %p to %p", outstanding, node.inflight[2])
	}
	if len(ready.Messages) != 0 || len(ready.CommittedEntries) != 0 {
		t.Fatalf("out-of-range acknowledgement emitted messages=%d committed=%s", len(ready.Messages), replicationEntries(ready.CommittedEntries))
	}
}

func TestCommitRequiresMajorityAtCurrentTerm(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3, 4, 5}},
		InitialState{
			HardState: HardState{Term: 4},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL},
				{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("x")},
			},
		},
		fixedTestRand{},
	)
	node.role = leader
	node.nextIndex = make(map[NodeID]uint64)
	node.matchIndex = map[NodeID]uint64{1: 2, 2: 2, 3: 2, 4: 0, 5: 0}
	node.inflight = make(map[NodeID]*appendInflight)
	if node.advanceCommit() || node.commitIndex != 0 {
		t.Fatalf("old-term majority advanced commitIndex to %d", node.commitIndex)
	}

	if index := node.appendLocal(raftpb.EntryType_NOOP, nil); index != 3 {
		t.Fatalf("NOOP index = %d, want 3", index)
	}
	if node.advanceCommit() || node.commitIndex != 0 {
		t.Fatalf("unreplicated current-term NOOP advanced commitIndex to %d", node.commitIndex)
	}
	node.matchIndex[2] = 3
	node.matchIndex[3] = 3
	if !node.advanceCommit() || node.commitIndex != 3 {
		t.Fatalf("current-term majority commitIndex = %d, want 3", node.commitIndex)
	}
}

func TestLeaderAppendsNOOPAtEveryTermStart(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}, ElectionTickMin: 1, ElectionTickMax: 2},
		InitialState{
			HardState: HardState{Term: 3},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL},
				{Index: 2, Term: 2, Type: raftpb.EntryType_NORMAL},
			},
		},
		fixedTestRand{},
	)

	node.Tick()
	ready := node.Ready()
	for _, message := range ready.Messages {
		req := message.GetRequestVote()
		if req == nil || req.LastLogIndex != 2 || req.LastLogTerm != 2 {
			t.Fatalf("term-4 RequestVote = %v, want last log (2,2)", req)
		}
	}
	node.Advance()
	node.Step(requestVoteResponse(2, 1, 4, true))
	ready = node.Ready()
	if len(ready.Entries) != 1 || !replicationTypedEntry(&ready.Entries[0], 3, 4, raftpb.EntryType_NOOP, nil) {
		t.Fatalf("first leadership entries = %s, want term-4 NOOP", replicationEntries(ready.Entries))
	}
	node.Advance()

	node.Step(requestVoteResponse(2, 1, 5, false))
	node.Advance()
	node.Tick()
	node.Advance()
	node.Step(requestVoteResponse(2, 1, 6, true))
	ready = node.Ready()
	if len(ready.Entries) != 1 || !replicationTypedEntry(&ready.Entries[0], 4, 6, raftpb.EntryType_NOOP, nil) {
		t.Fatalf("second leadership entries = %s, want term-6 NOOP", replicationEntries(ready.Entries))
	}
}

func TestReadyCommittedEntriesOrderedAndExactlyOncePerAdvance(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1}, ElectionTickMin: 1, ElectionTickMax: 2},
		InitialState{
			HardState: HardState{Term: 2},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("old")},
			},
		},
		fixedTestRand{},
	)
	node.Tick()
	first := node.Ready()
	second := node.Ready()
	for call, ready := range []Ready{first, second} {
		if len(ready.CommittedEntries) != 2 ||
			!replicationEntry(&ready.CommittedEntries[0], 1, 1, []byte("old")) ||
			!replicationTypedEntry(&ready.CommittedEntries[1], 2, 3, raftpb.EntryType_NOOP, nil) {
			t.Fatalf("Ready call %d committed = %s, want ordered [old, NOOP]", call+1, replicationEntries(ready.CommittedEntries))
		}
	}
	if node.lastApplied != 0 {
		t.Fatalf("lastApplied before Advance = %d, want 0", node.lastApplied)
	}
	node.Advance()
	if node.lastApplied != 2 {
		t.Fatalf("lastApplied after Advance = %d, want 2", node.lastApplied)
	}
	if ready := node.Ready(); len(ready.CommittedEntries) != 0 {
		t.Fatalf("committed entries re-emitted after Advance: %s", replicationEntries(ready.CommittedEntries))
	}
}

func replicationFollower(entries []raftpb.Entry) *Node {
	return NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}},
		InitialState{HardState: HardState{Term: 3}, Entries: entries},
		fixedTestRand{},
	)
}

func replicationAppendRequest(from, to, term, prevIndex, prevTerm, leaderCommit uint64, entries ...*raftpb.Entry) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: term,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term:         term,
			LeaderId:     from,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}},
	}
}

func replicationAppendResponse(from, to, term uint64, success bool, matchIndex uint64) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: term,
		Body: &raftpb.Message_AppendEntriesResp{AppendEntriesResp: &raftpb.AppendEntriesResp{
			Term:       term,
			Success:    success,
			MatchIndex: matchIndex,
		}},
	}
}

func replicationAppendSuccess(t *testing.T, ready Ready) bool {
	t.Helper()
	if len(ready.Messages) != 1 || ready.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("Ready messages = %v, want one AppendEntriesResp", ready.Messages)
	}
	return ready.Messages[0].GetAppendEntriesResp().Success
}

func replicationMessageTo(t *testing.T, messages []*raftpb.Message, to NodeID) *raftpb.Message {
	t.Helper()
	for _, message := range messages {
		if NodeID(message.To) == to && message.GetAppendEntries() != nil {
			return message
		}
	}
	t.Fatalf("no AppendEntries to %d in %v", to, messages)
	return nil
}

func replicationTypedEntry(entry *raftpb.Entry, index, term uint64, typ raftpb.EntryType, data []byte) bool {
	return entry != nil && entry.Index == index && entry.Term == term && entry.Type == typ && bytes.Equal(entry.Data, data)
}

func replicationEntry(entry *raftpb.Entry, index, term uint64, data []byte) bool {
	return replicationTypedEntry(entry, index, term, entry.GetType(), data)
}

func replicationEntries(entries []raftpb.Entry) string {
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
