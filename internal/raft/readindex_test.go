package raft

import (
	"bytes"
	"reflect"
	"testing"

	raftpb "miniquorum/proto"
)

func TestReadIndexFreshLeaderWaitsForCurrentTermNOOPCommit(t *testing.T) {
	node := NewNode(
		Config{
			ID:              1,
			Peers:           []NodeID{1, 2, 3},
			ElectionTickMin: 1,
			ElectionTickMax: 2,
		},
		InitialState{
			HardState: HardState{Term: 1},
			Entries: []raftpb.Entry{
				{Index: 1, Term: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("old-term")},
			},
		},
		fixedTestRand{},
	)
	// Model an already-applied old-term prefix. The freshly elected term-2
	// leader must still commit its term-start NOOP before ReadIndex can begin.
	node.commitIndex = 1
	node.lastApplied = 1

	node.Tick()
	node.Advance()
	node.Step(requestVoteResponse(2, 1, 2, true))
	leadership := node.Ready()
	if len(leadership.Entries) != 1 ||
		!replicationTypedEntry(&leadership.Entries[0], 2, 2, raftpb.EntryType_NOOP, nil) {
		t.Fatalf("leadership entries = %s, want term-2 NOOP@2", replicationEntries(leadership.Entries))
	}
	probe := replicationMessageTo(t, leadership.Messages, 2).GetAppendEntries()
	if probe.GetPrevLogIndex() != 1 || len(probe.GetEntries()) != 0 {
		t.Fatalf("initial probe = %v, want empty request covering old-term index 1", probe)
	}
	node.Advance()

	node.RequestRead([]byte("fresh-leader"))
	guarded := node.Ready()
	if got := readIndexRequests(guarded.Messages); len(got) != 0 {
		t.Fatalf("read confirmation heartbeat emitted before current-term NOOP commit: %s", readIndexRequestTrace(got))
	}
	if len(guarded.ReadStates) != 0 {
		t.Fatalf("read completed before current-term NOOP commit: %+v", guarded.ReadStates)
	}
	node.Advance()

	// Acknowledging the empty probe only proves the old-term prefix. It causes
	// the leader to send the term-2 NOOP, but still cannot start ReadIndex.
	node.Step(replicationAppendResponse(2, 1, 2, true, 1))
	suffixReady := node.Ready()
	suffix := replicationMessageTo(t, suffixReady.Messages, 2).GetAppendEntries()
	if suffix.GetPrevLogIndex() != 1 || len(suffix.GetEntries()) != 1 ||
		!replicationTypedEntry(suffix.GetEntries()[0], 2, 2, raftpb.EntryType_NOOP, nil) {
		t.Fatalf("NOOP suffix = %v, want term-2 NOOP@2 after old prefix", suffix)
	}
	if got := readIndexRequests(suffixReady.Messages); len(got) != 0 {
		t.Fatalf("read round started after old-prefix ack but before NOOP ack: %s", readIndexRequestTrace(got))
	}
	node.Advance()

	// The NOOP acknowledgement gives the leader a current-term commit. The
	// queued token now starts exactly one explicitly tagged heartbeat round.
	node.Step(replicationAppendResponse(2, 1, 2, true, 2))
	started := node.Ready()
	requests := readIndexRequests(started.Messages)
	if len(requests) != 2 {
		t.Fatalf("ReadIndex messages after NOOP commit = %s, want one heartbeat to each follower", readIndexRequestTrace(requests))
	}
	round := requests[0].GetAppendEntries().GetReadRound()
	if round == 0 || requests[1].GetAppendEntries().GetReadRound() != round {
		t.Fatalf("ReadIndex round tags = %s, want one shared non-zero round", readIndexRequestTrace(requests))
	}
	if len(started.ReadStates) != 0 {
		t.Fatalf("read completed before follower heartbeat ack: %+v", started.ReadStates)
	}
	node.Advance()

	node.Step(readIndexResponse(2, 1, 2, 2, round, 2, true))
	confirmed := node.Ready()
	assertReadStates(t, confirmed.ReadStates, []expectedReadState{{ctx: "fresh-leader", index: 2}})
}

func TestReadIndexBatchesQueuedTokensIntoOneExactHeartbeatRoundAndCountsSelf(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{3, 1, 2}, 5)
	contexts := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for _, ctx := range contexts {
		node.RequestRead(ctx)
	}
	contexts[0][0] = 'X'

	ready := node.Ready()
	if len(ready.Messages) != 2 {
		t.Fatalf("three queued reads emitted %d total messages, want exactly two follower heartbeats", len(ready.Messages))
	}
	requests := readIndexRequests(ready.Messages)
	if len(requests) != 2 || requests[0].GetTo() != 2 || requests[1].GetTo() != 3 {
		t.Fatalf("ReadIndex request trace = %s, want sorted recipients [2 3]", readIndexRequestTrace(requests))
	}
	round := requests[0].GetAppendEntries().GetReadRound()
	for _, message := range requests {
		req := message.GetAppendEntries()
		if round == 0 || req.GetReadRound() != round || len(req.GetEntries()) != 0 ||
			req.GetTerm() != 5 || req.GetPrevLogIndex() != 1 || req.GetPrevLogTerm() != 5 ||
			req.GetLeaderCommit() != 1 {
			t.Fatalf("batched ReadIndex heartbeat = %v, shared round %d", req, round)
		}
	}
	if len(ready.ReadStates) != 0 {
		t.Fatalf("reads completed before quorum ack: %+v", ready.ReadStates)
	}
	node.Advance()

	// Self plus one follower is a majority of three; follower 3 is not needed.
	node.Step(readIndexResponse(2, 1, 5, 5, round, 1, true))
	confirmed := node.Ready()
	assertReadStates(t, confirmed.ReadStates, []expectedReadState{
		{ctx: "alpha", index: 1},
		{ctx: "beta", index: 1},
		{ctx: "gamma", index: 1},
	})
}

func TestReadIndexAckIdentityDedupAndMatchProgressIsolation(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{1, 2, 3, 4, 5}, 7)
	ordinaryInflight := &appendInflight{prevIndex: 1, prevTerm: 7, lastIndex: 1, leaderCommit: 1}
	node.inflight[2] = ordinaryInflight
	wantMatches := cloneMatchIndexes(node.matchIndex)

	node.RequestRead([]byte("first"))
	started := node.Ready()
	requests := readIndexRequests(started.Messages)
	if len(requests) != 4 {
		t.Fatalf("first round trace = %s, want one heartbeat per follower", readIndexRequestTrace(requests))
	}
	round := requests[0].GetAppendEntries().GetReadRound()
	node.Advance()

	// Self plus one distinct follower is only two votes in a five-node cluster.
	node.Step(readIndexResponse(2, 1, 7, 7, round, 1, true))
	assertNoReadStatesAndAdvance(t, node, "first follower ack")
	node.Step(readIndexResponse(2, 1, 7, 7, round, 1, true))
	assertNoReadStatesAndAdvance(t, node, "duplicate same-follower ack")
	if got := len(node.activeRead.acks); got != 2 {
		t.Fatalf("ack set after duplicate = %d, want self plus follower 2", got)
	}

	// The outer message term keeps Step in term 7 while the response body
	// exercises the explicit wrong-term check.
	node.Step(readIndexResponse(3, 1, 7, 6, round, 1, true))
	assertNoReadStatesAndAdvance(t, node, "wrong response term")
	node.Step(readIndexResponse(3, 1, 7, 7, round+1, 1, true))
	assertNoReadStatesAndAdvance(t, node, "wrong round")
	node.Step(readIndexResponse(3, 1, 7, 7, round, 0, true))
	assertNoReadStatesAndAdvance(t, node, "wrong request-covered match index")
	node.Step(readIndexResponse(3, 1, 7, 7, round, 1, false))
	assertNoReadStatesAndAdvance(t, node, "rejected heartbeat")
	node.Step(readIndexResponse(99, 1, 7, 7, round, 1, true))
	assertNoReadStatesAndAdvance(t, node, "non-peer ack")

	if !reflect.DeepEqual(node.matchIndex, wantMatches) {
		t.Fatalf("ReadIndex acks mutated replication matchIndex: got %v want %v", node.matchIndex, wantMatches)
	}
	if node.inflight[2] != ordinaryInflight {
		t.Fatalf("ReadIndex ack consumed ordinary inflight request: got %p want %p", node.inflight[2], ordinaryInflight)
	}

	node.Step(readIndexResponse(3, 1, 7, 7, round, 1, true))
	confirmed := node.Ready()
	assertReadStates(t, confirmed.ReadStates, []expectedReadState{{ctx: "first", index: 1}})
	node.Advance()

	// A newer round can cover the same match index. A delayed equal-match reply
	// from the prior round must not be reattributed to it; the explicit round
	// echo, not matchIndex, distinguishes the requests.
	node.RequestRead([]byte("second"))
	secondReady := node.Ready()
	secondRequests := readIndexRequests(secondReady.Messages)
	if len(secondRequests) != 4 {
		t.Fatalf("second round trace = %s, want one heartbeat per follower", readIndexRequestTrace(secondRequests))
	}
	secondRound := secondRequests[0].GetAppendEntries().GetReadRound()
	if secondRound == round || secondRound == 0 {
		t.Fatalf("second round = %d, first = %d; identities must never be reused", secondRound, round)
	}
	node.Advance()

	node.Step(readIndexResponse(2, 1, 7, 7, round, 1, true))
	assertNoReadStatesAndAdvance(t, node, "delayed equal-match prior-round ack")
	node.Step(readIndexResponse(2, 1, 7, 7, secondRound, 1, true))
	assertNoReadStatesAndAdvance(t, node, "one current-round follower ack")
	node.Step(readIndexResponse(2, 1, 7, 7, secondRound, 1, true))
	assertNoReadStatesAndAdvance(t, node, "duplicate current-round follower ack")
	node.Step(readIndexResponse(3, 1, 7, 7, secondRound, 1, true))
	secondConfirmed := node.Ready()
	assertReadStates(t, secondConfirmed.ReadStates, []expectedReadState{{ctx: "second", index: 1}})
	if !reflect.DeepEqual(node.matchIndex, wantMatches) || node.inflight[2] != ordinaryInflight {
		t.Fatalf("read rounds disturbed decision-15 progress: match=%v inflight2=%p", node.matchIndex, node.inflight[2])
	}
}

func TestReadIndexHigherTermStepDownCancelsActiveAndQueuedExactlyOnce(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{1, 2, 3}, 4)
	node.RequestRead([]byte("active"))
	started := node.Ready()
	requests := readIndexRequests(started.Messages)
	if len(requests) != 2 {
		t.Fatalf("active round trace = %s, want two follower heartbeats", readIndexRequestTrace(requests))
	}
	round := requests[0].GetAppendEntries().GetReadRound()
	node.Advance()
	node.RequestRead([]byte("queued"))

	// A higher-term response is not a usable read ack: normal Raft term handling
	// first steps the node down and emits retryable cancellation for both sets.
	node.Step(readIndexResponse(2, 1, 5, 5, round, 1, true))
	first := node.Ready()
	assertReadStates(t, first.ReadStates, []expectedReadState{
		{ctx: "active", canceled: true},
		{ctx: "queued", canceled: true},
	})
	second := node.Ready()
	if !reflect.DeepEqual(first.ReadStates, second.ReadStates) {
		t.Fatalf("step-down cancellations changed before Advance:\nfirst=%+v\nsecond=%+v", first.ReadStates, second.ReadStates)
	}
	node.Advance()
	if ready := node.Ready(); len(ready.ReadStates) != 0 {
		t.Fatalf("step-down cancellations repeated after Advance: %+v", ready.ReadStates)
	}

	// Replaying the higher-term response cannot cancel either token twice.
	node.Step(readIndexResponse(2, 1, 5, 5, round, 1, true))
	if ready := node.Ready(); len(ready.ReadStates) != 0 {
		t.Fatalf("duplicate higher-term response re-emitted cancellation: %+v", ready.ReadStates)
	}
	node.Advance()

	// A follower rejects a new token immediately and distinctly, allowing the
	// server to return NotLeader rather than a generic leadership-loss error.
	token := []byte("follower")
	node.RequestRead(token)
	token[0] = 'X'
	rejected := node.Ready()
	assertReadStates(t, rejected.ReadStates, []expectedReadState{{ctx: "follower", canceled: true, rejected: true}})
}

func TestReadIndexClientCancellationRemovesActiveAndQueuedWithoutResult(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{1, 2, 3}, 9)
	node.config.HeartbeatTicks = 1
	node.RequestRead([]byte("active"))
	started := node.Ready()
	requests := readIndexRequests(started.Messages)
	if len(requests) != 2 {
		t.Fatalf("active round trace = %s, want two follower heartbeats", readIndexRequestTrace(requests))
	}
	firstRound := requests[0].GetAppendEntries().GetReadRound()
	node.Advance()
	node.RequestRead([]byte("queued"))

	if !node.CancelRead([]byte("queued")) {
		t.Fatal("CancelRead(queued) = false, want removal")
	}
	if !node.CancelRead([]byte("active")) {
		t.Fatal("CancelRead(active) = false, want removal")
	}
	if node.CancelRead([]byte("active")) {
		t.Fatal("second CancelRead(active) = true, want exactly-once removal")
	}
	if node.activeRead != nil || len(node.pendingReads) != 0 {
		t.Fatalf("cancellation retained core state: active=%+v queued=%q", node.activeRead, node.pendingReads)
	}
	if ready := node.Ready(); len(ready.ReadStates) != 0 || len(readIndexRequests(ready.Messages)) != 0 {
		t.Fatalf("client cancellation emitted/retained read output: states=%+v messages=%s", ready.ReadStates, readIndexRequestTrace(readIndexRequests(ready.Messages)))
	}
	node.Advance()

	// A periodic heartbeat may emit ordinary replication probes, but never a
	// tagged retry for either canceled token.
	node.Tick()
	if ready := node.Ready(); len(readIndexRequests(ready.Messages)) != 0 {
		t.Fatalf("canceled read retried on heartbeat: %s", readIndexRequestTrace(readIndexRequests(ready.Messages)))
	}
	node.Advance()

	// The next request gets a fresh identity; cancellation cannot make a stale
	// response from the abandoned round eligible for the new request.
	node.RequestRead([]byte("next"))
	next := node.Ready()
	nextRequests := readIndexRequests(next.Messages)
	if len(nextRequests) != 2 {
		t.Fatalf("next round trace = %s, want two follower heartbeats", readIndexRequestTrace(nextRequests))
	}
	if nextRound := nextRequests[0].GetAppendEntries().GetReadRound(); nextRound <= firstRound {
		t.Fatalf("round after cancellation = %d, want greater than abandoned round %d", nextRound, firstRound)
	}
}

func TestReadStatesReadyAdvanceOwnershipAndCancellationPrefix(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{1}, 3)
	original := []byte("alpha")
	node.RequestRead(original)
	original[0] = 'X'

	first := node.Ready()
	assertReadStates(t, first.ReadStates, []expectedReadState{{ctx: "alpha", index: 1}})
	repeated := node.Ready()
	if !reflect.DeepEqual(first.ReadStates, repeated.ReadStates) {
		t.Fatalf("ReadStates changed without Advance:\nfirst=%+v\nsecond=%+v", first.ReadStates, repeated.ReadStates)
	}
	first.ReadStates[0].Ctx[0] = 'Y'
	first.ReadStates[0].Index = 99
	again := node.Ready()
	assertReadStates(t, again.ReadStates, []expectedReadState{{ctx: "alpha", index: 1}})

	// alpha has already become an immutable Ready-prefix result. CancelRead
	// must not delete it. Produce beta internally before acknowledging alpha;
	// Advance may consume only the prefix actually exposed by the last Ready.
	if node.CancelRead([]byte("alpha")) {
		t.Fatal("CancelRead(materialized alpha) = true, want completed result left for Advance")
	}
	node.RequestRead([]byte("beta"))
	node.maybeStartReadRound()
	if got := len(node.readStates); got != 2 {
		t.Fatalf("internal states before Advance = %d, want exposed alpha plus unseen beta", got)
	}
	node.Advance()

	next := node.Ready()
	assertReadStates(t, next.ReadStates, []expectedReadState{{ctx: "beta", index: 1}})
	next.ReadStates[0].Ctx[0] = 'Z'
	next.ReadStates[0].Index = 88
	nextRepeat := node.Ready()
	assertReadStates(t, nextRepeat.ReadStates, []expectedReadState{{ctx: "beta", index: 1}})
	node.Advance()
	if final := node.Ready(); len(final.ReadStates) != 0 {
		t.Fatalf("beta repeated after Advance: %+v", final.ReadStates)
	}
}

func TestReadStatesExposedPrefixSurvivesSameTermStepDownCancellationInterleave(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{1, 2, 3}, 6)
	node.RequestRead([]byte("exposed"))
	started := node.Ready()
	requests := readIndexRequests(started.Messages)
	if len(requests) != 2 {
		t.Fatalf("exposed round trace = %s, want two follower heartbeats", readIndexRequestTrace(requests))
	}
	round := requests[0].GetAppendEntries().GetReadRound()
	node.Advance()
	node.Step(readIndexResponse(2, 1, 6, 6, round, 1, true))
	exposed := node.Ready()
	assertReadStates(t, exposed.ReadStates, []expectedReadState{{ctx: "exposed", index: 1}})

	// Leave the exposed Ready unadvanced, then create an active and a queued
	// token. A same-term AppendEntries forces becomeFollowerSameTerm, appending
	// both cancellations after the already-exposed immutable prefix.
	node.RequestRead([]byte("active-at-stepdown"))
	node.maybeStartReadRound()
	node.RequestRead([]byte("queued-at-stepdown"))
	node.Step(readIndexRequest(2, 1, 6, 0, 1, 6, 1))
	if node.role != follower {
		t.Fatalf("role after same-term AppendEntries = %v, want follower", node.role)
	}
	if got := len(node.readStates); got != 3 {
		t.Fatalf("internal ReadStates before prefix Advance = %d, want exposed plus two cancellations", got)
	}

	// Advance acknowledges only the ReadState prefix the host actually saw.
	node.Advance()
	cancellations := node.Ready()
	assertReadStates(t, cancellations.ReadStates, []expectedReadState{
		{ctx: "active-at-stepdown", canceled: true},
		{ctx: "queued-at-stepdown", canceled: true},
	})
	node.Advance()
	if final := node.Ready(); len(final.ReadStates) != 0 {
		t.Fatalf("same-term step-down cancellations repeated after Advance: %+v", final.ReadStates)
	}
}

func TestAppendEntriesReadRoundEchoesExactRequestWithoutChangingCoverage(t *testing.T) {
	node := NewNode(
		Config{ID: 2, Peers: []NodeID{1, 2, 3}},
		InitialState{
			HardState: HardState{Term: 5},
			Entries:   []raftpb.Entry{{Index: 1, Term: 5, Type: raftpb.EntryType_NOOP}},
		},
		fixedTestRand{},
	)

	node.Step(readIndexRequest(1, 2, 5, 73, 1, 5, 1))
	success := node.Ready()
	assertReadRoundResponse(t, success, 73, 5, true, 1)
	node.Advance()

	// A mismatching heartbeat still echoes the exact tag but cannot claim any
	// log coverage. This keeps the additive field separate from matchIndex.
	node.Step(readIndexRequest(1, 2, 5, 74, 2, 5, 1))
	rejected := node.Ready()
	assertReadRoundResponse(t, rejected, 74, 5, false, 0)
	node.Advance()

	// A lower-term leader also receives the exact tag back, now carrying the
	// follower's current term so normal Raft processing will force step-down.
	node.Step(readIndexRequest(1, 2, 4, 75, 1, 5, 1))
	lowerTerm := node.Ready()
	assertReadRoundResponse(t, lowerTerm, 75, 5, false, 0)
}

func TestReadIndexRoundExhaustionCancelsRetryablyWithoutWrapping(t *testing.T) {
	node := committedReadIndexLeader([]NodeID{1, 2, 3}, 11)
	node.nextReadRound = ^uint64(0)
	node.RequestRead([]byte("exhausted"))
	ready := node.Ready()
	assertReadStates(t, ready.ReadStates, []expectedReadState{{ctx: "exhausted", canceled: true}})
	if len(readIndexRequests(ready.Messages)) != 0 || node.nextReadRound != ^uint64(0) {
		t.Fatalf("round exhaustion emitted heartbeat or wrapped: messages=%s next=%d", readIndexRequestTrace(readIndexRequests(ready.Messages)), node.nextReadRound)
	}
}

type expectedReadState struct {
	ctx      string
	index    uint64
	canceled bool
	rejected bool
}

func committedReadIndexLeader(peers []NodeID, term uint64) *Node {
	node := NewNode(
		Config{ID: 1, Peers: peers},
		InitialState{
			HardState: HardState{Term: term},
			Entries:   []raftpb.Entry{{Index: 1, Term: term, Type: raftpb.EntryType_NOOP}},
		},
		fixedTestRand{},
	)
	node.role = leader
	node.commitIndex = 1
	node.lastApplied = 1
	node.nextIndex = make(map[NodeID]uint64, len(node.peers))
	node.matchIndex = make(map[NodeID]uint64, len(node.peers))
	node.inflight = make(map[NodeID]*appendInflight, len(node.peers))
	for _, peer := range node.peers {
		node.nextIndex[peer] = 2
		node.matchIndex[peer] = 1
	}
	return node
}

func readIndexRequest(from, to, term, round, prevIndex, prevTerm, leaderCommit uint64) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: term,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term:         term,
			LeaderId:     from,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			LeaderCommit: leaderCommit,
			ReadRound:    round,
		}},
	}
}

func readIndexResponse(from, to, outerTerm, responseTerm, round, matchIndex uint64, success bool) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: outerTerm,
		Body: &raftpb.Message_AppendEntriesResp{AppendEntriesResp: &raftpb.AppendEntriesResp{
			Term:       responseTerm,
			Success:    success,
			MatchIndex: matchIndex,
			ReadRound:  round,
		}},
	}
}

func readIndexRequests(messages []*raftpb.Message) []*raftpb.Message {
	requests := make([]*raftpb.Message, 0, len(messages))
	for _, message := range messages {
		if req := message.GetAppendEntries(); req != nil && req.GetReadRound() != 0 {
			requests = append(requests, message)
		}
	}
	return requests
}

func readIndexRequestTrace(messages []*raftpb.Message) string {
	if len(messages) == 0 {
		return "[]"
	}
	trace := "["
	for i, message := range messages {
		if i > 0 {
			trace += ", "
		}
		req := message.GetAppendEntries()
		trace += req.String()
	}
	return trace + "]"
}

func assertReadStates(t *testing.T, got []ReadState, want []expectedReadState) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ReadStates = %+v, want %+v", got, want)
	}
	for i := range want {
		if !bytes.Equal(got[i].Ctx, []byte(want[i].ctx)) || got[i].Index != want[i].index ||
			got[i].Canceled != want[i].canceled || got[i].Rejected != want[i].rejected {
			t.Fatalf("ReadStates[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func assertNoReadStatesAndAdvance(t *testing.T, node *Node, after string) {
	t.Helper()
	ready := node.Ready()
	if len(ready.ReadStates) != 0 {
		t.Fatalf("ReadStates after %s = %+v, want none", after, ready.ReadStates)
	}
	node.Advance()
}

func assertReadRoundResponse(t *testing.T, ready Ready, round, term uint64, success bool, matchIndex uint64) {
	t.Helper()
	if len(ready.Messages) != 1 || ready.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("Ready messages = %v, want one AppendEntriesResp", ready.Messages)
	}
	response := ready.Messages[0].GetAppendEntriesResp()
	if response.GetReadRound() != round || response.GetTerm() != term ||
		response.GetSuccess() != success || response.GetMatchIndex() != matchIndex {
		t.Fatalf("AppendEntriesResp = %v, want round=%d term=%d success=%t match=%d", response, round, term, success, matchIndex)
	}
}

func cloneMatchIndexes(indexes map[NodeID]uint64) map[NodeID]uint64 {
	cloned := make(map[NodeID]uint64, len(indexes))
	for peer, index := range indexes {
		cloned[peer] = index
	}
	return cloned
}
