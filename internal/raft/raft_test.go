package raft

import (
	"reflect"
	"testing"

	raftpb "miniquorum/proto"
)

func TestNewNodeAppliesElectionTickMinDefault(t *testing.T) {
	node := NewNode(Config{}, InitialState{}, nil)
	if node.config.ElectionTickMin != 10 {
		t.Fatalf("ElectionTickMin = %d, want 10", node.config.ElectionTickMin)
	}
}

func TestNewNodeAppliesElectionTickMaxDefault(t *testing.T) {
	node := NewNode(Config{}, InitialState{}, nil)
	if node.config.ElectionTickMax != 20 {
		t.Fatalf("ElectionTickMax = %d, want 20", node.config.ElectionTickMax)
	}
}

func TestNewNodeAppliesHeartbeatTicksDefault(t *testing.T) {
	node := NewNode(Config{}, InitialState{}, nil)
	if node.config.HeartbeatTicks != 2 {
		t.Fatalf("HeartbeatTicks = %d, want 2", node.config.HeartbeatTicks)
	}
}

func TestVoteGrantedAtMostOncePerTerm(t *testing.T) {
	node := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, InitialState{HardState: HardState{Term: 7}}, fixedTestRand{})

	node.Step(requestVote(2, 1, 7, 2))
	ready := node.Ready()
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 7, VotedFor: 2}) {
		t.Fatalf("Ready().HardState = %#v, want term 7 vote 2", ready.HardState)
	}
	if got, want := voteResponse(t, ready), true; got != want {
		t.Fatalf("first vote granted = %v, want %v", got, want)
	}
	node.Advance()

	node.Step(requestVote(2, 1, 7, 2))
	ready = node.Ready()
	if ready.HardState != nil {
		t.Fatalf("repeat vote Ready().HardState = %#v, want nil", ready.HardState)
	}
	if got, want := voteResponse(t, ready), true; got != want {
		t.Fatalf("repeat vote granted = %v, want %v", got, want)
	}
	node.Advance()

	node.Step(requestVote(3, 1, 7, 3))
	ready = node.Ready()
	if ready.HardState != nil {
		t.Fatalf("second candidate Ready().HardState = %#v, want nil", ready.HardState)
	}
	if got, want := voteResponse(t, ready), false; got != want {
		t.Fatalf("second candidate vote granted = %v, want %v", got, want)
	}
}

func TestVotedForResetsOnlyWhenTermChanges(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}, ElectionTickMin: 1, ElectionTickMax: 2},
		InitialState{HardState: HardState{Term: 4, VotedFor: 2}},
		fixedTestRand{},
	)

	node.Tick()
	if node.role != candidate || node.hardState != (HardState{Term: 5, VotedFor: 1}) {
		t.Fatalf("election state = role %v hard state %#v, want candidate term 5 vote 1", node.role, node.hardState)
	}
	node.Advance()

	node.Step(appendEntries(2, 1, 5))
	if node.role != follower {
		t.Fatalf("role after same-term AppendEntries = %v, want follower", node.role)
	}
	if node.hardState != (HardState{Term: 5, VotedFor: 1}) {
		t.Fatalf("same-term role change hard state = %#v, want term 5 vote 1", node.hardState)
	}
	if ready := node.Ready(); ready.HardState != nil {
		t.Fatalf("same-term role change Ready().HardState = %#v, want nil", ready.HardState)
	}
	node.Advance()

	node.Step(requestVoteResponse(2, 1, 6, false))
	if node.hardState != (HardState{Term: 6}) {
		t.Fatalf("higher-term response hard state = %#v, want term 6 with no vote", node.hardState)
	}
	ready := node.Ready()
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 6}) {
		t.Fatalf("higher-term response Ready().HardState = %#v, want term 6 with no vote", ready.HardState)
	}
	if len(ready.Messages) != 0 {
		t.Fatalf("higher-term response messages = %v, want none", ready.Messages)
	}
}

func TestElectionTimeoutStartsCandidateWithSortedRequestVotes(t *testing.T) {
	rnd := &sequenceRand{values: []int{1, 2}}
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{3, 1, 2}, ElectionTickMin: 2, ElectionTickMax: 5},
		InitialState{},
		rnd,
	)

	for range 3 {
		node.Tick()
	}
	if node.role != candidate {
		t.Fatalf("role = %v, want candidate", node.role)
	}
	if node.electionTimeout != 4 {
		t.Fatalf("redrawn election timeout = %d, want 4", node.electionTimeout)
	}
	if got, want := rnd.bounds, []int{3, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rand.IntN bounds = %v, want %v", got, want)
	}
	ready := node.Ready()
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 1, VotedFor: 1}) {
		t.Fatalf("Ready().HardState = %#v, want term 1 vote 1", ready.HardState)
	}
	if got, want := requestVoteRecipients(t, ready), []uint64{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RequestVote recipients = %v, want %v", got, want)
	}
	for _, message := range ready.Messages {
		req := message.GetRequestVote()
		if req == nil || message.Term != 1 || req.Term != 1 || req.CandidateId != 1 || req.LastLogIndex != 0 || req.LastLogTerm != 0 {
			t.Fatalf("RequestVote = %#v, want term 1 candidate 1 empty log", message)
		}
	}
}

func TestTermTransitionsAndLowerTermMessages(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}},
		InitialState{HardState: HardState{Term: 3, VotedFor: 2}},
		fixedTestRand{},
	)

	node.Step(requestVote(2, 1, 2, 2))
	if node.hardState != (HardState{Term: 3, VotedFor: 2}) {
		t.Fatalf("lower-term RequestVote changed state to %#v", node.hardState)
	}
	if got, want := voteResponse(t, node.Ready()), false; got != want {
		t.Fatalf("lower-term RequestVote granted = %v, want %v", got, want)
	}
	node.Advance()

	node.Step(requestVoteResponse(2, 1, 2, true))
	if ready := node.Ready(); ready.HardState != nil || len(ready.Messages) != 0 {
		t.Fatalf("lower-term response Ready() = %#v, want empty", ready)
	}

	node.Step(appendEntries(2, 1, 4))
	ready := node.Ready()
	if ready.HardState == nil || *ready.HardState != (HardState{Term: 4}) {
		t.Fatalf("higher-term AppendEntries Ready().HardState = %#v, want term 4 with no vote", ready.HardState)
	}
	if got, want := appendEntriesResponse(t, ready), true; got != want {
		t.Fatalf("higher-term AppendEntries success = %v, want %v", got, want)
	}
}

func TestMajorityElectionSendsImmediateAndPeriodicHeartbeats(t *testing.T) {
	node := NewNode(
		Config{ID: 1, Peers: []NodeID{1, 2, 3}, ElectionTickMin: 1, ElectionTickMax: 2, HeartbeatTicks: 2},
		InitialState{},
		fixedTestRand{},
	)
	node.Tick()
	node.Advance()

	node.Step(requestVoteResponse(2, 1, 1, true))
	if node.role != leader {
		t.Fatalf("role after majority = %v, want leader", node.role)
	}
	if got, want := heartbeatRecipients(t, node.Ready()), []uint64{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("immediate heartbeat recipients = %v, want %v", got, want)
	}
	node.Advance()

	node.Tick()
	if ready := node.Ready(); len(ready.Messages) != 0 {
		t.Fatalf("heartbeat before interval messages = %v, want none", ready.Messages)
	}
	node.Tick()
	if got, want := heartbeatRecipients(t, node.Ready()), []uint64{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("periodic heartbeat recipients = %v, want %v", got, want)
	}
}

type fixedTestRand struct{}

func (fixedTestRand) IntN(int) int { return 0 }

type sequenceRand struct {
	values []int
	bounds []int
}

func (r *sequenceRand) IntN(n int) int {
	r.bounds = append(r.bounds, n)
	value := r.values[0]
	r.values = r.values[1:]
	return value
}

func requestVote(from, to, term, candidateID uint64) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: term,
		Body: &raftpb.Message_RequestVote{RequestVote: &raftpb.RequestVoteReq{
			Term:        term,
			CandidateId: candidateID,
		}},
	}
}

func requestVoteResponse(from, to, term uint64, granted bool) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: term,
		Body: &raftpb.Message_RequestVoteResp{RequestVoteResp: &raftpb.RequestVoteResp{
			Term:        term,
			VoteGranted: granted,
		}},
	}
}

func appendEntries(from, to, term uint64) *raftpb.Message {
	return &raftpb.Message{
		From: from,
		To:   to,
		Term: term,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term:     term,
			LeaderId: from,
		}},
	}
}

func voteResponse(t *testing.T, ready Ready) bool {
	t.Helper()
	if len(ready.Messages) != 1 || ready.Messages[0].GetRequestVoteResp() == nil {
		t.Fatalf("Ready().Messages = %#v, want one RequestVoteResp", ready.Messages)
	}
	return ready.Messages[0].GetRequestVoteResp().VoteGranted
}

func appendEntriesResponse(t *testing.T, ready Ready) bool {
	t.Helper()
	if len(ready.Messages) != 1 || ready.Messages[0].GetAppendEntriesResp() == nil {
		t.Fatalf("Ready().Messages = %#v, want one AppendEntriesResp", ready.Messages)
	}
	return ready.Messages[0].GetAppendEntriesResp().Success
}

func requestVoteRecipients(t *testing.T, ready Ready) []uint64 {
	t.Helper()
	to := make([]uint64, 0, len(ready.Messages))
	for _, message := range ready.Messages {
		if message.GetRequestVote() == nil {
			t.Fatalf("message = %#v, want RequestVote", message)
		}
		to = append(to, message.To)
	}
	return to
}

func heartbeatRecipients(t *testing.T, ready Ready) []uint64 {
	t.Helper()
	to := make([]uint64, 0, len(ready.Messages))
	for _, message := range ready.Messages {
		req := message.GetAppendEntries()
		if req == nil || len(req.Entries) != 0 || req.PrevLogIndex != 0 || req.PrevLogTerm != 0 || req.LeaderCommit != 0 {
			t.Fatalf("message = %#v, want empty AppendEntries heartbeat", message)
		}
		to = append(to, message.To)
	}
	return to
}
