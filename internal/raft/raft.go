// Package raft contains the deterministic, synchronous Raft state machine.
package raft

import (
	"sort"

	raftpb "miniquorum/proto"
)

// NodeID identifies a Raft peer.
type NodeID uint64

// HardState is the durable Raft term and vote state.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// Rand provides the election-timeout jitter chosen by the host.
type Rand interface {
	IntN(n int) int
}

// Config configures a Raft node.
type Config struct {
	ID              NodeID
	Peers           []NodeID
	ElectionTickMin int
	ElectionTickMax int
	HeartbeatTicks  int
}

// SnapshotMeta describes the compacted predecessor of the in-memory log.
type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

// InitialState is loaded by the host before constructing a node.
type InitialState struct {
	HardState HardState
	Entries   []raftpb.Entry
	Snapshot  SnapshotMeta
}

// Ready is the host-owned output batch. It remains pending until Advance.
type Ready struct {
	HardState        *HardState
	Entries          []raftpb.Entry
	Messages         []*raftpb.Message
	CommittedEntries []raftpb.Entry
}

type role uint8

const (
	follower role = iota
	candidate
	leader
)

// Node is a deterministic, synchronous Raft state machine.
type Node struct {
	config Config
	peers  []NodeID
	rnd    Rand

	hardState HardState
	role      role

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	votes            map[NodeID]struct{}

	hardStateDirty bool
	messages       []*raftpb.Message
}

// NewNode constructs a node, applying the pinned tick defaults when omitted.
func NewNode(cfg Config, init InitialState, rnd Rand) *Node {
	if cfg.ElectionTickMin == 0 {
		cfg.ElectionTickMin = 10
	}
	if cfg.ElectionTickMax == 0 {
		cfg.ElectionTickMax = 20
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 2
	}
	n := &Node{
		config:    cfg,
		peers:     orderedPeers(cfg.Peers),
		rnd:       rnd,
		hardState: init.HardState,
		role:      follower,
	}
	n.resetElectionTimeout()
	return n
}

// Tick supplies one host tick.
func (n *Node) Tick() {
	if n.role == leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.config.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.sendHeartbeats()
		}
		return
	}

	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.startElection()
	}
}

// Step supplies one inbound transport message.
func (n *Node) Step(m *raftpb.Message) {
	if m == nil {
		return
	}

	term := messageTerm(m)
	if term > n.hardState.Term {
		n.becomeFollower(term)
	} else if term < n.hardState.Term {
		n.rejectLowerTermRequest(m)
		return
	}

	switch {
	case m.GetRequestVote() != nil:
		n.handleRequestVote(m, m.GetRequestVote())
	case m.GetAppendEntries() != nil:
		n.handleAppendEntries(m, m.GetAppendEntries())
	case m.GetRequestVoteResp() != nil:
		n.handleRequestVoteResponse(m, m.GetRequestVoteResp())
	case m.GetAppendEntriesResp() != nil:
		// Replication response processing begins in Phase 2.
	}
}

// Propose is reserved for Phase 2.
func (n *Node) Propose(data []byte) (index, term uint64, isLeader bool) {
	return 0, 0, false
}

// Ready reports pending output without discarding it.
func (n *Node) Ready() Ready {
	ready := Ready{Messages: n.messages}
	if n.hardStateDirty {
		hardState := n.hardState
		ready.HardState = &hardState
	}
	return ready
}

// Advance acknowledges that the most recently reported Ready batch was handled.
func (n *Node) Advance() {
	n.hardStateDirty = false
	n.messages = nil
}

func orderedPeers(peers []NodeID) []NodeID {
	ordered := append([]NodeID(nil), peers...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered
}

func (n *Node) resetElectionTimeout() {
	timeout := n.config.ElectionTickMin
	span := n.config.ElectionTickMax - n.config.ElectionTickMin
	if span > 0 && n.rnd != nil {
		timeout += n.rnd.IntN(span)
	}
	n.electionTimeout = timeout
	n.electionElapsed = 0
}

func (n *Node) startElection() {
	n.hardState.Term++
	n.hardState.VotedFor = n.config.ID
	n.hardStateDirty = true
	n.role = candidate
	n.heartbeatElapsed = 0
	n.votes = map[NodeID]struct{}{n.config.ID: {}}
	n.resetElectionTimeout()

	if n.hasMajority() {
		n.becomeLeader()
		return
	}
	for _, peer := range n.peers {
		if peer == n.config.ID {
			continue
		}
		n.messages = append(n.messages, &raftpb.Message{
			From: uint64(n.config.ID),
			To:   uint64(peer),
			Term: n.hardState.Term,
			Body: &raftpb.Message_RequestVote{RequestVote: &raftpb.RequestVoteReq{
				Term:         n.hardState.Term,
				CandidateId:  uint64(n.config.ID),
				LastLogIndex: 0,
				LastLogTerm:  0,
			}},
		})
	}
}

func (n *Node) becomeFollower(term uint64) {
	if term != n.hardState.Term {
		n.hardState.Term = term
		n.hardState.VotedFor = 0
		n.hardStateDirty = true
		n.resetElectionTimeout()
	}
	n.role = follower
	n.heartbeatElapsed = 0
	n.votes = nil
}

func (n *Node) becomeFollowerSameTerm() {
	n.role = follower
	n.heartbeatElapsed = 0
	n.votes = nil
	n.electionElapsed = 0
}

func (n *Node) becomeLeader() {
	n.role = leader
	n.heartbeatElapsed = 0
	n.votes = nil
	// TODO(phase2): append the leader's term-start no-op entry.
	n.sendHeartbeats()
}

func (n *Node) hasMajority() bool {
	return len(n.votes) >= len(n.peers)/2+1
}

func (n *Node) handleRequestVote(m *raftpb.Message, req *raftpb.RequestVoteReq) {
	canVote := n.hardState.VotedFor == 0 || n.hardState.VotedFor == NodeID(req.CandidateId)
	granted := canVote && n.candidateLogIsUpToDate(req.LastLogIndex, req.LastLogTerm)
	if granted {
		if n.hardState.VotedFor == 0 {
			n.hardState.VotedFor = NodeID(req.CandidateId)
			n.hardStateDirty = true
		}
		n.electionElapsed = 0
	}
	n.sendRequestVoteResponse(NodeID(m.From), granted)
}

func (n *Node) candidateLogIsUpToDate(index, term uint64) bool {
	// Phase 1 has an empty log. Keep the full Raft comparison wired for Phase 2.
	const lastLogIndex, lastLogTerm uint64 = 0, 0
	return term > lastLogTerm || (term == lastLogTerm && index >= lastLogIndex)
}

func (n *Node) handleRequestVoteResponse(m *raftpb.Message, resp *raftpb.RequestVoteResp) {
	if n.role != candidate || !resp.VoteGranted || !n.isPeer(NodeID(m.From)) {
		return
	}
	n.votes[NodeID(m.From)] = struct{}{}
	if n.hasMajority() {
		n.becomeLeader()
	}
}

func (n *Node) handleAppendEntries(m *raftpb.Message, req *raftpb.AppendEntriesReq) {
	if !n.validAppendEntries(req) {
		n.sendAppendEntriesResponse(NodeID(m.From), false)
		return
	}
	if n.role != leader {
		n.becomeFollowerSameTerm()
	}
	n.electionElapsed = 0
	n.sendAppendEntriesResponse(NodeID(m.From), true)
}

func (n *Node) validAppendEntries(req *raftpb.AppendEntriesReq) bool {
	// Phase 1 only has empty-log heartbeats; replication begins in Phase 2.
	return req.PrevLogIndex == 0 && req.PrevLogTerm == 0 && len(req.Entries) == 0 && req.LeaderCommit == 0
}

func (n *Node) rejectLowerTermRequest(m *raftpb.Message) {
	switch {
	case m.GetRequestVote() != nil:
		n.sendRequestVoteResponse(NodeID(m.From), false)
	case m.GetAppendEntries() != nil:
		n.sendAppendEntriesResponse(NodeID(m.From), false)
	}
}

func (n *Node) sendRequestVoteResponse(to NodeID, granted bool) {
	n.messages = append(n.messages, &raftpb.Message{
		From: uint64(n.config.ID),
		To:   uint64(to),
		Term: n.hardState.Term,
		Body: &raftpb.Message_RequestVoteResp{RequestVoteResp: &raftpb.RequestVoteResp{
			Term:        n.hardState.Term,
			VoteGranted: granted,
		}},
	})
}

func (n *Node) sendAppendEntriesResponse(to NodeID, success bool) {
	n.messages = append(n.messages, &raftpb.Message{
		From: uint64(n.config.ID),
		To:   uint64(to),
		Term: n.hardState.Term,
		Body: &raftpb.Message_AppendEntriesResp{AppendEntriesResp: &raftpb.AppendEntriesResp{
			Term:    n.hardState.Term,
			Success: success,
		}},
	})
}

func (n *Node) sendHeartbeats() {
	for _, peer := range n.peers {
		if peer == n.config.ID {
			continue
		}
		n.messages = append(n.messages, &raftpb.Message{
			From: uint64(n.config.ID),
			To:   uint64(peer),
			Term: n.hardState.Term,
			Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
				Term:         n.hardState.Term,
				LeaderId:     uint64(n.config.ID),
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				LeaderCommit: 0,
			}},
		})
	}
}

func (n *Node) isPeer(id NodeID) bool {
	index := sort.Search(len(n.peers), func(i int) bool { return n.peers[i] >= id })
	return index < len(n.peers) && n.peers[index] == id
}

func messageTerm(m *raftpb.Message) uint64 {
	term := m.Term
	switch {
	case m.GetRequestVote() != nil && m.GetRequestVote().Term > term:
		term = m.GetRequestVote().Term
	case m.GetRequestVoteResp() != nil && m.GetRequestVoteResp().Term > term:
		term = m.GetRequestVoteResp().Term
	case m.GetAppendEntries() != nil && m.GetAppendEntries().Term > term:
		term = m.GetAppendEntries().Term
	case m.GetAppendEntriesResp() != nil && m.GetAppendEntriesResp().Term > term:
		term = m.GetAppendEntriesResp().Term
	}
	return term
}
