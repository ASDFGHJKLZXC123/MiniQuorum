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

type appendInflight struct {
	prevIndex    uint64
	prevTerm     uint64
	lastIndex    uint64
	leaderCommit uint64
	hasEntries   bool
}

type readyAck struct {
	valid        bool
	hardState    bool
	hard         HardState
	messageCount int
	entries      bool
	stableTo     uint64
	committed    bool
	appliedTo    uint64
}

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

	log           raftLog
	unstableIndex uint64
	commitIndex   uint64
	lastApplied   uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	inflight   map[NodeID]*appendInflight

	hardStateDirty bool
	messages       []*raftpb.Message
	readyAck       readyAck
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
		config:      cfg,
		peers:       orderedPeers(cfg.Peers),
		rnd:         rnd,
		hardState:   init.HardState,
		role:        follower,
		log:         newRaftLog(init.Snapshot, init.Entries),
		commitIndex: init.Snapshot.Index,
		lastApplied: init.Snapshot.Index,
	}
	n.unstableIndex = n.log.lastIndex() + 1
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
		n.handleAppendEntriesResponse(m, m.GetAppendEntriesResp())
	}
}

// Propose appends a normal entry when this node is leader. The core remains
// synchronous: persistence and transport happen only through the next Ready.
func (n *Node) Propose(data []byte) (index, term uint64, isLeader bool) {
	if n.role != leader {
		return 0, n.hardState.Term, false
	}
	index = n.appendLocal(raftpb.EntryType_NORMAL, data)
	n.advanceCommit()
	n.broadcastAvailableAppend()
	return index, n.hardState.Term, true
}

// Ready reports pending output without discarding it.
func (n *Node) Ready() Ready {
	ready := Ready{Messages: n.messages}
	if n.hardStateDirty {
		hardState := n.hardState
		ready.HardState = &hardState
	}
	lastIndex := n.log.lastIndex()
	if n.unstableIndex <= lastIndex {
		ready.Entries = n.log.rangeEntries(n.unstableIndex, lastIndex+1)
	}
	if n.lastApplied < n.commitIndex {
		ready.CommittedEntries = n.log.rangeEntries(n.lastApplied+1, n.commitIndex+1)
	}
	n.captureReady(ready)
	return ready
}

// Advance acknowledges that the most recently reported Ready batch was handled.
func (n *Node) Advance() {
	// Preserve the Phase 1 behavior for callers that acknowledge immediately
	// after driving the node without first materializing Ready.
	if !n.readyAck.valid {
		n.captureReady(n.readyWithoutCapture())
	}
	ack := n.readyAck
	if ack.hardState && n.hardStateDirty && n.hardState == ack.hard {
		n.hardStateDirty = false
	}
	if ack.messageCount >= len(n.messages) {
		n.messages = nil
	} else if ack.messageCount > 0 {
		n.messages = n.messages[ack.messageCount:]
	}
	if ack.entries && n.unstableIndex <= ack.stableTo {
		n.unstableIndex = ack.stableTo + 1
	}
	if ack.committed && ack.appliedTo > n.lastApplied {
		n.lastApplied = ack.appliedTo
	}
	n.readyAck = readyAck{}
}

func (n *Node) readyWithoutCapture() Ready {
	ready := Ready{Messages: n.messages}
	if n.hardStateDirty {
		hardState := n.hardState
		ready.HardState = &hardState
	}
	lastIndex := n.log.lastIndex()
	if n.unstableIndex <= lastIndex {
		ready.Entries = n.log.rangeEntries(n.unstableIndex, lastIndex+1)
	}
	if n.lastApplied < n.commitIndex {
		ready.CommittedEntries = n.log.rangeEntries(n.lastApplied+1, n.commitIndex+1)
	}
	return ready
}

func (n *Node) captureReady(ready Ready) {
	ack := readyAck{
		valid:        true,
		hardState:    ready.HardState != nil,
		messageCount: len(ready.Messages),
		entries:      len(ready.Entries) > 0,
		committed:    len(ready.CommittedEntries) > 0,
	}
	if ready.HardState != nil {
		ack.hard = *ready.HardState
	}
	if len(ready.Entries) > 0 {
		ack.stableTo = ready.Entries[len(ready.Entries)-1].Index
	}
	if len(ready.CommittedEntries) > 0 {
		ack.appliedTo = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
	}
	n.readyAck = ack
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
				LastLogIndex: n.log.lastIndex(),
				LastLogTerm:  n.log.lastTerm(),
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
	n.nextIndex = nil
	n.matchIndex = nil
	n.inflight = nil
}

func (n *Node) becomeFollowerSameTerm() {
	n.role = follower
	n.heartbeatElapsed = 0
	n.votes = nil
	n.electionElapsed = 0
	n.nextIndex = nil
	n.matchIndex = nil
	n.inflight = nil
}

func (n *Node) becomeLeader() {
	n.role = leader
	n.heartbeatElapsed = 0
	n.votes = nil

	// Probe the pre-NOOP end of the log first. This keeps heartbeats empty and
	// learns each follower's match point before suffix transmission. The NOOP
	// itself is nevertheless appended synchronously in this transition and is
	// part of the same Ready persistence batch as the probes.
	probeIndex := n.log.lastIndex()
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	n.inflight = make(map[NodeID]*appendInflight, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = probeIndex + 1
		n.matchIndex[peer] = 0
	}
	n.appendLocal(raftpb.EntryType_NOOP, nil)
	n.advanceCommit()
	for _, peer := range n.peers {
		if peer != n.config.ID {
			n.sendAppend(peer, true)
		}
	}
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
	lastLogIndex, lastLogTerm := n.log.lastIndex(), n.log.lastTerm()
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
	if n.role != follower {
		n.becomeFollowerSameTerm()
	}
	n.electionElapsed = 0
	requestLastIndex, valid := appendRequestLastIndex(req)
	if !valid {
		n.sendAppendEntriesResponse(NodeID(m.From), false, 0)
		return
	}
	if !n.log.matches(req.PrevLogIndex, req.PrevLogTerm) {
		n.sendAppendEntriesResponse(NodeID(m.From), false, 0)
		return
	}
	if changed := n.log.appendFromLeader(req.Entries); changed != 0 && changed < n.unstableIndex {
		n.unstableIndex = changed
	}
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, requestLastIndex)
	}
	n.sendAppendEntriesResponse(NodeID(m.From), true, requestLastIndex)
}

func (n *Node) handleAppendEntriesResponse(m *raftpb.Message, resp *raftpb.AppendEntriesResp) {
	peer := NodeID(m.From)
	if n.role != leader || !n.isPeer(peer) || peer == n.config.ID {
		return
	}
	pending := n.inflight[peer]
	if pending == nil {
		return
	}
	if resp.Success {
		acknowledged := resp.MatchIndex
		// Success identifies the exact request by its covered index. A lower
		// acknowledgement belongs to an older request, while a higher one is
		// impossible for the request currently outstanding. Neither may disturb
		// that request or manufacture progress.
		if acknowledged != pending.lastIndex || acknowledged < n.matchIndex[peer] ||
			acknowledged > n.log.lastIndex() || acknowledged == ^uint64(0) {
			return
		}

		n.inflight[peer] = nil
		if acknowledged > n.matchIndex[peer] {
			n.matchIndex[peer] = acknowledged
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		commitAdvanced := n.advanceCommit()
		if n.nextIndex[peer] <= n.log.lastIndex() || commitAdvanced {
			n.sendAppend(peer, false)
		}
		return
	}

	n.inflight[peer] = nil
	retryFloor := n.log.firstIndex()
	if matched := n.matchIndex[peer]; matched != ^uint64(0) && matched+1 > retryFloor {
		retryFloor = matched + 1
	}
	if n.nextIndex[peer] > retryFloor {
		n.nextIndex[peer]--
	}
	n.sendAppend(peer, false)
}

func (n *Node) rejectLowerTermRequest(m *raftpb.Message) {
	switch {
	case m.GetRequestVote() != nil:
		n.sendRequestVoteResponse(NodeID(m.From), false)
	case m.GetAppendEntries() != nil:
		n.sendAppendEntriesResponse(NodeID(m.From), false, 0)
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

func (n *Node) sendAppendEntriesResponse(to NodeID, success bool, matchIndex uint64) {
	n.messages = append(n.messages, &raftpb.Message{
		From: uint64(n.config.ID),
		To:   uint64(to),
		Term: n.hardState.Term,
		Body: &raftpb.Message_AppendEntriesResp{AppendEntriesResp: &raftpb.AppendEntriesResp{
			Term:       n.hardState.Term,
			Success:    success,
			MatchIndex: matchIndex,
		}},
	})
}

func appendRequestLastIndex(req *raftpb.AppendEntriesReq) (uint64, bool) {
	if req == nil || uint64(len(req.Entries)) > ^uint64(0)-req.PrevLogIndex {
		return 0, false
	}
	lastIndex := req.PrevLogIndex + uint64(len(req.Entries))
	for i, entry := range req.Entries {
		if entry == nil || entry.Index != req.PrevLogIndex+uint64(i)+1 {
			return 0, false
		}
	}
	return lastIndex, true
}

func (n *Node) sendHeartbeats() {
	n.broadcastAppend(false)
}

func (n *Node) appendLocal(typ raftpb.EntryType, data []byte) uint64 {
	index := n.log.appendLocal(n.hardState.Term, typ, data)
	if index < n.unstableIndex {
		n.unstableIndex = index
	}
	if n.matchIndex != nil {
		n.matchIndex[n.config.ID] = index
		n.nextIndex[n.config.ID] = index + 1
	}
	return index
}

func (n *Node) broadcastAppend(probe bool) {
	for _, peer := range n.peers {
		if peer == n.config.ID {
			continue
		}
		n.sendAppend(peer, probe)
	}
}

// broadcastAvailableAppend starts one logical AppendEntries request per idle
// follower. A follower that already has an in-flight request will receive the
// newly appended suffix as soon as that request resolves.
func (n *Node) broadcastAvailableAppend() {
	for _, peer := range n.peers {
		if peer == n.config.ID || n.inflight[peer] != nil {
			continue
		}
		n.sendAppend(peer, false)
	}
}

func (n *Node) sendAppend(peer NodeID, probe bool) {
	if pending := n.inflight[peer]; pending != nil {
		n.emitAppend(peer, pending)
		return
	}

	lastIndex := n.log.lastIndex()
	next := n.nextIndex[peer]
	if next < n.log.firstIndex() {
		next = n.log.firstIndex()
	}
	if next > lastIndex+1 {
		next = lastIndex + 1
	}
	n.nextIndex[peer] = next
	prevIndex := next - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok {
		return
	}
	pending := &appendInflight{
		prevIndex:    prevIndex,
		prevTerm:     prevTerm,
		lastIndex:    prevIndex,
		leaderCommit: n.commitIndex,
	}
	if !probe && next <= lastIndex {
		pending.hasEntries = true
		pending.lastIndex = lastIndex
	}
	n.inflight[peer] = pending
	n.emitAppend(peer, pending)
}

func (n *Node) emitAppend(peer NodeID, pending *appendInflight) {
	var entries []*raftpb.Entry
	if pending.hasEntries {
		entries = entryPointers(n.log.rangeEntries(pending.prevIndex+1, pending.lastIndex+1))
	}
	n.messages = append(n.messages, &raftpb.Message{
		From: uint64(n.config.ID),
		To:   uint64(peer),
		Term: n.hardState.Term,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term:         n.hardState.Term,
			LeaderId:     uint64(n.config.ID),
			PrevLogIndex: pending.prevIndex,
			PrevLogTerm:  pending.prevTerm,
			Entries:      entries,
			LeaderCommit: pending.leaderCommit,
		}},
	})
}

// advanceCommit implements Raft section 5.4.2. A majority can directly
// commit only an entry from this leader's current term; preceding entries
// become committed indirectly when that current-term entry advances.
func (n *Node) advanceCommit() bool {
	if n.role != leader {
		return false
	}
	majority := len(n.peers)/2 + 1
	for index := n.log.lastIndex(); index > n.commitIndex; index-- {
		term, ok := n.log.term(index)
		if !ok || term != n.hardState.Term {
			continue
		}
		replicas := 0
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= index {
				replicas++
			}
		}
		if replicas >= majority {
			n.commitIndex = index
			return true
		}
	}
	return false
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
