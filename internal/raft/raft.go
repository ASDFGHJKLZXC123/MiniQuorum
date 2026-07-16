// Package raft contains the deterministic, synchronous Raft state machine.
package raft

import raftpb "miniquorum/proto"

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

// Node is the Phase 0 synchronous Raft API skeleton.
type Node struct {
	config Config
	init   InitialState
	rnd    Rand
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
	return &Node{config: cfg, init: init, rnd: rnd}
}

// Tick supplies one host tick. Raft behavior begins in Phase 1.
func (n *Node) Tick() {}

// Step supplies one inbound transport message. Raft behavior begins in Phase 1.
func (n *Node) Step(m *raftpb.Message) {}

// Propose is reserved for Phase 2.
func (n *Node) Propose(data []byte) (index, term uint64, isLeader bool) {
	return 0, 0, false
}

// Ready reports pending output without discarding it. Phase 0 has no output.
func (n *Node) Ready() Ready { return Ready{} }

// Advance acknowledges that the most recently reported Ready batch was handled.
func (n *Node) Advance() {}
