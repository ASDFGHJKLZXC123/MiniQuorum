// Package transport defines the outbound Raft message boundary.
package transport

import (
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// Transport asynchronously sends best-effort outbound Raft messages. Inbound
// messages are supplied to raft.Node.Step by the host.
type Transport interface {
	Send(to raft.NodeID, m *raftpb.Message)
}
