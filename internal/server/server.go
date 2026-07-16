// Package server owns the nondeterministic host loop around a Raft node.
package server

import (
	"fmt"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	"miniquorum/internal/transport"
	raftpb "miniquorum/proto"
)

// Applier applies one committed entry. The Phase 0 node has no committed entries.
type Applier interface {
	Apply(*raftpb.Entry) error
}

// Host coordinates a deterministic Raft node and its external dependencies.
type Host struct {
	Node      *raft.Node
	Storage   storage.Storage
	Transport transport.Transport
	Applier   Applier
}

type readyNode interface {
	Ready() raft.Ready
	Advance()
}

type readyStorage interface {
	Save(hs *raft.HardState, entries []raftpb.Entry) error
}

// ProcessReady executes the required Ready lifecycle in order. A persistence or
// apply error returns immediately, leaving the batch unacknowledged.
func (h *Host) ProcessReady() error {
	return processReady(h.Node, h.Storage, h.Transport, h.Applier)
}

func processReady(node readyNode, store readyStorage, transport transport.Transport, applier Applier) error {
	ready := node.Ready()

	// 1. Persist the hard state and entries atomically before any side effect.
	if err := store.Save(ready.HardState, ready.Entries); err != nil {
		return fmt.Errorf("raft storage save: %w", err)
	}
	// 2. Send only after the batch is durable.
	for _, message := range ready.Messages {
		transport.Send(raft.NodeID(message.To), message)
	}
	// 3. Apply committed entries in their provided order.
	for i := range ready.CommittedEntries {
		if applier != nil {
			if err := applier.Apply(&ready.CommittedEntries[i]); err != nil {
				return fmt.Errorf("state-machine apply: %w", err)
			}
		}
	}
	// 4. Acknowledge only after all preceding work completed.
	node.Advance()
	return nil
}
