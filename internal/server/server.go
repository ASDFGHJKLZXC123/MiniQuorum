// Package server owns the nondeterministic host loop around a Raft node.
package server

import (
	"fmt"
	"sync"

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
// raft.Node is not safe for concurrent use, and the real server drives it
// from three sources (a tick timer, inbound transport messages, and KV
// Execute proposals): Host.mu serializes every one of those into the single
// Ready-processing path the core requires. Once any Ready-processing error
// occurs the node is fail-stopped: every subsequent call returns that error
// without touching the node again.
type Host struct {
	Node      *raft.Node
	Storage   storage.Storage
	Transport transport.Transport
	Applier   Applier

	mu      sync.Mutex
	stopped error
}

type readyNode interface {
	Ready() raft.Ready
	Advance()
}

type readyStorage interface {
	Save(hs *raft.HardState, entries []raftpb.Entry) error
}

// Tick supplies one host tick and drains the resulting Ready.
func (h *Host) Tick() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped != nil {
		return h.stopped
	}
	h.Node.Tick()
	return h.processReadyLocked()
}

// Step supplies one inbound transport message and drains the resulting Ready.
func (h *Host) Step(m *raftpb.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped != nil {
		return h.stopped
	}
	h.Node.Step(m)
	return h.processReadyLocked()
}

// Propose submits data to the Raft node and drains the resulting Ready.
// onProposed, when the node is leader, runs after Node.Propose but before the
// Ready is drained, so a caller can register interest in (index, term) before
// that entry has any chance of being committed and applied.
func (h *Host) Propose(data []byte, onProposed func(index, term uint64)) (index, term uint64, isLeader bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped != nil {
		return 0, 0, false, h.stopped
	}
	index, term, isLeader = h.Node.Propose(data)
	if isLeader && onProposed != nil {
		onProposed(index, term)
	}
	return index, term, isLeader, h.processReadyLocked()
}

// LeaderHint returns the node's best-known leader without racing Tick, Step,
// or Propose. See raft.Node.LeaderHint for the staleness guarantees.
func (h *Host) LeaderHint() (raft.NodeID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.Node.LeaderHint()
}

func (h *Host) processReadyLocked() error {
	if err := processReady(h.Node, h.Storage, h.Transport, h.Applier); err != nil {
		h.stopped = err
		return err
	}
	return nil
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
