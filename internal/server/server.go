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

// FailStopNotifier is optionally implemented by an Applier that must learn
// when the host permanently fail-stops (a Ready batch's Storage.Save or
// state-machine Apply failed). The Host notifies exactly once, on the
// transition to stopped, while holding Host.mu — after it, no Ready will ever
// be processed again, so anything still waiting on a commit or apply from
// this node (e.g. KVApplier's registered waiters) can never be resolved by
// normal means and must be released here.
type FailStopNotifier interface {
	FailStop(err error)
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
	// SelfID is this node's own ID, used only to record itself as the
	// best-known leader after a successful leader Propose. raft.Node keeps
	// no exported notion of "self" the host can query, so the host is told.
	SelfID raft.NodeID

	mu      sync.Mutex
	stopped error

	// leaderHint is the best-known leader ID, observed from inbound
	// AppendEntries or recorded as self after a successful leader Propose.
	// Zero means unknown. It is a hint only: it may be stale or wrong around
	// elections, and that staleness is explicitly acceptable (see
	// LeaderHint) — internal/raft's frozen API carries no such state.
	leaderHint raft.NodeID
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
	if req := m.GetAppendEntries(); req != nil {
		h.recordLeaderHintLocked(raft.NodeID(req.GetLeaderId()))
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
	if isLeader {
		h.recordLeaderHintLocked(h.SelfID)
		if onProposed != nil {
			onProposed(index, term)
		}
	}
	return index, term, isLeader, h.processReadyLocked()
}

// LeaderHint returns the node's best-known leader without racing Tick, Step,
// or Propose. It is a best-known hint only, tracked entirely by the host
// (see the leaderHint field): it may be stale or unknown (ok==false) around
// elections.
func (h *Host) LeaderHint() (raft.NodeID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.leaderHint == 0 {
		return 0, false
	}
	return h.leaderHint, true
}

// recordLeaderHintLocked updates the best-known leader. Callers hold h.mu.
// A zero ID (unset LeaderId, or SelfID left at its zero value) leaves the
// existing hint untouched rather than recording "no leader".
func (h *Host) recordLeaderHintLocked(id raft.NodeID) {
	if id == 0 {
		return
	}
	h.leaderHint = id
}

func (h *Host) processReadyLocked() error {
	if err := processReady(h.Node, h.Storage, h.Transport, h.Applier); err != nil {
		h.stopped = err
		// This is the sole stopped transition and it runs at most once: every
		// entry point returns early while stopped, so processReadyLocked can
		// never error again. Notifying under h.mu means no Propose (and thus
		// no waiter registration) can interleave with the drain.
		if notifier, ok := h.Applier.(FailStopNotifier); ok {
			notifier.FailStop(err)
		}
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
