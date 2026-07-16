package sim

import (
	"fmt"

	"miniquorum/internal/raft"
)

// InvariantFunc inspects sim state after an event and returns an error if a
// safety property was violated. Registered functions run, in registration
// order, after every processed event.
type InvariantFunc func(*Sim) error

// leaderTracker maintains leaders[term] = set of node IDs observed as
// leader in that term, per the phase-1 single-leader-per-term invariant.
// Leadership is observed strictly from a deterministic Raft output: a node
// is recorded as term T's leader the first time it emits an
// AppendEntriesReq at term T (only a leader ever sends one, per the spec).
// No raft-internal role field is read.
type leaderTracker struct {
	leaders map[uint64]map[raft.NodeID]bool
}

func newLeaderTracker() *leaderTracker {
	return &leaderTracker{leaders: make(map[uint64]map[raft.NodeID]bool)}
}

// observe records that id sent an AppendEntriesReq at term. It returns the
// number of distinct leaders now on record for that term.
func (t *leaderTracker) observe(term uint64, id raft.NodeID) int {
	set, ok := t.leaders[term]
	if !ok {
		set = make(map[raft.NodeID]bool)
		t.leaders[term] = set
	}
	set[id] = true
	return len(set)
}

// SingleLeaderPerTerm is the phase-1 safety invariant: at most one node may
// ever be observed as leader of a given term.
func SingleLeaderPerTerm(s *Sim) error {
	for term, set := range s.leaders.leaders {
		if len(set) > 1 {
			ids := make([]raft.NodeID, 0, len(set))
			for id := range set {
				ids = append(ids, id)
			}
			return fmt.Errorf("sim: invariant violated: term %d has %d leaders: %v", term, len(set), sortedIDs(ids))
		}
	}
	return nil
}

func sortedIDs(ids []raft.NodeID) []raft.NodeID {
	out := append([]raft.NodeID(nil), ids...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
