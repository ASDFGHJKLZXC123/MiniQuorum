package sim

import (
	"bytes"
	"fmt"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
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

// LogMatching is the Phase 2 log safety invariant. For every node pair, a
// shared (index, term) proves that the complete prefixes ending at that index
// are byte-for-byte identical. Independently, the portion committed by both
// nodes must be identical even when their entries have different terms.
//
// NewSim installs this invariant by default, so Sim.Run checks it after every
// scheduled event. Phase 2 has no snapshots or log compaction, hence every
// non-empty prefix starts at index 1.
func LogMatching(s *Sim) error {
	for leftPos, leftID := range s.order {
		leftLog := s.Log(leftID)
		for _, rightID := range s.order[leftPos+1:] {
			rightLog := s.Log(rightID)

			for entryPos := range leftLog {
				leftEntry := &leftLog[entryPos]
				rightEntry := entryAt(rightLog, leftEntry.Index)
				if rightEntry == nil || rightEntry.Term != leftEntry.Term {
					continue
				}
				for index := uint64(1); index <= leftEntry.Index; index++ {
					leftPrefix := entryAt(leftLog, index)
					rightPrefix := entryAt(rightLog, index)
					if !sameEntry(leftPrefix, rightPrefix) {
						return fmt.Errorf("sim: log-matching invariant violated: nodes %d and %d share (%d,%d) but differ at prefix index %d: left=%s right=%s", leftID, rightID, leftEntry.Index, leftEntry.Term, index, formatEntry(leftPrefix), formatEntry(rightPrefix))
					}
				}
			}

			committedThrough := min(s.LastApplied(leftID), s.LastApplied(rightID))
			for index := uint64(1); index <= committedThrough; index++ {
				leftEntry := entryAt(leftLog, index)
				rightEntry := entryAt(rightLog, index)
				if !sameEntry(leftEntry, rightEntry) {
					return fmt.Errorf("sim: committed-prefix invariant violated: nodes %d and %d are both committed through %d but differ at index %d: left=%s right=%s", leftID, rightID, committedThrough, index, formatEntry(leftEntry), formatEntry(rightEntry))
				}
			}
		}
	}
	return nil
}

func entryAt(entries []raftpb.Entry, index uint64) *raftpb.Entry {
	if len(entries) == 0 || index < entries[0].Index {
		return nil
	}
	offset := index - entries[0].Index
	if offset >= uint64(len(entries)) || entries[offset].Index != index {
		return nil
	}
	return &entries[offset]
}

func sameEntry(left, right *raftpb.Entry) bool {
	return left != nil && right != nil && left.Index == right.Index && left.Term == right.Term && left.Type == right.Type && bytes.Equal(left.Data, right.Data)
}

func formatEntry(entry *raftpb.Entry) string {
	if entry == nil {
		return "<missing>"
	}
	return fmt.Sprintf("(%d,%d,%s,%x)", entry.Index, entry.Term, entry.Type, entry.Data)
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
