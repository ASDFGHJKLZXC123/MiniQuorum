package raft

import (
	"reflect"
	"testing"

	raftpb "miniquorum/proto"
)

// TestReadyCommittedEntriesRemainPendingUntilAdvance is the direct
// apply-before-Advance regression for the frozen Ready contract: committed
// entries stay in every subsequent Ready until the host acknowledges the
// batch with Advance, so a host that crashes after applying but before
// Advance re-observes the same entries, and a host that Advances is promised
// never to see them again. Advance — not Ready — is the application
// acknowledgment boundary.
func TestReadyCommittedEntriesRemainPendingUntilAdvance(t *testing.T) {
	node := NewNode(Config{
		ID:              1,
		Peers:           []NodeID{1},
		ElectionTickMin: 2,
		ElectionTickMax: 2,
	}, InitialState{}, nil)
	node.Tick()
	node.Tick()

	index, term, isLeader := node.Propose([]byte("pending-until-advance"))
	if !isLeader || index != 2 || term != 1 {
		t.Fatalf("Propose = (%d, %d, %t), want committed single-node entry (2, 1, true)", index, term, isLeader)
	}

	first := node.Ready()
	if len(first.CommittedEntries) != 2 ||
		first.CommittedEntries[0].Type != raftpb.EntryType_NOOP ||
		first.CommittedEntries[1].Index != index ||
		string(first.CommittedEntries[1].Data) != "pending-until-advance" {
		t.Fatalf("first Ready committed = %+v, want the term-start no-op then the proposal", first.CommittedEntries)
	}

	// No Advance yet: the batch is still pending, so Ready must re-emit the
	// identical committed entries rather than treat reporting as applying.
	second := node.Ready()
	if !reflect.DeepEqual(first.CommittedEntries, second.CommittedEntries) {
		t.Fatalf("Ready without Advance changed the pending batch:\nfirst=%+v\nsecond=%+v",
			first.CommittedEntries, second.CommittedEntries)
	}
	if !reflect.DeepEqual(first.Entries, second.Entries) {
		t.Fatalf("Ready without Advance changed unstable entries:\nfirst=%+v\nsecond=%+v", first.Entries, second.Entries)
	}

	node.Advance()
	after := node.Ready()
	if len(after.CommittedEntries) != 0 || len(after.Entries) != 0 || after.HardState != nil {
		t.Fatalf("Ready after Advance still reports pending work: %+v", after)
	}

	// New work after the acknowledgment must surface only the new entry:
	// acknowledged entries are never re-delivered for a second application.
	nextIndex, _, isLeader := node.Propose([]byte("after-advance"))
	if !isLeader || nextIndex != index+1 {
		t.Fatalf("second Propose = (%d, %t), want index %d on the leader", nextIndex, isLeader, index+1)
	}
	next := node.Ready()
	if len(next.CommittedEntries) != 1 || next.CommittedEntries[0].Index != nextIndex {
		t.Fatalf("Ready after new proposal = %+v, want exactly the new committed entry %d", next.CommittedEntries, nextIndex)
	}
	node.Advance()
}
