package sim

import (
	"strings"
	"testing"

	"miniquorum/internal/raft"
)

// TestPartitionBlocksOrderedPairOnly verifies the mask blocks the exact
// ordered pair only: from->to blocked does not imply to->from is blocked.
func TestPartitionBlocksOrderedPairOnly(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.Partition(1, 2)

	s.handleMessage(heartbeatMessage(1, 2, 1))
	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "drop(partition)") {
		t.Fatalf("last trace entry = %q, want a partition drop", got)
	}

	s.handleMessage(heartbeatMessage(2, 1, 1))
	if got := s.trace[len(s.trace)-1]; strings.Contains(got, "drop(partition)") {
		t.Fatalf("last trace entry = %q, want delivery (reverse direction is not partitioned)", got)
	}
}

// TestHealRemovesBlock verifies Heal reverses a prior Partition.
func TestHealRemovesBlock(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.Partition(1, 2)
	s.Heal(1, 2)

	s.handleMessage(heartbeatMessage(1, 2, 1))
	if got := s.trace[len(s.trace)-1]; strings.Contains(got, "drop(partition)") {
		t.Fatalf("last trace entry = %q, want delivery after Heal", got)
	}
}

// TestCrashDiscardsNodeRestartRebuildsFromStorage verifies the sim crash
// model: crash discards the live *raft.Node, but its sim storage survives
// and seeds the rebuilt node on restart.
func TestCrashDiscardsNodeRestartRebuildsFromStorage(t *testing.T) {
	s := newTestSim(t, 1, 2)
	sn := s.nodes[1]
	if err := sn.storage.Save(&raft.HardState{Term: 7, VotedFor: 1}, nil); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	s.handleCrash(1)
	if sn.node != nil {
		t.Fatal("node still live after crash, want nil")
	}

	s.handleRestart(1)
	if sn.node == nil {
		t.Fatal("node nil after restart, want rebuilt")
	}
	hs, err := sn.storage.HardState()
	if err != nil || hs.Term != 7 {
		t.Fatalf("post-restart storage HardState = %+v, err = %v, want Term 7 (storage survives crash)", hs, err)
	}
}

// TestTickSkippedWhileCrashed verifies a crashed node neither ticks nor
// reschedules its own next tick; only Restart resumes it.
func TestTickSkippedWhileCrashed(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.handleCrash(1)

	before := s.queue.Len()
	s.handleTick(1)
	if got := s.queue.Len(); got != before {
		t.Fatalf("queue len = %d, want %d (crashed node must not reschedule)", got, before)
	}
	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "skip(down)") {
		t.Fatalf("last trace entry = %q, want a skip(down) tick", got)
	}
}

// TestConfigurableNodeCount proves the sim supports an arbitrary cluster
// size (deployment is 3, Phase 2 needs 5).
func TestConfigurableNodeCount(t *testing.T) {
	s := newTestSim(t, 1, 2, 3, 4, 5)
	if len(s.order) != 5 {
		t.Fatalf("len(order) = %d, want 5", len(s.order))
	}
	if len(s.nodes) != 5 {
		t.Fatalf("len(nodes) = %d, want 5", len(s.nodes))
	}
	for _, id := range s.order {
		if len(s.nodes[id].cfg.Peers) != 4 {
			t.Fatalf("node %d has %d peers, want 4", id, len(s.nodes[id].cfg.Peers))
		}
	}
}

// TestNewSimRejectsDuplicateNodeIDs guards a caller mistake that would
// otherwise silently collapse two nodes into one.
func TestNewSimRejectsDuplicateNodeIDs(t *testing.T) {
	if _, err := NewSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 1, 2}}); err == nil {
		t.Fatal("NewSim() error = nil, want an error for duplicate NodeIDs")
	}
}

// TestRunSeedsSmoke exercises the seed-runner mechanism (box 12): distinct
// seeds each deterministically drive their own Sim, and the invariant runs
// on every one. It intentionally uses a small N; the 500-seed CI acceptance
// gate is out of this packet's scope.
func TestRunSeedsSmoke(t *testing.T) {
	seeds := make([]int64, 25)
	for i := range seeds {
		seeds[i] = int64(i)
	}
	cfg := Config{NodeIDs: []raft.NodeID{1, 2, 3}}
	if err := RunSeeds(cfg, seeds, 1000, nil); err != nil {
		t.Fatalf("RunSeeds() error = %v", err)
	}
}
