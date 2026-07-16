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

// TestRestartBeforeStaleTickDropsDuplicateStream is a regression for a
// stale-event bug: a tick queued before a crash could still be sitting in
// the event queue when Restart ran early (crash then restart, both before
// that queued tick's virtual time). Because Restart schedules its own next
// tick independently of the queue, that pre-crash tick would later fire
// against the freshly restarted node and reschedule itself, producing two
// interleaved tick streams for one node. Ticks must be tied to a generation
// that a crash invalidates so the pre-crash tick is dropped as stale and
// only the stream Restart started survives.
func TestRestartBeforeStaleTickDropsDuplicateStream(t *testing.T) {
	s := newTestSim(t, 1, 2)

	// t=50: both nodes' first tick fires, queuing node 1's next (gen-0) tick
	// at t=100.
	if err := s.Run(s.tickInterval); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	s.ScheduleCrash(1, 60)   // bumps node 1's generation to 1; the queued t=100 tick is now stale
	s.ScheduleRestart(1, 70) // schedules a fresh gen-1 tick at t=120, before the stale t=100 tick fires

	until := s.tickInterval * 5 // t=250: past the stale tick and several gen-1 ticks
	if err := s.Run(until); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var successful, stale int
	for _, line := range s.Trace() {
		if !strings.Contains(line, "tick node=1") {
			continue
		}
		switch {
		case strings.Contains(line, "skip(stale"):
			stale++
		case strings.Contains(line, "skip(down"):
			// not expected in this scenario; ignored either way
		default:
			successful++
		}
	}

	if stale != 1 {
		t.Fatalf("stale tick skips for node 1 = %d, want 1 (only the pre-crash t=100 tick)", stale)
	}
	// One tick before the crash (t=50) plus the gen-1 chain Restart started
	// (t=120, 170, 220) = 4. Under the duplicate-stream bug the stale gen-0
	// chain would also fire at t=100, 150, 200, 250, doubling this to 8.
	if successful != 4 {
		t.Fatalf("successful ticks for node 1 = %d, want 4 (no duplicate stream)", successful)
	}
}

// TestConfigurableNodeCount proves the sim supports an arbitrary cluster
// size (deployment is 3, Phase 2 needs 5), and that each node's Peers is the
// full ordered membership including itself, per the frozen raft.Config
// contract that Node.hasMajority divides by (internal/raft/raft.go).
func TestConfigurableNodeCount(t *testing.T) {
	s := newTestSim(t, 1, 2, 3, 4, 5)
	if len(s.order) != 5 {
		t.Fatalf("len(order) = %d, want 5", len(s.order))
	}
	if len(s.nodes) != 5 {
		t.Fatalf("len(nodes) = %d, want 5", len(s.nodes))
	}
	for _, id := range s.order {
		if len(s.nodes[id].cfg.Peers) != 5 {
			t.Fatalf("node %d has %d peers, want 5 (full membership including self)", id, len(s.nodes[id].cfg.Peers))
		}
	}
}

// TestEvenMembershipUsesFullClusterSize is a regression for a majority-math
// bug: NewSim used to give each node only the *other* nodes in
// raft.Config.Peers, so a 4-node cluster's majority was computed against 3
// peers (3/2+1 = 2) instead of the full cluster size (4/2+1 = 3). That
// silently permitted a "majority" with only 2 of 4 votes. Peers must be the
// full ordered membership including self, so hasMajority's len(peers)/2+1
// (internal/raft/raft.go) matches the true cluster size for every node,
// including even-sized clusters where the off-by-one is easiest to miss.
func TestEvenMembershipUsesFullClusterSize(t *testing.T) {
	s := newTestSim(t, 1, 2, 3, 4)
	if len(s.order) != 4 {
		t.Fatalf("len(order) = %d, want 4", len(s.order))
	}
	for _, id := range s.order {
		got := s.nodes[id].cfg.Peers
		if len(got) != 4 {
			t.Fatalf("node %d has %d peers, want 4 (full membership including self)", id, len(got))
		}
		found := false
		for _, p := range got {
			if p == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("node %d Peers = %v, want to include itself", id, got)
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
