package sim

import (
	"testing"

	"miniquorum/internal/raft"
)

const (
	phase1TickInterval = VirtualTime(50)
	phase1MaxTimeout   = 20
	phase1Window       = phase1TickInterval * phase1MaxTimeout
)

func phase1Sim(t *testing.T, seed int64) *Sim {
	t.Helper()
	s, err := NewSim(Config{Seed: seed, NodeIDs: []raft.NodeID{1, 2, 3}})
	if err != nil {
		t.Fatalf("NewSim() error = %v", err)
	}
	// Every Phase 1 scenario registers the safety invariant, which makes Run
	// check it after every scheduler event rather than only at scenario end.
	s.RegisterInvariant(SingleLeaderPerTerm)
	return s
}

func runFor(t *testing.T, s *Sim, duration VirtualTime) {
	t.Helper()
	if err := s.Run(s.Now() + duration); err != nil {
		t.Fatalf("Run() error = %v\ntrace:\n%v", err, s.Trace())
	}
}

func requireLeaderAtHighestTerm(t *testing.T, s *Sim, within VirtualTime) (uint64, raft.NodeID) {
	t.Helper()
	deadline := s.Now() + within
	for s.Now() < deadline {
		runFor(t, s, phase1TickInterval)
		term := s.HighestTerm()
		if ids := s.Leaderships()[term]; len(ids) == 1 {
			return term, ids[0]
		}
	}
	t.Fatalf("no single leader at highest term by t=%d; highest term=%d leaderships=%v", deadline, s.HighestTerm(), s.Leaderships())
	return 0, 0
}

func partitionGroups(s *Sim, left, right []raft.NodeID) {
	for _, from := range left {
		for _, to := range right {
			s.Partition(from, to)
			s.Partition(to, from)
		}
	}
}

func healGroups(s *Sim, left, right []raft.NodeID) {
	for _, from := range left {
		for _, to := range right {
			s.Heal(from, to)
			s.Heal(to, from)
		}
	}
}

func without(ids []raft.NodeID, excluded raft.NodeID) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(ids)-1)
	for _, id := range ids {
		if id != excluded {
			out = append(out, id)
		}
	}
	return out
}

func TestPhase1ColdStartSteadyStateThirtySeconds(t *testing.T) {
	s := phase1Sim(t, 101)
	term, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	// Once the initial election has converged, heartbeats must prevent any
	// additional term changes for at least 30 virtual seconds.
	runFor(t, s, 30_000)
	if got := s.HighestTerm(); got != term {
		t.Fatalf("highest term after 30s steady state = %d, want %d (leader %d)", got, term, leader)
	}
	ids := s.Leaderships()[term]
	if len(ids) != 1 || ids[0] != leader {
		t.Fatalf("leaders in steady-state term %d = %v, want only %d", term, ids, leader)
	}
}

func TestPhase1LeaderCrashRecoversWithinTenElectionWindows(t *testing.T) {
	s := phase1Sim(t, 202)
	oldTerm, oldLeader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	s.ScheduleCrash(oldLeader, s.Now())
	runFor(t, s, 10*phase1Window)

	term := s.HighestTerm()
	ids := s.Leaderships()[term]
	if term <= oldTerm || len(ids) != 1 || ids[0] == oldLeader {
		t.Fatalf("after crashing leader %d in term %d: highest term=%d leaders=%v, want a different single leader in a newer term", oldLeader, oldTerm, term, ids)
	}
}

func TestPhase1SymmetricNoQuorumPartitionHeals(t *testing.T) {
	s := phase1Sim(t, 303)
	partitionGroups(s, []raft.NodeID{1}, []raft.NodeID{2})
	partitionGroups(s, []raft.NodeID{1}, []raft.NodeID{3})
	partitionGroups(s, []raft.NodeID{2}, []raft.NodeID{3})

	// The partition is installed before the first tick, so any observed leader
	// would be a quorum win while every node is isolated.
	runFor(t, s, 10*phase1Window)
	if got := s.Leaderships(); len(got) != 0 {
		t.Fatalf("leaders while 1|1|1 partitioned = %v, want no election win", got)
	}

	healGroups(s, []raft.NodeID{1}, []raft.NodeID{2})
	healGroups(s, []raft.NodeID{1}, []raft.NodeID{3})
	healGroups(s, []raft.NodeID{2}, []raft.NodeID{3})
	term, _ := requireLeaderAtHighestTerm(t, s, 10*phase1Window)
	if len(s.Leaderships()[term]) != 1 {
		t.Fatalf("leaders after 1|1|1 heal in term %d = %v, want exactly one", term, s.Leaderships()[term])
	}
}

func TestPhase1MinorityCannotNewlyElectAndHealConverges(t *testing.T) {
	s := phase1Sim(t, 404)
	oldTerm, isolatedLeader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	majority := without([]raft.NodeID{1, 2, 3}, isolatedLeader)
	partitionGroups(s, []raft.NodeID{isolatedLeader}, majority)

	// The isolated pre-elected leader may retain its role without CheckQuorum.
	// It must not win a new term; the two-node side must elect one instead.
	runFor(t, s, 10*phase1Window)
	leaderships := s.Leaderships()
	var majorityTerm uint64
	for term, ids := range leaderships {
		if term <= oldTerm {
			continue
		}
		for _, id := range ids {
			if id == isolatedLeader {
				t.Fatalf("isolated minority node %d newly led term %d; leaderships=%v", isolatedLeader, term, leaderships)
			}
		}
		if len(ids) == 1 && term > majorityTerm {
			majorityTerm = term
		}
	}
	if majorityTerm == 0 {
		t.Fatalf("majority did not elect after isolating leader %d; leaderships=%v", isolatedLeader, leaderships)
	}

	// Without CheckQuorum the isolated old leader retains its current role.
	// Healing still must converge to exactly one leader at the highest term.
	healGroups(s, []raft.NodeID{isolatedLeader}, majority)
	term, leader := requireLeaderAtHighestTerm(t, s, 10*phase1Window)
	runFor(t, s, 3*phase1Window)
	if got := s.HighestTerm(); got != term {
		t.Fatalf("term changed during post-heal convergence: got %d, want %d; leaderships=%v", got, term, s.Leaderships())
	}
	ids := s.Leaderships()[term]
	if len(ids) != 1 || ids[0] != leader {
		t.Fatalf("post-heal leaders at highest term %d = %v, want only %d", term, ids, leader)
	}
}

func TestPhase1HealedFollowerRejoinDisruptsWithoutPreVote(t *testing.T) {
	s := phase1Sim(t, 505)
	oldTerm, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	isolate := without([]raft.NodeID{1, 2, 3}, leader)[0]
	majority := without([]raft.NodeID{1, 2, 3}, isolate)
	partitionGroups(s, []raft.NodeID{isolate}, majority)

	// Unlike an isolated leader, an isolated follower times out, starts
	// unsuccessful elections, and advances its durable term.
	runFor(t, s, 5*phase1Window)
	isolatedTerm := s.HighestTerm()
	if isolatedTerm <= oldTerm {
		t.Fatalf("isolated follower did not advance its term: before=%d after=%d", oldTerm, isolatedTerm)
	}

	healGroups(s, []raft.NodeID{isolate}, majority)
	term, _ := requireLeaderAtHighestTerm(t, s, 10*phase1Window)
	if term < isolatedTerm {
		t.Fatalf("healed term=%d, want at least isolated follower term=%d", term, isolatedTerm)
	}
	t.Logf("seed 505: isolated follower raised term %d -> %d; healing converged at term %d", oldTerm, isolatedTerm, term)
}

func TestPhase1FiveHundredSeeds(t *testing.T) {
	seeds := make([]int64, 500)
	for i := range seeds {
		seeds[i] = int64(i + 1)
	}
	cfg := Config{NodeIDs: []raft.NodeID{1, 2, 3}}
	if err := RunSeeds(cfg, seeds, 5*phase1Window, nil); err != nil {
		t.Fatalf("500-seed single-leader corpus failed: %v", err)
	}
}
