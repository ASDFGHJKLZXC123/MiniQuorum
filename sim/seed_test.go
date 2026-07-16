package sim

import (
	"flag"
	"testing"

	"miniquorum/internal/raft"
)

// seedFlag backs `make sim SEED=<seed>`: a single deterministic run driven
// entirely by one seed, per the phase-1 seed-runner requirement.
var seedFlag = flag.Int64("seed", 1, "run seed for `make sim SEED=<seed>`")

// TestSim is the `make sim` entry point: build a 3-node cluster from
// -seed, run it, and check the single-leader-per-term invariant throughout.
func TestSim(t *testing.T) {
	s, err := NewSim(Config{Seed: *seedFlag, NodeIDs: []raft.NodeID{1, 2, 3}})
	if err != nil {
		t.Fatalf("NewSim() error = %v", err)
	}
	s.RegisterInvariant(SingleLeaderPerTerm)
	if err := s.Run(10000); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}
