package sim

import (
	"reflect"
	"testing"

	"miniquorum/internal/raft"
)

// TestDeterminismSameSeedByteIdentical is the phase-1 required determinism
// gate: two independently constructed Sim runs from the same seed and
// Config must produce byte-identical event logs. It also exercises crash,
// restart, and partition mechanics so those code paths are covered by the
// determinism guarantee, not just plain ticking.
func TestDeterminismSameSeedByteIdentical(t *testing.T) {
	cfg := Config{
		Seed:    424242,
		NodeIDs: []raft.NodeID{1, 2, 3},
	}

	run := func() []string {
		s, err := NewSim(cfg)
		if err != nil {
			t.Fatalf("NewSim() error = %v", err)
		}
		s.RegisterInvariant(SingleLeaderPerTerm)
		s.Partition(2, 3)
		s.ScheduleCrash(1, 500)
		s.ScheduleRestart(1, 900)
		if err := s.Run(2000); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		s.Heal(2, 3)
		return s.Trace()
	}

	first := run()
	second := run()

	if len(first) == 0 {
		t.Fatal("Trace() is empty, want a non-trivial run")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same-seed runs diverged:\nfirst:  %v\nsecond: %v", first, second)
	}
}
