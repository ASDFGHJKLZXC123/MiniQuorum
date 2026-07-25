package sim

import (
	"testing"

	"miniquorum/internal/raft"
)

// TestGeneratedBeforeSyncCrashesCoverRetentionShapes is the v5 canary: across
// the closed seed campaign, generated before-sync storage crashes must cover
// all three surviving-unsynced-prefix shapes — none (0), a positive partial
// (torn) prefix, and all (RetainAllUnsynced) — so the 1k/10k gates actually
// combine Phase 3's before-sync loss and torn-write survivor states with the
// network/pause/skew faults rather than only ever retaining everything. It also
// pins the harmless-ignored contract: a post-sync crash, whose unsynced buffer
// is already drained, must keep the RetainAllUnsynced sentinel so an ignored
// retention value can never be misread as a deliberate torn prefix.
func TestGeneratedBeforeSyncCrashesCoverRetentionShapes(t *testing.T) {
	nodeIDs := []raft.NodeID{1, 2, 3, 4, 5}
	var none, partial, all int
	// The PR and nightly gates use the closed contiguous campaign 1..10000.
	// Generate every schedule here (rather than sampling it) so a future RNG
	// change cannot leave one survivor state outside the campaign the gates
	// actually execute.
	const closedCampaignLastSeed = int64(10_000)
	for seed := int64(1); seed <= closedCampaignLastSeed; seed++ {
		schedule, err := GenerateFaultSchedule(seed, nodeIDs, 6000)
		if err != nil {
			t.Fatalf("seed %d: GenerateFaultSchedule: %v", seed, err)
		}
		for _, directive := range schedule.Crashes {
			if directive.Point != CrashBeforeSync {
				if directive.RetainUnsynced != RetainAllUnsynced {
					t.Fatalf("seed %d: post-sync crash %+v carries non-sentinel retention %d; the drained buffer makes it a no-op that must stay RetainAllUnsynced",
						seed, directive, directive.RetainUnsynced)
				}
				continue
			}
			switch {
			case directive.RetainUnsynced == 0:
				none++
			case directive.RetainUnsynced == RetainAllUnsynced:
				all++
			case directive.RetainUnsynced > 0 && directive.RetainUnsynced <= maxGeneratedPartialRetention:
				partial++
			default:
				t.Fatalf("seed %d: before-sync crash %+v carries out-of-range retention %d",
					seed, directive, directive.RetainUnsynced)
			}
		}
	}
	if none == 0 || partial == 0 || all == 0 {
		t.Fatalf("before-sync retention coverage incomplete over the closed seeds 1..%d: none=%d partial=%d all=%d (want each > 0)",
			closedCampaignLastSeed,
			none, partial, all)
	}
}

// TestGeneratedRetentionIsDeterministicPerSeed guards that adding the retention
// draws did not smuggle any nondeterminism into generation: the same seed still
// produces a byte-identical schedule, including its crash retention.
func TestGeneratedRetentionIsDeterministicPerSeed(t *testing.T) {
	nodeIDs := []raft.NodeID{1, 2, 3, 4, 5}
	for _, seed := range []int64{1, 2, 7, 42, 300} {
		first, err := GenerateFaultSchedule(seed, nodeIDs, 6000)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		second, err := GenerateFaultSchedule(seed, nodeIDs, 6000)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		firstBytes, err := EncodeFaultSchedule(first)
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := EncodeFaultSchedule(second)
		if err != nil {
			t.Fatal(err)
		}
		if string(firstBytes) != string(secondBytes) {
			t.Fatalf("seed %d: generation is not deterministic", seed)
		}
	}
}
