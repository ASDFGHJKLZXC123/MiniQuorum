package sim

import (
	"math/rand"

	"miniquorum/internal/raft"
)

// GenerateFaultSchedule builds a fault schedule deterministically from seed:
// identical seed, nodeIDs, and until always produce a byte-identical
// schedule (see fault_test.go). It draws from its own local *rand.Rand
// and never touches a *Sim, so a schedule can be generated once, serialized
// with EncodeFaultSchedule, and later replayed by NewFaultSim independently
// of how or when it was generated — the random-from-seed half of the
// phase-4 "usable in random-from-seed and scripted modes" requirement.
// Bumping the algorithm below (new fault kinds, different draw order or
// parameter ranges) must bump FaultScheduleGeneratorVersion, since it
// silently remaps seed -> schedule.
func GenerateFaultSchedule(seed int64, nodeIDs []raft.NodeID, until VirtualTime) FaultSchedule {
	order := append([]raft.NodeID(nil), nodeIDs...)
	r := rand.New(rand.NewSource(seed))
	schedule := FaultSchedule{Version: FaultScheduleGeneratorVersion}

	kinds := []FaultKind{FaultDropRate, FaultDuplicateRate, FaultPause, FaultResume, FaultClockSkew}
	if len(order) >= 2 {
		kinds = append(kinds, FaultPartition, FaultHeal, FaultPartitionGroups, FaultHealGroups)
	}

	const eventCount = 12
	for i := 0; i < eventCount; i++ {
		t := randVirtualTime(r, until)
		kind := kinds[r.Intn(len(kinds))]
		ev := FaultEvent{Time: t, Kind: kind}
		switch kind {
		case FaultDropRate, FaultDuplicateRate:
			// Capped well under 1.0: a 100% loss/duplicate rate makes
			// liveness impossible to observe within a short seeded run, and
			// this generator's job is to find bugs, not to wedge the sim.
			ev.Rate = r.Float64() * 0.3
		case FaultPartition, FaultHeal:
			ev.From, ev.To = randomDistinctPair(r, order)
		case FaultPartitionGroups, FaultHealGroups:
			ev.Groups = randomGroupSplit(r, order)
		case FaultPause, FaultResume:
			ev.Node = order[r.Intn(len(order))]
		case FaultClockSkew:
			ev.Node = order[r.Intn(len(order))]
			ev.Multiplier = minClockMultiplier + r.Float64()*(maxClockMultiplier-minClockMultiplier)
		}
		schedule.Events = append(schedule.Events, ev)
	}
	sortFaultEvents(schedule.Events)

	// A couple of storage-level crash points, reusing the Phase 3 model, so
	// a generated schedule also exercises "the schedule selects the crash
	// point." Save ordinals are spaced far enough apart (per distinct node)
	// that these two directives can never collide on (Node, Save), which
	// NewCrashStorage would otherwise reject.
	const crashCount = 2
	points := []CrashPoint{CrashBeforeSync, CrashAfterSyncBeforeSend, CrashAfterSend}
	for i := 0; i < crashCount; i++ {
		node := order[i%len(order)]
		save := uint64(5 + i*25 + r.Intn(20))
		schedule.Crashes = append(schedule.Crashes, CrashDirective{
			Node:           node,
			Save:           save,
			Point:          points[r.Intn(len(points))],
			RetainUnsynced: RetainAllUnsynced,
		})
	}

	return schedule
}

func randVirtualTime(r *rand.Rand, until VirtualTime) VirtualTime {
	if until <= 0 {
		return 0
	}
	return VirtualTime(r.Int63n(int64(until)))
}

// randomDistinctPair picks two different node IDs from order. order must
// have at least 2 elements.
func randomDistinctPair(r *rand.Rand, order []raft.NodeID) (raft.NodeID, raft.NodeID) {
	i := r.Intn(len(order))
	j := r.Intn(len(order) - 1)
	if j >= i {
		j++
	}
	return order[i], order[j]
}

// randomGroupSplit partitions order into exactly two non-empty groups at a
// random cut of a randomly shuffled copy. order must have at least 2
// elements.
func randomGroupSplit(r *rand.Rand, order []raft.NodeID) [][]raft.NodeID {
	shuffled := append([]raft.NodeID(nil), order...)
	r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	split := 1 + r.Intn(len(shuffled)-1)
	left := append([]raft.NodeID(nil), shuffled[:split]...)
	right := append([]raft.NodeID(nil), shuffled[split:]...)
	return [][]raft.NodeID{left, right}
}
