package sim

import (
	"errors"
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
// nodeIDs must be non-empty; an empty cluster has no valid fault target and
// is rejected deterministically rather than panicking on the first node-
// indexed draw.
//
// Every pause/resume, partition/heal, partition-groups/heal-groups, and
// host crash/restart is generated as one matched (open, close) pair on the
// same target, so no generated transition can be a guaranteed no-op (a
// Resume with no preceding Pause on that node, or a Heal with no preceding
// Partition on that pair, changes nothing). Host FaultCrash/FaultRestart
// pairs are event-time-scheduled, so they always fire by `until` regardless
// of runtime dynamics; the storage-level Save-ordinal Crashes directives
// below remain best-effort (whether a given Save ordinal is actually
// reached depends on election/heartbeat timing this generator does not
// model), but each is paired with its own later FaultRestart so a node it
// does crash is never left stranded.
//
// Bumping the algorithm below (new fault kinds, different draw order or
// parameter ranges) must bump FaultScheduleGeneratorVersion, since it
// silently remaps seed -> schedule.
func GenerateFaultSchedule(seed int64, nodeIDs []raft.NodeID, until VirtualTime) (FaultSchedule, error) {
	if len(nodeIDs) == 0 {
		return FaultSchedule{}, errors.New("sim: GenerateFaultSchedule: nodeIDs must not be empty")
	}
	order := append([]raft.NodeID(nil), nodeIDs...)
	r := rand.New(rand.NewSource(seed))
	schedule := FaultSchedule{Version: FaultScheduleGeneratorVersion}

	type pairKind int
	const (
		pairPauseResume pairKind = iota
		pairPartitionHeal
		pairGroupsHealGroups
		pairCrashRestart
	)
	kinds := []pairKind{pairPauseResume, pairCrashRestart}
	if len(order) >= 2 {
		kinds = append(kinds, pairPartitionHeal, pairGroupsHealGroups)
	}

	const pairCount = 6
	for i := 0; i < pairCount; i++ {
		t1, t2 := randTimePair(r, until)
		switch kinds[r.Intn(len(kinds))] {
		case pairPauseResume:
			node := order[r.Intn(len(order))]
			schedule.Events = append(schedule.Events,
				FaultEvent{Time: t1, Kind: FaultPause, Node: node},
				FaultEvent{Time: t2, Kind: FaultResume, Node: node})
		case pairPartitionHeal:
			from, to := randomDistinctPair(r, order)
			schedule.Events = append(schedule.Events,
				FaultEvent{Time: t1, Kind: FaultPartition, From: from, To: to},
				FaultEvent{Time: t2, Kind: FaultHeal, From: from, To: to})
		case pairGroupsHealGroups:
			groups := randomGroupSplit(r, order)
			schedule.Events = append(schedule.Events,
				FaultEvent{Time: t1, Kind: FaultPartitionGroups, Groups: groups},
				FaultEvent{Time: t2, Kind: FaultHealGroups, Groups: groups})
		case pairCrashRestart:
			node := order[r.Intn(len(order))]
			schedule.Events = append(schedule.Events,
				FaultEvent{Time: t1, Kind: FaultCrash, Node: node},
				FaultEvent{Time: t2, Kind: FaultRestart, Node: node})
		}
	}

	// drop-rate/duplicate-rate/clock-skew are safe to draw independently:
	// setting a rate or a per-node multiplier is meaningful on its own, it
	// is never conditioned on a prior fault the way Resume/Heal are.
	const soloCount = 6
	soloKinds := []FaultKind{FaultDropRate, FaultDuplicateRate, FaultClockSkew}
	for i := 0; i < soloCount; i++ {
		kind := soloKinds[r.Intn(len(soloKinds))]
		ev := FaultEvent{Time: randVirtualTime(r, until), Kind: kind}
		switch kind {
		case FaultDropRate, FaultDuplicateRate:
			// Capped well under 1.0: a 100% loss/duplicate rate makes
			// liveness impossible to observe within a short seeded run, and
			// this generator's job is to find bugs, not to wedge the sim.
			ev.Rate = r.Float64() * 0.3
		case FaultClockSkew:
			ev.Node = order[r.Intn(len(order))]
			ev.Multiplier = minClockMultiplier + r.Float64()*(maxClockMultiplier-minClockMultiplier)
		}
		schedule.Events = append(schedule.Events, ev)
	}

	// A couple of storage-level crash points, reusing the Phase 3 model, so
	// a generated schedule also exercises "the schedule selects the crash
	// point." Save ordinals are spaced far enough apart (per distinct node)
	// that these two directives can never collide on (Node, Save), which
	// NewCrashStorage would otherwise reject. Each directive's node gets a
	// FaultRestart late in the run: if the storage crash does fire, the
	// node is rebuilt instead of stranded; if it never fires, the restart
	// targets a still-live node and is safely rejected as a no-op.
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
		schedule.Events = append(schedule.Events, FaultEvent{Time: lateRestartTime(r, until), Kind: FaultRestart, Node: node})
	}

	sortFaultEvents(schedule.Events)
	return schedule, nil
}

func randVirtualTime(r *rand.Rand, until VirtualTime) VirtualTime {
	if until <= 0 {
		return 0
	}
	return VirtualTime(r.Int63n(int64(until)))
}

// randTimePair draws two virtual times t1 < t2 in [0,until), for pairing an
// opening fault (Pause, Partition, PartitionGroups, Crash) with the closing
// fault (Resume, Heal, HealGroups, Restart) that undoes it. When until is
// too small to fit two distinct instants it degenerates to t1 == t2: the
// pair still fires in schedule order (installFaultEvents' stable sort keeps
// the opening event first), it just doesn't hold open for a stretch.
func randTimePair(r *rand.Rand, until VirtualTime) (VirtualTime, VirtualTime) {
	t1 := randVirtualTime(r, until)
	remaining := until - t1 - 1
	if remaining <= 0 {
		return t1, t1
	}
	t2 := t1 + 1 + VirtualTime(r.Int63n(int64(remaining)))
	return t1, t2
}

// lateRestartTime picks a virtual time in the last quarter of the run (or 0
// if until is degenerate): a generated storage crash directive uses low
// Save ordinals that are likely, but not certain, to have fired by then, so
// its paired FaultRestart gets the widest reasonable window to actually
// find the node crashed and rebuild it.
func lateRestartTime(r *rand.Rand, until VirtualTime) VirtualTime {
	if until <= 0 {
		return 0
	}
	windowStart := until - until/4
	if windowStart >= until {
		return until - 1
	}
	span := int64(until - windowStart)
	return windowStart + VirtualTime(r.Int63n(span))
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
