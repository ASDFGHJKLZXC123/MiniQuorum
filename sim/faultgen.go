package sim

import (
	"errors"
	"math"
	"math/rand"
	"sort"

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
// same target. The six intervals are globally non-overlapping in schedule
// order, so a directed partition can never open an edge already owned by a
// group partition (or vice versa), and every generated stateful open is a
// real transition under the generator's own schedule state. Host
// FaultCrash/FaultRestart pairs are event-time-scheduled, so they always
// fire by `until` regardless of runtime dynamics. Storage-level Save-ordinal
// Crashes directives remain conditional on the runtime reaching that Save,
// but each carries RestartAfterCrash: NewFaultSim schedules its restart only
// after observing that exact directive fire, at the same virtual time, so
// recovery is causal and cannot precede or miss a late crash.
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
	pairs := make([][2]FaultEvent, 0, pairCount)
	for i := 0; i < pairCount; i++ {
		t1, t2 := randTimePair(r, until)
		switch kinds[r.Intn(len(kinds))] {
		case pairPauseResume:
			node := order[r.Intn(len(order))]
			pairs = append(pairs, [2]FaultEvent{
				FaultEvent{Time: t1, Kind: FaultPause, Node: node},
				FaultEvent{Time: t2, Kind: FaultResume, Node: node},
			})
		case pairPartitionHeal:
			from, to := randomDistinctPair(r, order)
			pairs = append(pairs, [2]FaultEvent{
				FaultEvent{Time: t1, Kind: FaultPartition, From: from, To: to},
				FaultEvent{Time: t2, Kind: FaultHeal, From: from, To: to},
			})
		case pairGroupsHealGroups:
			groups := randomGroupSplit(r, order)
			pairs = append(pairs, [2]FaultEvent{
				FaultEvent{Time: t1, Kind: FaultPartitionGroups, Groups: groups},
				FaultEvent{Time: t2, Kind: FaultHealGroups, Groups: groups},
			})
		case pairCrashRestart:
			node := order[r.Intn(len(order))]
			pairs = append(pairs, [2]FaultEvent{
				FaultEvent{Time: t1, Kind: FaultCrash, Node: node},
				FaultEvent{Time: t2, Kind: FaultRestart, Node: node},
			})
		}
	}
	normalizeFaultPairTimes(pairs)
	for _, pair := range pairs {
		schedule.Events = append(schedule.Events, pair[0], pair[1])
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
			// FMA defines one correctly rounded operation on every platform.
			// A plain multiply-add is allowed to fuse on arm64 while remaining
			// separate on amd64, remapping the serialized schedule by one ULP.
			ev.Multiplier = math.FMA(r.Float64(), maxClockMultiplier-minClockMultiplier, minClockMultiplier)
		}
		schedule.Events = append(schedule.Events, ev)
	}

	// A couple of storage-level crash points, reusing the Phase 3 model, so
	// a generated schedule also exercises "the schedule selects the crash
	// point." Save ordinals are spaced far enough apart (per distinct node)
	// that these two directives can never collide on (Node, Save), which
	// NewCrashStorage would otherwise reject. RestartAfterCrash makes the
	// recovery conditional on this directive actually firing; it does not
	// guess a virtual time from the Save ordinal.
	//
	// A before-sync crash also draws its surviving-unsynced-prefix policy (v5):
	// the closed seed campaign must combine the none / all / positive-partial
	// (torn) survivor states Phase 3 exposed with the network/pause/skew faults,
	// instead of pinning every generated crash at RetainAllUnsynced. At the
	// post-sync points the unsynced buffer is already drained, so retention is
	// ignored and left at the harmless RetainAllUnsynced sentinel.
	const crashCount = 2
	points := []CrashPoint{CrashBeforeSync, CrashAfterSyncBeforeSend, CrashAfterSend}
	for i := 0; i < crashCount; i++ {
		node := order[i%len(order)]
		save := uint64(5 + i*25 + r.Intn(20))
		point := points[r.Intn(len(points))]
		schedule.Crashes = append(schedule.Crashes, CrashDirective{
			Node:              node,
			Save:              save,
			Point:             point,
			RetainUnsynced:    generatedUnsyncedRetention(r, point),
			RestartAfterCrash: true,
		})
	}

	// End every randomized campaign with a deterministic clean-network tail.
	// Rates are otherwise sticky by design, so without these explicit events a
	// final random setter could dominate the remainder of a short run and make
	// workload coverage depend on timeout retries instead of Raft progress.
	schedule.Events = append(schedule.Events,
		FaultEvent{Time: until, Kind: FaultDropRate, Rate: 0},
		FaultEvent{Time: until, Kind: FaultDuplicateRate, Rate: 0},
	)
	for _, node := range order {
		schedule.Events = append(schedule.Events, FaultEvent{
			Time:       until,
			Kind:       FaultClockSkew,
			Node:       node,
			Multiplier: 1,
		})
	}

	sortFaultEvents(schedule.Events)
	return schedule, nil
}

// maxGeneratedPartialRetention bounds the positive partial-prefix retention a
// generated before-sync crash may keep. A small byte count tears the tail of
// the not-yet-synced batch — a batch carrying an EntriesRecord (a client write)
// is well over this many bytes — leaving a genuine torn-prefix survivor rather
// than the whole batch. CrashStorage clamps a value past the actual buffer down
// to it, so a tiny hard-state-only batch degrades harmlessly to "keep all"
// rather than misbehaving.
const maxGeneratedPartialRetention = 24

// generatedUnsyncedRetention draws the surviving-unsynced-prefix policy for a
// generated crash directive. It is meaningful only at CrashBeforeSync, where
// the Phase 3 model retains an arbitrary prefix of the not-yet-synced batch:
//
//	0                          lose the whole unsynced batch (none survives)
//	1..maxGeneratedPartial     keep a positive partial prefix (a torn tail)
//	RetainAllUnsynced          keep every unsynced byte
//
// Drawing all three across the closed seed campaign is what lets the 1k/10k
// gates combine before-sync loss and torn-prefix survivors with the
// network/pause/skew faults, instead of only ever exercising RetainAllUnsynced;
// TestGeneratedBeforeSyncCrashesCoverRetentionShapes is the canary. At the
// post-sync points the unsynced buffer is already drained, so retention has no
// effect and the directive keeps the RetainAllUnsynced sentinel: the value is
// ignored there, and the sentinel documents "nothing was torn."
func generatedUnsyncedRetention(r *rand.Rand, point CrashPoint) int {
	if point != CrashBeforeSync {
		return RetainAllUnsynced
	}
	switch r.Intn(3) {
	case 0:
		return 0
	case 1:
		return 1 + r.Intn(maxGeneratedPartialRetention)
	default:
		return RetainAllUnsynced
	}
}

// normalizeFaultPairTimes turns independently drawn pair endpoints into a
// globally non-overlapping interval sequence. Sorting all 2N endpoints and
// assigning consecutive endpoints to pair i proves, by construction,
//
//	open[i] <= close[i] <= open[i+1].
//
// Equal endpoints are safe for tiny horizons: pairs are appended in this
// same order and sortFaultEvents is stable, so close[i] is processed before
// open[i+1]. The original interleaved random draws still choose pair kinds,
// targets, and endpoint distribution; this step only removes overlap.
func normalizeFaultPairTimes(pairs [][2]FaultEvent) {
	times := make([]VirtualTime, 0, len(pairs)*2)
	for i := range pairs {
		times = append(times, pairs[i][0].Time, pairs[i][1].Time)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	for i := range pairs {
		pairs[i][0].Time = times[2*i]
		pairs[i][1].Time = times[2*i+1]
	}
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
