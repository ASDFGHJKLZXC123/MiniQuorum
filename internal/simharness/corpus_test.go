//go:build !buggy

package simharness

import (
	"path/filepath"
	"sync"
	"testing"

	"miniquorum/sim"
)

const corpusDir = "../../corpus"

// TestCommittedCorpusSchedulesReplayCleanly is the committed-corpus gate: the
// whole regression corpus — generated schedule files, the three scripted
// scenario schedules Phase 5/6 reuse, and the pinned negative-control
// schedule — must replay green on the ordinary build, from the files exactly
// as committed.
func TestCommittedCorpusSchedulesReplayCleanly(t *testing.T) {
	corpus, err := LoadCorpus(corpusDir)
	if err != nil {
		t.Fatalf("LoadCorpus: %v", err)
	}
	if len(corpus.Generated) < 100 {
		t.Fatalf("generated corpus has %d schedules, want >= 100", len(corpus.Generated))
	}
	if corpus.Manifest.Pinned.Schedule == "" {
		t.Fatal("manifest has no pinned negative-control schedule")
	}
	entries := append([]CorpusEntry(nil), corpus.Manifest.Scripted...)
	entries = append(entries, corpus.Manifest.Pinned)
	entries = append(entries, corpus.Generated...)

	isScripted := make(map[string]bool, len(corpus.Manifest.Scripted))
	for _, scriptedEntry := range corpus.Manifest.Scripted {
		isScripted[scriptedEntry.Name] = true
	}

	var mu sync.Mutex
	scripted := make(map[string]Result, len(corpus.Manifest.Scripted))
	var replaySlots chan struct{}
	if raceCorpusReplayLimit > 0 {
		replaySlots = make(chan struct{}, raceCorpusReplayLimit)
	}

	// Each entry is an independent single-threaded simulation, so the replays
	// run concurrently — the whole corpus is otherwise minutes of wall clock
	// under -race. Race instrumentation makes throughput collapse when all host
	// cores enter full replays at once, so race builds admit only the measured
	// bounded number while ordinary builds keep the testing package's default.
	// The enclosing t.Run returns only after every parallel child has finished,
	// which makes the coverage assertions below see every scripted summary.
	t.Run("replay", func(t *testing.T) {
		for _, entry := range entries {
			t.Run(entry.Name, func(t *testing.T) {
				t.Parallel()
				if replaySlots != nil {
					replaySlots <- struct{}{}
					defer func() { <-replaySlots }()
				}
				schedule, err := ReadCorpusSchedule(corpusDir, entry)
				if err != nil {
					t.Fatal(err)
				}
				result, err := RunSeed(RunConfig{Seed: entry.Seed, Schedule: &schedule})
				if err != nil {
					t.Fatalf("corpus entry %s (seed %d): %v", entry.Name, entry.Seed, err)
				}
				if !result.Summary.CheckerRan || !result.Summary.Linearizable {
					t.Fatalf("corpus entry %s (seed %d): checker_ran=%t linearizable=%t", entry.Name, entry.Seed,
						result.Summary.CheckerRan, result.Summary.Linearizable)
				}
				if isScripted[entry.Name] {
					mu.Lock()
					scripted[entry.Name] = result
					mu.Unlock()
				}
			})
		}
	})
	if t.Failed() {
		// A replay already failed and reported why; the coverage assertions
		// below would only add a misleading "missing entry" on top of it.
		return
	}
	assertScriptedScenarioCoverage(t, scripted)
}

// assertScriptedScenarioCoverage pins each scripted scenario to the faults it
// exists to exercise, so a scripted file that silently stopped firing its
// faults cannot stay green.
func assertScriptedScenarioCoverage(t *testing.T, scripted map[string]Result) {
	t.Helper()
	crash, ok := scripted["crash-at-point-x"]
	if !ok {
		t.Fatal("manifest is missing the crash-at-point-x scripted entry")
	}
	for _, point := range []sim.CrashPoint{sim.CrashBeforeSync, sim.CrashAfterSyncBeforeSend, sim.CrashAfterSend} {
		if crash.Summary.CrashPointCounts[point.String()] == 0 {
			t.Fatalf("crash-at-point-x did not fire crash point %s: %#v", point, crash.Summary.CrashPointCounts)
		}
	}
	if crash.Summary.FaultsDuringInFlight == 0 {
		t.Fatal("crash-at-point-x fired no crash during an in-flight operation")
	}

	partition, ok := scripted["partition-during-write"]
	if !ok {
		t.Fatal("manifest is missing the partition-during-write scripted entry")
	}
	if partition.Summary.FaultEventCounts[sim.FaultPartition.String()] == 0 {
		t.Fatalf("partition-during-write fired no asymmetric directed partition: %#v", partition.Summary.FaultEventCounts)
	}
	if partition.Summary.FaultEventCounts[sim.FaultPartitionGroups.String()] == 0 {
		t.Fatalf("partition-during-write fired no symmetric group split: %#v", partition.Summary.FaultEventCounts)
	}
	if partition.Summary.FaultsDuringInFlight == 0 {
		t.Fatal("partition-during-write fired no fault during an in-flight operation")
	}
	// "partition-during-write" must actually overlap a *write*: at least one
	// partition (directed or group) has to fire while a PUT or DELETE is in
	// flight, not merely while a GET is. An isolated write is where a partition
	// can threaten linearizability, and it is the shape the Phase 5/6
	// crash-during-write regressions reuse; a drift to reads-only overlap would
	// silently gut the scenario, so it fails loudly here. The committed seed-42
	// artifact overlaps an in-flight DELETE at both its directed and group
	// partitions, so this holds today.
	if !partitionOverlapsMutation(partition) {
		t.Fatal("partition-during-write fired no partition while a PUT or DELETE was in flight; it must overlap a mutation, not only reads")
	}

	pause, ok := scripted["pause-leader"]
	if !ok {
		t.Fatal("manifest is missing the pause-leader scripted entry")
	}
	if pause.Summary.FaultEventCounts[sim.FaultPause.String()] < 5 {
		t.Fatalf("pause-leader paused %d times, want each of the five nodes paused in turn: %#v",
			pause.Summary.FaultEventCounts[sim.FaultPause.String()], pause.Summary.FaultEventCounts)
	}
}

// partitionOverlapsMutation reports whether any directed or group partition in
// the scenario fired while a PUT or DELETE was genuinely in flight. It honors
// the same pre-enqueued fault ordering as the harness's FaultsDuringInFlight
// accounting (a partition fired within the run, catching a mutation that was
// invoked before it and had not yet returned).
func partitionOverlapsMutation(result Result) bool {
	for _, event := range result.Schedule.Events {
		if event.Kind != sim.FaultPartition && event.Kind != sim.FaultPartitionGroups {
			continue
		}
		if int64(event.Time) > result.Summary.FinalVirtualTime {
			continue
		}
		if mutatingClientInFlightForFault(result.History, int64(event.Time)) {
			return true
		}
	}
	return false
}

// TestCommittedGeneratedCorpusMatchesGeneratorOutput cross-checks every
// committed generated file against the current generator: the committed bytes
// must decode to exactly the schedule the generator produces for that seed, so
// the corpus cannot drift from the seed replays CI runs. Regeneration is pure
// schedule construction (no simulation), so checking the whole corpus rather
// than a sample costs little and leaves no unverified file.
func TestCommittedGeneratedCorpusMatchesGeneratorOutput(t *testing.T) {
	corpus, err := LoadCorpus(corpusDir)
	if err != nil {
		t.Fatalf("LoadCorpus: %v", err)
	}
	if len(corpus.Generated) == 0 {
		t.Fatal("no generated corpus entries")
	}
	for _, entry := range corpus.Generated {
		committed, err := ReadCorpusSchedule(corpusDir, entry)
		if err != nil {
			t.Fatal(err)
		}
		regenerated, err := sim.GenerateFaultSchedule(entry.Seed, DefaultNodeIDs(), DefaultFaultHorizon)
		if err != nil {
			t.Fatalf("GenerateFaultSchedule(%d): %v", entry.Seed, err)
		}
		committedBytes, err := sim.EncodeFaultSchedule(committed)
		if err != nil {
			t.Fatal(err)
		}
		regeneratedBytes, err := sim.EncodeFaultSchedule(regenerated)
		if err != nil {
			t.Fatal(err)
		}
		if string(committedBytes) != string(regeneratedBytes) {
			t.Fatalf("corpus entry %s (%s) drifted from generator v%d output for seed %d",
				entry.Name, filepath.FromSlash(entry.Schedule), sim.FaultScheduleGeneratorVersion, entry.Seed)
		}
	}
}
