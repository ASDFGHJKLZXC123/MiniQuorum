//go:build !buggy

package simharness

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
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
		committedPath := filepath.Join(corpusDir, filepath.FromSlash(entry.Schedule))
		committedBytes, err := os.ReadFile(committedPath)
		if err != nil {
			t.Fatal(err)
		}
		committed, err := ReadCorpusSchedule(corpusDir, entry)
		if err != nil {
			t.Fatal(err)
		}
		regenerated, err := sim.GenerateFaultSchedule(entry.Seed, DefaultNodeIDs(), DefaultFaultHorizon)
		if err != nil {
			t.Fatalf("GenerateFaultSchedule(%d): %v", entry.Seed, err)
		}
		regeneratedBytes, err := sim.EncodeFaultSchedule(regenerated)
		if err != nil {
			t.Fatal(err)
		}
		// schedgen terminates every artifact with one newline. Compare the raw
		// committed file so whitespace or final-newline drift is also visible.
		regeneratedBytes = append(regeneratedBytes, '\n')
		if !bytes.Equal(committedBytes, regeneratedBytes) {
			firstByte := firstDifferentByte(committedBytes, regeneratedBytes)
			t.Fatalf("corpus entry %s (%s) drifted from generator v%d output for seed %d:\n"+
				"committed_sha256=%x regenerated_sha256=%x\n"+
				"first_diff_byte=%d committed_byte=%s regenerated_byte=%s\n"+
				"first_diff_field=%s\ncommitted_context=%s\nregenerated_context=%s",
				entry.Name, filepath.FromSlash(entry.Schedule), sim.FaultScheduleGeneratorVersion, entry.Seed,
				sha256.Sum256(committedBytes), sha256.Sum256(regeneratedBytes),
				firstByte, byteAt(committedBytes, firstByte), byteAt(regeneratedBytes, firstByte),
				firstScheduleDifference(committed, regenerated),
				byteContext(committedBytes, firstByte), byteContext(regeneratedBytes, firstByte))
		}
	}
}

func firstDifferentByte(committed, regenerated []byte) int {
	limit := min(len(committed), len(regenerated))
	for i := 0; i < limit; i++ {
		if committed[i] != regenerated[i] {
			return i
		}
	}
	return limit
}

func byteAt(data []byte, index int) string {
	if index >= len(data) {
		return "<EOF>"
	}
	return fmt.Sprintf("%q (0x%02x)", data[index], data[index])
}

func byteContext(data []byte, index int) string {
	const radius = 24
	start := max(0, index-radius)
	end := min(len(data), index+radius)
	return fmt.Sprintf("%q", data[start:end])
}

func firstScheduleDifference(committed, regenerated sim.FaultSchedule) string {
	if difference := firstValueDifference("schedule", reflect.ValueOf(committed), reflect.ValueOf(regenerated)); difference != "" {
		return difference
	}
	return "<decoded schedules equal; serialized bytes differ>"
}

func firstValueDifference(path string, committed, regenerated reflect.Value) string {
	if reflect.DeepEqual(committed.Interface(), regenerated.Interface()) {
		return ""
	}
	if committed.Type() != regenerated.Type() {
		return fmt.Sprintf("%s type: committed=%s regenerated=%s", path, committed.Type(), regenerated.Type())
	}

	switch committed.Kind() {
	case reflect.Struct:
		for i := 0; i < committed.NumField(); i++ {
			fieldPath := path + "." + committed.Type().Field(i).Name
			if difference := firstValueDifference(fieldPath, committed.Field(i), regenerated.Field(i)); difference != "" {
				return difference
			}
		}
	case reflect.Slice:
		if committed.IsNil() != regenerated.IsNil() {
			return fmt.Sprintf("%s nil: committed=%t regenerated=%t", path, committed.IsNil(), regenerated.IsNil())
		}
		if committed.Len() != regenerated.Len() {
			return fmt.Sprintf("%s length: committed=%d regenerated=%d", path, committed.Len(), regenerated.Len())
		}
		for i := 0; i < committed.Len(); i++ {
			elementPath := fmt.Sprintf("%s[%d]", path, i)
			if difference := firstValueDifference(elementPath, committed.Index(i), regenerated.Index(i)); difference != "" {
				return difference
			}
		}
	case reflect.Float64:
		committedFloat := committed.Float()
		regeneratedFloat := regenerated.Float()
		return fmt.Sprintf("%s: committed=%g (bits=%#016x) regenerated=%g (bits=%#016x)",
			path, committedFloat, math.Float64bits(committedFloat), regeneratedFloat, math.Float64bits(regeneratedFloat))
	default:
		return fmt.Sprintf("%s: committed=%v regenerated=%v", path, committed.Interface(), regenerated.Interface())
	}
	return fmt.Sprintf("%s differs", path)
}
