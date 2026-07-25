//go:build !buggy

package simharness

import (
	"bytes"
	"testing"
)

// TestPinnedNegativeControlSchedulePassesOrdinaryBuild is the positive half
// of the section 5.4.2 negative control: the exact committed (seed, schedule)
// pair that fails Porcupine under -tags buggy must be a clean, checked,
// linearizable run on the ordinary build. Without this half, a "negative
// control" could be satisfied by a schedule that simply breaks the simulator.
func TestPinnedNegativeControlSchedulePassesOrdinaryBuild(t *testing.T) {
	corpus, err := LoadCorpus(corpusDir)
	if err != nil {
		t.Fatalf("LoadCorpus: %v", err)
	}
	pinned := corpus.Manifest.Pinned
	if pinned.Schedule == "" {
		t.Fatal("manifest has no pinned negative-control schedule")
	}
	if pinned.Seed == 0 {
		t.Fatal("manifest pinned entry has no seed; (seed, schedule) identifies a replay, a schedule alone does not")
	}
	schedule, err := ReadCorpusSchedule(corpusDir, pinned)
	if err != nil {
		t.Fatal(err)
	}
	result, err := RunSeed(RunConfig{Seed: pinned.Seed, Schedule: &schedule})
	if err != nil {
		t.Fatalf("ordinary build failed the pinned schedule (seed %d): %v", pinned.Seed, err)
	}
	if !result.Summary.CheckerRan || !result.Summary.Linearizable {
		t.Fatalf("ordinary build on pinned schedule: checker_ran=%t linearizable=%t, want a checked pass",
			result.Summary.CheckerRan, result.Summary.Linearizable)
	}
	// RunSeed already enforces the anti-vacuity floors, but state them here
	// too: the whole point of the pin is that the SAME workload which the
	// buggy build breaks is a substantial, fault-exercised, green run here.
	if result.Summary.AntiVacuityFailure != "" {
		t.Fatalf("ordinary build on pinned schedule is vacuous: %s", result.Summary.AntiVacuityFailure)
	}
	if result.Summary.CrashPointCounts["after-sync-before-send"] == 0 {
		t.Fatal("pinned schedule fired no after-sync-before-send crash on the ordinary build")
	}
	if result.Summary.FaultsDuringInFlight == 0 {
		t.Fatal("pinned schedule fired no fault during an in-flight operation on the ordinary build")
	}

	// Determinism: the ordinary replay of the pin is byte-identical too, so
	// the pinned pair is a stable regression artifact for Phase 5 onward.
	again, err := RunSeed(RunConfig{Seed: pinned.Seed, Schedule: &schedule})
	if err != nil {
		t.Fatalf("second ordinary replay of the pinned schedule failed: %v", err)
	}
	first, err := ArtifactBytes(result)
	if err != nil {
		t.Fatalf("ArtifactBytes: %v", err)
	}
	second, err := ArtifactBytes(again)
	if err != nil {
		t.Fatalf("ArtifactBytes: %v", err)
	}
	for name, content := range first {
		if !bytes.Equal(content, second[name]) {
			t.Fatalf("ordinary pinned artifact %s differs across identical replays", name)
		}
	}
}
