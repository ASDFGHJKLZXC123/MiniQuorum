//go:build buggy

package simharness

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPinnedNegativeControlScheduleFailsPorcupineUnderBuggyTag is the phase-4
// negative control and its replay proof. Under -tags buggy, internal/raft
// commits by replica count alone (the section 5.4.2 bug); replaying the
// pinned committed schedule must then produce a genuine Porcupine
// linearizability violation — the checker itself must catch the bug, not a
// simulator invariant and not a run error. The replay is executed twice and
// every failure artifact (schedule, history, summary, trace, violation
// report, and the rendered Porcupine visualization) must be byte-for-byte
// identical across the two runs.
//
// This file only compiles under the buggy tag; run it as
//
//	go test -tags buggy ./internal/simharness -run '^TestPinnedNegativeControlScheduleFailsPorcupineUnderBuggyTag$' -count=1
//
// (the rest of the test suite intentionally does not pass under the tag —
// a harness that cannot fail is not evidence).
func TestPinnedNegativeControlScheduleFailsPorcupineUnderBuggyTag(t *testing.T) {
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

	replay := func() map[string][]byte {
		result, runErr := RunSeed(RunConfig{Seed: pinned.Seed, Schedule: &schedule})
		if runErr == nil {
			t.Fatalf("buggy build passed the pinned schedule (seed %d); the negative control is broken", pinned.Seed)
		}
		// The failure must be the checker's verdict, not an abort on the way to
		// it. Each of these would leave a "failing" negative control that proves
		// nothing about Porcupine: a construction/setup error, a simulator
		// safety invariant firing first, or the anti-vacuity guard rejecting a
		// workload that never really ran.
		if !result.Summary.CheckerRan {
			t.Fatalf("buggy replay aborted before Porcupine ran (%v); the pinned schedule must fail the checker itself", runErr)
		}
		if result.Summary.Linearizable {
			t.Fatalf("buggy replay was linearizable but errored (%v); want a checker violation", runErr)
		}
		if !strings.Contains(runErr.Error(), "Porcupine found a linearizability violation") {
			t.Fatalf("buggy replay failed for the wrong reason: %v", runErr)
		}
		if result.Summary.AntiVacuityFailure != "" {
			t.Fatalf("buggy replay tripped the anti-vacuity guard (%s); the violation must come from a real workload",
				result.Summary.AntiVacuityFailure)
		}
		// The violation must be carried by a workload substantial enough to
		// mean something: the same per-seed floors every ordinary gate applies.
		if result.Summary.LogicalOperations < MinimumLogicalOperations {
			t.Fatalf("buggy replay recorded %d logical operations, want at least %d",
				result.Summary.LogicalOperations, MinimumLogicalOperations)
		}
		if result.Summary.CompletedOperations < MinimumCompleted {
			t.Fatalf("buggy replay completed %d operations, want at least %d",
				result.Summary.CompletedOperations, MinimumCompleted)
		}
		// The bug is only reachable when the schedule actually drives the
		// Figure 8 shape: a node must be crashed after its Save is durable but
		// before it sends (that is what leaves one node holding the competing
		// higher-term entry alone), and faults must land on in-flight work.
		if result.Summary.CrashPointCounts["after-sync-before-send"] == 0 {
			t.Fatal("pinned schedule fired no after-sync-before-send crash; the competing log cannot be created without one")
		}
		if result.Summary.FaultsDuringInFlight == 0 {
			t.Fatal("pinned schedule fired no fault during an in-flight operation")
		}
		artifacts, err := ArtifactBytes(result)
		if err != nil {
			t.Fatalf("ArtifactBytes on the violating result: %v", err)
		}
		// The schedule the replay reports must be exactly the committed file,
		// so the pinned artifact and the failure dump can never drift apart.
		committed, err := os.ReadFile(filepath.Join(corpusDir, filepath.FromSlash(pinned.Schedule)))
		if err != nil {
			t.Fatalf("read committed pinned schedule: %v", err)
		}
		if !bytes.Equal(committed, artifacts[ScheduleFile]) {
			t.Fatalf("replayed schedule artifact differs from the committed pinned file (%d vs %d bytes)",
				len(artifacts[ScheduleFile]), len(committed))
		}
		return artifacts
	}

	first := replay()
	second := replay()
	if len(first) != len(second) {
		t.Fatalf("replay artifact sets differ: %d vs %d files", len(first), len(second))
	}
	for name, content := range first {
		if !bytes.Equal(content, second[name]) {
			t.Fatalf("failure artifact %s differs across identical buggy replays (%d vs %d bytes)",
				name, len(content), len(second[name]))
		}
	}
	assertVisualizationRenders(t, first[VisualizationFile])
	if len(first[HistoryFile]) == 0 || len(first[ScheduleFile]) == 0 || len(first[SummaryFile]) == 0 {
		t.Fatal("violation history, schedule, or summary artifact is empty")
	}
	if len(first[TraceFile]) == 0 || len(first[ViolationFile]) == 0 {
		t.Fatal("violation trace or report artifact is empty")
	}
	if !bytes.Contains(first[ViolationFile], []byte("result=Illegal")) {
		t.Fatalf("violation report does not record an illegal history:\n%s", first[ViolationFile])
	}
}

const corpusDir = "../../corpus"
