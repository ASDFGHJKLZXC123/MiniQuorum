//go:build !buggy

package simharness

import (
	"strings"
	"testing"

	"miniquorum/sim"
)

// TestNeutralRecoveryTailEventsAreNotFaultEvidence proves the recovery-tail
// shapes (drop rate 0, duplicate rate 0, 1x clock skew) can never satisfy
// fault coverage: a run whose schedule contains only neutral events counts
// zero fault events and zero faults during in-flight operations, and the
// anti-vacuity tripwire therefore rejects the run as gate evidence.
func TestNeutralRecoveryTailEventsAreNotFaultEvidence(t *testing.T) {
	schedule := sim.FaultSchedule{
		Events: []sim.FaultEvent{
			{Time: 100, Kind: sim.FaultDropRate, Rate: 0},
			{Time: 200, Kind: sim.FaultDuplicateRate, Rate: 0},
			{Time: 300, Kind: sim.FaultClockSkew, Node: 1, Multiplier: 1},
			{Time: 300, Kind: sim.FaultClockSkew, Node: 2, Multiplier: 1},
			{Time: 300, Kind: sim.FaultClockSkew, Node: 3, Multiplier: 1},
		},
	}
	result, err := RunSeed(RunConfig{Seed: 5, Schedule: &schedule})
	if err == nil || !strings.Contains(err.Error(), "no fault fired during an in-flight operation") {
		t.Fatalf("RunSeed error = %v, want the anti-vacuity rejection of a fault-free run", err)
	}
	summary := result.Summary
	if summary.NeutralFaultEvents != len(schedule.Events) {
		t.Fatalf("neutral fault events = %d, want %d", summary.NeutralFaultEvents, len(schedule.Events))
	}
	if len(summary.FaultEventCounts) != 0 {
		t.Fatalf("neutral events were counted as fault kinds: %#v", summary.FaultEventCounts)
	}
	if summary.FaultsDuringInFlight != 0 {
		t.Fatalf("neutral events counted as faults during in-flight operations: %d", summary.FaultsDuringInFlight)
	}
	if !summary.CheckerRan || !summary.Linearizable {
		t.Fatalf("checker_ran=%t linearizable=%t, want the fault-free history checked and linearizable",
			summary.CheckerRan, summary.Linearizable)
	}
}

// TestHealingEventsCountAsKindsButNotAsInFlightFaultEvidence pins the split
// semantics: an injected partition counts both as a fired fault kind and as a
// fault during an in-flight operation, while its heal counts only as a fired
// kind — recovery overlapping a write is not fault evidence.
func TestHealingEventsCountAsKindsButNotAsInFlightFaultEvidence(t *testing.T) {
	schedule := sim.FaultSchedule{
		Events: []sim.FaultEvent{
			{Time: 700, Kind: sim.FaultPartition, From: 1, To: 2},
			{Time: 1400, Kind: sim.FaultHeal, From: 1, To: 2},
		},
	}
	result, err := RunSeed(RunConfig{Seed: 6, Schedule: &schedule})
	if err != nil {
		t.Fatalf("RunSeed: %v", err)
	}
	summary := result.Summary
	if summary.FaultEventCounts[sim.FaultPartition.String()] != 1 || summary.FaultEventCounts[sim.FaultHeal.String()] != 1 {
		t.Fatalf("fault kind counts = %#v, want one partition and one heal", summary.FaultEventCounts)
	}
	// The workload runs continuously from t=0, so both events overlap
	// in-flight operations; only the injected partition may count.
	if summary.FaultsDuringInFlight != 1 {
		t.Fatalf("faults during in-flight = %d, want exactly the injected partition", summary.FaultsDuringInFlight)
	}
}

// TestStorageCrashFiringsAreInFlightFaultEvidence proves an events-free
// schedule whose only faults are storage crash directives still yields
// crash-point coverage and in-flight fault evidence — the crash-at-point-x
// scripted shape must be able to pass the per-seed anti-vacuity floors.
func TestStorageCrashFiringsAreInFlightFaultEvidence(t *testing.T) {
	schedule := sim.FaultSchedule{
		Crashes: []sim.CrashDirective{
			{Node: 2, Save: 6, Point: sim.CrashBeforeSync, RetainUnsynced: 0, RestartAfterCrash: true},
			{Node: 3, Save: 10, Point: sim.CrashAfterSyncBeforeSend, RetainUnsynced: sim.RetainAllUnsynced, RestartAfterCrash: true},
			{Node: 1, Save: 14, Point: sim.CrashAfterSend, RetainUnsynced: sim.RetainAllUnsynced, RestartAfterCrash: true},
		},
	}
	result, err := RunSeed(RunConfig{Seed: 41, Schedule: &schedule})
	if err != nil {
		t.Fatalf("RunSeed: %v", err)
	}
	summary := result.Summary
	for _, point := range []sim.CrashPoint{sim.CrashBeforeSync, sim.CrashAfterSyncBeforeSend, sim.CrashAfterSend} {
		if summary.CrashPointCounts[point.String()] == 0 {
			t.Fatalf("crash point %s did not fire: %#v", point, summary.CrashPointCounts)
		}
	}
	if summary.FaultsDuringInFlight == 0 {
		t.Fatal("storage crashes did not count as faults during in-flight operations")
	}
	if len(summary.FaultEventCounts) != 0 {
		t.Fatalf("events-free schedule counted fault events: %#v", summary.FaultEventCounts)
	}
}

// TestGeneratedScheduleRecoveryTailIsExcludedFromCounts runs a real generated
// schedule and confirms the recovery tail (two rate resets plus one 1x
// skew per node) lands in NeutralFaultEvents, not in the fault-kind counts.
func TestGeneratedScheduleRecoveryTailIsExcludedFromCounts(t *testing.T) {
	schedule, err := sim.GenerateFaultSchedule(9, DefaultNodeIDs(), DefaultFaultHorizon)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule: %v", err)
	}
	neutral := 0
	for _, event := range schedule.Events {
		if neutralFaultEvent(event) {
			neutral++
		}
	}
	if tail := 2 + len(DefaultNodeIDs()); neutral < tail {
		t.Fatalf("generated schedule has %d neutral events, want at least the %d-event recovery tail", neutral, tail)
	}
	result := mustRunSeed(t, RunConfig{Seed: 9})
	if result.Summary.NeutralFaultEvents != neutral {
		t.Fatalf("summary neutral events = %d, want %d from the schedule", result.Summary.NeutralFaultEvents, neutral)
	}
	counted := 0
	for _, count := range result.Summary.FaultEventCounts {
		counted += count
	}
	if counted+result.Summary.NeutralFaultEvents != len(schedule.Events) {
		t.Fatalf("counted %d + neutral %d != fired schedule events %d",
			counted, result.Summary.NeutralFaultEvents, len(schedule.Events))
	}
}
