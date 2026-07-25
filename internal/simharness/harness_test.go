package simharness

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"miniquorum/checker"
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
	"miniquorum/sim"
	workloadpkg "miniquorum/sim/workload"
)

// TestRunSeedSameSeedByteIdenticalArtifacts is the phase-4 determinism gate at
// artifact granularity: two independent RunSeed executions of the same seed
// must serialize byte-for-byte identically — schedule, history, summary,
// trace, violation report, and the rendered Porcupine visualization.
func TestRunSeedSameSeedByteIdenticalArtifacts(t *testing.T) {
	first := mustRunSeed(t, RunConfig{Seed: 7})
	second := mustRunSeed(t, RunConfig{Seed: 7})
	assertArtifactsByteIdentical(t, first, second)
}

// TestRunSeedExplicitScheduleReplaysByteIdentically proves the -schedule
// replay path: running the serialized schedule file contents for a seed is
// byte-for-byte the run that generated the schedule from that seed.
func TestRunSeedExplicitScheduleReplaysByteIdentically(t *testing.T) {
	generated := mustRunSeed(t, RunConfig{Seed: 7})
	schedule, err := sim.GenerateFaultSchedule(7, DefaultNodeIDs(), DefaultFaultHorizon)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule: %v", err)
	}
	explicit := mustRunSeed(t, RunConfig{Seed: 7, Schedule: &schedule})
	assertArtifactsByteIdentical(t, generated, explicit)
}

// TestRunSeedAntiVacuityEvidenceAcrossSampleSeeds is the interleaving-sanity
// canary, asserted programmatically on a sample of ordinary seeds: histories
// must overlap across at least two clients, faults must fire during in-flight
// operations, all three operation kinds must appear, and the checker must
// actually have run and passed.
func TestRunSeedAntiVacuityEvidenceAcrossSampleSeeds(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		result := mustRunSeed(t, RunConfig{Seed: seed})
		summary := result.Summary
		if !summary.CheckerRan || !summary.Linearizable {
			t.Fatalf("seed %d: checker_ran=%t linearizable=%t, want a checked linearizable history", seed, summary.CheckerRan, summary.Linearizable)
		}
		if summary.MaxConcurrentClients < 2 {
			t.Fatalf("seed %d: max concurrent clients = %d, want >= 2", seed, summary.MaxConcurrentClients)
		}
		if summary.FaultsDuringInFlight == 0 {
			t.Fatalf("seed %d: no fault fired during an in-flight operation", seed)
		}
		for _, op := range []raftpb.Op{raftpb.Op_PUT, raftpb.Op_GET, raftpb.Op_DELETE} {
			if summary.OperationCounts[op.String()] == 0 {
				t.Fatalf("seed %d: no %s operation in history", seed, op)
			}
		}
		if summary.Attempts < summary.LogicalOperations {
			t.Fatalf("seed %d: attempts %d < logical operations %d", seed, summary.Attempts, summary.LogicalOperations)
		}
		if summary.LogicalOperations < MinimumLogicalOperations || summary.CompletedOperations < MinimumCompleted {
			t.Fatalf("seed %d: logical=%d completed=%d below floors %d/%d", seed,
				summary.LogicalOperations, summary.CompletedOperations, MinimumLogicalOperations, MinimumCompleted)
		}
	}
}

// TestRunSeedHeavyFaultTimeoutsRemainOpenWhilePorcupinePasses induces genuine
// client timeouts with a never-healed 90% drop rate. The run must end with
// open (indeterminate) operations, the checker must still pass — a timeout is
// never recorded as a failed operation — and the anti-vacuity tripwire must
// trip, proving the harness rejects exactly this kind of stalled workload as
// gate evidence.
func TestRunSeedHeavyFaultTimeoutsRemainOpenWhilePorcupinePasses(t *testing.T) {
	schedule := sim.FaultSchedule{
		Events: []sim.FaultEvent{{Time: 0, Kind: sim.FaultDropRate, Rate: 0.9}},
	}
	result, err := RunSeed(RunConfig{Seed: 3, Schedule: &schedule})
	if err == nil || !strings.Contains(err.Error(), "weak workload") {
		t.Fatalf("RunSeed error = %v, want the anti-vacuity tripwire", err)
	}
	summary := result.Summary
	if summary.AntiVacuityFailure == "" {
		t.Fatal("anti-vacuity failure detail is empty")
	}
	if !summary.CheckerRan || !summary.Linearizable {
		t.Fatalf("checker_ran=%t linearizable=%t, want Porcupine to run and pass on the open history", summary.CheckerRan, summary.Linearizable)
	}
	if summary.OpenOperations == 0 {
		t.Fatal("heavy-fault run recorded no open operations, want genuine timeouts left open")
	}
	if summary.WorkloadReportedDone {
		t.Fatal("workload reported done despite open operations")
	}
	open := 0
	for _, operation := range result.History {
		if operation.ReturnTime == nil {
			open++
		}
	}
	if open != summary.OpenOperations {
		t.Fatalf("history open records = %d, summary open = %d", open, summary.OpenOperations)
	}
}

// TestHarnessSchedulesSameSequenceRetryAfterDeadlineWithOpenObservationPoint
// drives the harness retry protocol deterministically. A full three-way group
// partition guarantees every first operation times out. At the observation
// point the timed-out operations are open in the history and Porcupine
// passes; afterwards the harness schedules explicit same-sequence retries,
// and the retried operations complete as the SAME logical records with their
// original invocation times and identical command bytes across attempts.
func TestHarnessSchedulesSameSequenceRetryAfterDeadlineWithOpenObservationPoint(t *testing.T) {
	groups := [][]raft.NodeID{{1}, {2}, {3}, {4}, {5}}
	schedule := sim.FaultSchedule{
		Events: []sim.FaultEvent{
			{Time: 0, Kind: sim.FaultPartitionGroups, Groups: groups},
			{Time: 2000, Kind: sim.FaultHealGroups, Groups: groups},
		},
	}
	s, err := sim.NewFaultSim(sim.Config{Seed: 11, NodeIDs: DefaultNodeIDs()}, schedule)
	if err != nil {
		t.Fatalf("NewFaultSim: %v", err)
	}
	s.RegisterInvariant(sim.SingleLeaderPerTerm)
	if err := s.StartWorkload(workloadpkg.Config{
		OperationsPerClient: DefaultOperationsPerClient,
		OperationTimeout:    DefaultOperationTimeout,
		ThinkTime:           DefaultThinkTime,
	}); err != nil {
		t.Fatalf("StartWorkload: %v", err)
	}

	// Observation point: past every client's first virtual deadline, before
	// the partition heals and before any harness retry is scheduled.
	if err := s.Run(1500); err != nil {
		t.Fatalf("Run(observation point): %v", err)
	}
	observed := s.WorkloadHistory()
	if len(observed) != workloadpkg.DefaultClientCount {
		t.Fatalf("observed history length = %d, want %d first operations", len(observed), workloadpkg.DefaultClientCount)
	}
	invokeTimes := make(map[uint64]int64)
	for _, operation := range observed {
		if operation.ReturnTime != nil {
			t.Fatalf("operation completed under a full three-way partition: %#v", operation)
		}
		if operation.Seq != 1 {
			t.Fatalf("operation seq = %d, want 1", operation.Seq)
		}
		invokeTimes[operation.ClientID] = operation.InvokeTime
	}
	for _, client := range s.WorkloadClients() {
		if !client.TimedOut || client.OutstandingSeq != 1 {
			t.Fatalf("client at observation point = %#v, want timed-out outstanding seq 1", client)
		}
	}
	if !checker.Check(observed) {
		t.Fatal("Porcupine rejected the open-timeout observation history")
	}

	// Harness protocol: after each deadline scan, schedule an explicit
	// same-sequence retry for every timed-out session, exactly as RunSeed does.
	for through := sim.VirtualTime(1600); through <= DefaultDuration; through += DefaultRetryScanInterval {
		if err := s.Run(through); err != nil {
			t.Fatalf("Run(%d): %v", through, err)
		}
		if through == DefaultDuration {
			break
		}
		for _, client := range s.WorkloadClients() {
			if !client.TimedOut {
				continue
			}
			if err := s.ScheduleWorkloadRetry(client.ClientID, s.Now()+1); err != nil {
				t.Fatalf("ScheduleWorkloadRetry(%d): %v", client.ClientID, err)
			}
		}
	}
	if !s.WorkloadDone() {
		t.Fatalf("workload did not complete after heal and retries: %#v", s.WorkloadClients())
	}
	final := s.WorkloadHistory()
	if len(final) != workloadpkg.DefaultClientCount*DefaultOperationsPerClient {
		t.Fatalf("final history length = %d, want %d", len(final), workloadpkg.DefaultClientCount*DefaultOperationsPerClient)
	}
	if !checker.Check(final) {
		t.Fatal("Porcupine rejected the completed history")
	}
	for _, operation := range final {
		if operation.Seq != 1 {
			continue
		}
		if operation.ReturnTime == nil {
			t.Fatalf("timed-out seq-1 operation never completed: %#v", operation)
		}
		if operation.InvokeTime != invokeTimes[operation.ClientID] {
			t.Fatalf("client %d seq 1 invoke time changed from %d to %d; a retry must not re-invoke",
				operation.ClientID, invokeTimes[operation.ClientID], operation.InvokeTime)
		}
	}
	assertSameSequenceRetryIdentity(t, s.WorkloadAttempts())
}

// TestRunBatchIsDeterministicAndAggregatesEvidence runs a small bounded
// parallel batch twice: identical aggregates in seed order regardless of host
// scheduling, no violations, and populated coverage evidence.
func TestRunBatchIsDeterministicAndAggregatesEvidence(t *testing.T) {
	run := func() Aggregate {
		return RunBatch(BatchConfig{StartSeed: 1, Count: 6, Workers: 3})
	}
	first := run()
	second := run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same batch diverged:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.Failed != 0 || first.Passed != 6 || first.FirstViolationSeed != nil {
		t.Fatalf("batch = %#v, want 6 clean passes", first)
	}
	if first.MinLogicalOperations < MinimumLogicalOperations || first.MinCompletedOperations < MinimumCompleted {
		t.Fatalf("batch minima %d/%d below floors %d/%d",
			first.MinLogicalOperations, first.MinCompletedOperations, MinimumLogicalOperations, MinimumCompleted)
	}
	for _, op := range []raftpb.Op{raftpb.Op_PUT, raftpb.Op_GET, raftpb.Op_DELETE} {
		if first.OperationCounts[op.String()] == 0 {
			t.Fatalf("batch has no %s operations: %#v", op, first.OperationCounts)
		}
	}
	if len(first.FaultEventCounts) == 0 {
		t.Fatal("batch observed no fault events")
	}
}

func mustRunSeed(t *testing.T, config RunConfig) Result {
	t.Helper()
	result, err := RunSeed(config)
	if err != nil {
		t.Fatalf("RunSeed(%d): %v", config.Seed, err)
	}
	return result
}

func assertArtifactsByteIdentical(t *testing.T, first, second Result) {
	t.Helper()
	firstArtifacts, err := ArtifactBytes(first)
	if err != nil {
		t.Fatalf("ArtifactBytes(first): %v", err)
	}
	secondArtifacts, err := ArtifactBytes(second)
	if err != nil {
		t.Fatalf("ArtifactBytes(second): %v", err)
	}
	if len(firstArtifacts) != len(secondArtifacts) {
		t.Fatalf("artifact sets differ: %d vs %d files", len(firstArtifacts), len(secondArtifacts))
	}
	for name, content := range firstArtifacts {
		if !bytes.Equal(content, secondArtifacts[name]) {
			t.Fatalf("artifact %s differs between identical runs (%d vs %d bytes)", name, len(content), len(secondArtifacts[name]))
		}
	}
	if len(firstArtifacts[HistoryFile]) == 0 {
		t.Fatal("history artifact is empty")
	}
	assertVisualizationRenders(t, firstArtifacts[VisualizationFile])
}

// assertVisualizationRenders proves the Porcupine artifact is a structurally
// complete, self-contained HTML document whose embedded payload actually
// carries the checked history — not the truncated or operation-free page a
// bare non-empty check would accept. Driving a real browser is out of scope;
// what is proven here is that Porcupine produced a whole document and that the
// history reached its render call.
func assertVisualizationRenders(t *testing.T, visualization []byte) {
	t.Helper()
	page := string(visualization)
	if !strings.HasPrefix(page, "<!doctype html>") {
		t.Fatalf("visualization does not begin with an HTML doctype: %.40q", page)
	}
	if !strings.HasSuffix(strings.TrimRight(page, "\n"), "</html>") {
		t.Fatal("visualization is truncated: no closing </html>")
	}
	if !strings.Contains(page, `<div id="canvas">`) || !strings.Contains(page, "render(data)") {
		t.Fatal("visualization is missing Porcupine's canvas element or render call")
	}
	_, payload, found := strings.Cut(page, "const data = ")
	if !found {
		t.Fatal("visualization embeds no data payload")
	}
	payload, _, found = strings.Cut(payload, "\n")
	if !found {
		t.Fatal("visualization data payload is unterminated")
	}
	var data struct {
		Partitions []struct {
			History []json.RawMessage
		}
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(payload)), &data); err != nil {
		t.Fatalf("visualization data payload is not valid JSON: %v", err)
	}
	rendered := 0
	for _, partition := range data.Partitions {
		rendered += len(partition.History)
	}
	if rendered == 0 {
		t.Fatal("visualization renders no operations: the checked history never reached it")
	}
}

func assertSameSequenceRetryIdentity(t *testing.T, attempts []workloadpkg.AttemptRecord) {
	t.Helper()
	type logicalKey struct {
		clientID uint64
		seq      uint64
	}
	first := make(map[logicalKey]workloadpkg.AttemptRecord)
	retried := 0
	for _, attempt := range attempts {
		key := logicalKey{attempt.AttemptID.ClientID, attempt.AttemptID.Seq}
		prior, ok := first[key]
		if !ok {
			first[key] = attempt
			continue
		}
		retried++
		if attempt.AttemptID.Attempt <= prior.AttemptID.Attempt {
			t.Fatalf("retry of %+v did not advance the attempt ordinal: %+v after %+v", key, attempt.AttemptID, prior.AttemptID)
		}
		if prior.Command.GetClientId() != attempt.Command.GetClientId() ||
			prior.Command.GetSeq() != attempt.Command.GetSeq() ||
			prior.Command.GetOp() != attempt.Command.GetOp() ||
			!bytes.Equal(prior.Command.GetKey(), attempt.Command.GetKey()) ||
			!bytes.Equal(prior.Command.GetValue(), attempt.Command.GetValue()) {
			t.Fatalf("retry of %+v changed the command: first=%+v retry=%+v", key, prior.Command, attempt.Command)
		}
	}
	if retried == 0 {
		t.Fatal("retry identity assertion was vacuous: no logical operation was retried")
	}
}
