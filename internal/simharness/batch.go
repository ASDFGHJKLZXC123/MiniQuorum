package simharness

import (
	"fmt"
	"runtime"
	"sort"
	"sync"

	raftpb "miniquorum/proto"
	"miniquorum/sim"
)

// BatchConfig defines a contiguous seed range. Workers are bounded and each
// writes into a preassigned result slot, so result collection order is always
// seed order regardless of host scheduling.
type BatchConfig struct {
	StartSeed int64
	Count     int
	Workers   int
	Run       RunConfig
}

// Aggregate is the deterministic result of a batch; wall-clock timing belongs
// only in the command's human-readable output.
type Aggregate struct {
	ArtifactVersion     int   `json:"artifact_version"`
	StartSeed           int64 `json:"start_seed"`
	Count               int   `json:"count"`
	Workers             int   `json:"workers"`
	Passed              int   `json:"passed"`
	Failed              int   `json:"failed"`
	LogicalOperations   int   `json:"logical_operations"`
	CompletedOperations int   `json:"completed_operations"`
	OpenOperations      int   `json:"open_operations"`
	Attempts            int   `json:"attempts"`
	// MinLogicalOperations and MinCompletedOperations are the weakest single
	// seed in the batch; MaxOpenOperations and SeedsWithOpenOperations size
	// the genuinely-indeterminate tail. All are 0 for an empty batch. They
	// are the evidence backing the per-seed anti-vacuity floors in run.go.
	MinLogicalOperations    int            `json:"min_logical_operations"`
	MinCompletedOperations  int            `json:"min_completed_operations"`
	MaxOpenOperations       int            `json:"max_open_operations"`
	SeedsWithOpenOperations int            `json:"seeds_with_open_operations"`
	OperationCounts         map[string]int `json:"operation_counts"`
	FaultEventCounts        map[string]int `json:"fault_event_counts"`
	CrashPointCounts        map[string]int `json:"crash_point_counts"`
	NetworkDrops            int            `json:"network_drops"`
	NetworkDuplicates       int            `json:"network_duplicates"`
	FaultsDuringInFlight    int            `json:"faults_during_in_flight"`
	NeutralFaultEvents      int            `json:"neutral_fault_events"`
	FirstFailureSeed        *int64         `json:"first_failure_seed,omitempty"`
	FirstFailure            string         `json:"first_failure,omitempty"`
	// FirstViolationSeed is set only when Porcupine actually ran and rejected
	// the history. A run aborted earlier (for example on a simulator
	// invariant) is a failure but never a linearizability violation.
	FirstViolationSeed *int64 `json:"first_violation_seed,omitempty"`
}

type indexedResult struct {
	summary Summary
	err     error
}

// RunBatch runs independent seeds concurrently outside the sim path.
func RunBatch(config BatchConfig) Aggregate {
	if config.Count < 0 {
		config.Count = 0
	}
	if config.Workers <= 0 {
		config.Workers = runtime.GOMAXPROCS(0)
	}
	if config.Workers < 1 {
		config.Workers = 1
	}
	if config.Workers > config.Count && config.Count > 0 {
		config.Workers = config.Count
	}
	results := make([]indexedResult, config.Count)
	jobs := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < config.Workers; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				runConfig := config.Run
				runConfig.Seed = config.StartSeed + int64(index)
				result, err := RunSeed(runConfig)
				results[index] = indexedResult{summary: result.Summary, err: err}
			}
		}()
	}
	for index := range results {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	aggregate := Aggregate{
		ArtifactVersion:  ArtifactVersion,
		StartSeed:        config.StartSeed,
		Count:            config.Count,
		Workers:          config.Workers,
		OperationCounts:  make(map[string]int),
		FaultEventCounts: make(map[string]int),
		CrashPointCounts: make(map[string]int),
	}
	for index, result := range results {
		if result.summary.CheckerRan && !result.summary.Linearizable && aggregate.FirstViolationSeed == nil {
			seed := config.StartSeed + int64(index)
			aggregate.FirstViolationSeed = &seed
		}
		if result.err != nil {
			aggregate.Failed++
			if aggregate.FirstFailureSeed == nil {
				seed := config.StartSeed + int64(index)
				aggregate.FirstFailureSeed = &seed
				aggregate.FirstFailure = result.err.Error()
			}
		} else {
			aggregate.Passed++
		}
		if index == 0 || result.summary.LogicalOperations < aggregate.MinLogicalOperations {
			aggregate.MinLogicalOperations = result.summary.LogicalOperations
		}
		if index == 0 || result.summary.CompletedOperations < aggregate.MinCompletedOperations {
			aggregate.MinCompletedOperations = result.summary.CompletedOperations
		}
		if result.summary.OpenOperations > aggregate.MaxOpenOperations {
			aggregate.MaxOpenOperations = result.summary.OpenOperations
		}
		if result.summary.OpenOperations > 0 {
			aggregate.SeedsWithOpenOperations++
		}
		aggregate.LogicalOperations += result.summary.LogicalOperations
		aggregate.CompletedOperations += result.summary.CompletedOperations
		aggregate.OpenOperations += result.summary.OpenOperations
		aggregate.Attempts += result.summary.Attempts
		aggregate.NetworkDrops += result.summary.NetworkDrops
		aggregate.NetworkDuplicates += result.summary.NetworkDuplicates
		aggregate.FaultsDuringInFlight += result.summary.FaultsDuringInFlight
		aggregate.NeutralFaultEvents += result.summary.NeutralFaultEvents
		addCounts(aggregate.OperationCounts, result.summary.OperationCounts)
		addCounts(aggregate.FaultEventCounts, result.summary.FaultEventCounts)
		addCounts(aggregate.CrashPointCounts, result.summary.CrashPointCounts)
	}
	if aggregate.Failed == 0 {
		if err := aggregateCoverageError(aggregate); err != nil {
			aggregate.Failed = 1
			aggregate.Passed--
			aggregate.FirstFailure = err.Error()
		}
	}
	return aggregate
}

func addCounts(target, source map[string]int) {
	for key, count := range source {
		target[key] += count
	}
}

func aggregateCoverageError(aggregate Aggregate) error {
	var missing []string
	for _, op := range []raftpb.Op{raftpb.Op_PUT, raftpb.Op_GET, raftpb.Op_DELETE} {
		if aggregate.OperationCounts[op.String()] == 0 {
			missing = append(missing, "operation "+op.String())
		}
	}
	for kind := sim.FaultDropRate; kind <= sim.FaultRestart; kind++ {
		if aggregate.FaultEventCounts[kind.String()] == 0 {
			missing = append(missing, "fault "+kind.String())
		}
	}
	for _, point := range []sim.CrashPoint{sim.CrashBeforeSync, sim.CrashAfterSyncBeforeSend, sim.CrashAfterSend} {
		if aggregate.CrashPointCounts[point.String()] == 0 {
			missing = append(missing, "crash point "+point.String())
		}
	}
	if aggregate.NetworkDrops == 0 {
		missing = append(missing, "actual network drop")
	}
	if aggregate.NetworkDuplicates == 0 {
		missing = append(missing, "actual network duplicate")
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("batch coverage missing: %v", missing)
}
