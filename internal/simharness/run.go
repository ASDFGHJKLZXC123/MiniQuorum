// Package simharness runs deterministic Phase 4 fault schedules around the
// single-threaded simulator. Concurrency and process I/O live in this package,
// outside sim's determinism boundary.
package simharness

import (
	"fmt"
	"strconv"
	"strings"

	"miniquorum/checker"
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
	"miniquorum/sim"
	workloadpkg "miniquorum/sim/workload"
)

const (
	ArtifactVersion = 1

	DefaultDuration          = sim.VirtualTime(12_000)
	DefaultFaultHorizon      = sim.VirtualTime(6_000)
	DefaultRetryScanInterval = sim.VirtualTime(100)
	DefaultOperationTimeout  = int64(600)
	DefaultThinkTime         = int64(50)
	// DefaultOperationsPerClient x the workload's five default clients is the
	// packet's pinned 200-logical-operation target per ordinary seed.
	DefaultOperationsPerClient = 40
	// MinimumLogicalOperations and MinimumCompleted are per-seed anti-vacuity
	// floors over seeds 1..10000, the closed seed set every PR/nightly/local
	// gate draws from. They sit below the minima measured across that whole
	// set (Aggregate.MinLogicalOperations / MinCompletedOperations) so a
	// workload that stalls into timeouts or retries fails loudly without
	// flaking on observed straggler variance.
	MinimumLogicalOperations = 150
	MinimumCompleted         = 120
)

// defaultNodeIDs pins the harness cluster at five nodes. Five — not the
// three-node deployment shape — because the section 5.4.2 negative control is
// unobservable at three nodes: any entry on a majority (2 of 3) is immortal
// there, since the single node without it can never win an election against
// two log-superior peers, so commit-by-count never commits anything that can
// later be overwritten. Figure 8 needs at least four servers; the Phase 2
// commit-rule gate uses five, and the harness patrols the same shape.
var defaultNodeIDs = []raft.NodeID{1, 2, 3, 4, 5}

// DefaultNodeIDs is the pinned harness cluster. Corpus generation must use
// exactly this set or a committed schedule would stop matching the schedule
// RunSeed regenerates from the same seed.
func DefaultNodeIDs() []raft.NodeID {
	return append([]raft.NodeID(nil), defaultNodeIDs...)
}

// RunConfig identifies one replay. A nil Schedule uses the current versioned
// generator. An explicit schedule is replayed as-is and is never regenerated.
type RunConfig struct {
	Seed int64
	// Engine is map (the default) or lsm. It selects the simulator's
	// StateMachine implementation without changing schedules, histories, or
	// checker semantics.
	Engine string
	// LSMFlushThreshold configures the LSM's active-memtable flush seam when
	// Engine is lsm. Zero selects lsm.DefaultFlushThreshold.
	LSMFlushThreshold int64
	Schedule          *sim.FaultSchedule
	Duration          sim.VirtualTime
	FaultHorizon      sim.VirtualTime
	RetryScanInterval sim.VirtualTime
	Operations        int
	OperationTimeout  int64
	ThinkTime         int64
}

// Summary is deterministic evidence collected from one run. It deliberately
// excludes wall-clock elapsed time so replays serialize byte-for-byte.
type Summary struct {
	ArtifactVersion   int    `json:"artifact_version"`
	Seed              int64  `json:"seed"`
	Engine            string `json:"engine"`
	LSMFlushThreshold int64  `json:"lsm_flush_threshold"`
	ScheduleVersion   int    `json:"schedule_version"`
	// CheckerRan distinguishes "Porcupine accepted the history" from "the run
	// aborted (for example on a simulator invariant) before the checker could
	// run": Linearizable is meaningful only when CheckerRan is true.
	CheckerRan           bool           `json:"checker_ran"`
	Linearizable         bool           `json:"linearizable"`
	NeutralFaultEvents   int            `json:"neutral_fault_events"`
	LogicalOperations    int            `json:"logical_operations"`
	CompletedOperations  int            `json:"completed_operations"`
	OpenOperations       int            `json:"open_operations"`
	Attempts             int            `json:"attempts"`
	OperationCounts      map[string]int `json:"operation_counts"`
	FaultEventCounts     map[string]int `json:"fault_event_counts"`
	CrashPointCounts     map[string]int `json:"crash_point_counts"`
	NetworkDrops         int            `json:"network_drops"`
	NetworkDuplicates    int            `json:"network_duplicates"`
	MaxConcurrentClients int            `json:"max_concurrent_clients"`
	FaultsDuringInFlight int            `json:"faults_during_in_flight"`
	FinalVirtualTime     int64          `json:"final_virtual_time"`
	WorkloadReportedDone bool           `json:"workload_reported_done"`
	AntiVacuityFailure   string         `json:"anti_vacuity_failure,omitempty"`
}

// Result retains the exact deterministic inputs and outputs needed for replay
// diagnostics. Batch callers aggregate Summary and discard the larger fields.
type Result struct {
	Seed              int64
	Engine            string
	LSMFlushThreshold int64
	Schedule          sim.FaultSchedule
	History           checker.History
	Trace             []string
	Summary           Summary
}

// RunSeed executes one complete full-fault/full-workload replay. It advances
// in deterministic virtual-time slices solely so timed-out sessions can issue
// explicit same-sequence retries after their deadline; each individual Sim is
// still driven synchronously from one scheduler thread.
func RunSeed(config RunConfig) (Result, error) {
	config = normalizeRunConfig(config)
	if config.Engine == "" {
		config.Engine = "map"
	}
	schedule, err := scheduleFor(config)
	if err != nil {
		return baseResult(config, sim.FaultSchedule{}), err
	}
	base := baseResult(config, schedule)
	if config.LSMFlushThreshold < 0 {
		return base, fmt.Errorf("seed %d: negative LSM flush threshold %d", config.Seed, config.LSMFlushThreshold)
	}
	if config.Engine == "map" && config.LSMFlushThreshold != 0 {
		return base, fmt.Errorf("seed %d: LSM flush threshold requires engine lsm", config.Seed)
	}
	s, err := sim.NewFaultSim(sim.Config{
		Seed:              config.Seed,
		Engine:            config.Engine,
		LSMFlushThreshold: config.LSMFlushThreshold,
		NodeIDs:           append([]raft.NodeID(nil), defaultNodeIDs...),
	}, schedule)
	if err != nil {
		return base, fmt.Errorf("seed %d: build fault sim: %w", config.Seed, err)
	}
	s.RegisterInvariant(sim.SingleLeaderPerTerm)
	if err := s.StartWorkload(workloadpkg.Config{
		ClientCount:         workloadpkg.DefaultClientCount,
		KeyCount:            workloadpkg.DefaultKeyCount,
		OperationsPerClient: config.Operations,
		OperationTimeout:    config.OperationTimeout,
		RetryDelay:          workloadpkg.DefaultRetryDelay,
		ThinkTime:           config.ThinkTime,
	}); err != nil {
		return base, fmt.Errorf("seed %d: start workload: %w", config.Seed, err)
	}

	for through := config.RetryScanInterval; through <= config.Duration; through += config.RetryScanInterval {
		if err := s.Run(through); err != nil {
			result := collectResult(config.Seed, config.Engine, config.LSMFlushThreshold, schedule, s)
			return result, fmt.Errorf("seed %d: simulator invariant: %w", config.Seed, err)
		}
		if through == config.Duration {
			break
		}
		for _, client := range s.WorkloadClients() {
			if !client.TimedOut {
				continue
			}
			if err := s.ScheduleWorkloadRetry(client.ClientID, s.Now()+1); err != nil {
				result := collectResult(config.Seed, config.Engine, config.LSMFlushThreshold, schedule, s)
				return result, fmt.Errorf("seed %d: schedule retry for client %d: %w", config.Seed, client.ClientID, err)
			}
		}
	}
	// Duration need not be divisible by the retry scan interval.
	if s.Now() < config.Duration {
		if err := s.Run(config.Duration); err != nil {
			result := collectResult(config.Seed, config.Engine, config.LSMFlushThreshold, schedule, s)
			return result, fmt.Errorf("seed %d: simulator invariant: %w", config.Seed, err)
		}
	}

	result := collectResult(config.Seed, config.Engine, config.LSMFlushThreshold, schedule, s)
	result.Summary.CheckerRan = true
	result.Summary.Linearizable = checker.Check(result.History)
	result.Summary.AntiVacuityFailure = antiVacuityFailure(result.Summary)
	if !result.Summary.Linearizable {
		return result, fmt.Errorf("seed %d: Porcupine found a linearizability violation", config.Seed)
	}
	if result.Summary.AntiVacuityFailure != "" {
		return result, fmt.Errorf("seed %d: weak workload: %s", config.Seed, result.Summary.AntiVacuityFailure)
	}
	return result, nil
}

func normalizeRunConfig(config RunConfig) RunConfig {
	if config.Duration == 0 {
		config.Duration = DefaultDuration
	}
	if config.FaultHorizon == 0 {
		config.FaultHorizon = DefaultFaultHorizon
	}
	if config.RetryScanInterval == 0 {
		config.RetryScanInterval = DefaultRetryScanInterval
	}
	if config.Operations == 0 {
		config.Operations = DefaultOperationsPerClient
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = DefaultOperationTimeout
	}
	if config.ThinkTime == 0 {
		config.ThinkTime = DefaultThinkTime
	}
	return config
}

func scheduleFor(config RunConfig) (sim.FaultSchedule, error) {
	if config.Schedule != nil {
		encoded, err := sim.EncodeFaultSchedule(*config.Schedule)
		if err != nil {
			return sim.FaultSchedule{}, err
		}
		return sim.DecodeFaultSchedule(encoded)
	}
	return sim.GenerateFaultSchedule(config.Seed, defaultNodeIDs, config.FaultHorizon)
}

func baseResult(config RunConfig, schedule sim.FaultSchedule) Result {
	return Result{
		Seed:              config.Seed,
		Engine:            config.Engine,
		LSMFlushThreshold: config.LSMFlushThreshold,
		Schedule:          schedule,
		Summary: Summary{
			ArtifactVersion:   ArtifactVersion,
			Seed:              config.Seed,
			Engine:            config.Engine,
			LSMFlushThreshold: config.LSMFlushThreshold,
			ScheduleVersion:   schedule.Version,
			OperationCounts:   make(map[string]int),
			FaultEventCounts:  make(map[string]int),
			CrashPointCounts:  make(map[string]int),
		},
	}
}

func collectResult(seed int64, engine string, lsmFlushThreshold int64, schedule sim.FaultSchedule, s *sim.Sim) Result {
	history := s.WorkloadHistory()
	trace := s.Trace()
	result := baseResult(RunConfig{Seed: seed, Engine: engine, LSMFlushThreshold: lsmFlushThreshold}, schedule)
	summary := result.Summary
	summary.LogicalOperations = len(history)
	summary.Attempts = len(s.WorkloadAttempts())
	summary.FinalVirtualTime = int64(s.Now())
	summary.WorkloadReportedDone = s.WorkloadDone()
	for _, operation := range history {
		summary.OperationCounts[operation.Input.Op.String()]++
		if operation.ReturnTime == nil {
			summary.OpenOperations++
		} else {
			summary.CompletedOperations++
		}
	}
	for _, fault := range schedule.Events {
		if fault.Time > s.Now() {
			continue
		}
		if neutralFaultEvent(fault) {
			summary.NeutralFaultEvents++
			continue
		}
		summary.FaultEventCounts[fault.Kind.String()]++
		if injectedFaultEvent(fault) && faultInFlightClients(history, int64(fault.Time)) > 0 {
			summary.FaultsDuringInFlight++
		}
	}
	for _, firing := range s.CrashFirings() {
		summary.CrashPointCounts[firing.Point.String()]++
		// A storage crash fires inside processReady — including the Ready a
		// client's own just-invoked proposal drives synchronously at this exact
		// virtual time — so unlike a pre-enqueued schedule fault it is not
		// ordered ahead of every workload event. An operation invoked at the
		// firing instant may therefore genuinely be in flight, so crash overlap
		// keeps the closed-interval measure rather than excluding it.
		if clientsInFlightAt(history, int64(firing.Time)) > 0 {
			summary.FaultsDuringInFlight++
		}
	}
	summary.MaxConcurrentClients = maxConcurrentClients(history)
	for _, line := range trace {
		switch {
		case strings.Contains(line, " drop(fault)"):
			summary.NetworkDrops++
		case strings.Contains(line, " duplicate(fault)"):
			summary.NetworkDuplicates++
		}
	}
	result.History = history
	result.Trace = trace
	result.Summary = summary
	return result
}

// neutralFaultEvent reports whether a schedule event restores the neutral
// baseline rather than injecting a fault: a zero drop/duplicate rate or a 1x
// clock multiplier (the generator's recovery tail has exactly these
// shapes). Neutral events are excluded from fault-kind counts entirely, so
// they can never satisfy fault coverage or in-flight fault evidence.
func neutralFaultEvent(event sim.FaultEvent) bool {
	switch event.Kind {
	case sim.FaultDropRate, sim.FaultDuplicateRate:
		return event.Rate == 0
	case sim.FaultClockSkew:
		return event.Multiplier == 1
	default:
		return false
	}
}

// injectedFaultEvent reports whether a non-neutral schedule event degrades
// the system: it opens a partition, pauses or host-crashes a node, raises a
// loss/duplication rate, or skews a clock. Heal, HealGroups, Resume, and
// Restart close an injected fault — they count as fired fault kinds, but a
// recovery action overlapping an in-flight operation is not evidence that a
// fault hit one, so only injected events (and storage crash firings) advance
// FaultsDuringInFlight.
func injectedFaultEvent(event sim.FaultEvent) bool {
	switch event.Kind {
	case sim.FaultDropRate, sim.FaultDuplicateRate:
		return event.Rate > 0
	case sim.FaultClockSkew:
		return event.Multiplier != 1
	case sim.FaultPartition, sim.FaultPartitionGroups, sim.FaultPause, sim.FaultCrash:
		return true
	default:
		return false
	}
}

func antiVacuityFailure(summary Summary) string {
	var failures []string
	if summary.LogicalOperations < MinimumLogicalOperations {
		failures = append(failures, "logical operations "+strconv.Itoa(summary.LogicalOperations)+" < "+strconv.Itoa(MinimumLogicalOperations))
	}
	if summary.CompletedOperations < MinimumCompleted {
		failures = append(failures, "completed operations "+strconv.Itoa(summary.CompletedOperations)+" < "+strconv.Itoa(MinimumCompleted))
	}
	if summary.Attempts < summary.LogicalOperations {
		failures = append(failures, "physical attempts fewer than logical operations")
	}
	for _, op := range []raftpb.Op{raftpb.Op_PUT, raftpb.Op_GET, raftpb.Op_DELETE} {
		if summary.OperationCounts[op.String()] == 0 {
			failures = append(failures, "no "+op.String()+" operation")
		}
	}
	if summary.MaxConcurrentClients < 2 {
		failures = append(failures, "fewer than two clients overlap")
	}
	if summary.FaultsDuringInFlight == 0 {
		failures = append(failures, "no fault fired during an in-flight operation")
	}
	return strings.Join(failures, "; ")
}

// clientsInFlightAt counts the distinct clients whose operation interval
// [InvokeTime, ReturnTime] covers virtual time `at` with both endpoints closed:
// an operation is in flight at its own invoke and return instants, and two
// operations that merely touch at a shared instant are concurrent. This is the
// operation-overlap measure maxConcurrentClients reports and the storage-crash
// overlap path uses; a scheduled fault uses faultInFlightClients instead, which
// is deliberately stricter on the invoke side.
func clientsInFlightAt(history checker.History, at int64) int {
	clients := make(map[uint64]struct{})
	for _, operation := range history {
		if operation.InvokeTime > at {
			continue
		}
		// Closed intervals: a return at t still overlaps an invocation/fault at t.
		if operation.ReturnTime == nil || *operation.ReturnTime >= at {
			clients[operation.ClientID] = struct{}{}
		}
	}
	return len(clients)
}

// faultInFlightClients counts the distinct clients whose operation is genuinely
// in flight when a scheduled fault fires at virtual time `at`. Every
// FaultSchedule.Events entry is enqueued at NewFaultSim construction, before
// StartWorkload enqueues any client event, so on the sim's (time, seq) event
// queue a fault at time T fires strictly before any workload event at T. An
// operation invoked at T has therefore not begun when the fault fires and must
// not count (exclusive invoke), while an operation returning at T has not yet
// closed and still counts (inclusive return). Without this a t=0 fault would
// credit itself with an operation the workload only invokes after the fault has
// already fired, inflating FaultsDuringInFlight and the anti-vacuity floor.
func faultInFlightClients(history checker.History, at int64) int {
	clients := make(map[uint64]struct{})
	for _, operation := range history {
		if faultCatchesOperation(operation, at) {
			clients[operation.ClientID] = struct{}{}
		}
	}
	return len(clients)
}

// mutatingClientInFlightForFault reports whether a scheduled fault firing at
// virtual time `at` catches at least one PUT or DELETE in flight, using the
// same pre-enqueued ordering as faultInFlightClients. It backs the
// partition-during-write scripted gate, which must stay tied to a mutation in
// flight rather than drifting to a partition that only ever overlaps reads.
func mutatingClientInFlightForFault(history checker.History, at int64) bool {
	for _, operation := range history {
		if !faultCatchesOperation(operation, at) {
			continue
		}
		if operation.Input.Op == raftpb.Op_PUT || operation.Input.Op == raftpb.Op_DELETE {
			return true
		}
	}
	return false
}

// faultCatchesOperation reports whether a scheduled fault firing at virtual
// time `at` catches operation in flight: exclusive on invoke (a fault enqueued
// before every workload event fires ahead of an equal-time invocation, which
// has not begun) and inclusive on return (an equal-time return has not closed).
func faultCatchesOperation(operation checker.Operation, at int64) bool {
	if operation.InvokeTime >= at {
		return false
	}
	return operation.ReturnTime == nil || *operation.ReturnTime >= at
}

func maxConcurrentClients(history checker.History) int {
	max := 0
	for _, operation := range history {
		if concurrent := clientsInFlightAt(history, operation.InvokeTime); concurrent > max {
			max = concurrent
		}
		if operation.ReturnTime != nil {
			if concurrent := clientsInFlightAt(history, *operation.ReturnTime); concurrent > max {
				max = concurrent
			}
		}
	}
	return max
}
