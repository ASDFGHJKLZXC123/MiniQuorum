package workload

import (
	"errors"
	"fmt"

	"miniquorum/checker"
	raftpb "miniquorum/proto"
)

// EventKind identifies a scheduler callback into Runner.
type EventKind uint8

const (
	EventStart EventKind = iota
	EventRetry
	EventDeadline
	EventApplied
	EventLeadershipLost
)

// AttemptID uniquely identifies one physical submission of a logical
// (client, sequence) operation.
type AttemptID struct {
	ClientID uint64
	Seq      uint64
	Attempt  uint64
}

// Event is pure scheduler data. DeadlineGeneration rejects stale deadlines;
// Output is meaningful only for EventApplied.
type Event struct {
	Kind               EventKind
	AttemptID          AttemptID
	DeadlineGeneration uint64
	Output             checker.Output
}

// Submission is one physical attempt. Every retry of a logical operation has
// a new Attempt number but the exact same Command client ID, sequence, op,
// key, and value.
type Submission struct {
	AttemptID AttemptID
	Target    uint64
	Command   *raftpb.Command
}

// SubmitStatus is the immediate result of trying a target node.
type SubmitStatus uint8

const (
	SubmitAccepted SubmitStatus = iota
	SubmitNotLeader
)

// Host is implemented by the single-threaded simulator. Runner never blocks;
// every future action is put back on the host's virtual-time scheduler.
type Host interface {
	Now() int64
	Schedule(at int64, event Event)
	Submit(submission Submission) (SubmitStatus, error)
	Cancel(attempt AttemptID)
}

// AttemptRecord is a deterministic inspection record for retry-identity tests
// and failure diagnostics.
type AttemptRecord struct {
	Time      int64
	AttemptID AttemptID
	Target    uint64
	Status    SubmitStatus
	Command   *raftpb.Command
}

// ClientSnapshot is a stable inspection view of one logical session.
type ClientSnapshot struct {
	ClientID       uint64
	NextSeq        uint64
	Generated      int
	Completed      int
	OutstandingSeq uint64
	TimedOut       bool
}

type logicalOperation struct {
	seq                uint64
	command            *raftpb.Command
	deadline           int64
	deadlineGeneration uint64
	attempt            uint64
	waiting            bool
	timedOut           bool
}

type client struct {
	id           uint64
	nextSeq      uint64
	generated    int
	completed    int
	targetCursor int
	generator    *Generator
	active       *logicalOperation
}

// Runner owns the deterministic client sessions and their one logical
// outstanding operation each.
type Runner struct {
	config   Config
	clients  []*client
	byID     map[uint64]*client
	recorder *checker.Recorder
	attempts []AttemptRecord
	started  bool
}

// New constructs a workload runner without scheduling it.
func New(config Config) (*Runner, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	runner := &Runner{
		config:   normalized,
		clients:  make([]*client, 0, normalized.ClientCount),
		byID:     make(map[uint64]*client, normalized.ClientCount),
		recorder: checker.NewRecorder(),
	}
	for i := 0; i < normalized.ClientCount; i++ {
		clientID := normalized.ClientIDBase + uint64(i)
		generator, err := NewGenerator(normalized.Seed, clientID, normalized.KeyCount)
		if err != nil {
			return nil, err
		}
		state := &client{
			id:           clientID,
			nextSeq:      1,
			targetCursor: i % len(normalized.Targets),
			generator:    generator,
		}
		runner.clients = append(runner.clients, state)
		runner.byID[clientID] = state
	}
	return runner, nil
}

// Start schedules every stable client in fixed client-ID order.
func (r *Runner) Start(host Host) error {
	if r.started {
		return errors.New("workload: runner already started")
	}
	if r.config.StartTime < host.Now() {
		return fmt.Errorf("workload: start time %d precedes current virtual time %d", r.config.StartTime, host.Now())
	}
	r.started = true
	for _, state := range r.clients {
		host.Schedule(r.config.StartTime, Event{
			Kind:      EventStart,
			AttemptID: AttemptID{ClientID: state.id},
		})
	}
	return nil
}

// Handle processes exactly one scheduler event synchronously.
func (r *Runner) Handle(host Host, event Event) error {
	state, ok := r.byID[event.AttemptID.ClientID]
	if !ok {
		return fmt.Errorf("workload: unknown client %d", event.AttemptID.ClientID)
	}
	switch event.Kind {
	case EventStart:
		return r.handleStart(host, state)
	case EventRetry:
		return r.handleRetry(host, state, event)
	case EventDeadline:
		return r.handleDeadline(host, state, event)
	case EventApplied:
		return r.handleApplied(host, state, event)
	case EventLeadershipLost:
		return r.handleLeadershipLost(host, state, event)
	default:
		return fmt.Errorf("workload: unsupported event kind %d", event.Kind)
	}
}

// RetryEvent returns a manual same-sequence retry for a timed-out session.
// It never creates a new logical invocation.
func (r *Runner) RetryEvent(clientID uint64) (Event, error) {
	state, ok := r.byID[clientID]
	if !ok {
		return Event{}, fmt.Errorf("workload: unknown client %d", clientID)
	}
	if state.active == nil || !state.active.timedOut {
		return Event{}, fmt.Errorf("workload: client %d has no timed-out operation to retry", clientID)
	}
	return Event{
		Kind: EventRetry,
		AttemptID: AttemptID{
			ClientID: clientID,
			Seq:      state.active.seq,
		},
	}, nil
}

// History returns a deep copy of the logical history.
func (r *Runner) History() checker.History { return r.recorder.History() }

// Attempts returns physical submissions in deterministic submission order.
func (r *Runner) Attempts() []AttemptRecord {
	records := make([]AttemptRecord, len(r.attempts))
	for i := range r.attempts {
		records[i] = cloneAttemptRecord(r.attempts[i])
	}
	return records
}

// Clients returns stable client snapshots in ascending session-ID order.
func (r *Runner) Clients() []ClientSnapshot {
	snapshots := make([]ClientSnapshot, 0, len(r.clients))
	for _, state := range r.clients {
		snapshot := ClientSnapshot{
			ClientID:  state.id,
			NextSeq:   state.nextSeq,
			Generated: state.generated,
			Completed: state.completed,
		}
		if state.active != nil {
			snapshot.OutstandingSeq = state.active.seq
			snapshot.TimedOut = state.active.timedOut
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

// Done reports whether every configured operation completed with an observed
// response. A timed-out open operation intentionally keeps the runner undone.
func (r *Runner) Done() bool {
	for _, state := range r.clients {
		if state.active != nil || state.generated != r.config.OperationsPerClient {
			return false
		}
	}
	return true
}

func (r *Runner) handleStart(host Host, state *client) error {
	if state.active != nil {
		return fmt.Errorf("%w: client %d seq %d", checker.ErrOutstandingOperation, state.id, state.active.seq)
	}
	if state.generated >= r.config.OperationsPerClient {
		return nil
	}

	seq := state.nextSeq
	input := state.generator.Next(seq)
	if err := r.recorder.Invoke(state.id, seq, host.Now(), input); err != nil {
		return err
	}
	state.generated++
	state.active = &logicalOperation{
		seq:      seq,
		command:  commandFor(state.id, seq, input),
		deadline: host.Now() + r.config.OperationTimeout,
	}
	r.scheduleDeadline(host, state)
	return r.submit(host, state, false)
}

func (r *Runner) handleRetry(host Host, state *client, event Event) error {
	operation := state.active
	if operation == nil || (event.AttemptID.Seq != 0 && event.AttemptID.Seq != operation.seq) {
		return nil
	}
	if event.AttemptID.Attempt != 0 && event.AttemptID.Attempt != operation.attempt {
		return nil
	}
	if operation.waiting {
		return nil
	}
	if operation.timedOut {
		operation.timedOut = false
		operation.deadline = host.Now() + r.config.OperationTimeout
		r.scheduleDeadline(host, state)
	}
	return r.submit(host, state, true)
}

func (r *Runner) handleDeadline(host Host, state *client, event Event) error {
	operation := state.active
	if operation == nil || event.AttemptID.Seq != operation.seq || event.DeadlineGeneration != operation.deadlineGeneration {
		return nil
	}
	if operation.waiting {
		host.Cancel(AttemptID{ClientID: state.id, Seq: operation.seq, Attempt: operation.attempt})
	}
	if err := r.recorder.Timeout(state.id, operation.seq, host.Now()); err != nil {
		return err
	}
	operation.waiting = false
	operation.timedOut = true
	return nil
}

func (r *Runner) handleApplied(host Host, state *client, event Event) error {
	operation := state.active
	if !matchesWaitingAttempt(state, event.AttemptID) {
		return nil
	}
	if err := r.recorder.Complete(state.id, operation.seq, host.Now(), event.Output); err != nil {
		return err
	}
	state.completed++
	state.nextSeq++
	state.active = nil
	if state.generated < r.config.OperationsPerClient {
		host.Schedule(host.Now()+r.config.ThinkTime, Event{
			Kind:      EventStart,
			AttemptID: AttemptID{ClientID: state.id},
		})
	}
	return nil
}

func (r *Runner) handleLeadershipLost(host Host, state *client, event Event) error {
	if !matchesWaitingAttempt(state, event.AttemptID) {
		return nil
	}
	state.active.waiting = false
	return r.scheduleRetry(host, state)
}

func (r *Runner) submit(host Host, state *client, retry bool) error {
	operation := state.active
	if retry {
		if err := r.recorder.Retry(state.id, operation.seq); err != nil {
			return err
		}
	}
	operation.attempt++
	attemptID := AttemptID{ClientID: state.id, Seq: operation.seq, Attempt: operation.attempt}
	target := r.config.Targets[state.targetCursor]
	state.targetCursor = (state.targetCursor + 1) % len(r.config.Targets)
	operation.waiting = true
	submission := Submission{
		AttemptID: attemptID,
		Target:    target,
		Command:   cloneCommand(operation.command),
	}
	status, err := host.Submit(submission)
	if err != nil {
		operation.waiting = false
		return err
	}
	r.attempts = append(r.attempts, AttemptRecord{
		Time:      host.Now(),
		AttemptID: attemptID,
		Target:    target,
		Status:    status,
		Command:   cloneCommand(operation.command),
	})
	switch status {
	case SubmitAccepted:
		return nil
	case SubmitNotLeader:
		operation.waiting = false
		return r.scheduleRetry(host, state)
	default:
		operation.waiting = false
		return fmt.Errorf("workload: unsupported submit status %d", status)
	}
}

func (r *Runner) scheduleRetry(host Host, state *client) error {
	operation := state.active
	retryAt := host.Now() + r.config.RetryDelay
	if retryAt >= operation.deadline {
		return nil
	}
	host.Schedule(retryAt, Event{
		Kind: EventRetry,
		AttemptID: AttemptID{
			ClientID: state.id,
			Seq:      operation.seq,
			Attempt:  operation.attempt,
		},
	})
	return nil
}

func (r *Runner) scheduleDeadline(host Host, state *client) {
	operation := state.active
	operation.deadlineGeneration++
	host.Schedule(operation.deadline, Event{
		Kind: EventDeadline,
		AttemptID: AttemptID{
			ClientID: state.id,
			Seq:      operation.seq,
		},
		DeadlineGeneration: operation.deadlineGeneration,
	})
}

func matchesWaitingAttempt(state *client, attempt AttemptID) bool {
	operation := state.active
	return operation != nil && operation.waiting && attempt.ClientID == state.id && attempt.Seq == operation.seq && attempt.Attempt == operation.attempt
}

func commandFor(clientID, seq uint64, input checker.Input) *raftpb.Command {
	return &raftpb.Command{
		ClientId: clientID,
		Seq:      seq,
		Op:       input.Op,
		Key:      append([]byte(nil), input.Key...),
		Value:    append([]byte(nil), input.Value...),
	}
}

func cloneAttemptRecord(record AttemptRecord) AttemptRecord {
	record.Command = cloneCommand(record.Command)
	return record
}

func cloneCommand(command *raftpb.Command) *raftpb.Command {
	if command == nil {
		return nil
	}
	return &raftpb.Command{
		ClientId: command.GetClientId(),
		Seq:      command.GetSeq(),
		Op:       command.GetOp(),
		Key:      append([]byte(nil), command.GetKey()...),
		Value:    append([]byte(nil), command.GetValue()...),
	}
}
