package sim

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"miniquorum/checker"
	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	raftpb "miniquorum/proto"
	workloadpkg "miniquorum/sim/workload"
)

type workloadEvent struct {
	event workloadpkg.Event
}

type workloadApplyKey struct {
	node  raft.NodeID
	index uint64
}

type pendingWorkloadAttempt struct {
	submission workloadpkg.Submission
	term       uint64
}

type simWorkload struct {
	runner *workloadpkg.Runner

	pending   map[workloadApplyKey][]pendingWorkloadAttempt
	byAttempt map[workloadpkg.AttemptID]workloadApplyKey
}

// StartWorkload installs fresh per-node map state machines and schedules a
// deterministic workload inside this simulator. It must be called before the
// scheduler advances; replacing state machines after Raft application begins
// would discard replicated state.
func (s *Sim) StartWorkload(config workloadpkg.Config) error {
	if s.workload != nil {
		return fmt.Errorf("sim: workload already started")
	}
	if s.now != 0 {
		return fmt.Errorf("sim: workload must start before virtual time advances (now %d)", s.now)
	}
	config.Seed = s.seed
	config.Targets = make([]uint64, len(s.order))
	for i, id := range s.order {
		config.Targets[i] = uint64(id)
	}
	runner, err := workloadpkg.New(config)
	if err != nil {
		return err
	}
	s.setStateMachineFactory(func() statemachine.StateMachine { return mapsm.New() })
	s.workload = &simWorkload{
		runner:    runner,
		pending:   make(map[workloadApplyKey][]pendingWorkloadAttempt),
		byAttempt: make(map[workloadpkg.AttemptID]workloadApplyKey),
	}
	if err := runner.Start(workloadHost{sim: s}); err != nil {
		return err
	}
	return nil
}

// WorkloadHistory returns a deep copy of the logical client history. Timed-out
// operations retain nil ReturnTime even though the Porcupine adapter later
// appends paired synthetic unknown returns.
func (s *Sim) WorkloadHistory() checker.History {
	if s.workload == nil {
		return nil
	}
	return s.workload.runner.History()
}

// WorkloadAttempts returns every physical submission in deterministic order.
func (s *Sim) WorkloadAttempts() []workloadpkg.AttemptRecord {
	if s.workload == nil {
		return nil
	}
	return s.workload.runner.Attempts()
}

// WorkloadClients returns stable per-session inspection state.
func (s *Sim) WorkloadClients() []workloadpkg.ClientSnapshot {
	if s.workload == nil {
		return nil
	}
	return s.workload.runner.Clients()
}

// WorkloadDone reports whether every configured logical operation returned.
func (s *Sim) WorkloadDone() bool {
	return s.workload != nil && s.workload.runner.Done()
}

// ScheduleWorkloadRetry schedules an explicit retry of a timed-out logical
// operation. Runner supplies the still-outstanding sequence; this method can
// never advance a session to a new logical operation.
func (s *Sim) ScheduleWorkloadRetry(clientID uint64, at VirtualTime) error {
	if s.workload == nil {
		return fmt.Errorf("sim: workload is not started")
	}
	if at < s.now {
		return fmt.Errorf("sim: workload retry time %d precedes now %d", at, s.now)
	}
	event, err := s.workload.runner.RetryEvent(clientID)
	if err != nil {
		return err
	}
	wrapped := eventWrapper(event)
	s.pushAt(at, &wrapped)
	return nil
}

func eventWrapper(payload workloadpkg.Event) event {
	return event{kind: eventWorkload, workload: &workloadEvent{event: payload}}
}

type workloadHost struct {
	sim *Sim
}

func (h workloadHost) Now() int64 { return int64(h.sim.now) }

func (h workloadHost) Schedule(at int64, workloadEvent workloadpkg.Event) {
	event := eventWrapper(workloadEvent)
	h.sim.pushAt(VirtualTime(at), &event)
}

func (h workloadHost) Submit(submission workloadpkg.Submission) (workloadpkg.SubmitStatus, error) {
	data, err := proto.Marshal(submission.Command)
	if err != nil {
		return workloadpkg.SubmitNotLeader, fmt.Errorf("sim: marshal workload command: %w", err)
	}
	target := raft.NodeID(submission.Target)
	_, _, isLeader := h.sim.propose(target, data, func(index, term uint64) {
		h.sim.registerWorkloadAttempt(target, index, term, submission)
	})
	if !isLeader {
		h.sim.record("workload client=%d seq=%d attempt=%d target=%d not_leader", submission.AttemptID.ClientID, submission.AttemptID.Seq, submission.AttemptID.Attempt, target)
		return workloadpkg.SubmitNotLeader, nil
	}
	h.sim.record("workload client=%d seq=%d attempt=%d target=%d accepted", submission.AttemptID.ClientID, submission.AttemptID.Seq, submission.AttemptID.Attempt, target)
	return workloadpkg.SubmitAccepted, nil
}

func (h workloadHost) Cancel(attempt workloadpkg.AttemptID) {
	h.sim.cancelWorkloadAttempt(attempt)
}

func (s *Sim) registerWorkloadAttempt(node raft.NodeID, index, term uint64, submission workloadpkg.Submission) {
	key := workloadApplyKey{node: node, index: index}
	s.workload.pending[key] = append(s.workload.pending[key], pendingWorkloadAttempt{
		submission: submission,
		term:       term,
	})
	s.workload.byAttempt[submission.AttemptID] = key
	s.record("workload client=%d seq=%d attempt=%d wait=(%d,%d,%d)", submission.AttemptID.ClientID, submission.AttemptID.Seq, submission.AttemptID.Attempt, node, index, term)
}

func (s *Sim) cancelWorkloadAttempt(attempt workloadpkg.AttemptID) {
	key, ok := s.workload.byAttempt[attempt]
	if !ok {
		return
	}
	delete(s.workload.byAttempt, attempt)
	pending := s.workload.pending[key]
	kept := pending[:0]
	for _, pendingAttempt := range pending {
		if pendingAttempt.submission.AttemptID != attempt {
			kept = append(kept, pendingAttempt)
		}
	}
	if len(kept) == 0 {
		delete(s.workload.pending, key)
	} else {
		s.workload.pending[key] = kept
	}
	s.record("workload client=%d seq=%d attempt=%d cancel(timeout)", attempt.ClientID, attempt.Seq, attempt.Attempt)
}

func (s *Sim) observeWorkloadApply(node raft.NodeID, entry *raftpb.Entry, result statemachine.Result) {
	if s.workload == nil || entry == nil {
		return
	}
	key := workloadApplyKey{node: node, index: entry.GetIndex()}
	pending := s.workload.pending[key]
	if len(pending) == 0 {
		return
	}
	delete(s.workload.pending, key)
	for _, pendingAttempt := range pending {
		submission := pendingAttempt.submission
		delete(s.workload.byAttempt, submission.AttemptID)
		kind := workloadpkg.EventLeadershipLost
		output := checker.Output{}
		if entry.GetTerm() == pendingAttempt.term {
			kind = workloadpkg.EventApplied
			output = successfulWorkloadOutput(result)
		}
		s.pushAt(s.now, &event{
			kind: eventWorkload,
			workload: &workloadEvent{event: workloadpkg.Event{
				Kind:      kind,
				AttemptID: submission.AttemptID,
				Output:    output,
			}},
		})
	}
}

func successfulWorkloadOutput(result statemachine.Result) checker.Output {
	return checker.Output{
		OK:    true,
		Value: append([]byte(nil), result.Value...),
		Found: result.Found,
	}
}

func (s *Sim) handleWorkloadEvent(payload *workloadEvent) {
	if payload == nil || s.workload == nil || s.runErr != nil {
		return
	}
	if err := s.workload.runner.Handle(workloadHost{sim: s}, payload.event); err != nil {
		s.runErr = fmt.Errorf("sim: workload event at t=%d: %w", s.now, err)
		s.record("workload error=%v", err)
	}
}
