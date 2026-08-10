package sim

import (
	"encoding/binary"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	"miniquorum/checker"
	"miniquorum/internal/lsm"
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

type pendingWorkloadRead struct {
	node       raft.NodeID
	submission workloadpkg.Submission
	token      []byte
	confirmed  bool
	index      uint64
}

type simWorkload struct {
	runner *workloadpkg.Runner

	pending       map[workloadApplyKey][]pendingWorkloadAttempt
	byAttempt     map[workloadpkg.AttemptID]workloadApplyKey
	pendingReads  map[string]*pendingWorkloadRead
	readByAttempt map[workloadpkg.AttemptID]string
	// deferredReadCancels preserves client timeout order while a target process
	// is paused. A frozen process cannot execute Node.CancelRead; Resume flushes
	// these tokens synchronously before the node handles later activity.
	deferredReadCancels map[raft.NodeID][][]byte
}

// StartWorkload installs fresh per-node state machines selected by Config's
// simulator engine and schedules a deterministic workload. It must be called
// before the scheduler advances; replacing state machines after Raft
// application begins would discard replicated state.
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
	switch s.engine {
	case "map":
		s.setStateMachineFactory(func() statemachine.StateMachine { return mapsm.New() })
	case "lsm":
		if err := s.installLSMStateMachines(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("sim: unsupported engine %q", s.engine)
	}
	s.workload = &simWorkload{
		runner:              runner,
		pending:             make(map[workloadApplyKey][]pendingWorkloadAttempt),
		byAttempt:           make(map[workloadpkg.AttemptID]workloadApplyKey),
		pendingReads:        make(map[string]*pendingWorkloadRead),
		readByAttempt:       make(map[workloadpkg.AttemptID]string),
		deferredReadCancels: make(map[raft.NodeID][][]byte),
	}
	if err := runner.Start(workloadHost{sim: s}); err != nil {
		return err
	}
	return nil
}

// installLSMStateMachines gives each simulated node an independent durable
// SimFS and deterministic skip-list stream. The closures reopen that same
// node-local directory after a simulated process restart, so the retained
// Raft log rebuilds both logical state and in-memory deduplication without
// crossing the simulator's I/O or wall-clock boundary.
func (s *Sim) installLSMStateMachines() error {
	for _, id := range s.order {
		sn := s.nodes[id]
		fs := lsm.NewSimFS()
		sn.lsmFS = fs
		if s.lsmFS == nil {
			s.lsmFS = make(map[raft.NodeID]*lsm.SimFS)
		}
		s.lsmFS[id] = fs
		var directives []lsm.SimCrashDirective
		for _, directive := range s.lsmCrashDirectives {
			if directive.Node != id {
				continue
			}
			directives = append(directives, lsm.SimCrashDirective{Op: directive.Op, Occurrence: directive.Occurrence, Point: directive.Point, RetainUnsynced: directive.RetainUnsynced})
		}
		dir := fmt.Sprintf("/node-%d", id)
		seed := lsmRandSeed(s.seed, id)
		factory := func() (statemachine.StateMachine, error) {
			if fs.Crashed() {
				if err := fs.Recover(); err != nil {
					return nil, fmt.Errorf("recover LSM filesystem for node %d: %w", id, err)
				}
			}
			return lsm.OpenStateMachine(dir, lsm.Options{FS: fs, Rand: newSeededLSMRand(seed), FlushThreshold: s.lsmFlushThreshold})
		}
		stateMachine, err := factory()
		if err != nil {
			return fmt.Errorf("sim: open LSM state machine for node %d: %w", id, err)
		}
		// Opening an existing engine performs ordinary filesystem reads and
		// cleanup. Arm the validated crash schedule only after that initial
		// open succeeds, so a directive's first occurrence always belongs to
		// the replicated workload or a later reopen, never startup plumbing.
		// SetCrashSchedule also resets per-operation counters when directives
		// is empty; calibration and scheduled runs therefore share the same
		// post-open occurrence origin.
		if err := fs.SetCrashSchedule(directives); err != nil {
			if closer, ok := stateMachine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			return fmt.Errorf("sim: configure LSM crash directives for node %d: %w", id, err)
		}
		sn.newStateMachine = factory
		sn.sm = stateMachine
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
	target := raft.NodeID(submission.Target)
	if submission.Command.GetOp() == raftpb.Op_GET && h.sim.reads == ReadModeReadIndex {
		return h.sim.submitWorkloadRead(target, submission)
	}
	data, err := proto.Marshal(submission.Command)
	if err != nil {
		return workloadpkg.SubmitNotLeader, fmt.Errorf("sim: marshal workload command: %w", err)
	}
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

func (s *Sim) submitWorkloadRead(target raft.NodeID, submission workloadpkg.Submission) (workloadpkg.SubmitStatus, error) {
	sn, ok := s.nodes[target]
	if !ok || sn.node == nil || sn.halted || sn.paused {
		s.record("workload client=%d seq=%d attempt=%d target=%d readindex_reject(unavailable)", submission.AttemptID.ClientID, submission.AttemptID.Seq, submission.AttemptID.Attempt, target)
		return workloadpkg.SubmitNotLeader, nil
	}
	token := workloadReadToken(target, submission.AttemptID)
	id := string(token)
	if _, exists := s.workload.pendingReads[id]; exists {
		return workloadpkg.SubmitNotLeader, fmt.Errorf("sim: duplicate ReadIndex token for attempt %+v", submission.AttemptID)
	}
	s.workload.pendingReads[id] = &pendingWorkloadRead{
		node:       target,
		submission: submission,
		token:      token,
	}
	s.workload.readByAttempt[submission.AttemptID] = id
	s.record("workload client=%d seq=%d attempt=%d target=%d readindex_request", submission.AttemptID.ClientID, submission.AttemptID.Seq, submission.AttemptID.Attempt, target)
	sn.node.RequestRead(token)
	s.processReady(sn, sn.node.Ready())
	return workloadpkg.SubmitAccepted, nil
}

func workloadReadToken(node raft.NodeID, attempt workloadpkg.AttemptID) []byte {
	token := make([]byte, 32)
	binary.BigEndian.PutUint64(token[0:8], uint64(node))
	binary.BigEndian.PutUint64(token[8:16], attempt.ClientID)
	binary.BigEndian.PutUint64(token[16:24], attempt.Seq)
	binary.BigEndian.PutUint64(token[24:32], attempt.Attempt)
	return token
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
	if tokenID, ok := s.workload.readByAttempt[attempt]; ok {
		pending := s.workload.pendingReads[tokenID]
		delete(s.workload.readByAttempt, attempt)
		delete(s.workload.pendingReads, tokenID)
		if pending != nil {
			if sn := s.nodes[pending.node]; sn != nil && sn.node != nil && !sn.halted {
				if sn.paused {
					s.workload.deferredReadCancels[pending.node] = append(
						s.workload.deferredReadCancels[pending.node], append([]byte(nil), pending.token...))
				} else {
					sn.node.CancelRead(pending.token)
					s.processReady(sn, sn.node.Ready())
				}
			}
		}
		s.record("workload client=%d seq=%d attempt=%d cancel(timeout readindex)", attempt.ClientID, attempt.Seq, attempt.Attempt)
		return
	}
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

func (s *Sim) flushDeferredWorkloadReadCancels(sn *simNode) {
	if s.workload == nil || sn == nil {
		return
	}
	tokens := s.workload.deferredReadCancels[sn.id]
	delete(s.workload.deferredReadCancels, sn.id)
	if len(tokens) == 0 || sn.node == nil || sn.halted || sn.paused {
		return
	}
	for _, token := range tokens {
		sn.node.CancelRead(token)
	}
	s.processReady(sn, sn.node.Ready())
	s.record("workload node=%d readindex_cancel_flush count=%d", sn.id, len(tokens))
}

func (s *Sim) discardDeferredWorkloadReadCancels(node raft.NodeID) {
	if s.workload != nil {
		delete(s.workload.deferredReadCancels, node)
	}
}

func (s *Sim) observeWorkloadReadState(node raft.NodeID, state raft.ReadState) {
	if s.workload == nil {
		return
	}
	id := string(state.Ctx)
	pending, ok := s.workload.pendingReads[id]
	if !ok || pending.node != node {
		return
	}
	if state.Canceled {
		s.removePendingWorkloadRead(id, pending)
		s.record("workload client=%d seq=%d attempt=%d target=%d readindex_canceled rejected=%t", pending.submission.AttemptID.ClientID, pending.submission.AttemptID.Seq, pending.submission.AttemptID.Attempt, node, state.Rejected)
		s.scheduleWorkloadReadResult(pending, workloadpkg.EventLeadershipLost, checker.Output{})
		return
	}
	pending.confirmed = true
	pending.index = state.Index
	s.record("workload client=%d seq=%d attempt=%d target=%d readindex_confirm index=%d", pending.submission.AttemptID.ClientID, pending.submission.AttemptID.Seq, pending.submission.AttemptID.Attempt, node, state.Index)
}

func (s *Sim) fulfillConfirmedWorkloadReads(sn *simNode) {
	if s.workload == nil || sn == nil || sn.sm == nil {
		return
	}
	for _, id := range sortedPendingReadIDs(s.workload.pendingReads) {
		pending := s.workload.pendingReads[id]
		if pending == nil || pending.node != sn.id || !pending.confirmed || pending.index > sn.lastApplied {
			continue
		}
		result, err := sn.sm.Read(pending.submission.Command.GetKey())
		s.removePendingWorkloadRead(id, pending)
		if err != nil {
			s.record("workload client=%d seq=%d attempt=%d target=%d readindex_read_error=%v", pending.submission.AttemptID.ClientID, pending.submission.AttemptID.Seq, pending.submission.AttemptID.Attempt, sn.id, err)
			s.scheduleWorkloadReadResult(pending, workloadpkg.EventLeadershipLost, checker.Output{})
			continue
		}
		s.record("workload client=%d seq=%d attempt=%d target=%d readindex_serve index=%d", pending.submission.AttemptID.ClientID, pending.submission.AttemptID.Seq, pending.submission.AttemptID.Attempt, sn.id, pending.index)
		s.scheduleWorkloadReadResult(pending, workloadpkg.EventApplied, successfulWorkloadOutput(result))
	}
}

func sortedPendingReadIDs(pending map[string]*pendingWorkloadRead) []string {
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *Sim) removePendingWorkloadRead(id string, pending *pendingWorkloadRead) {
	delete(s.workload.pendingReads, id)
	delete(s.workload.readByAttempt, pending.submission.AttemptID)
}

func (s *Sim) scheduleWorkloadReadResult(pending *pendingWorkloadRead, kind workloadpkg.EventKind, output checker.Output) {
	s.pushAt(s.now, &event{
		kind: eventWorkload,
		workload: &workloadEvent{event: workloadpkg.Event{
			Kind:      kind,
			AttemptID: pending.submission.AttemptID,
			Output:    output,
		}},
	})
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
