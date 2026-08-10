package sim

import (
	"errors"
	"fmt"

	"miniquorum/internal/lsm"
	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

func (s *Sim) handleEvent(ev *event) {
	switch ev.kind {
	case eventTick:
		if s.tickIsStale(ev) {
			s.record("tick node=%d skip(stale gen=%d current=%d)", ev.node, ev.generation, s.nodes[ev.node].generation)
			return
		}
		s.handleTick(ev.node)
	case eventMessage:
		s.handleMessage(ev.msg)
	case eventCrash:
		s.handleCrash(ev.node)
	case eventRestart:
		s.handleRestart(ev.node)
	case eventWorkload:
		s.handleWorkloadEvent(ev.workload)
	case eventFault:
		s.applyFault(ev.fault)
	}
}

func (s *Sim) handleTick(id raft.NodeID) {
	sn := s.nodes[id]
	if sn.node == nil || sn.halted {
		s.record("tick node=%d skip(down)", id)
		return
	}
	if sn.paused {
		// A GC pause / VM freeze does not stop the wall clock: the node's
		// tick schedule keeps advancing underneath it so ticking resumes at
		// the right cadence on FaultResume, but the tick itself has no
		// effect while frozen.
		s.record("tick node=%d skip(paused)", id)
		s.scheduleTick(id, s.now+s.nextTickDelay(sn))
		return
	}
	sn.node.Tick()
	s.record("tick node=%d", id)
	s.processReady(sn, sn.node.Ready())
	if sn.node != nil && !sn.halted {
		s.scheduleTick(id, s.now+s.nextTickDelay(sn))
	}
}

func (s *Sim) handleMessage(m *raftpb.Message) {
	from, to := raft.NodeID(m.GetFrom()), raft.NodeID(m.GetTo())
	if s.blocked(from, to) {
		s.record("msg %d->%d drop(partition)", from, to)
		return
	}
	sn, ok := s.nodes[to]
	if !ok || sn.node == nil || sn.halted {
		s.record("msg %d->%d drop(unreachable)", from, to)
		return
	}
	if sn.paused {
		s.record("msg %d->%d drop(paused)", from, to)
		return
	}
	sn.node.Step(m)
	s.record("msg %d->%d deliver", from, to)
	s.processReady(sn, sn.node.Ready())
}

func (s *Sim) handleCrash(id raft.NodeID) {
	sn := s.nodes[id]
	if store, ok := sn.storage.(*CrashStorage); ok && !store.Crashed() {
		if err := store.Crash(); err != nil {
			s.record("crash node=%d storage_error=%v", id, err)
		}
	}
	s.markProcessCrashed(sn)
	s.record("crash node=%d", id)
}

func (s *Sim) handleRestart(id raft.NodeID) {
	sn := s.nodes[id]
	if sn.node != nil && !sn.halted {
		// Restart only follows a crash (sn.node == nil) or a fail-stop
		// (sn.halted). A still-running process owns one current-generation
		// pending tick, so replacing it would create a second stream.
		s.record("restart node=%d skip(live)", id)
		return
	}
	if store, ok := sn.storage.(*CrashStorage); ok && store.Crashed() {
		if err := store.Recover(); err != nil {
			s.markProcessHalted(sn)
			s.record("restart node=%d recovery_error=%v fail-stop", id, err)
			return
		}
	}
	init, err := recoveredInitialState(sn.storage)
	if err != nil {
		s.markProcessHalted(sn)
		s.record("restart node=%d recovery_error=%v fail-stop", id, err)
		return
	}
	s.replaceProcess(sn, raft.NewNode(sn.cfg, init, sn.rnd))
	if sn.newStateMachine != nil {
		stateMachine, stateMachineErr := sn.newStateMachine()
		if stateMachineErr != nil {
			if errors.Is(stateMachineErr, lsm.ErrSimulatedCrash) {
				s.handleLSMProcessCrash(sn, "node=%d lsm_reopen_crash %v", sn.id, stateMachineErr)
				return
			}
			s.markProcessHalted(sn)
			s.record("restart node=%d state_machine_error=%v fail-stop", id, stateMachineErr)
			return
		}
		if replay, ok := stateMachine.(interface{ BeginReplay(uint64, uint64) error }); ok {
			if replayErr := replay.BeginReplay(init.Applied+1, sn.storage.LastIndex()); replayErr != nil {
				s.markProcessHalted(sn)
				s.record("restart node=%d replay_lifecycle_error=%v fail-stop", id, replayErr)
				return
			}
		}
		sn.sm = stateMachine
		sn.applied = nil
		sn.results = nil
		sn.lastApplied = init.Applied
	}
	s.record("restart node=%d", id)
	s.scheduleTick(id, s.now+s.nextTickDelay(sn))
}

func (s *Sim) markProcessCrashed(sn *simNode) {
	if crashing, ok := sn.sm.(interface{ Crash() error }); ok {
		if err := crashing.Crash(); err != nil {
			s.record("crash node=%d state_machine_error=%v", sn.id, err)
		}
	}
	sn.sm = nil
	sn.node = nil
	sn.halted = false
	s.invalidateTickStream(sn)
	// A crashed process cannot also be "paused" (frozen but alive); clear it
	// so a later restart starts fresh rather than inheriting a stale freeze
	// from before the crash.
	sn.paused = false
}

// markProcessHalted enters fail-stop and invalidates any pending tick owned
// by the process. This matters when the error is reached from a message or
// proposal: unlike a tick handler, those paths leave the process's next tick
// queued. Restart may run before that event drains, but its old generation
// can no longer become active or reschedule itself.
func (s *Sim) markProcessHalted(sn *simNode) {
	if !sn.halted {
		s.invalidateTickStream(sn)
	}
	sn.halted = true
}

// replaceProcess installs a rebuilt raft.Node under a fresh tick-stream
// generation. Crashes and fail-stops invalidate the stream they kill;
// replacement invalidates the process identity once more so every tick
// scheduled before this rebuild is structurally stale.
func (s *Sim) replaceProcess(sn *simNode, node *raft.Node) {
	s.invalidateTickStream(sn)
	sn.node = node
	sn.halted = false
}

func (s *Sim) invalidateTickStream(sn *simNode) {
	sn.generation++
}

// processReady applies the frozen Ready contract in the mandated order:
// Storage.Save, then send messages, then apply committed entries in order,
// then Node.Advance. A Save error is fail-stop for this node's batch: no
// send, no apply, no Advance.
func (s *Sim) processReady(sn *simNode, rd raft.Ready) {
	if err := sn.storage.Save(rd.HardState, rd.Entries); err != nil {
		if errors.Is(err, ErrCrashed) {
			var info CrashInfo
			if store, ok := sn.storage.(*CrashStorage); ok {
				info, _ = store.LastCrash()
			}
			s.markProcessCrashed(sn)
			s.record("node=%d save_crash %v", sn.id, err)
			s.recordCrashFiring(sn.id, info)
			s.scheduleRestartAfterStorageCrash(sn.id, info)
			return
		}
		s.markProcessHalted(sn)
		s.record("node=%d save_error %v fail-stop", sn.id, err)
		return
	}
	for _, m := range rd.Messages {
		s.observeLeader(m)
		s.scheduleMessage(m)
	}
	if store, ok := sn.storage.(*CrashStorage); ok && store.CrashIfArmed() {
		info, _ := store.LastCrash()
		s.markProcessCrashed(sn)
		s.record("node=%d after_send_crash save=%d", sn.id, info.Save)
		s.recordCrashFiring(sn.id, info)
		s.scheduleRestartAfterStorageCrash(sn.id, info)
		return
	}
	for i := range rd.CommittedEntries {
		var result statemachine.Result
		if sn.sm != nil {
			var err error
			result, err = sn.sm.Apply(&rd.CommittedEntries[i])
			if err != nil {
				if errors.Is(err, lsm.ErrSimulatedCrash) {
					s.handleLSMProcessCrash(sn, "node=%d lsm_apply_crash index=%d %v", sn.id, rd.CommittedEntries[i].Index, err)
					return
				}
				s.markProcessHalted(sn)
				s.record("node=%d apply_error index=%d %v fail-stop", sn.id, rd.CommittedEntries[i].Index, err)
				return
			}
		}
		s.observeWorkloadApply(sn.id, &rd.CommittedEntries[i], result)
		sn.applied = append(sn.applied, raftpb.Entry{
			Index: rd.CommittedEntries[i].Index,
			Term:  rd.CommittedEntries[i].Term,
			Type:  rd.CommittedEntries[i].Type,
			Data:  append([]byte(nil), rd.CommittedEntries[i].Data...),
		})
		sn.results = append(sn.results, statemachine.Result{
			Value: append([]byte(nil), result.Value...),
			Found: result.Found,
		})
		sn.lastApplied = rd.CommittedEntries[i].Index
	}
	sn.node.Advance()
	s.record("node=%d ready hardstate=%v msgs=%d committed=%d", sn.id, rd.HardState != nil, len(rd.Messages), len(rd.CommittedEntries))
}

// handleLSMProcessCrash is the common host boundary for a SimFS crash while
// applying a committed entry or while reopening the LSM engine. Both cases
// identify the fired policy from the final SimFS event and consume it exactly
// once before scheduling the requested causal recovery.
func (s *Sim) handleLSMProcessCrash(sn *simNode, format string, args ...any) {
	s.markProcessCrashed(sn)
	s.record(format, args...)
	s.scheduleRestartAfterLSMCrash(sn.id)
}

func (s *Sim) scheduleRestartAfterLSMCrash(id raft.NodeID) {
	key, ok := s.firedLSMCrashKey(id)
	if !ok {
		return
	}
	restart, ok := s.lsmRestartOnCrash[key]
	if !ok {
		return
	}
	delete(s.lsmRestartOnCrash, key)
	if !restart {
		return
	}
	s.pushAt(s.now, &event{kind: eventRestart, node: id})
	s.record("node=%d lsm_restart_after_crash scheduled", id)
}

func (s *Sim) firedLSMCrashKey(id raft.NodeID) (lsmCrashDirectiveKey, bool) {
	fs := s.lsmFS[id]
	if fs == nil {
		return lsmCrashDirectiveKey{}, false
	}
	events := fs.Events()
	if len(events) == 0 {
		return lsmCrashDirectiveKey{}, false
	}
	e := events[len(events)-1]
	point := lsm.SimBeforeOperation
	if e.Completed {
		point = lsm.SimAfterOperation
	}
	// FSEvent supplies the operation, its per-op occurrence, and whether the
	// effect completed, so this exact policy key is derived from crash evidence
	// rather than inferred from schedule order or a node-global counter.
	key := lsmCrashDirectiveKey{node: id, op: e.Op, occurrence: e.Occurrence, point: point}
	if _, ok := s.lsmRestartOnCrash[key]; ok {
		return key, true
	}
	return lsmCrashDirectiveKey{}, false
}

// recordCrashFiring appends the structured record of a storage crash that
// just fired. Called only from the two processReady crash branches, so every
// entry corresponds to exactly one save_crash/after_send_crash trace line.
func (s *Sim) recordCrashFiring(id raft.NodeID, info CrashInfo) {
	s.crashFirings = append(s.crashFirings, CrashFiring{
		Time:  s.now,
		Node:  id,
		Save:  info.Save,
		Point: info.Point,
	})
}

// scheduleRestartAfterStorageCrash causally bridges a serialized crash
// directive to host recovery. It is called only after CrashStorage reports
// the crash that actually fired. A matching RestartAfterCrash directive is
// consumed and enqueues eventRestart at s.now, so even a crash on the run's
// final virtual-time boundary is recovered before Run returns. Queueing the
// restart (rather than rebuilding inline) leaves the current tick/message/
// proposal handler seeing a down process and therefore prevents it from
// scheduling a second tick stream.
func (s *Sim) scheduleRestartAfterStorageCrash(id raft.NodeID, info CrashInfo) {
	key := crashDirectiveKey{node: id, save: info.Save}
	point, ok := s.restartAfterCrash[key]
	if !ok || point != info.Point {
		return
	}
	delete(s.restartAfterCrash, key)
	s.pushAt(s.now, &event{kind: eventRestart, node: id})
	s.record("node=%d restart_after_crash save=%d scheduled", id, info.Save)
}

func recoveredInitialState(store storageReader) (raft.InitialState, error) {
	hard, err := store.HardState()
	if err != nil {
		return raft.InitialState{}, err
	}
	snapshot, err := store.Snapshot()
	if err != nil {
		return raft.InitialState{}, err
	}
	compacted, err := store.Compacted()
	if err != nil {
		return raft.InitialState{}, err
	}
	if err := validateRecoveredMetadata(snapshot, compacted); err != nil {
		return raft.InitialState{}, fmt.Errorf("sim: recovery metadata: %w", err)
	}
	first, last := store.FirstIndex(), store.LastIndex()
	if compacted.Index == ^uint64(0) {
		return raft.InitialState{}, fmt.Errorf("sim: compacted index overflows half-open range")
	}
	wantFirst := compacted.Index + 1
	if first != wantFirst {
		return raft.InitialState{}, fmt.Errorf("sim: first index %d, want compacted index + 1 = %d", first, wantFirst)
	}
	if last < snapshot.Index {
		return raft.InitialState{}, fmt.Errorf("sim: last index %d precedes snapshot index %d", last, snapshot.Index)
	}
	var entries []raftpb.Entry
	if last >= first {
		if last == ^uint64(0) {
			return raft.InitialState{}, fmt.Errorf("sim: last index overflows half-open range")
		}
		entries, err = store.Entries(first, last+1)
		if err != nil {
			return raft.InitialState{}, err
		}
	}
	if err := validateRecoveredEntries(snapshot, compacted, first, last, entries); err != nil {
		return raft.InitialState{}, fmt.Errorf("sim: recovery log: %w", err)
	}
	return raft.InitialState{HardState: hard, Entries: entries, Snapshot: compacted, Applied: snapshot.Index}, nil
}

func validateRecoveredMetadata(snapshot, compacted raft.SnapshotMeta) error {
	for _, item := range []struct {
		name string
		meta raft.SnapshotMeta
	}{{"snapshot", snapshot}, {"compacted", compacted}} {
		if item.meta.Index == 0 && item.meta.Term != 0 || item.meta.Index != 0 && item.meta.Term == 0 {
			return fmt.Errorf("%s metadata has invalid index/term %d/%d", item.name, item.meta.Index, item.meta.Term)
		}
	}
	if compacted.Index > snapshot.Index {
		return fmt.Errorf("compacted index %d exceeds snapshot index %d", compacted.Index, snapshot.Index)
	}
	if compacted.Index == snapshot.Index && compacted.Term != snapshot.Term {
		return fmt.Errorf("compacted term %d differs from snapshot term %d at index %d", compacted.Term, snapshot.Term, snapshot.Index)
	}
	return nil
}

func validateRecoveredEntries(snapshot, compacted raft.SnapshotMeta, first, last uint64, entries []raftpb.Entry) error {
	wantCount := uint64(0)
	if last >= first {
		wantCount = last - first + 1
	}
	if uint64(len(entries)) != wantCount {
		return fmt.Errorf("loaded %d entries for contiguous range [%d,%d], want %d", len(entries), first, last, wantCount)
	}
	for offset := range entries {
		want := first + uint64(offset)
		if entries[offset].Index != want {
			return fmt.Errorf("entry offset %d has index %d, want %d", offset, entries[offset].Index, want)
		}
	}
	if compacted.Index < snapshot.Index {
		offset := snapshot.Index - first
		if offset >= uint64(len(entries)) {
			return fmt.Errorf("retained overlap is missing snapshot index %d", snapshot.Index)
		}
		if entries[offset].Term != snapshot.Term {
			return fmt.Errorf("snapshot term %d at index %d differs from retained log term %d", snapshot.Term, snapshot.Index, entries[offset].Term)
		}
	}
	return nil
}

type storageReader interface {
	HardState() (raft.HardState, error)
	Snapshot() (raft.SnapshotMeta, error)
	Compacted() (raft.SnapshotMeta, error)
	Entries(lo, hi uint64) ([]raftpb.Entry, error)
	FirstIndex() uint64
	LastIndex() uint64
}

// observeLeader records term/leader observations strictly from a
// deterministic Raft output: only a Leader ever emits AppendEntriesReq
// (heartbeats), per the phase-1 spec, so seeing one from a node is proof of
// that node's leadership for the message's term. No raft-internal role
// field is read.
func (s *Sim) observeLeader(m *raftpb.Message) {
	if m.GetAppendEntries() != nil {
		s.leaders.observe(m.GetTerm(), raft.NodeID(m.GetFrom()))
	}
}
