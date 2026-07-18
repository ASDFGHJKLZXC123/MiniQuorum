package sim

import (
	"container/heap"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	raftpb "miniquorum/proto"
)

// ---------------------------------------------------------------------
// Network: drop, duplication, reorder
// ---------------------------------------------------------------------

// TestScheduleMessageReordersUnderRandomDelay is the phase-4 out-of-order
// canary: reorder is supposed to fall out of the existing randomized-delay
// mechanism with no new code, but the spec requires proving it actually
// happens rather than trusting it's theoretical. It schedules many messages
// at the same virtual instant and checks the delivery order the event queue
// produces is not simply send order.
func TestScheduleMessageReordersUnderRandomDelay(t *testing.T) {
	s := newTestSim(t, 1, 2)
	const n = 40
	for i := 0; i < n; i++ {
		s.scheduleMessage(&raftpb.Message{From: 1, To: 2, Term: uint64(i)})
	}

	var deliveryOrder []uint64
	for s.queue.Len() > 0 {
		ev := heap.Pop(&s.queue).(*event)
		if ev.kind == eventMessage {
			deliveryOrder = append(deliveryOrder, ev.msg.GetTerm())
		}
	}
	if len(deliveryOrder) != n {
		t.Fatalf("queued message events = %d, want %d", len(deliveryOrder), n)
	}

	inverted := false
	for i := 1; i < len(deliveryOrder); i++ {
		if deliveryOrder[i] < deliveryOrder[i-1] {
			inverted = true
			break
		}
	}
	if !inverted {
		t.Fatalf("delivery order = %v, want at least one inversion relative to send order %d..%d (reorder must actually occur, not just be theoretically possible)", deliveryOrder, 0, n-1)
	}
}

// TestScheduleMessageDropsAtRateOne is the unit-level half of the drop
// canary: at dropRate 1.0 a scheduled message never reaches the queue and is
// traced as such.
func TestScheduleMessageDropsAtRateOne(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.dropRate = 1.0
	before := s.queue.Len()

	s.scheduleMessage(&raftpb.Message{From: 1, To: 2, Term: 7})

	if got := s.queue.Len(); got != before {
		t.Fatalf("queue len = %d, want unchanged %d (a rate-1.0 drop must never be queued)", got, before)
	}
	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "drop(fault)") {
		t.Fatalf("last trace entry = %q, want a drop(fault) record", got)
	}
}

// TestScheduleMessageDuplicatesAtRateOneWithIndependentDelay is the
// unit-level half of the duplication canary: at dupRate 1.0 a surviving
// message is enqueued twice, as an independent clone (not an aliased
// pointer) so mutating one can never affect the other, each with its own
// delay draw per "duplication re-enqueued at a new delay."
func TestScheduleMessageDuplicatesAtRateOneWithIndependentDelay(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.dupRate = 1.0

	s.scheduleMessage(&raftpb.Message{From: 1, To: 2, Term: 7})

	var msgEvents []*event
	for s.queue.Len() > 0 {
		ev := heap.Pop(&s.queue).(*event)
		if ev.kind == eventMessage {
			msgEvents = append(msgEvents, ev)
		}
	}
	if len(msgEvents) != 2 {
		t.Fatalf("queued message events = %d, want 2 (original + duplicate)", len(msgEvents))
	}
	if msgEvents[0].msg == msgEvents[1].msg {
		t.Fatal("duplicate shares the original's *Message pointer, want an independent clone")
	}
	for _, ev := range msgEvents {
		if ev.msg.GetFrom() != 1 || ev.msg.GetTo() != 2 || ev.msg.GetTerm() != 7 {
			t.Fatalf("duplicate content = %+v, want From=1 To=2 Term=7", ev.msg)
		}
	}
}

// TestDuplicationCanaryDeliversSameMessageTwiceWithoutInvariantFailure is the
// phase-4 duplication canary end to end: a duplicated message is actually
// delivered twice through the ordinary handleMessage path (not just queued
// twice), and doing so does not trip any registered invariant. Run(15) caps
// the window before the first real tick (t=50) so only the two
// duplicate-delivery events, and whatever they provoke, are in play.
func TestDuplicationCanaryDeliversSameMessageTwiceWithoutInvariantFailure(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.RegisterInvariant(SingleLeaderPerTerm)
	s.dupRate = 1.0

	s.scheduleMessage(heartbeatMessage(1, 2, 1))
	if err := s.Run(15); err != nil {
		t.Fatalf("Run() error = %v (invariants must hold under duplication)\ntrace: %v", err, s.Trace())
	}

	delivered := 0
	for _, line := range s.trace {
		if strings.Contains(line, "msg 1->2 deliver") {
			delivered++
		}
	}
	if delivered != 2 {
		t.Fatalf("msg 1->2 deliver count = %d, want 2 (the duplicate must be independently delivered)\ntrace: %v", delivered, s.trace)
	}
	found := false
	for _, line := range s.trace {
		if strings.Contains(line, "duplicate(fault)") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("trace has no duplicate(fault) record\ntrace: %v", s.trace)
	}
}

// ---------------------------------------------------------------------
// Pause/unpause and clock skew
// ---------------------------------------------------------------------

// TestPauseSkipsTicksButKeepsReschedulingCadence verifies a paused node's
// tick has no effect (no Ready is processed, so no messages are produced)
// while the tick schedule itself keeps advancing so resuming does not lose
// cadence.
func TestPauseSkipsTicksButKeepsReschedulingCadence(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.Pause(1)

	before := s.queue.Len()
	s.handleTick(1)

	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "skip(paused)") {
		t.Fatalf("last trace entry = %q, want a paused tick skip", got)
	}
	if got := s.queue.Len(); got != before+1 {
		t.Fatalf("queue len = %d, want %d (the next tick must still be scheduled while paused)", got, before+1)
	}
}

// TestPauseDropsInboundDelivery verifies "no deliveries while paused": a
// message addressed to a paused node is dropped, not queued for later.
func TestPauseDropsInboundDelivery(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.Pause(2)

	s.handleMessage(heartbeatMessage(1, 2, 1))

	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "drop(paused)") {
		t.Fatalf("last trace entry = %q, want drop(paused)", got)
	}
}

// TestResumeRestoresTicksAndDeliveries verifies Resume reverses Pause for
// both ticks and deliveries.
func TestResumeRestoresTicksAndDeliveries(t *testing.T) {
	s := newTestSim(t, 1, 2)
	s.Pause(1)
	s.Resume(1)

	s.handleTick(1)
	if got := s.trace[len(s.trace)-1]; strings.Contains(got, "skip(paused)") {
		t.Fatalf("last trace entry = %q, want a real tick after Resume", got)
	}
}

// TestSetClockSkewRejectsOutOfRangeMultiplier verifies the 0.5x-2x bound.
func TestSetClockSkewRejectsOutOfRangeMultiplier(t *testing.T) {
	s := newTestSim(t, 1, 2)
	for _, bad := range []float64{0, 0.49, 2.01, 5, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := s.SetClockSkew(1, bad); err == nil {
			t.Fatalf("SetClockSkew(%v) error = nil, want out-of-range rejection", bad)
		}
	}
	// A NaN multiplier must never reach sn.tickMultiplier: NaN compares
	// false against every bound, so a plain range check alone would let it
	// through and nextTickDelay would then cast a NaN float64 to
	// VirtualTime, an implementation-defined conversion.
	if got := s.nodes[1].tickMultiplier; got != 0 {
		t.Fatalf("tickMultiplier after every SetClockSkew rejection = %v, want unchanged 0 (unset)", got)
	}
}

// TestClockSkewScalesTickInterval verifies nextTickDelay applies the
// per-node multiplier deterministically: a 2x node ticks twice as often (a
// halved interval), a 0.5x node ticks half as often (a doubled interval) —
// per the phase-4 contract's "0.5x-2x per-node tick-rate multiplier" (with a
// 50ms base, 2x ticks every 25ms and 0.5x ticks every 100ms).
func TestClockSkewScalesTickInterval(t *testing.T) {
	s := newTestSim(t, 1, 2)
	base := s.nextTickDelay(s.nodes[1])

	if err := s.SetClockSkew(1, 2.0); err != nil {
		t.Fatalf("SetClockSkew(2.0) error = %v", err)
	}
	if got, want := s.nextTickDelay(s.nodes[1]), base/2; got != want {
		t.Fatalf("nextTickDelay at 2x = %d, want %d (2x is a faster tick rate: half the interval)", got, want)
	}

	if err := s.SetClockSkew(1, 0.5); err != nil {
		t.Fatalf("SetClockSkew(0.5) error = %v", err)
	}
	if got, want := s.nextTickDelay(s.nodes[1]), base*2; got != want {
		t.Fatalf("nextTickDelay at 0.5x = %d, want %d (0.5x is a slower tick rate: double the interval)", got, want)
	}
	// node 2 is unaffected by node 1's skew.
	if got := s.nextTickDelay(s.nodes[2]); got != base {
		t.Fatalf("unrelated node's nextTickDelay = %d, want unchanged %d", got, base)
	}
}

// TestClockSkewMatchesPhase4ExampleNumbers pins the phase-4 spec's own
// worked example: with the 50ms default base interval, a 2x multiplier
// ticks every 25ms and a 0.5x multiplier ticks every 100ms.
func TestClockSkewMatchesPhase4ExampleNumbers(t *testing.T) {
	s := newTestSim(t, 1)
	if s.tickInterval != 50 {
		t.Fatalf("default tickInterval = %d, want 50 (phase-4 spec's example base)", s.tickInterval)
	}

	if err := s.SetClockSkew(1, 2.0); err != nil {
		t.Fatalf("SetClockSkew(2.0) error = %v", err)
	}
	if got := s.nextTickDelay(s.nodes[1]); got != 25 {
		t.Fatalf("nextTickDelay at 2x with a 50ms base = %d, want 25", got)
	}

	if err := s.SetClockSkew(1, 0.5); err != nil {
		t.Fatalf("SetClockSkew(0.5) error = %v", err)
	}
	if got := s.nextTickDelay(s.nodes[1]); got != 100 {
		t.Fatalf("nextTickDelay at 0.5x with a 50ms base = %d, want 100", got)
	}
}

// TestProposeOnPausedNodeIsRejectedNotPersistedSentOrApplied verifies "a
// paused node receives nothing" extends to Propose: pausing a real elected
// leader and proposing to it must reject before touching raft.Node at all,
// not persist, not enqueue a message, not grow the leader's log.
func TestProposeOnPausedNodeIsRejectedNotPersistedSentOrApplied(t *testing.T) {
	s := phase1Sim(t, 2026071801)
	_, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	beforeQueue := s.queue.Len()
	beforeLog := s.Log(leader)
	beforeLastApplied := s.LastApplied(leader)

	s.Pause(leader)
	index, term, ok := s.Propose(leader, []byte("data"))

	if ok || index != 0 || term != 0 {
		t.Fatalf("Propose(paused leader) = (index=%d, term=%d, isLeader=%t), want (0, 0, false)", index, term, ok)
	}
	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "reject(unavailable)") {
		t.Fatalf("last trace entry = %q, want a reject(unavailable) record", got)
	}
	if got := s.queue.Len(); got != beforeQueue {
		t.Fatalf("queue len = %d, want unchanged %d (a paused Propose must not send anything)", got, beforeQueue)
	}
	if got := s.Log(leader); !reflect.DeepEqual(got, beforeLog) {
		t.Fatalf("leader log after a paused Propose = %v, want unchanged %v", got, beforeLog)
	}
	if got := s.LastApplied(leader); got != beforeLastApplied {
		t.Fatalf("leader LastApplied after a paused Propose = %d, want unchanged %d", got, beforeLastApplied)
	}
}

// ---------------------------------------------------------------------
// Asymmetric directed partitions and symmetric group splits
// ---------------------------------------------------------------------

// TestPartitionGroupsBlocksBothDirectionsAcrossGroupsOnly verifies the
// symmetric-split fault: cross-group traffic is blocked in both directions,
// same-group traffic is untouched.
func TestPartitionGroupsBlocksBothDirectionsAcrossGroupsOnly(t *testing.T) {
	s := newTestSim(t, 1, 2, 3)
	s.PartitionGroups([][]raft.NodeID{{1}, {2, 3}})

	if !s.blocked(1, 2) || !s.blocked(2, 1) || !s.blocked(1, 3) || !s.blocked(3, 1) {
		t.Fatal("cross-group pairs must be blocked in both directions")
	}
	if s.blocked(2, 3) || s.blocked(3, 2) {
		t.Fatal("same-group pair 2,3 must remain connected")
	}

	s.HealGroups([][]raft.NodeID{{1}, {2, 3}})
	if s.blocked(1, 2) || s.blocked(2, 1) || s.blocked(1, 3) || s.blocked(3, 1) {
		t.Fatal("HealGroups must reverse every block PartitionGroups installed")
	}
}

// TestFaultPartitionIsAsymmetricByDefault confirms the schedule-driven
// FaultPartition event, like Sim.Partition, blocks exactly the directed pair
// it names, distinct from the symmetric FaultPartitionGroups above.
func TestFaultPartitionIsAsymmetricByDefault(t *testing.T) {
	schedule := FaultSchedule{Events: []FaultEvent{
		{Time: 10, Kind: FaultPartition, From: 1, To: 2},
	}}
	s, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 2}}, schedule)
	if err != nil {
		t.Fatalf("NewFaultSim() error = %v", err)
	}
	if err := s.Run(10); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !s.blocked(1, 2) {
		t.Fatal("1->2 must be blocked after the FaultPartition event fires")
	}
	if s.blocked(2, 1) {
		t.Fatal("2->1 must remain open: FaultPartition is directed, not symmetric")
	}
}

// ---------------------------------------------------------------------
// Crash/restart wired to Phase 3 crash points via the schedule
// ---------------------------------------------------------------------

// TestFaultScheduleSelectsCrashPointAndScriptedRestartRebuildsNode drives a
// node to its first term-bearing Save exactly as the Phase 3 matrix does,
// installs a schedule.Crashes directive picking CrashAfterSyncBeforeSend for
// that save, and schedules a FaultRestart event: the schedule alone (no
// imperative ScheduleCrash/ScheduleRestart calls) must both fire the crash
// at the right point and rebuild the node afterward.
func TestFaultScheduleSelectsCrashPointAndScriptedRestartRebuildsNode(t *testing.T) {
	const target = raft.NodeID(1)
	baseline, err := newCrashSim(Config{Seed: 55001, NodeIDs: []raft.NodeID{1, 2, 3}}, FaultSchedule{})
	if err != nil {
		t.Fatalf("newCrashSim() error = %v", err)
	}
	baselineStore := phase3Store(t, baseline, target)
	for drive := 0; ; drive++ {
		if drive > 100 {
			t.Fatalf("target never persisted a term within 100 direct ticks")
		}
		baseline.nodes[target].node.Tick()
		baseline.processReady(baseline.nodes[target], baseline.nodes[target].node.Ready())
		hard, _ := baselineStore.HardState()
		if hard.Term > 0 {
			break
		}
	}
	crashSave := baselineStore.SaveCount()

	schedule := FaultSchedule{
		Crashes: []CrashDirective{{Node: target, Save: crashSave, Point: CrashAfterSyncBeforeSend}},
		Events:  []FaultEvent{{Time: 100_000, Kind: FaultRestart, Node: target}},
	}
	s, err := NewFaultSim(Config{Seed: 55001, NodeIDs: []raft.NodeID{1, 2, 3}}, schedule)
	if err != nil {
		t.Fatalf("NewFaultSim() error = %v", err)
	}
	s.RegisterInvariant(SingleLeaderPerTerm)
	store := phase3Store(t, s, target)
	for drive := 0; !store.Crashed(); drive++ {
		if drive > 100 {
			t.Fatalf("target never crashed within 100 direct ticks; planned save %d", crashSave)
		}
		s.nodes[target].node.Tick()
		s.processReady(s.nodes[target], s.nodes[target].node.Ready())
	}
	info, _ := store.LastCrash()
	if info.Save != crashSave || info.Point != CrashAfterSyncBeforeSend {
		t.Fatalf("crash = %+v, want save %d at %s (the schedule must select this crash point)", info, crashSave, CrashAfterSyncBeforeSend)
	}
	if s.nodes[target].node != nil {
		t.Fatal("node still live immediately after the scheduled crash, want nil")
	}

	if err := s.Run(100_000); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if s.nodes[target].node == nil {
		t.Fatal("node still nil after the scheduled FaultRestart event's virtual time, want rebuilt")
	}
	if store.Crashed() {
		t.Fatal("storage still reports crashed after FaultRestart, want recovered")
	}
}

// TestCrashDirectiveRestartAfterCrashIsCausalAtRunBoundary proves the host
// side of the generated-crash guarantee without relying on Save timing or a
// seed sample. At every Phase 3 crash point, the first Save's serialized
// policy must enqueue recovery at that same virtual time. Run(0) therefore
// recovers it even though there is no later heuristic restart window.
func TestCrashDirectiveRestartAfterCrashIsCausalAtRunBoundary(t *testing.T) {
	for _, point := range []CrashPoint{CrashBeforeSync, CrashAfterSyncBeforeSend, CrashAfterSend} {
		t.Run(point.String(), func(t *testing.T) {
			schedule := FaultSchedule{Crashes: []CrashDirective{{
				Node:              1,
				Save:              1,
				Point:             point,
				RetainUnsynced:    RetainAllUnsynced,
				RestartAfterCrash: true,
			}}}
			s, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 2, 3}}, schedule)
			if err != nil {
				t.Fatalf("NewFaultSim() error = %v", err)
			}
			store := phase3Store(t, s, 1)

			s.handleTick(1)
			info, crashed := store.LastCrash()
			if !crashed || info.Save != 1 || info.Point != point {
				t.Fatalf("LastCrash() = %+v, %t, want save 1 at %s", info, crashed, point)
			}
			if s.nodes[1].node != nil || !store.Crashed() {
				t.Fatalf("before queued recovery: node_live=%t storage_crashed=%t, want false/true", s.nodes[1].node != nil, store.Crashed())
			}

			if err := s.Run(0); err != nil {
				t.Fatalf("Run(0) error = %v", err)
			}
			if s.nodes[1].node == nil || store.Crashed() {
				t.Fatalf("after same-time causal recovery: node_live=%t storage_crashed=%t, want true/false; trace=%v", s.nodes[1].node != nil, store.Crashed(), s.Trace())
			}
			if _, pending := s.restartAfterCrash[crashDirectiveKey{node: 1, save: 1}]; pending {
				t.Fatal("fired RestartAfterCrash directive remains pending, want it consumed exactly once")
			}
		})
	}
}

// failFirstMatchingApply wraps the ordinary map state machine and injects
// exactly one Apply error for marker. The shared failed flag means a fresh
// state machine built by restart accepts the durably logged command, so the
// regression can observe the restarted tick stream for several ticks rather
// than immediately fail-stopping on the same entry again.
type failFirstMatchingApply struct {
	statemachine.StateMachine
	marker string
	failed *bool
}

func (sm *failFirstMatchingApply) Apply(entry *raftpb.Entry) (statemachine.Result, error) {
	if entry.GetType() == raftpb.EntryType_NORMAL && string(entry.GetData()) == sm.marker && !*sm.failed {
		*sm.failed = true
		return statemachine.Result{}, fmt.Errorf("forced apply failure")
	}
	return sm.StateMachine.Apply(entry)
}

// TestApplyErrorRestartInvalidatesPendingTickStream reaches an Apply
// fail-stop through ordinary single-node election and public Sim.Propose.
// Propose runs between scheduled ticks, so the old tick is still queued when
// public ScheduleRestart rebuilds the halted process. That old event must be
// stale, while exactly one new-generation tick continues rescheduling.
func TestApplyErrorRestartInvalidatesPendingTickStream(t *testing.T) {
	s := newTestSim(t, 1)
	commandData, err := proto.Marshal(&raftpb.Command{
		ClientId: 1,
		Seq:      1,
		Op:       raftpb.Op_PUT,
		Key:      []byte("tick-stream"),
		Value:    []byte("apply-error"),
	})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	var failed bool
	var factoryCalls int
	s.setStateMachineFactory(func() statemachine.StateMachine {
		factoryCalls++
		return &failFirstMatchingApply{
			StateMachine: mapsm.New(),
			marker:       string(commandData),
			failed:       &failed,
		}
	})

	var proposed bool
	var generationBeforeFailure uint64
	for attempt := 0; attempt < phase1MaxTimeout+1; attempt++ {
		runFor(t, s, s.tickInterval)
		generationBeforeFailure = s.nodes[1].generation
		index, term, isLeader := s.Propose(1, commandData)
		if !isLeader {
			continue
		}
		if index == 0 || term == 0 {
			t.Fatalf("leader proposal = (index=%d term=%d), want non-zero identity", index, term)
		}
		proposed = true
		break
	}
	if !proposed || !failed || !s.nodes[1].halted {
		t.Fatalf("ordinary proposal did not reach injected Apply fail-stop: proposed=%t failed=%t halted=%t trace=%v", proposed, failed, s.nodes[1].halted, s.Trace())
	}
	trace := s.Trace()
	if len(trace) < 2 || !strings.Contains(trace[len(trace)-2], "propose node=1") || !strings.Contains(trace[len(trace)-1], "apply_error") {
		t.Fatalf("fail-stop was not reached directly through Propose: trace tail=%v", trace[max(0, len(trace)-3):])
	}

	assertFailStopRestartHasOneTickStream(t, s, 1, generationBeforeFailure)
	if factoryCalls != 2 {
		t.Fatalf("state-machine factory calls = %d, want 2 (initial process plus restarted process)", factoryCalls)
	}
}

// TestSaveErrorRestartInvalidatesPendingTickStream covers the other
// fail-stop class with ready_test.go's smallest existing storage seam. An
// arbitrary non-ErrCrashed Save error cannot be configured through public
// Sim construction, so this host-level test injects spyStorage, then uses
// public ScheduleRestart for the lifecycle edge under test.
func TestSaveErrorRestartInvalidatesPendingTickStream(t *testing.T) {
	s, err := NewSim(Config{
		Seed:            1,
		NodeIDs:         []raft.NodeID{1},
		ElectionTickMin: 1,
		ElectionTickMax: 1,
	})
	if err != nil {
		t.Fatalf("NewSim() error = %v", err)
	}
	sn := s.nodes[1]
	sn.sm = mapsm.New()
	sn.node.Tick()
	rd := sn.node.Ready()
	if rd.HardState == nil || len(rd.Entries) == 0 || len(rd.CommittedEntries) == 0 {
		t.Fatalf("single-node election Ready is vacuous: %+v", rd)
	}
	rd.Messages = append(rd.Messages, heartbeatMessage(1, 1, 1))

	spy := newSpyStorage()
	spy.failSave = true
	sn.storage = spy
	generationBeforeFailure := sn.generation
	queueBeforeFailure := s.queue.Len()

	s.processReady(sn, rd)

	if !sn.halted || !strings.Contains(s.trace[len(s.trace)-1], "save_error") {
		t.Fatalf("non-ErrCrashed Save did not fail-stop the process: halted=%t trace=%v", sn.halted, s.Trace())
	}
	if s.queue.Len() != queueBeforeFailure {
		t.Fatalf("queue len after failed Save = %d, want unchanged %d (no send for failed Ready)", s.queue.Len(), queueBeforeFailure)
	}
	if applied := sn.sm.AppliedIndex(); applied != 0 {
		t.Fatalf("state machine applied through index %d, want 0 (no apply for failed Ready)", applied)
	}
	stillPending := sn.node.Ready()
	if stillPending.HardState == nil || len(stillPending.Entries) != len(rd.Entries) || len(stillPending.CommittedEntries) != len(rd.CommittedEntries) {
		t.Fatalf("Ready was advanced after failed Save: pending=(hardstate=%t entries=%d committed=%d), want (%t,%d,%d)", stillPending.HardState != nil, len(stillPending.Entries), len(stillPending.CommittedEntries), rd.HardState != nil, len(rd.Entries), len(rd.CommittedEntries))
	}
	spy.failSave = false

	assertFailStopRestartHasOneTickStream(t, s, 1, generationBeforeFailure)
}

// assertFailStopRestartHasOneTickStream verifies the lifecycle invariant
// shared by Apply and Save fail-stops. A tick event is a live stream exactly
// when its generation matches the node's current generation: firing it will
// schedule the stream's successor. Stale physical events may remain in the
// heap, but must be observed once as stale and must never reschedule.
func assertFailStopRestartHasOneTickStream(t *testing.T, s *Sim, id raft.NodeID, generationBeforeFailure uint64) {
	t.Helper()
	sn := s.nodes[id]
	if !sn.halted {
		t.Fatalf("node %d is not halted before restart", id)
	}
	queuedBeforeRestart := pendingTickEvents(s, id)
	if len(queuedBeforeRestart) != 1 {
		t.Fatalf("pending physical ticks before restart = %d, want 1 non-vacuous pre-fail-stop tick", len(queuedBeforeRestart))
	}
	stale := queuedBeforeRestart[0]
	if stale.time <= s.Now() {
		t.Fatalf("pre-fail-stop tick time = %d, want later than current time %d", stale.time, s.Now())
	}
	generationAfterFailure := sn.generation
	failStopInvalidated := generationAfterFailure != generationBeforeFailure

	s.ScheduleRestart(id, s.Now())
	if err := s.Run(s.Now()); err != nil {
		t.Fatalf("Run(restart at t=%d) error = %v", s.Now(), err)
	}
	if sn.node == nil || sn.halted {
		t.Fatalf("node %d after public ScheduleRestart: live=%t halted=%t, want live and running", id, sn.node != nil, sn.halted)
	}
	live := livePendingTickEvents(s, id)
	if len(live) != 1 {
		t.Fatalf("live pending ticks after public ScheduleRestart = %d, want exactly 1; physical ticks=%d generations=(before_failure=%d after_failure=%d current=%d) trace=%v", len(live), len(pendingTickEvents(s, id)), generationBeforeFailure, generationAfterFailure, sn.generation, s.Trace())
	}
	if !failStopInvalidated {
		t.Fatalf("fail-stop left generation at %d, want the killed tick stream structurally invalidated", generationAfterFailure)
	}
	if sn.generation == generationAfterFailure {
		t.Fatalf("restart left generation at %d, want process replacement to start a new tick-stream generation", sn.generation)
	}
	if stale.generation == sn.generation {
		t.Fatalf("pre-restart tick generation %d is still current after restart", stale.generation)
	}

	for tick := 1; tick <= 5; tick++ {
		live = livePendingTickEvents(s, id)
		if len(live) != 1 {
			t.Fatalf("before subsequent tick %d: live pending ticks = %d, want 1", tick, len(live))
		}
		if err := s.Run(live[0].time); err != nil {
			t.Fatalf("Run(subsequent tick %d at t=%d) error = %v", tick, live[0].time, err)
		}
		if got := len(livePendingTickEvents(s, id)); got != 1 {
			t.Fatalf("after subsequent tick %d: live pending ticks = %d, want 1; trace=%v", tick, got, s.Trace())
		}
	}

	staleMarker := fmt.Sprintf("tick node=%d skip(stale gen=%d", id, stale.generation)
	var staleSkips int
	for _, line := range s.Trace() {
		if strings.Contains(line, staleMarker) {
			staleSkips++
		}
	}
	if staleSkips != 1 {
		t.Fatalf("pre-restart tick stale-skip count = %d, want exactly 1 (stale tick must drain once without rescheduling); marker=%q trace=%v", staleSkips, staleMarker, s.Trace())
	}
}

func pendingTickEvents(s *Sim, id raft.NodeID) []*event {
	var ticks []*event
	for _, ev := range s.queue {
		if ev.kind == eventTick && ev.node == id {
			ticks = append(ticks, ev)
		}
	}
	return ticks
}

func livePendingTickEvents(s *Sim, id raft.NodeID) []*event {
	sn := s.nodes[id]
	var ticks []*event
	for _, ev := range pendingTickEvents(s, id) {
		if ev.generation == sn.generation {
			ticks = append(ticks, ev)
		}
	}
	return ticks
}

// TestHandleRestartOnLiveNodeIsRejectedNotDuplicateTickStream is a
// regression for a duplicate-tick-stream bug: restarting a node that never
// crashed (sn.node != nil, not halted) used to rebuild a fresh *raft.Node
// and schedule an independent second tick stream alongside the one already
// running, since only a real crash bumps the generation that invalidates a
// stale pending tick. Restart must reject a live node instead.
func TestHandleRestartOnLiveNodeIsRejectedNotDuplicateTickStream(t *testing.T) {
	s := newTestSim(t, 1, 2)
	beforeQueue := s.queue.Len()

	s.handleRestart(1) // node 1 never crashed; must be rejected, not rebuilt

	if got := s.trace[len(s.trace)-1]; !strings.Contains(got, "restart node=1 skip(live)") {
		t.Fatalf("last trace entry = %q, want a rejected live-restart record", got)
	}
	if got := s.queue.Len(); got != beforeQueue {
		t.Fatalf("queue len = %d, want unchanged %d (a rejected restart must not schedule a second tick stream)", got, beforeQueue)
	}

	const ticks = 5
	if err := s.Run(s.tickInterval * ticks); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	var fired int
	for _, line := range s.trace {
		if strings.Contains(line, "tick node=1") && !strings.Contains(line, "skip") {
			fired++
		}
	}
	if fired != ticks {
		t.Fatalf("successful tick node=1 count over %d intervals = %d, want exactly %d (one tick stream)\ntrace: %v", ticks, fired, ticks, s.trace)
	}
}

// TestFaultRestartOnLiveNodeDoesNotDuplicateTickStream is the schedule-driven
// counterpart: a FaultRestart event targeting a node that never crashed must
// have the same reject-not-rebuild behavior when it fires off the event
// queue, not just when handleRestart is called directly.
func TestFaultRestartOnLiveNodeDoesNotDuplicateTickStream(t *testing.T) {
	schedule := FaultSchedule{Events: []FaultEvent{{Time: 10, Kind: FaultRestart, Node: 1}}}
	s, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 2}}, schedule)
	if err != nil {
		t.Fatalf("NewFaultSim() error = %v", err)
	}

	const ticks = 5
	if err := s.Run(s.tickInterval * ticks); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var fired int
	for _, line := range s.trace {
		if strings.Contains(line, "tick node=1") && !strings.Contains(line, "skip") {
			fired++
		}
	}
	if fired != ticks {
		t.Fatalf("successful tick node=1 count over %d intervals = %d, want exactly %d (one tick stream)\ntrace: %v", ticks, fired, ticks, s.trace)
	}
}

// ---------------------------------------------------------------------
// FaultSchedule: validation, serialization/versioning, random generation,
// scripted mode, and same-seed deterministic replay.
// ---------------------------------------------------------------------

func TestFaultScheduleValidateRejectsMalformedEvents(t *testing.T) {
	cases := []struct {
		name string
		ev   FaultEvent
	}{
		{"drop rate below 0", FaultEvent{Kind: FaultDropRate, Rate: -0.1}},
		{"drop rate above 1", FaultEvent{Kind: FaultDropRate, Rate: 1.1}},
		{"duplicate rate above 1", FaultEvent{Kind: FaultDuplicateRate, Rate: 2}},
		{"clock skew below 0.5x", FaultEvent{Kind: FaultClockSkew, Multiplier: 0.1}},
		{"clock skew above 2x", FaultEvent{Kind: FaultClockSkew, Multiplier: 9}},
		{"partition from==to", FaultEvent{Kind: FaultPartition, From: 1, To: 1}},
		{"heal from==to", FaultEvent{Kind: FaultHeal, From: 2, To: 2}},
		{"partition groups too few", FaultEvent{Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1, 2}}}},
		{"heal groups too few", FaultEvent{Kind: FaultHealGroups, Groups: nil}},
		{"partition groups empty group", FaultEvent{Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1}, {}}}},
		{"partition groups duplicate node within a group", FaultEvent{Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1, 1}, {2}}}},
		{"partition groups node in two groups", FaultEvent{Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1, 2}, {2, 3}}}},
		{"unknown kind", FaultEvent{Kind: FaultKind(999)}},
		{"negative time", FaultEvent{Time: -1, Kind: FaultPause, Node: 1}},
		{"NaN drop rate", FaultEvent{Kind: FaultDropRate, Rate: math.NaN()}},
		{"+Inf duplicate rate", FaultEvent{Kind: FaultDuplicateRate, Rate: math.Inf(1)}},
		{"NaN clock skew multiplier", FaultEvent{Kind: FaultClockSkew, Multiplier: math.NaN()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schedule := FaultSchedule{Events: []FaultEvent{tc.ev}}
			if err := schedule.Validate(); err == nil {
				t.Fatalf("Validate() error = nil, want rejection for %+v", tc.ev)
			}
		})
	}
}

// TestFaultScheduleValidateRejectsUnsupportedVersion is the "exact supported
// generator version" requirement: only 0 (a hand-written scripted schedule
// that predates versioning) and the current FaultScheduleGeneratorVersion
// are accepted — anything else (including a future or garbage version) is
// rejected rather than silently reinterpreted.
func TestFaultScheduleValidateRejectsUnsupportedVersion(t *testing.T) {
	for _, bad := range []int{-1, FaultScheduleGeneratorVersion + 1, 999} {
		schedule := FaultSchedule{Version: bad}
		if err := schedule.Validate(); err == nil {
			t.Fatalf("Validate() error = nil for Version %d, want rejection", bad)
		}
	}
	for _, good := range []int{0, FaultScheduleGeneratorVersion} {
		schedule := FaultSchedule{Version: good}
		if err := schedule.Validate(); err != nil {
			t.Fatalf("Validate() error = %v for Version %d, want nil", err, good)
		}
	}
}

// TestFaultScheduleValidateRejectsMalformedCrashDirectives covers the
// CrashDirective half of "other malformed values": a 0 Save ordinal, a
// non-schedulable Point (CrashHostInitiated, or anything past CrashAfterSend),
// a RetainUnsynced below the RetainAllUnsynced sentinel, and a duplicate
// (Node,Save) pair across the schedule.
func TestFaultScheduleValidateRejectsMalformedCrashDirectives(t *testing.T) {
	cases := []struct {
		name       string
		directives []CrashDirective
	}{
		{"save 0", []CrashDirective{{Node: 1, Save: 0, Point: CrashBeforeSync, RetainUnsynced: RetainAllUnsynced}}},
		{"host-initiated point", []CrashDirective{{Node: 1, Save: 5, Point: CrashHostInitiated, RetainUnsynced: RetainAllUnsynced}}},
		{"point beyond CrashAfterSend", []CrashDirective{{Node: 1, Save: 5, Point: CrashPoint(99), RetainUnsynced: RetainAllUnsynced}}},
		{"retain below sentinel", []CrashDirective{{Node: 1, Save: 5, Point: CrashBeforeSync, RetainUnsynced: -2}}},
		{"duplicate (node,save)", []CrashDirective{
			{Node: 1, Save: 5, Point: CrashBeforeSync, RetainUnsynced: RetainAllUnsynced},
			{Node: 1, Save: 5, Point: CrashAfterSend, RetainUnsynced: RetainAllUnsynced},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schedule := FaultSchedule{Crashes: tc.directives}
			if err := schedule.Validate(); err == nil {
				t.Fatalf("Validate() error = nil, want rejection for %+v", tc.directives)
			}
		})
	}
}

// TestFaultScheduleValidateForClusterRejectsUnknownNodes is the cluster-
// membership requirement: Validate alone cannot catch a Node/From/To/group
// member/crash-directive that names a node outside the run, since it never
// sees the cluster; ValidateForCluster must reject every such case instead
// of the schedule silently no-oping when applyFault's node lookup misses.
func TestFaultScheduleValidateForClusterRejectsUnknownNodes(t *testing.T) {
	cluster := []raft.NodeID{1, 2, 3}
	const ghost = raft.NodeID(99)
	cases := []struct {
		name     string
		schedule FaultSchedule
	}{
		{"pause targets unknown node", FaultSchedule{Events: []FaultEvent{{Kind: FaultPause, Node: ghost}}}},
		{"clock skew targets unknown node", FaultSchedule{Events: []FaultEvent{{Kind: FaultClockSkew, Node: ghost, Multiplier: 1}}}},
		{"partition From unknown", FaultSchedule{Events: []FaultEvent{{Kind: FaultPartition, From: ghost, To: 1}}}},
		{"partition To unknown", FaultSchedule{Events: []FaultEvent{{Kind: FaultPartition, From: 1, To: ghost}}}},
		{"partition groups member unknown", FaultSchedule{Events: []FaultEvent{{Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1}, {ghost}}}}}},
		{"crash directive targets unknown node", FaultSchedule{Crashes: []CrashDirective{{Node: ghost, Save: 1, Point: CrashBeforeSync, RetainUnsynced: RetainAllUnsynced}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.schedule.Validate(); err != nil {
				t.Fatalf("Validate() error = %v, want nil (this is a ValidateForCluster-only defect)", err)
			}
			if err := tc.schedule.ValidateForCluster(cluster); err == nil {
				t.Fatalf("ValidateForCluster() error = nil, want rejection of node %d absent from %v", ghost, cluster)
			}
		})
	}
}

// TestNewFaultSimRejectsUnknownClusterNode is the construction-time
// integration proof: NewFaultSim must refuse to build a Sim from a schedule
// naming a node outside cfg.NodeIDs instead of building one that would later
// silently skip that fault at apply time.
func TestNewFaultSimRejectsUnknownClusterNode(t *testing.T) {
	schedule := FaultSchedule{Events: []FaultEvent{{Time: 10, Kind: FaultPause, Node: 99}}}
	if _, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 2}}, schedule); err == nil {
		t.Fatal("NewFaultSim() error = nil, want rejection of a fault event targeting a node outside the cluster")
	}
}

func TestFaultScheduleValidateAcceptsWellFormedEvents(t *testing.T) {
	schedule := FaultSchedule{Events: []FaultEvent{
		{Kind: FaultDropRate, Rate: 0.2},
		{Kind: FaultDuplicateRate, Rate: 0},
		{Kind: FaultPartition, From: 1, To: 2},
		{Kind: FaultHeal, From: 1, To: 2},
		{Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1}, {2, 3}}},
		{Kind: FaultHealGroups, Groups: [][]raft.NodeID{{1}, {2, 3}}},
		{Kind: FaultPause, Node: 1},
		{Kind: FaultResume, Node: 1},
		{Kind: FaultClockSkew, Node: 1, Multiplier: 1.5},
		{Kind: FaultCrash, Node: 1},
		{Kind: FaultRestart, Node: 1},
	}}
	if err := schedule.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil for a well-formed schedule", err)
	}
}

// TestGenerateFaultScheduleDeterministic is the random-from-seed mode
// requirement: identical seed, nodeIDs, and until must produce a
// byte-identical schedule.
func TestGenerateFaultScheduleDeterministic(t *testing.T) {
	nodeIDs := []raft.NodeID{1, 2, 3}
	first, err := GenerateFaultSchedule(20260716, nodeIDs, 5000)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}
	second, err := GenerateFaultSchedule(20260716, nodeIDs, 5000)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same-seed schedules diverged:\nfirst:  %+v\nsecond: %+v", first, second)
	}
	if len(first.Events) == 0 {
		t.Fatal("GenerateFaultSchedule produced no events, want a non-trivial schedule")
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("generated schedule failed Validate(): %v", err)
	}
	if first.Version != FaultScheduleGeneratorVersion {
		t.Fatalf("generated schedule Version = %d, want %d", first.Version, FaultScheduleGeneratorVersion)
	}

	third, err := GenerateFaultSchedule(999999, nodeIDs, 5000)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}
	if reflect.DeepEqual(first, third) {
		t.Fatal("different seeds produced identical schedules, want the seed to actually drive generation")
	}
}

// TestGenerateFaultScheduleSeed1PartitionOpensAreMeaningful pins the
// verifier's seed-1 counterexample. A group partition used to block 3->1
// before two directed 3->1 partitions opened, making both directed opens
// redundant. Replay the generated schedule as an edge-state machine and
// require every partition open to transition every edge it owns from open
// to blocked (and every matching heal to transition it back).
func TestGenerateFaultScheduleSeed1PartitionOpensAreMeaningful(t *testing.T) {
	schedule, err := GenerateFaultSchedule(1, []raft.NodeID{1, 2, 3}, 5000)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}

	ordered := append([]FaultEvent(nil), schedule.Events...)
	sortFaultEvents(ordered)
	blocked := make(map[partitionKey]struct{})
	var directedOpens, groupOpens int
	for i := range ordered {
		ev := ordered[i]
		var edges []partitionKey
		switch ev.Kind {
		case FaultPartition:
			directedOpens++
			edges = []partitionKey{{from: ev.From, to: ev.To}}
		case FaultPartitionGroups:
			groupOpens++
			edges = partitionGroupEdges(ev.Groups)
		case FaultHeal:
			edges = []partitionKey{{from: ev.From, to: ev.To}}
		case FaultHealGroups:
			edges = partitionGroupEdges(ev.Groups)
		default:
			continue
		}

		opening := ev.Kind == FaultPartition || ev.Kind == FaultPartitionGroups
		for _, edge := range edges {
			_, alreadyBlocked := blocked[edge]
			if opening && alreadyBlocked {
				t.Fatalf("event %d at t=%d (%s) redundantly opens already-blocked edge %d->%d; schedule=%+v", i, ev.Time, ev.Kind, edge.from, edge.to, schedule)
			}
			if !opening && !alreadyBlocked {
				t.Fatalf("event %d at t=%d (%s) closes already-open edge %d->%d; schedule=%+v", i, ev.Time, ev.Kind, edge.from, edge.to, schedule)
			}
			if opening {
				blocked[edge] = struct{}{}
			} else {
				delete(blocked, edge)
			}
		}
	}
	if directedOpens == 0 || groupOpens == 0 {
		t.Fatalf("seed-1 regression is vacuous: directed opens=%d group opens=%d; schedule=%+v", directedOpens, groupOpens, schedule)
	}
	if len(blocked) != 0 {
		t.Fatalf("generated partition schedule leaves %d blocked edges after all closes; schedule=%+v", len(blocked), schedule)
	}
}

// TestGeneratedTransitionIntervalsAreStructurallyNonOverlapping checks the
// generator's transition structure directly, rather than inferring safety
// from a few end-of-run node states. At most one Pause/Partition/GroupSplit/
// Crash interval may be open at a time, its next stateful event must be the
// matching close on the same target, and all four interval kinds must be
// observed across the property matrix.
func TestGeneratedTransitionIntervalsAreStructurallyNonOverlapping(t *testing.T) {
	horizons := []VirtualTime{0, 1, 2, 11, 250, 5000}
	observed := make(map[FaultKind]int)
	for seed := int64(1); seed <= 256; seed++ {
		for _, until := range horizons {
			schedule, err := GenerateFaultSchedule(seed, []raft.NodeID{1, 2, 3}, until)
			if err != nil {
				t.Fatalf("seed %d until %d: GenerateFaultSchedule() error = %v", seed, until, err)
			}
			assertGeneratedTransitionIntervals(t, seed, until, schedule, observed)
		}
	}
	for _, kind := range []FaultKind{FaultPause, FaultPartition, FaultPartitionGroups, FaultCrash} {
		if observed[kind] == 0 {
			t.Fatalf("property matrix observed no %s openings; coverage is vacuous", kind)
		}
	}
}

// TestGeneratedStorageCrashRecoveryPolicyIsStructural verifies the random
// generator marks every Save-ordinal crash for causal recovery. Together
// with TestCrashDirectiveRestartAfterCrashIsCausalAtRunBoundary, this is a
// generator-wide structural argument: whether a directive fires is runtime-
// dependent, but recovery is no longer time-guessed once it does fire.
func TestGeneratedStorageCrashRecoveryPolicyIsStructural(t *testing.T) {
	var directives int
	for seed := int64(1); seed <= 256; seed++ {
		for _, until := range []VirtualTime{0, 1, 250, 5000} {
			schedule, err := GenerateFaultSchedule(seed, []raft.NodeID{1, 2, 3}, until)
			if err != nil {
				t.Fatalf("seed %d until %d: GenerateFaultSchedule() error = %v", seed, until, err)
			}
			if len(schedule.Crashes) == 0 {
				t.Fatalf("seed %d until %d: generated no storage crash directives", seed, until)
			}
			for i, directive := range schedule.Crashes {
				directives++
				if !directive.RestartAfterCrash {
					t.Fatalf("seed %d until %d: crash directive %d lacks causal recovery policy: %+v", seed, until, i, directive)
				}
			}
		}
	}
	if directives == 0 {
		t.Fatal("generated crash-policy property observed no directives")
	}
}

func assertGeneratedTransitionIntervals(t *testing.T, seed int64, until VirtualTime, schedule FaultSchedule, observed map[FaultKind]int) {
	t.Helper()
	ordered := append([]FaultEvent(nil), schedule.Events...)
	sortFaultEvents(ordered)
	var active *FaultEvent
	var opens, closes int
	for i := range ordered {
		ev := ordered[i]
		if isGeneratedOpening(ev.Kind) {
			if active != nil {
				t.Fatalf("seed %d until %d: %s at t=%d overlaps active %s at t=%d; schedule=%+v", seed, until, ev.Kind, ev.Time, active.Kind, active.Time, schedule)
			}
			copy := ev
			active = &copy
			opens++
			observed[ev.Kind]++
			continue
		}
		if !isGeneratedClosing(ev.Kind) {
			continue
		}
		if active == nil {
			t.Fatalf("seed %d until %d: unmatched %s at t=%d; schedule=%+v", seed, until, ev.Kind, ev.Time, schedule)
		}
		if !closesGeneratedOpening(*active, ev) {
			t.Fatalf("seed %d until %d: %s at t=%d does not close active %s at t=%d on the same target; schedule=%+v", seed, until, ev.Kind, ev.Time, active.Kind, active.Time, schedule)
		}
		active = nil
		closes++
	}
	if active != nil {
		t.Fatalf("seed %d until %d: %s at t=%d remains open; schedule=%+v", seed, until, active.Kind, active.Time, schedule)
	}
	if opens != 6 || closes != 6 {
		t.Fatalf("seed %d until %d: matched transition counts=(opens=%d closes=%d), want (6,6); schedule=%+v", seed, until, opens, closes, schedule)
	}
}

func isGeneratedOpening(kind FaultKind) bool {
	switch kind {
	case FaultPause, FaultPartition, FaultPartitionGroups, FaultCrash:
		return true
	default:
		return false
	}
}

func isGeneratedClosing(kind FaultKind) bool {
	switch kind {
	case FaultResume, FaultHeal, FaultHealGroups, FaultRestart:
		return true
	default:
		return false
	}
}

func closesGeneratedOpening(open, close FaultEvent) bool {
	switch open.Kind {
	case FaultPause:
		return close.Kind == FaultResume && close.Node == open.Node
	case FaultPartition:
		return close.Kind == FaultHeal && close.From == open.From && close.To == open.To
	case FaultPartitionGroups:
		return close.Kind == FaultHealGroups && reflect.DeepEqual(close.Groups, open.Groups)
	case FaultCrash:
		return close.Kind == FaultRestart && close.Node == open.Node
	default:
		return false
	}
}

func partitionGroupEdges(groups [][]raft.NodeID) []partitionKey {
	var edges []partitionKey
	for i, left := range groups {
		for _, right := range groups[i+1:] {
			for _, from := range left {
				for _, to := range right {
					edges = append(edges, partitionKey{from: from, to: to}, partitionKey{from: to, to: from})
				}
			}
		}
	}
	return edges
}

// TestGenerateFaultScheduleRejectsEmptyNodeIDs verifies the empty-cluster
// case returns a deterministic error instead of panicking on the first
// node-indexed random draw (order[r.Intn(len(order))] with len(order)==0).
func TestGenerateFaultScheduleRejectsEmptyNodeIDs(t *testing.T) {
	if _, err := GenerateFaultSchedule(1, nil, 5000); err == nil {
		t.Fatal("GenerateFaultSchedule(nil nodeIDs) error = nil, want rejection")
	}
	if _, err := GenerateFaultSchedule(1, []raft.NodeID{}, 5000); err == nil {
		t.Fatal("GenerateFaultSchedule(empty nodeIDs) error = nil, want rejection")
	}
}

// nodeCrashedInTrace reports whether trace records id crashing by any
// mechanism: a host-initiated FaultCrash ("crash node=%d"), a storage crash
// mid-Save ("node=%d save_crash"), or the CrashAfterSend backstop firing on
// a later Save ("node=%d after_send_crash").
func nodeCrashedInTrace(trace []string, id raft.NodeID) bool {
	patterns := []string{
		fmt.Sprintf("crash node=%d", id),
		fmt.Sprintf("node=%d save_crash", id),
		fmt.Sprintf("node=%d after_send_crash", id),
	}
	for _, line := range trace {
		for _, p := range patterns {
			if strings.Contains(line, p) {
				return true
			}
		}
	}
	return false
}

// TestGenerateFaultScheduleNeverStrandsACrashedNode is the "must exercise
// crash and restart behavior, not strand a crashed node forever" coverage
// requirement: across a battery of generated seeds, any node observed
// crashing during the run (by either the host-level FaultCrash pair or a
// storage-level Save-ordinal directive) must be back up (a live *raft.Node
// over recovered storage) by the run's end. Host crashes have non-overlapping
// event-time pairs; storage crashes carry causal RestartAfterCrash policy.
func TestGenerateFaultScheduleNeverStrandsACrashedNode(t *testing.T) {
	nodeIDs := []raft.NodeID{1, 2, 3}
	const until = VirtualTime(20000)
	var sawAnyCrash bool
	for seed := int64(1); seed <= 25; seed++ {
		schedule, err := GenerateFaultSchedule(seed, nodeIDs, until)
		if err != nil {
			t.Fatalf("seed %d: GenerateFaultSchedule() error = %v", seed, err)
		}
		s, err := NewFaultSim(Config{Seed: seed, NodeIDs: nodeIDs}, schedule)
		if err != nil {
			t.Fatalf("seed %d: NewFaultSim() error = %v", seed, err)
		}
		if err := s.Run(until); err != nil {
			t.Fatalf("seed %d: Run() error = %v", seed, err)
		}

		for _, id := range nodeIDs {
			if !nodeCrashedInTrace(s.Trace(), id) {
				continue
			}
			sawAnyCrash = true
			store := phase3Store(t, s, id)
			if s.nodes[id].node == nil || store.Crashed() {
				t.Fatalf("seed %d: node %d crashed during the run and was not fully recovered by t=%d: node_live=%t storage_crashed=%t", seed, id, until, s.nodes[id].node != nil, store.Crashed())
			}
		}
	}
	if !sawAnyCrash {
		t.Fatal("no generated seed crashed any node across 25 seeds; this test proves nothing without at least one observed crash")
	}
}

// TestGenerateFaultScheduleSeed8LateStorageCrashRecovers pins the verifier's
// exact short-horizon counterexample: under generator v2 node 1 restarted at
// t=214, then its save-5 CrashAfterSend directive fired at t=249 and left the
// process and storage down at the t=250 run boundary. The generated crash
// must still fire in this regression and its recovery must be causally tied
// to that firing, not to a guessed earlier restart time.
func TestGenerateFaultScheduleSeed8LateStorageCrashRecovers(t *testing.T) {
	const until = VirtualTime(250)
	nodeIDs := []raft.NodeID{1, 2, 3}
	schedule, err := GenerateFaultSchedule(8, nodeIDs, until)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}
	s, err := NewFaultSim(Config{Seed: 8, NodeIDs: nodeIDs}, schedule)
	if err != nil {
		t.Fatalf("NewFaultSim() error = %v", err)
	}
	if err := s.Run(until); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	store := phase3Store(t, s, 1)
	info, crashed := store.LastCrash()
	if !crashed || info.Save != 5 || info.Point != CrashAfterSend {
		t.Fatalf("node 1 last crash = %+v, %t, want save 5 at %s (regression must exercise the original counterexample); schedule=%+v\ntrace=%v", info, crashed, CrashAfterSend, schedule, s.Trace())
	}
	times := make(map[string]VirtualTime)
	markers := []struct {
		name string
		text string
	}{
		{name: "crash", text: "node=1 after_send_crash save=5"},
		{name: "scheduled", text: "node=1 restart_after_crash save=5 scheduled"},
		{name: "restart", text: "restart node=1"},
	}
	for _, line := range s.Trace() {
		for _, marker := range markers {
			if !strings.Contains(line, marker.text) {
				continue
			}
			var at VirtualTime
			if _, err := fmt.Sscanf(line, "t=%d", &at); err != nil {
				t.Fatalf("parse trace time from %q: %v", line, err)
			}
			times[marker.name] = at
		}
	}
	if len(times) != 3 {
		t.Fatalf("trace lacks exact seed-8 crash/recovery sequence: times=%v; schedule=%+v\ntrace=%v", times, schedule, s.Trace())
	}
	if times["crash"] != times["scheduled"] || times["crash"] != times["restart"] {
		t.Fatalf("seed-8 recovery was not causal at the crash boundary: times=%v; trace=%v", times, s.Trace())
	}
	if s.nodes[1].node == nil || store.Crashed() {
		t.Fatalf("node 1 storage crash fired at save 5 but was stranded at t=%d: node_live=%t storage_crashed=%t; schedule=%+v\ntrace=%v", until, s.nodes[1].node != nil, store.Crashed(), schedule, s.Trace())
	}
}

// TestEncodeDecodeFaultScheduleRoundTrip is the serializable/versioned
// requirement: a schedule (random or scripted) must round-trip through the
// file encoding with every field, including Version, preserved.
func TestEncodeDecodeFaultScheduleRoundTrip(t *testing.T) {
	original, err := GenerateFaultSchedule(20260717, []raft.NodeID{1, 2, 3}, 5000)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}

	data, err := EncodeFaultSchedule(original)
	if err != nil {
		t.Fatalf("EncodeFaultSchedule() error = %v", err)
	}
	decoded, err := DecodeFaultSchedule(data)
	if err != nil {
		t.Fatalf("DecodeFaultSchedule() error = %v", err)
	}
	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("round trip diverged:\noriginal: %+v\ndecoded:  %+v", original, decoded)
	}

	data2, err := EncodeFaultSchedule(original)
	if err != nil {
		t.Fatalf("EncodeFaultSchedule() second call error = %v", err)
	}
	if !reflect.DeepEqual(data, data2) {
		t.Fatal("EncodeFaultSchedule is not byte-deterministic across identical input")
	}
}

// TestScriptedFaultScheduleHandWrittenLiteralWorks covers the second
// requirement of "usable in random-from-seed and scripted modes": a
// hand-authored FaultSchedule literal (no generator involved) mixing a
// partition-during-write shape and a pause-leader shape must apply exactly
// as scripted.
func TestScriptedFaultScheduleHandWrittenLiteralWorks(t *testing.T) {
	schedule := FaultSchedule{
		Version: FaultScheduleGeneratorVersion,
		Events: []FaultEvent{
			{Time: 200, Kind: FaultPartitionGroups, Groups: [][]raft.NodeID{{1}, {2, 3}}},
			{Time: 400, Kind: FaultPause, Node: 2},
			{Time: 600, Kind: FaultResume, Node: 2},
			{Time: 800, Kind: FaultHealGroups, Groups: [][]raft.NodeID{{1}, {2, 3}}},
		},
	}
	if err := schedule.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	s, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 2, 3}}, schedule)
	if err != nil {
		t.Fatalf("NewFaultSim() error = %v", err)
	}

	if err := s.Run(250); err != nil {
		t.Fatalf("Run(250) error = %v", err)
	}
	if !s.blocked(1, 2) || !s.blocked(2, 1) {
		t.Fatal("scripted partition-during-write shape must be in effect after t=200")
	}

	if err := s.Run(450); err != nil {
		t.Fatalf("Run(450) error = %v", err)
	}
	if !s.nodes[2].paused {
		t.Fatal("scripted pause-leader shape must be in effect after t=400")
	}

	if err := s.Run(650); err != nil {
		t.Fatalf("Run(650) error = %v", err)
	}
	if s.nodes[2].paused {
		t.Fatal("node 2 must be resumed after t=600")
	}

	if err := s.Run(850); err != nil {
		t.Fatalf("Run(850) error = %v", err)
	}
	if s.blocked(1, 2) || s.blocked(2, 1) {
		t.Fatal("scripted heal must clear the partition after t=800")
	}
}

// TestNewFaultSimSameSeedByteIdenticalTrace is the same-seed deterministic
// replay requirement across the full phase-4 fault model: two Sims built
// from the same Config and a schedule mixing every fault kind must produce
// byte-identical traces.
func TestNewFaultSimSameSeedByteIdenticalTrace(t *testing.T) {
	nodeIDs := []raft.NodeID{1, 2, 3}
	schedule, err := GenerateFaultSchedule(20260718, nodeIDs, 4000)
	if err != nil {
		t.Fatalf("GenerateFaultSchedule() error = %v", err)
	}
	cfg := Config{Seed: 20260718, NodeIDs: nodeIDs}

	run := func() []string {
		s, err := NewFaultSim(cfg, schedule)
		if err != nil {
			t.Fatalf("NewFaultSim() error = %v", err)
		}
		s.RegisterInvariant(SingleLeaderPerTerm)
		if err := s.Run(4000); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		return s.Trace()
	}

	first := run()
	second := run()

	if len(first) == 0 {
		t.Fatal("Trace() is empty, want a non-trivial run")
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same-seed fault runs diverged")
	}
}

// TestNewFaultSimRejectsInvalidSchedule verifies NewFaultSim surfaces
// FaultSchedule.Validate errors instead of building a Sim that would later
// misbehave on a malformed schedule.
func TestNewFaultSimRejectsInvalidSchedule(t *testing.T) {
	schedule := FaultSchedule{Events: []FaultEvent{{Kind: FaultDropRate, Rate: 5}}}
	if _, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1, 2}}, schedule); err == nil {
		t.Fatal("NewFaultSim() error = nil, want rejection of an out-of-range drop rate")
	}
}
