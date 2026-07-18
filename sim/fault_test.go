package sim

import (
	"container/heap"
	"reflect"
	"strings"
	"testing"

	"miniquorum/internal/raft"
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
	for _, bad := range []float64{0, 0.49, 2.01, 5} {
		if err := s.SetClockSkew(1, bad); err == nil {
			t.Fatalf("SetClockSkew(%v) error = nil, want out-of-range rejection", bad)
		}
	}
}

// TestClockSkewScalesTickInterval verifies nextTickDelay applies the
// per-node multiplier deterministically: a 2x node ticks half as often (a
// doubled interval), a 0.5x node ticks twice as often (a halved interval).
func TestClockSkewScalesTickInterval(t *testing.T) {
	s := newTestSim(t, 1, 2)
	base := s.nextTickDelay(s.nodes[1])

	if err := s.SetClockSkew(1, 2.0); err != nil {
		t.Fatalf("SetClockSkew(2.0) error = %v", err)
	}
	if got, want := s.nextTickDelay(s.nodes[1]), base*2; got != want {
		t.Fatalf("nextTickDelay at 2x = %d, want %d", got, want)
	}

	if err := s.SetClockSkew(1, 0.5); err != nil {
		t.Fatalf("SetClockSkew(0.5) error = %v", err)
	}
	if got, want := s.nextTickDelay(s.nodes[1]), base/2; got != want {
		t.Fatalf("nextTickDelay at 0.5x = %d, want %d", got, want)
	}
	// node 2 is unaffected by node 1's skew.
	if got := s.nextTickDelay(s.nodes[2]); got != base {
		t.Fatalf("unrelated node's nextTickDelay = %d, want unchanged %d", got, base)
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
		{"unknown kind", FaultEvent{Kind: FaultKind(999)}},
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
	first := GenerateFaultSchedule(20260716, nodeIDs, 5000)
	second := GenerateFaultSchedule(20260716, nodeIDs, 5000)

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

	third := GenerateFaultSchedule(999999, nodeIDs, 5000)
	if reflect.DeepEqual(first, third) {
		t.Fatal("different seeds produced identical schedules, want the seed to actually drive generation")
	}
}

// TestEncodeDecodeFaultScheduleRoundTrip is the serializable/versioned
// requirement: a schedule (random or scripted) must round-trip through the
// file encoding with every field, including Version, preserved.
func TestEncodeDecodeFaultScheduleRoundTrip(t *testing.T) {
	original := GenerateFaultSchedule(20260717, []raft.NodeID{1, 2, 3}, 5000)

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
	schedule := GenerateFaultSchedule(20260718, nodeIDs, 4000)
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
