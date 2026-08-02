package sim

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/lsm"
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
	workloadpkg "miniquorum/sim/workload"
)

// These are end-to-end deterministic crash schedules, not just Engine unit
// tests: a committed entry crosses an LSM flush/compaction fault, the process
// crashes before Advance, then ordinary Raft commitment after restart replays
// the retained log through StateMachine.Apply.
func TestLSMScriptedFlushCrashReplaysWithoutLossOrOrphans(t *testing.T) {
	calibration, calibrationFS := newLSMCrashBridgeSim(t, FaultSchedule{})
	before := len(calibrationFS.Events())
	bridgePut(t, calibration, 1, "flush-key", "flush-value")
	target := bridgeSSTWrite(t, calibrationFS.Events()[before:], false)

	schedule := roundTripBridgeSchedule(t, FaultSchedule{LSMCrashes: []LSMCrashDirective{{Node: 1, Op: target.Op, Occurrence: target.Occurrence, Point: target.Point, RetainUnsynced: target.RetainUnsynced, RestartAfterCrash: true}}})
	s, fs := newLSMCrashBridgeSim(t, schedule)
	bridgePut(t, s, 1, "flush-key", "flush-value")
	assertLSMApplyCrash(t, s)
	bridgeRecoverAndReplay(t, s, 2)
	assertBridgeValue(t, s, "flush-key", "flush-value")
	assertBridgeManifestAndOrphans(t, s, fs)
}

func TestLSMScriptedCompactionCrashReplaysWithoutLossOrOrphans(t *testing.T) {
	calibration, calibrationFS := newLSMCrashBridgeSim(t, FaultSchedule{})
	crashSequence, target := bridgeCompactionOutput(t, calibration, calibrationFS)

	schedule := roundTripBridgeSchedule(t, FaultSchedule{LSMCrashes: []LSMCrashDirective{{Node: 1, Op: target.Op, Occurrence: target.Occurrence, Point: target.Point, RetainUnsynced: target.RetainUnsynced, RestartAfterCrash: true}}})
	s, fs := newLSMCrashBridgeSim(t, schedule)
	for index := 1; index <= crashSequence; index++ {
		bridgePut(t, s, uint64(index), fmt.Sprintf("compact-%d", index), fmt.Sprintf("value-%d", index))
	}
	assertLSMApplyCrash(t, s)
	bridgeRecoverAndReplay(t, s, uint64(crashSequence+1))
	for index := 1; index <= crashSequence; index++ {
		assertBridgeValue(t, s, fmt.Sprintf("compact-%d", index), fmt.Sprintf("value-%d", index))
	}
	assertBridgeManifestAndOrphans(t, s, fs)
}

func TestLSMScriptedCrashRestartPoliciesAreExactAndCausal(t *testing.T) {
	calibration, calibrationFS := newLSMCrashBridgeSim(t, FaultSchedule{})
	before := len(calibrationFS.Events())
	bridgePut(t, calibration, 1, "policy-key", "policy-value")
	first := bridgeSSTWrite(t, calibrationFS.Events()[before:], false)
	second := first
	second.Occurrence++

	// Both directives target node 1 and the same filesystem operation. The
	// policy is therefore distinguished only by its exact occurrence (and the
	// retained Point): the first must not restart itself, the second must.
	schedule := roundTripBridgeSchedule(t, FaultSchedule{
		Events: []FaultEvent{{Time: 1_100, Kind: FaultRestart, Node: 1}},
		LSMCrashes: []LSMCrashDirective{
			{Node: 1, Op: first.Op, Occurrence: first.Occurrence, Point: first.Point, RetainUnsynced: first.RetainUnsynced, RestartAfterCrash: false},
			{Node: 1, Op: second.Op, Occurrence: second.Occurrence, Point: second.Point, RetainUnsynced: second.RetainUnsynced, RestartAfterCrash: true},
		},
	})

	s, _ := newLSMCrashBridgeSim(t, schedule)
	bridgePut(t, s, 1, "policy-key", "policy-value")
	if s.nodes[1].node != nil {
		t.Fatal("false-policy directive restarted node without an explicit restart")
	}
	trace := strings.Join(s.Trace(), "\n")
	if strings.Contains(trace, "lsm_restart_after_crash scheduled") {
		t.Fatalf("false-policy crash scheduled a restart: %s", trace)
	}

	// The first policy is deliberately false, so its serialized FaultRestart
	// event is the only recovery route. Before that event, the node remains
	// down; replay then consumes the next exact occurrence and its true policy
	// queues the causal restart.
	if err := s.Run(1_099); err != nil {
		t.Fatal(err)
	}
	if s.nodes[1].node != nil {
		t.Fatal("false-policy directive restarted before its serialized FaultRestart event")
	}
	if err := s.Run(2_100); err != nil {
		t.Fatal(err)
	}
	trace = strings.Join(s.Trace(), "\n")
	if got := strings.Count(trace, "lsm_apply_crash"); got != 2 {
		t.Fatalf("LSM crash count = %d, want false then true exact directives; trace=%s", got, trace)
	}
	if got := strings.Count(trace, "lsm_restart_after_crash scheduled"); got != 1 {
		t.Fatalf("causal LSM restart count = %d, want only true policy; trace=%s", got, trace)
	}
	if s.nodes[1].node == nil || s.nodes[1].halted {
		t.Fatalf("true-policy restart state = node:%t halted:%t, want live", s.nodes[1].node != nil, s.nodes[1].halted)
	}
}

func TestLSMScriptedReopenCrashRestartPoliciesAreExactAndCausal(t *testing.T) {
	calibration, calibrationFS := newLSMCrashBridgeSim(t, FaultSchedule{})
	target := bridgeReopenCrashDirective(t, calibration, calibrationFS)

	for _, test := range []struct {
		name    string
		restart bool
	}{
		{name: "false stays down", restart: false},
		{name: "true restarts", restart: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			schedule := roundTripBridgeSchedule(t, FaultSchedule{
				Events: []FaultEvent{
					{Time: 2_000, Kind: FaultCrash, Node: 1},
					{Time: 2_001, Kind: FaultRestart, Node: 1},
				},
				LSMCrashes: []LSMCrashDirective{{
					Node:              1,
					Op:                target.Op,
					Occurrence:        target.Occurrence,
					Point:             target.Point,
					RetainUnsynced:    target.RetainUnsynced,
					RestartAfterCrash: test.restart,
				}},
			})
			s, _ := newLSMCrashBridgeSim(t, schedule)
			if err := s.Run(2_002); err != nil {
				t.Fatal(err)
			}
			trace := strings.Join(s.Trace(), "\n")
			if got := strings.Count(trace, "lsm_reopen_crash"); got != 1 {
				t.Fatalf("reopen crash count = %d, want 1; trace=%s", got, trace)
			}
			if got := strings.Count(trace, "lsm_restart_after_crash scheduled"); got != boolCount(test.restart) {
				t.Fatalf("causal reopen restart count = %d, want %d; trace=%s", got, boolCount(test.restart), trace)
			}
			if test.restart && (s.nodes[1].node == nil || s.nodes[1].halted) {
				t.Fatalf("true reopen policy state = node:%t halted:%t, want live", s.nodes[1].node != nil, s.nodes[1].halted)
			}
			if !test.restart && s.nodes[1].node != nil {
				t.Fatalf("false reopen policy state = node live, want down until another serialized FaultRestart")
			}
		})
	}
}

func newLSMCrashBridgeSim(t *testing.T, schedule FaultSchedule) (*Sim, *lsm.SimFS) {
	t.Helper()
	s, err := NewFaultSim(Config{Seed: 5_500, Engine: "lsm", LSMFlushThreshold: 1, NodeIDs: []raft.NodeID{1}}, schedule)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartWorkload(workloadpkg.Config{ClientCount: 1, KeyCount: 1, OperationsPerClient: 1}); err != nil {
		t.Fatal(err)
	}
	bridgeElect(t, s)
	return s, s.nodes[1].lsmFS
}

func roundTripBridgeSchedule(t *testing.T, schedule FaultSchedule) FaultSchedule {
	t.Helper()
	encoded, err := EncodeFaultSchedule(schedule)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFaultSchedule(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func bridgeSSTWrite(t *testing.T, events []lsm.FSEvent, last bool) lsm.SimCrashDirective {
	t.Helper()
	var target *lsm.FSEvent
	for _, event := range events {
		if event.Op == lsm.FSOpWrite && strings.HasSuffix(event.Path, ".sst") {
			copy := event
			target = &copy
			if !last {
				break
			}
		}
	}
	if target != nil {
		return lsm.SimCrashDirective{Op: lsm.FSOpWrite, Occurrence: target.Occurrence, Point: lsm.SimAfterOperation, RetainUnsynced: lsm.RetainAllUnsynced}
	}
	t.Fatalf("no SSTable write in events: %+v", events)
	return lsm.SimCrashDirective{}
}

// bridgeCompactionOutput calibrates the exact SimFS occurrence that writes a
// compaction output. A threshold-sized flush contributes one SSTable write;
// the first Ready batch with two writes contains that flush followed by its
// compaction output, so choosing the latter cannot accidentally target only
// the flush path. The same deterministic prefix is reproduced below before
// the serialized directive is allowed to fire.
func bridgeCompactionOutput(t *testing.T, s *Sim, fs *lsm.SimFS) (int, lsm.SimCrashDirective) {
	t.Helper()
	for index := 1; index <= 32; index++ {
		before := len(fs.Events())
		bridgePut(t, s, uint64(index), fmt.Sprintf("compact-%d", index), fmt.Sprintf("value-%d", index))
		var writes []lsm.FSEvent
		for _, event := range fs.Events()[before:] {
			if event.Op == lsm.FSOpWrite && strings.HasSuffix(event.Path, ".sst") {
				writes = append(writes, event)
			}
		}
		if len(writes) < 2 {
			continue
		}
		flush, output := writes[0], writes[len(writes)-1]
		if flush.Path == output.Path {
			t.Fatalf("compaction output reuses flush path %q: events=%+v", output.Path, fs.Events()[before:])
		}
		return index, lsm.SimCrashDirective{Op: output.Op, Occurrence: output.Occurrence, Point: lsm.SimAfterOperation, RetainUnsynced: lsm.RetainAllUnsynced}
	}
	t.Fatalf("no compaction output after 32 threshold-sized flushes: %+v", fs.Events())
	return 0, lsm.SimCrashDirective{}
}

// bridgeReopenCrashDirective calibrates an operation performed by
// OpenStateMachine during the serialized host-restart path. The candidate
// schedule below repeats the same post-open operation prefix, so its exact
// occurrence fires while reopening rather than while applying an entry.
func bridgeReopenCrashDirective(t *testing.T, s *Sim, fs *lsm.SimFS) lsm.SimCrashDirective {
	t.Helper()
	before := len(fs.Events())
	s.ScheduleCrash(1, 2_000)
	s.ScheduleRestart(1, 2_001)
	if err := s.Run(2_001); err != nil {
		t.Fatal(err)
	}
	for _, event := range fs.Events()[before:] {
		if event.Completed {
			return lsm.SimCrashDirective{Op: event.Op, Occurrence: event.Occurrence, Point: lsm.SimAfterOperation, RetainUnsynced: lsm.RetainAllUnsynced}
		}
	}
	t.Fatalf("no completed LSM reopen operation: %+v", fs.Events()[before:])
	return lsm.SimCrashDirective{}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func bridgeElect(t *testing.T, s *Sim) {
	t.Helper()
	if err := s.Run(s.Now() + 1_000); err != nil {
		t.Fatal(err)
	}
}

func bridgePut(t *testing.T, s *Sim, sequence uint64, key, value string) {
	t.Helper()
	data, err := proto.Marshal(&raftpb.Command{ClientId: 99, Seq: sequence, Op: raftpb.Op_PUT, Key: []byte(key), Value: []byte(value)})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, leader := s.Propose(1, data); !leader {
		bridgeElect(t, s)
		if _, _, leader = s.Propose(1, data); !leader {
			t.Fatal("single-node bridge simulator has no leader")
		}
	}
}

func assertLSMApplyCrash(t *testing.T, s *Sim) {
	t.Helper()
	if s.nodes[1].node != nil || s.nodes[1].halted {
		t.Fatalf("LSM Apply crash state = node:%t halted:%t, want crashed process", s.nodes[1].node != nil, s.nodes[1].halted)
	}
	trace := strings.Join(s.Trace(), "\n")
	if !strings.Contains(trace, "lsm_apply_crash") {
		t.Fatalf("trace after LSM crash = %s, want lsm_apply_crash", trace)
	}
	if !strings.Contains(trace, "lsm_restart_after_crash scheduled") {
		t.Fatalf("trace after LSM crash = %s, want causal restart scheduling", trace)
	}
}

func bridgeRecoverAndReplay(t *testing.T, s *Sim, markerSequence uint64) {
	t.Helper()
	if err := s.Run(s.Now() + 1_000); err != nil {
		t.Fatal(err)
	}
	if s.nodes[1].node == nil || s.nodes[1].halted {
		t.Fatalf("restart state = node:%t halted:%t, want live", s.nodes[1].node != nil, s.nodes[1].halted)
	}
	bridgeElect(t, s)
	bridgePut(t, s, markerSequence, "recovery-marker", "ok")
}

func assertBridgeValue(t *testing.T, s *Sim, key, want string) {
	t.Helper()
	result, err := s.nodes[1].sm.Read([]byte(key))
	if err != nil || !result.Found || string(result.Value) != want {
		t.Fatalf("Read(%q) = (%#v,%v), want %q", key, result, err, want)
	}
}

func assertBridgeManifestAndOrphans(t *testing.T, s *Sim, fs *lsm.SimFS) {
	t.Helper()
	stateMachine, ok := s.nodes[1].sm.(*lsm.StateMachine)
	if !ok {
		t.Fatalf("state machine = %T, want *lsm.StateMachine", s.nodes[1].sm)
	}
	referenced := make(map[string]struct{})
	for _, filename := range stateMachine.Engine().ReferencedSSTables() {
		referenced[filename] = struct{}{}
	}
	names, err := fs.List("/node-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if strings.HasSuffix(name, ".sst") {
			if _, ok := referenced[name]; !ok {
				t.Fatalf("orphaned SSTable %q remains after replay; referenced=%v", name, stateMachine.Engine().ReferencedSSTables())
			}
		}
	}
}
