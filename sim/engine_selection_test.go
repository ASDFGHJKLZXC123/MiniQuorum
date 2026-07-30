package sim

import (
	"reflect"
	"strings"
	"testing"

	"miniquorum/internal/lsm"
	"miniquorum/internal/raft"
	workloadpkg "miniquorum/sim/workload"
)

func TestSerializedLSMCrashDirectivesRequireLSMEngine(t *testing.T) {
	schedule := FaultSchedule{LSMCrashes: []LSMCrashDirective{{Node: 1, Op: lsm.FSOpWrite, Occurrence: 1, Point: lsm.SimAfterOperation, RetainUnsynced: lsm.RetainAllUnsynced}}}
	if _, err := NewFaultSim(Config{Seed: 1, NodeIDs: []raft.NodeID{1}}, schedule); err == nil || !strings.Contains(err.Error(), "require engine lsm") {
		t.Fatalf("map engine LSM directive error = %v", err)
	}
	encoded, err := EncodeFaultSchedule(schedule)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeFaultSchedule(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.LSMCrashes, schedule.LSMCrashes) {
		t.Fatalf("LSM directive roundtrip = %#v", decoded.LSMCrashes)
	}
	if _, err := NewFaultSim(Config{Seed: 1, Engine: "lsm", NodeIDs: []raft.NodeID{1}}, decoded); err != nil {
		t.Fatalf("lsm directive schedule: %v", err)
	}
}

func TestLSMCrashDirectivesArmAfterInitialOpen(t *testing.T) {
	schedule := FaultSchedule{LSMCrashes: []LSMCrashDirective{{
		Node:           1,
		Op:             lsm.FSOpList,
		Occurrence:     1,
		Point:          lsm.SimAfterOperation,
		RetainUnsynced: lsm.RetainAllUnsynced,
	}}}
	s, err := NewFaultSim(Config{Seed: 5_053, Engine: "lsm", NodeIDs: []raft.NodeID{1}}, schedule)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartWorkload(workloadpkg.Config{ClientCount: 1, KeyCount: 1, OperationsPerClient: 1}); err != nil {
		t.Fatalf("StartWorkload consumed directive during initial OpenStateMachine: %v", err)
	}
	if s.nodes[1].lsmFS.Crashed() {
		t.Fatal("initial OpenStateMachine fired a directive before it was armed")
	}
}

func TestFaultScheduleRejectsUnknownLSMOperation(t *testing.T) {
	_, err := DecodeFaultSchedule([]byte(`{"LSMCrashes":[{"Node":1,"Op":"unknown","Occurrence":1,"Point":1,"RetainUnsynced":-1}]}`))
	if err == nil || !strings.Contains(err.Error(), "unknown fault operation") {
		t.Fatalf("unknown LSM op error = %v", err)
	}
}

func TestFiredLSMCrashKeyUsesSimFSEventPoint(t *testing.T) {
	fs := lsm.NewSimFS()
	directive := LSMCrashDirective{Node: 1, Op: lsm.FSOpWrite, Occurrence: 1, Point: lsm.SimBeforeOperation, RetainUnsynced: lsm.RetainAllUnsynced}
	if err := fs.SetCrashSchedule([]lsm.SimCrashDirective{{
		Op:             directive.Op,
		Occurrence:     directive.Occurrence,
		Point:          directive.Point,
		RetainUnsynced: directive.RetainUnsynced,
	}}); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create("/evidence")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("x")); err == nil {
		t.Fatal("scheduled before-write crash did not fire")
	}
	s := &Sim{
		lsmFS:              map[raft.NodeID]*lsm.SimFS{1: fs},
		lsmCrashDirectives: []LSMCrashDirective{directive},
		lsmRestartOnCrash:  map[lsmCrashDirectiveKey]bool{lsmDirectiveKey(directive): false},
	}
	key, ok := s.firedLSMCrashKey(1)
	if !ok || key != lsmDirectiveKey(directive) {
		t.Fatalf("fired key = (%+v,%t), want exact before-operation directive %+v", key, ok, lsmDirectiveKey(directive))
	}
}

func TestNewSimRejectsUnknownEngine(t *testing.T) {
	_, err := NewSim(Config{Seed: 1, Engine: "unknown", NodeIDs: []raft.NodeID{1}})
	if err == nil || !strings.Contains(err.Error(), "unsupported engine") {
		t.Fatalf("NewSim unknown engine error = %v, want unsupported-engine rejection", err)
	}
}

func TestNewSimRejectsInvalidLSMFlushThreshold(t *testing.T) {
	for _, test := range []struct {
		name      string
		engine    string
		threshold int64
	}{
		{name: "negative lsm", engine: "lsm", threshold: -1},
		{name: "map nonzero", engine: "map", threshold: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewSim(Config{Seed: 1, Engine: test.engine, LSMFlushThreshold: test.threshold, NodeIDs: []raft.NodeID{1}})
			if err == nil || !strings.Contains(err.Error(), "LSM flush threshold") {
				t.Fatalf("NewSim(%q,%d) error = %v, want threshold rejection", test.engine, test.threshold, err)
			}
		})
	}
}

func TestMapEngineDefaultPreservesDeterministicTrace(t *testing.T) {
	base := Config{Seed: 5_050, NodeIDs: []raft.NodeID{1, 2, 3}}
	implicit, err := NewSim(base)
	if err != nil {
		t.Fatal(err)
	}
	explicitConfig := base
	explicitConfig.Engine = "map"
	explicit, err := NewSim(explicitConfig)
	if err != nil {
		t.Fatal(err)
	}
	workload := workloadpkg.Config{ClientCount: 2, KeyCount: 2, OperationsPerClient: 3}
	if err := implicit.StartWorkload(workload); err != nil {
		t.Fatal(err)
	}
	if err := explicit.StartWorkload(workload); err != nil {
		t.Fatal(err)
	}
	if err := implicit.Run(2_000); err != nil {
		t.Fatal(err)
	}
	if err := explicit.Run(2_000); err != nil {
		t.Fatal(err)
	}
	if got, want := implicit.Trace(), explicit.Trace(); !reflect.DeepEqual(got, want) {
		t.Fatalf("implicit map trace diverged from explicit map\nimplicit=%v\nexplicit=%v", got, want)
	}
}

func TestLSMEngineRunsDeterministicWorkload(t *testing.T) {
	s, err := NewSim(Config{Seed: 5_051, Engine: "lsm", NodeIDs: []raft.NodeID{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartWorkload(workloadpkg.Config{ClientCount: 2, KeyCount: 2, OperationsPerClient: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(2_000); err != nil {
		t.Fatalf("LSM workload Run(): %v", err)
	}
}

func TestLSMEngineCrashDiscardsAndReopensStateMachine(t *testing.T) {
	s, err := NewSim(Config{Seed: 5_052, Engine: "lsm", NodeIDs: []raft.NodeID{1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartWorkload(workloadpkg.Config{ClientCount: 1, KeyCount: 1, OperationsPerClient: 1}); err != nil {
		t.Fatal(err)
	}
	s.handleCrash(1)
	if s.nodes[1].sm != nil {
		t.Fatal("LSM state machine remained live after simulated process crash")
	}
	s.handleRestart(1)
	if s.nodes[1].node == nil || s.nodes[1].halted || s.nodes[1].sm == nil {
		t.Fatalf("LSM restart state = node:%t halted:%t sm:%t, want live node and state machine", s.nodes[1].node != nil, s.nodes[1].halted, s.nodes[1].sm != nil)
	}
}
