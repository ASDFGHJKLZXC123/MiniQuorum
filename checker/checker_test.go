package checker

import (
	"bytes"
	"testing"

	"github.com/anishathalye/porcupine"

	raftpb "miniquorum/proto"
)

func TestKVModelPUTGETDELETEBehavior(t *testing.T) {
	state := KVModel.Init()
	put := Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("value")}
	ok, state := KVModel.Step(state, put, Output{OK: true})
	if !ok {
		t.Fatal("successful PUT was rejected")
	}
	if ok, _ := KVModel.Step(state, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Value: []byte("value"), Found: true}); !ok {
		t.Fatal("GET returning the current value was rejected")
	}
	if ok, _ := KVModel.Step(state, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Value: []byte("stale"), Found: true}); ok {
		t.Fatal("GET returning a stale value was accepted")
	}
	if ok, _ := KVModel.Step(state, put, Output{OK: false}); ok {
		t.Fatal("failed PUT response was accepted")
	}

	deleteInput := Input{Op: raftpb.Op_DELETE, Key: []byte("k")}
	ok, state = KVModel.Step(state, deleteInput, Output{OK: true})
	if !ok {
		t.Fatal("successful DELETE was rejected")
	}
	if ok, _ := KVModel.Step(state, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Found: false}); !ok {
		t.Fatal("GET returning absence after DELETE was rejected")
	}
	if ok, _ := KVModel.Step(state, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Value: nil, Found: true}); ok {
		t.Fatal("GET claiming an absent key was found was accepted")
	}
}

func TestKVModelPartitionsCompletedAndUnfinishedOperationsByKey(t *testing.T) {
	history := History{
		{
			ClientID:   1,
			Seq:        1,
			InvokeTime: 0,
			Input:      Input{Op: raftpb.Op_PUT, Key: []byte("z"), Value: []byte("open")},
		},
		completedOperation(2, 1, 1, 2,
			Input{Op: raftpb.Op_GET, Key: []byte("a")},
			Output{OK: true, Found: false}),
	}
	events := history.PorcupineEvents()
	partitions := KVModel.PartitionEvent(events)
	if len(partitions) != 2 {
		t.Fatalf("partition count = %d, want 2", len(partitions))
	}
	wantKeys := [][]byte{[]byte("a"), []byte("z")}
	for i, partition := range partitions {
		if len(partition) != 2 {
			t.Fatalf("partition %d event count = %d, want paired call/return", i, len(partition))
		}
		call := partition[0]
		if call.Kind != porcupine.CallEvent {
			t.Fatalf("partition %d first event = %v, want call", i, call.Kind)
		}
		input := call.Value.(Input)
		if !bytes.Equal(input.Key, wantKeys[i]) {
			t.Fatalf("partition %d key = %q, want %q", i, input.Key, wantKeys[i])
		}
		if partition[1].Id != call.Id || partition[1].Kind != porcupine.ReturnEvent {
			t.Fatalf("partition %d did not retain its matching return: %#v", i, partition)
		}
	}
}

func TestRecorderCollapsesRetriesIntoOneLogicalOperation(t *testing.T) {
	recorder := NewRecorder()
	input := Input{Op: raftpb.Op_GET, Key: []byte("k")}
	if err := recorder.Invoke(5, 8, 10, input); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	for retry := 0; retry < 3; retry++ {
		if err := recorder.Retry(5, 8); err != nil {
			t.Fatalf("Retry %d: %v", retry, err)
		}
	}
	if err := recorder.Complete(5, 8, 30, Output{OK: true, Value: []byte("v"), Found: true}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	history := recorder.History()
	if len(history) != 1 {
		t.Fatalf("history length = %d, want one logical operation", len(history))
	}
	if history[0].Seq != 8 || history[0].InvokeTime != 10 || history[0].ReturnTime == nil || *history[0].ReturnTime != 30 {
		t.Fatalf("collapsed operation = %#v, want seq 8 spanning [10,30]", history[0])
	}
}
