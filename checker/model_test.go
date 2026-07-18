package checker

import (
	"errors"
	"testing"

	"github.com/anishathalye/porcupine"

	raftpb "miniquorum/proto"
)

func TestKVModelLegalHistory(t *testing.T) {
	history := History{
		completedOperation(1, 1, 0, 1, Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("one")}, Output{OK: true}),
		completedOperation(2, 1, 2, 3, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Value: []byte("one"), Found: true}),
		completedOperation(1, 2, 4, 5, Input{Op: raftpb.Op_DELETE, Key: []byte("k")}, Output{OK: true}),
		completedOperation(2, 2, 6, 7, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Found: false}),
	}
	if !Check(history) {
		t.Fatal("legal PUT/GET/DELETE history was rejected")
	}
}

func TestKVModelRejectsStaleGET(t *testing.T) {
	history := History{
		completedOperation(1, 1, 0, 1, Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("new")}, Output{OK: true}),
		completedOperation(2, 1, 2, 3, Input{Op: raftpb.Op_GET, Key: []byte("k")}, Output{OK: true, Found: false}),
	}
	if Check(history) {
		t.Fatal("history containing a stale GET was accepted")
	}
}

func TestTimeoutLeavesPorcupineOperationUnfinished(t *testing.T) {
	recorder := NewRecorder()
	if err := recorder.Invoke(7, 11, 10, Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("maybe")}); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if err := recorder.Timeout(7, 11, 25); err != nil {
		t.Fatalf("Timeout: %v", err)
	}

	history := recorder.History()
	if len(history) != 1 {
		t.Fatalf("history length = %d, want 1", len(history))
	}
	if history[0].InvokeTime != 10 {
		t.Fatalf("invoke time = %d, want 10", history[0].InvokeTime)
	}
	if history[0].ReturnTime != nil {
		t.Fatalf("timeout recorded a completion at %d", *history[0].ReturnTime)
	}

	events := history.PorcupineEvents()
	if len(events) != 2 {
		t.Fatalf("Porcupine events = %d, want call plus synthetic unknown return", len(events))
	}
	if events[0].Kind != porcupine.CallEvent {
		t.Fatalf("event kind = %v, want CallEvent", events[0].Kind)
	}
	if events[1].Kind != porcupine.ReturnEvent {
		t.Fatalf("synthetic event kind = %v, want ReturnEvent", events[1].Kind)
	}
	output, ok := events[1].Value.(Output)
	if !ok || !output.Unknown {
		t.Fatalf("synthetic return = %#v, want unknown output", events[1].Value)
	}
	if !porcupine.CheckEvents(KVModel, events) {
		t.Fatal("Porcupine rejected history containing an unfinished timeout")
	}
}

func TestTimeoutKeepsSessionOutstandingAndOnlySameSequenceMayRetry(t *testing.T) {
	recorder := NewRecorder()
	input := Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("value")}
	if err := recorder.Invoke(9, 3, 10, input); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if err := recorder.Timeout(9, 3, 20); err != nil {
		t.Fatalf("Timeout: %v", err)
	}
	if !recorder.HasOutstanding(9) {
		t.Fatal("timeout released the stable session; want operation to remain outstanding")
	}
	if err := recorder.Retry(9, 3); err != nil {
		t.Fatalf("Retry(same seq): %v", err)
	}
	if err := recorder.Retry(9, 4); !errors.Is(err, ErrNoOutstandingOperation) {
		t.Fatalf("Retry(new seq) error = %v, want ErrNoOutstandingOperation", err)
	}
	if err := recorder.Invoke(9, 4, 21, input); !errors.Is(err, ErrOutstandingOperation) {
		t.Fatalf("Invoke(new seq) error = %v, want ErrOutstandingOperation", err)
	}

	history := recorder.History()
	if len(history) != 1 || history[0].ReturnTime != nil {
		t.Fatalf("history = %#v, want one still-open logical record", history)
	}
}

func TestTimedOutPUTMayLinearizeBeforeOrAfterLaterGET(t *testing.T) {
	openPUT := Operation{
		ClientID:   1,
		Seq:        1,
		InvokeTime: 0,
		Input:      Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("new")},
	}
	tests := []struct {
		name string
		get  Output
	}{
		{name: "after GET leaves old absent value", get: Output{OK: true, Found: false}},
		{name: "before GET exposes new value", get: Output{OK: true, Value: []byte("new"), Found: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			history := History{
				openPUT,
				completedOperation(2, 1, 2, 3, Input{Op: raftpb.Op_GET, Key: []byte("k")}, test.get),
			}
			if !Check(history) {
				t.Fatal("legal history with an outcome-unknown PUT was rejected")
			}
		})
	}
}

func TestTimedOutDELETEMayLinearizeBeforeOrAfterLaterGET(t *testing.T) {
	initialPUT := completedOperation(3, 1, 0, 1,
		Input{Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("old")},
		Output{OK: true})
	openDELETE := Operation{
		ClientID:   1,
		Seq:        1,
		InvokeTime: 2,
		Input:      Input{Op: raftpb.Op_DELETE, Key: []byte("k")},
	}
	tests := []struct {
		name string
		get  Output
	}{
		{name: "after GET preserves old value", get: Output{OK: true, Value: []byte("old"), Found: true}},
		{name: "before GET exposes absence", get: Output{OK: true, Found: false}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			history := History{
				initialPUT,
				openDELETE,
				completedOperation(2, 1, 3, 4, Input{Op: raftpb.Op_GET, Key: []byte("k")}, test.get),
			}
			if !Check(history) {
				t.Fatal("legal history with an outcome-unknown DELETE was rejected")
			}
		})
	}
}

func completedOperation(clientID, seq uint64, invoke, returnAt int64, input Input, output Output) Operation {
	returnTime := returnAt
	return Operation{
		ClientID:   clientID,
		Seq:        seq,
		InvokeTime: invoke,
		ReturnTime: &returnTime,
		Input:      input,
		Output:     output,
	}
}
