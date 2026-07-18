package workload

import (
	"errors"
	"reflect"
	"testing"

	"github.com/anishathalye/porcupine"

	"miniquorum/checker"
)

type scheduled struct {
	at    int64
	event Event
}

type fakeHost struct {
	now         int64
	statuses    []SubmitStatus
	scheduled   []scheduled
	submissions []Submission
	canceled    []AttemptID
}

func (h *fakeHost) Now() int64 { return h.now }

func (h *fakeHost) Schedule(at int64, event Event) {
	h.scheduled = append(h.scheduled, scheduled{at: at, event: event})
}

func (h *fakeHost) Submit(submission Submission) (SubmitStatus, error) {
	h.submissions = append(h.submissions, submission)
	if len(h.statuses) == 0 {
		return SubmitAccepted, nil
	}
	status := h.statuses[0]
	h.statuses = h.statuses[1:]
	return status, nil
}

func (h *fakeHost) Cancel(attempt AttemptID) {
	h.canceled = append(h.canceled, attempt)
}

func (h *fakeHost) lastScheduled(kind EventKind) (scheduled, bool) {
	for i := len(h.scheduled) - 1; i >= 0; i-- {
		if h.scheduled[i].event.Kind == kind {
			return h.scheduled[i], true
		}
	}
	return scheduled{}, false
}

func oneClientRunner(t *testing.T, timeout int64) *Runner {
	t.Helper()
	runner, err := New(Config{
		Seed:                91,
		ClientCount:         1,
		ClientIDBase:        10,
		KeyCount:            DefaultKeyCount,
		OperationsPerClient: 1,
		OperationTimeout:    timeout,
		RetryDelay:          5,
		ThinkTime:           1,
		Targets:             []uint64{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runner
}

func TestRunnerCollapsesNotLeaderAndLeadershipLostRetriesUsingSameSequence(t *testing.T) {
	runner := oneClientRunner(t, 100)
	host := &fakeHost{statuses: []SubmitStatus{SubmitNotLeader, SubmitAccepted, SubmitAccepted}}
	if err := runner.Start(host); err != nil {
		t.Fatalf("Start: %v", err)
	}
	start, ok := host.lastScheduled(EventStart)
	if !ok {
		t.Fatal("client start was not scheduled")
	}
	if err := runner.Handle(host, start.event); err != nil {
		t.Fatalf("Handle(start): %v", err)
	}
	if err := runner.Handle(host, Event{Kind: EventStart, AttemptID: AttemptID{ClientID: 10}}); !errors.Is(err, checker.ErrOutstandingOperation) {
		t.Fatalf("second logical start error = %v, want ErrOutstandingOperation", err)
	}

	retry, ok := host.lastScheduled(EventRetry)
	if !ok {
		t.Fatal("not-leader retry was not scheduled")
	}
	host.now = retry.at
	if err := runner.Handle(host, retry.event); err != nil {
		t.Fatalf("Handle(not-leader retry): %v", err)
	}
	secondAttempt := host.submissions[len(host.submissions)-1].AttemptID
	host.now++
	if err := runner.Handle(host, Event{Kind: EventLeadershipLost, AttemptID: secondAttempt}); err != nil {
		t.Fatalf("Handle(leadership lost): %v", err)
	}
	retry, ok = host.lastScheduled(EventRetry)
	if !ok {
		t.Fatal("leadership-lost retry was not scheduled")
	}
	host.now = retry.at
	if err := runner.Handle(host, retry.event); err != nil {
		t.Fatalf("Handle(leadership-lost retry): %v", err)
	}
	thirdAttempt := host.submissions[len(host.submissions)-1].AttemptID
	host.now++
	if err := runner.Handle(host, Event{Kind: EventApplied, AttemptID: thirdAttempt, Output: checker.Output{OK: true}}); err != nil {
		t.Fatalf("Handle(applied): %v", err)
	}

	if len(host.submissions) != 3 {
		t.Fatalf("submission count = %d, want 3 physical attempts", len(host.submissions))
	}
	firstCommand := host.submissions[0].Command
	for i, submission := range host.submissions {
		if submission.AttemptID.ClientID != 10 || submission.AttemptID.Seq != 1 {
			t.Fatalf("attempt %d identity = %+v, want client 10 seq 1", i, submission.AttemptID)
		}
		if submission.Command.GetClientId() != 10 || submission.Command.GetSeq() != 1 || !reflect.DeepEqual(submission.Command.GetKey(), firstCommand.GetKey()) || !reflect.DeepEqual(submission.Command.GetValue(), firstCommand.GetValue()) || submission.Command.GetOp() != firstCommand.GetOp() {
			t.Fatalf("attempt %d changed logical command: got %+v want %+v", i, submission.Command, firstCommand)
		}
	}
	history := runner.History()
	if len(history) != 1 || history[0].Seq != 1 || history[0].InvokeTime != 0 || history[0].ReturnTime == nil || *history[0].ReturnTime != host.now {
		t.Fatalf("history = %#v, want one operation spanning all retries", history)
	}
}

func TestRunnerTimeoutRetryCompletesSameLogicalRecordAndAdvancesSession(t *testing.T) {
	runner := oneClientRunner(t, 20)
	host := &fakeHost{statuses: []SubmitStatus{SubmitAccepted, SubmitAccepted}}
	if err := runner.Start(host); err != nil {
		t.Fatalf("Start: %v", err)
	}
	start, _ := host.lastScheduled(EventStart)
	if err := runner.Handle(host, start.event); err != nil {
		t.Fatalf("Handle(start): %v", err)
	}
	initialHistory := runner.History()
	if len(initialHistory) != 1 {
		t.Fatalf("history after invocation = %#v, want one logical record", initialHistory)
	}
	deadline, ok := host.lastScheduled(EventDeadline)
	if !ok {
		t.Fatal("deadline was not scheduled")
	}
	host.now = deadline.at
	if err := runner.Handle(host, deadline.event); err != nil {
		t.Fatalf("Handle(deadline): %v", err)
	}

	history := runner.History()
	if len(history) != 1 || history[0].ReturnTime != nil {
		t.Fatalf("history after timeout = %#v, want one open record", history)
	}
	clients := runner.Clients()
	if len(clients) != 1 || clients[0].OutstandingSeq != 1 || !clients[0].TimedOut {
		t.Fatalf("client after timeout = %#v, want timed-out seq 1 still outstanding", clients)
	}
	if len(host.canceled) != 1 || host.canceled[0].Seq != 1 {
		t.Fatalf("canceled attempts = %#v, want active seq 1 attempt", host.canceled)
	}

	retryEvent, err := runner.RetryEvent(10)
	if err != nil {
		t.Fatalf("RetryEvent: %v", err)
	}
	host.now++
	if err := runner.Handle(host, retryEvent); err != nil {
		t.Fatalf("Handle(manual retry): %v", err)
	}
	if len(host.submissions) != 2 {
		t.Fatalf("submission count = %d, want retry", len(host.submissions))
	}
	first, retry := host.submissions[0], host.submissions[1]
	if retry.AttemptID.Seq != first.AttemptID.Seq || retry.AttemptID.ClientID != first.AttemptID.ClientID {
		t.Fatalf("retry identity = %+v, want original logical identity %+v", retry.AttemptID, first.AttemptID)
	}
	if retry.AttemptID.Attempt <= first.AttemptID.Attempt ||
		retry.Command.GetClientId() != first.Command.GetClientId() ||
		retry.Command.GetSeq() != first.Command.GetSeq() ||
		retry.Command.GetOp() != first.Command.GetOp() ||
		!reflect.DeepEqual(retry.Command.GetKey(), first.Command.GetKey()) ||
		!reflect.DeepEqual(retry.Command.GetValue(), first.Command.GetValue()) {
		t.Fatalf("retry changed logical command: first=%+v retry=%+v", first, retry)
	}
	if clients := runner.Clients(); clients[0].NextSeq != 1 || clients[0].OutstandingSeq != 1 {
		t.Fatalf("client advanced after timeout retry: %#v", clients[0])
	}

	host.now++
	completionTime := host.now
	if err := runner.Handle(host, Event{
		Kind:      EventApplied,
		AttemptID: retry.AttemptID,
		Output:    checker.Output{OK: true},
	}); err != nil {
		t.Fatalf("Handle(applied retry): %v", err)
	}

	history = runner.History()
	if len(history) != 1 {
		t.Fatalf("history after retry completion = %#v, want exactly one logical record", history)
	}
	operation := history[0]
	if operation.ClientID != first.AttemptID.ClientID || operation.Seq != first.AttemptID.Seq ||
		operation.InvokeTime != initialHistory[0].InvokeTime || operation.ReturnTime == nil ||
		*operation.ReturnTime != completionTime || !reflect.DeepEqual(operation.Input, initialHistory[0].Input) {
		t.Fatalf("completed logical record = %#v, initial record = %#v", operation, initialHistory[0])
	}
	events := history.PorcupineEvents()
	if len(events) != 2 || events[0].Kind != porcupine.CallEvent || events[1].Kind != porcupine.ReturnEvent || events[0].Id != events[1].Id {
		t.Fatalf("Porcupine events = %#v, want exactly one matching call/return", events)
	}
	if output, ok := events[1].Value.(checker.Output); !ok || output.Unknown {
		t.Fatalf("retry completion output = %#v, want observed non-unknown return", events[1].Value)
	}
	clients = runner.Clients()
	if len(clients) != 1 || clients[0].NextSeq != 2 || clients[0].Generated != 1 ||
		clients[0].Completed != 1 || clients[0].OutstandingSeq != 0 || clients[0].TimedOut {
		t.Fatalf("client after retry completion = %#v, want session advanced to seq 2", clients)
	}
	if !runner.Done() {
		t.Fatal("runner is not done after the retried logical operation completed")
	}
}
