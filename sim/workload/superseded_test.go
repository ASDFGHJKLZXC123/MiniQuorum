package workload

import (
	"testing"

	"miniquorum/checker"
)

// TestSupersededAttemptAndStaleDeadlineCannotAffectCurrentAttempt is the
// superseded-attempt guard regression. After a timeout and a same-sequence
// retry, the session's current attempt ordinal has advanced: a delayed
// EventApplied carrying the superseded attempt must not complete the logical
// operation, and a stale EventDeadline from the pre-retry deadline generation
// must not re-time-out the rearmed operation. Only the current attempt's
// completion may resolve the record.
func TestSupersededAttemptAndStaleDeadlineCannotAffectCurrentAttempt(t *testing.T) {
	runner := oneClientRunner(t, 20)
	host := &fakeHost{}
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
	firstAttempt := host.submissions[0].AttemptID
	firstDeadline, ok := host.lastScheduled(EventDeadline)
	if !ok {
		t.Fatal("deadline was not scheduled")
	}

	host.now = firstDeadline.at
	if err := runner.Handle(host, firstDeadline.event); err != nil {
		t.Fatalf("Handle(deadline): %v", err)
	}
	retryEvent, err := runner.RetryEvent(10)
	if err != nil {
		t.Fatalf("RetryEvent: %v", err)
	}
	host.now++
	if err := runner.Handle(host, retryEvent); err != nil {
		t.Fatalf("Handle(retry): %v", err)
	}
	if len(host.submissions) != 2 {
		t.Fatalf("submissions = %d, want the same-sequence retry", len(host.submissions))
	}
	currentAttempt := host.submissions[1].AttemptID
	if currentAttempt.Attempt <= firstAttempt.Attempt || currentAttempt.Seq != firstAttempt.Seq {
		t.Fatalf("retry attempt = %+v, want same seq with a later attempt than %+v", currentAttempt, firstAttempt)
	}

	// A delayed completion for the superseded first attempt arrives now. It
	// must be ignored: the client is waiting on the retry attempt.
	host.now++
	if err := runner.Handle(host, Event{Kind: EventApplied, AttemptID: firstAttempt, Output: checker.Output{OK: true}}); err != nil {
		t.Fatalf("Handle(superseded applied): %v", err)
	}
	if history := runner.History(); history[0].ReturnTime != nil {
		t.Fatalf("superseded attempt completed the logical operation: %#v", history[0])
	}
	if clients := runner.Clients(); clients[0].OutstandingSeq != 1 || clients[0].Completed != 0 {
		t.Fatalf("session state after superseded completion = %#v, want seq 1 still outstanding", clients[0])
	}

	// The stale deadline from before the retry (an older generation) fires
	// late. It must not re-time-out the rearmed operation.
	if err := runner.Handle(host, Event{
		Kind:               EventDeadline,
		AttemptID:          AttemptID{ClientID: 10, Seq: 1},
		DeadlineGeneration: firstDeadline.event.DeadlineGeneration,
	}); err != nil {
		t.Fatalf("Handle(stale deadline): %v", err)
	}
	if clients := runner.Clients(); clients[0].TimedOut {
		t.Fatalf("stale deadline generation re-timed-out the operation: %#v", clients[0])
	}
	if len(host.canceled) != 1 {
		t.Fatalf("cancellations = %d, want only the original deadline's cancel", len(host.canceled))
	}

	// Only the current attempt's completion resolves the record.
	host.now++
	completion := host.now
	if err := runner.Handle(host, Event{Kind: EventApplied, AttemptID: currentAttempt, Output: checker.Output{OK: true}}); err != nil {
		t.Fatalf("Handle(current applied): %v", err)
	}
	history := runner.History()
	if history[0].ReturnTime == nil || *history[0].ReturnTime != completion {
		t.Fatalf("current attempt did not complete the logical operation: %#v", history[0])
	}
	if clients := runner.Clients(); clients[0].Completed != 1 || clients[0].NextSeq != 2 {
		t.Fatalf("session did not advance after the current attempt completed: %#v", clients[0])
	}
}
