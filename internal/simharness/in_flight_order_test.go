//go:build !buggy

package simharness

import (
	"testing"

	"miniquorum/checker"
	raftpb "miniquorum/proto"
)

// ptr returns a pointer to a return time for a completed operation.
func ptr(v int64) *int64 { return &v }

// orderingHistory is a hand-built history exercising every equal-time boundary
// the fault-overlap accounting must respect:
//
//	A  client 10  PUT     [100, 200]
//	B  client 20  GET     [150, open)
//	C  client 30  DELETE  [300, 300]  (instantaneous)
//	D  client 40  DELETE  [400, 500]
func orderingHistory() checker.History {
	return checker.History{
		{ClientID: 10, Seq: 1, InvokeTime: 100, ReturnTime: ptr(200), Input: checker.Input{Op: raftpb.Op_PUT}},
		{ClientID: 20, Seq: 1, InvokeTime: 150, ReturnTime: nil, Input: checker.Input{Op: raftpb.Op_GET}},
		{ClientID: 30, Seq: 1, InvokeTime: 300, ReturnTime: ptr(300), Input: checker.Input{Op: raftpb.Op_DELETE}},
		{ClientID: 40, Seq: 1, InvokeTime: 400, ReturnTime: ptr(500), Input: checker.Input{Op: raftpb.Op_DELETE}},
	}
}

// TestFaultCaughtOperationHonorsPreEnqueuedOrdering pins the predicate a
// scheduled fault uses: a fault enqueued before every workload event fires
// ahead of an equal-time invocation (exclusive invoke) but before an equal-time
// return (inclusive return).
func TestFaultCaughtOperationHonorsPreEnqueuedOrdering(t *testing.T) {
	history := orderingHistory()
	a, b, c := history[0], history[1], history[2]

	cases := []struct {
		name string
		op   checker.Operation
		at   int64
		want bool
	}{
		{"before invoke", a, 99, false},
		{"equal invoke excluded", a, 100, false},
		{"strictly after invoke", a, 101, true},
		{"equal return included", a, 200, true},
		{"after return excluded", a, 201, false},
		{"open equal invoke excluded", b, 150, false},
		{"open after invoke", b, 151, true},
		{"open far future still in flight", b, 1_000_000, true},
		{"instantaneous equal instant excluded", c, 300, false},
	}
	for _, tc := range cases {
		if got := faultCatchesOperation(tc.op, tc.at); got != tc.want {
			t.Errorf("%s: faultCatchesOperation(op invoke=%d return=%v, %d) = %t, want %t",
				tc.name, tc.op.InvokeTime, tc.op.ReturnTime, tc.at, got, tc.want)
		}
	}
}

// TestFaultInFlightClientsExcludesEqualTimeInvocation proves the corrected
// fault-overlap count excludes an operation invoked at the fault instant while
// the unchanged closed-interval operation-overlap count still includes it —
// the exact "a fault executed before an equal-time invocation does not count,
// while a genuine fault after invocation and before/equal return does" contract
// from the packet, and the guarantee that operation-overlap is not weakened.
func TestFaultInFlightClientsExcludesEqualTimeInvocation(t *testing.T) {
	history := orderingHistory()

	faultCases := []struct {
		at   int64
		want int
	}{
		{100, 0}, // A's invoke instant: the pre-enqueued fault precedes it
		{101, 1}, // A genuinely in flight
		{150, 1}, // A in flight; B's invoke instant excluded
		{151, 2}, // A and B in flight
		{200, 2}, // A's return instant still counts; B open
		{201, 1}, // A returned; B open
		{300, 1}, // only B open; C's equal instant excluded
		{450, 2}, // B open and D in flight
	}
	for _, tc := range faultCases {
		if got := faultInFlightClients(history, tc.at); got != tc.want {
			t.Errorf("faultInFlightClients(%d) = %d, want %d", tc.at, got, tc.want)
		}
	}

	// The closed operation-overlap measure must still credit the equal-time
	// invocations the fault measure now excludes — this is the boundary where
	// the two measures deliberately differ.
	closedContrast := map[int64][2]int{
		// at: {faultInFlight, clientsInFlight}
		100: {0, 1}, // A's invoke: fault excludes, operation-overlap includes
		150: {1, 2}, // B's invoke: fault excludes, operation-overlap includes
		300: {1, 2}, // C's invoke: fault excludes, operation-overlap includes
	}
	for at, want := range closedContrast {
		if got := faultInFlightClients(history, at); got != want[0] {
			t.Errorf("faultInFlightClients(%d) = %d, want %d", at, got, want[0])
		}
		if got := clientsInFlightAt(history, at); got != want[1] {
			t.Errorf("clientsInFlightAt(%d) = %d, want %d (operation-overlap must not be weakened)", at, got, want[1])
		}
	}
}

// TestMutatingFaultOverlapRequiresPutOrDelete pins the helper backing the
// partition-during-write gate: a fault overlaps a mutation only when a PUT or
// DELETE is genuinely in flight — an in-flight GET, or a mutation invoked at
// the pre-enqueued fault's own instant, does not qualify.
func TestMutatingFaultOverlapRequiresPutOrDelete(t *testing.T) {
	history := orderingHistory()

	cases := []struct {
		name string
		at   int64
		want bool
	}{
		{"PUT in flight", 150, true},               // A (PUT) [100,200]
		{"GET-only in flight", 250, false},         // only B (GET) open
		{"equal-time DELETE excluded", 300, false}, // C (DELETE) invoked at 300, only B (GET) open
		{"DELETE in flight", 450, true},            // D (DELETE) [400,500]
	}
	for _, tc := range cases {
		if got := mutatingClientInFlightForFault(history, tc.at); got != tc.want {
			t.Errorf("%s: mutatingClientInFlightForFault(%d) = %t, want %t", tc.name, tc.at, got, tc.want)
		}
	}
}

// TestScheduledFaultAtInvocationInstantIsNotCounted is the end-to-end proof on
// a real deterministic run: every client's first operation is invoked at t=0
// (the workload start), and every FaultSchedule.Events entry is enqueued before
// the workload starts, so a t=0 scheduled fault fires ahead of every t=0
// invocation. The corrected fault measure catches none of them; the unweakened
// operation-overlap measure still counts them.
func TestScheduledFaultAtInvocationInstantIsNotCounted(t *testing.T) {
	result := mustRunSeed(t, RunConfig{Seed: 1})
	history := result.History

	atZero := 0
	for _, operation := range history {
		if operation.InvokeTime == 0 {
			atZero++
		}
	}
	if atZero < 2 {
		t.Fatalf("history has %d operations invoked at t=0, want the concurrent first operations", atZero)
	}
	if got := faultInFlightClients(history, 0); got != 0 {
		t.Fatalf("faultInFlightClients(0) = %d, want 0: a pre-enqueued t=0 fault precedes every t=0 invocation", got)
	}
	if got := clientsInFlightAt(history, 0); got < 2 {
		t.Fatalf("clientsInFlightAt(0) = %d, want the closed measure to still count the concurrent t=0 invocations", got)
	}
}
