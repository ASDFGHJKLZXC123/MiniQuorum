package sim

import (
	"errors"
	"fmt"

	"miniquorum/internal/raft"
)

// ErrCrashed is the distinguished error surfaced by CrashStorage once a
// crash has taken effect: Save returns it (possibly wrapped) when the
// simulated process dies inside that Save, and again for any Save attempted
// while the storage is down. Under the frozen Ready contract a Save error is
// fail-stop — no send, no apply, no Advance — which is exactly the behavior
// of a host whose process died mid-batch. Hosts match it with errors.Is.
var ErrCrashed = errors.New("sim: storage crashed")

// CrashPoint is one schedulable crash instant in the life of a single Ready
// batch. Save stages the batch's record bytes into the unsynced (dirty)
// buffer, moves them to durable at its internal sync barrier, and returns;
// only then does the host send the batch's messages.
type CrashPoint uint8

const (
	// CrashHostInitiated tags a crash triggered by Crash() between batches
	// (e.g. a virtual-time-scheduled kill). It is not schedulable from a
	// directive; validation rejects it.
	CrashHostInitiated CrashPoint = iota
	// CrashBeforeSync fires after the batch's records are staged dirty but
	// before the sync barrier. Only the directive's retained prefix of the
	// unsynced bytes survives — none, all, or a mid-record torn tail.
	CrashBeforeSync
	// CrashAfterSyncBeforeSend fires after the sync barrier, so the batch
	// is fully durable, but Save still returns ErrCrashed: the host never
	// sends the batch's messages (persist-before-send's observable half).
	CrashAfterSyncBeforeSend
	// CrashAfterSend lets Save succeed with the batch fully durable and
	// arms the crash. The host must fire it with CrashIfArmed immediately
	// after sending the batch's messages and before applying; a Save
	// attempted while armed fires it as a deterministic backstop.
	CrashAfterSend
)

// String names the crash point for traces and error text.
func (p CrashPoint) String() string {
	switch p {
	case CrashHostInitiated:
		return "host-initiated"
	case CrashBeforeSync:
		return "before-sync"
	case CrashAfterSyncBeforeSend:
		return "after-sync-before-send"
	case CrashAfterSend:
		return "after-send"
	default:
		return fmt.Sprintf("crashpoint(%d)", uint8(p))
	}
}

// RetainAllUnsynced is the CrashDirective.RetainUnsynced sentinel that keeps
// every unsynced byte at a CrashBeforeSync crash.
const RetainAllUnsynced = -1

// CrashDirective schedules exactly one crash: node Node dies at its Save-th
// accepted Save call, at Point. Ordinals are 1-based, count every accepted
// Save (including batches with no hard state and no entries), never reset
// across Recover, and exclude calls rejected while crashed or armed.
//
// RetainUnsynced applies only to CrashBeforeSync: it is the surviving prefix
// length, in bytes, of the unsynced buffer — 0 loses the whole batch,
// RetainAllUnsynced keeps it all, anything between may tear the tail record
// mid-byte; values beyond the buffer are clamped to it. At the other points
// the buffer is already empty, so the field is ignored.
//
// A directive is pure data, so a schedule replays byte-identically: same
// directives + same Save sequence = same survivors. Phase 3C's crash-point
// matrix and the Phase 4/5 fault harnesses reuse this struct verbatim.
type CrashDirective struct {
	Node           raft.NodeID
	Save           uint64
	Point          CrashPoint
	RetainUnsynced int
}

// FaultSchedule is the deterministic storage-fault plan for one simulated
// run: every scheduled crash for every node, in plain data.
type FaultSchedule struct {
	Crashes []CrashDirective
}

// For returns a fresh slice of the directives targeting node id, preserving
// schedule order. Directives for absent nodes simply never fire.
func (s FaultSchedule) For(id raft.NodeID) []CrashDirective {
	var out []CrashDirective
	for _, d := range s.Crashes {
		if d.Node == id {
			out = append(out, d)
		}
	}
	return out
}

// CrashInfo reports what one crash actually did, for assertions and traces.
type CrashInfo struct {
	Save          uint64     // triggering Save ordinal; last accepted Save for host-initiated crashes
	Point         CrashPoint // where in the batch the crash fired
	UnsyncedBytes int        // dirty bytes at the crash instant, before retention
	RetainedBytes int        // dirty bytes that reached the platter
}
