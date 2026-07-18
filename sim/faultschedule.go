package sim

import (
	"encoding/json"
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

// FaultScheduleGeneratorVersion is bumped whenever GenerateFaultSchedule's
// algorithm changes in a way that would remap seed -> schedule (new fault
// kinds, different draw order, different parameter ranges, ...). Every
// generated schedule is stamped with the version that produced it so a
// committed corpus file is self-describing: a generator change never
// silently reinterprets an old file's meaning, per the phase-4 spec.
const FaultScheduleGeneratorVersion = 1

// FaultKind identifies one schedulable fault in FaultSchedule.Events. The
// zero value is intentionally not a valid kind (see FaultSchedule.Validate),
// so a zero-valued FaultEvent read from a malformed or truncated file is
// rejected instead of silently acting as FaultDropRate.
type FaultKind int

const (
	faultKindUnspecified FaultKind = iota

	// FaultDropRate sets the sim-wide probability, in [0,1], that a
	// scheduled message is dropped instead of delivered. It stays in effect
	// until superseded by a later FaultDropRate event.
	FaultDropRate
	// FaultDuplicateRate sets the sim-wide probability, in [0,1], that a
	// message which is not dropped is also re-enqueued a second time at an
	// independently drawn delay. It stays in effect until superseded.
	FaultDuplicateRate
	// FaultPartition blocks messages sent From -> To, mirroring Sim.Partition.
	FaultPartition
	// FaultHeal removes a block previously installed by FaultPartition (or
	// Sim.Partition), mirroring Sim.Heal.
	FaultHeal
	// FaultPartitionGroups splits Groups into mutually unreachable clusters:
	// every cross-group pair is blocked in both directions, mirroring
	// Sim.PartitionGroups. Nodes within a group are left unaffected.
	FaultPartitionGroups
	// FaultHealGroups reverses a prior FaultPartitionGroups split for the
	// same Groups, mirroring Sim.HealGroups.
	FaultHealGroups
	// FaultPause freezes Node: no ticks fire and no inbound message is
	// delivered until a matching FaultResume.
	FaultPause
	// FaultResume un-freezes a node previously paused by FaultPause.
	FaultResume
	// FaultClockSkew sets Node's per-node tick-rate multiplier in
	// [0.5, 2.0]: 0.5 ticks twice as often as the cluster default, 2.0 half
	// as often.
	FaultClockSkew
	// FaultCrash discards Node's live *raft.Node at this event's virtual
	// time, mirroring Sim.ScheduleCrash. Node's sim storage survives.
	FaultCrash
	// FaultRestart rebuilds Node from its surviving sim storage at this
	// event's virtual time, mirroring Sim.ScheduleRestart.
	FaultRestart
)

// String names the fault kind for traces and error text.
func (k FaultKind) String() string {
	switch k {
	case faultKindUnspecified:
		return "unspecified"
	case FaultDropRate:
		return "drop-rate"
	case FaultDuplicateRate:
		return "duplicate-rate"
	case FaultPartition:
		return "partition"
	case FaultHeal:
		return "heal"
	case FaultPartitionGroups:
		return "partition-groups"
	case FaultHealGroups:
		return "heal-groups"
	case FaultPause:
		return "pause"
	case FaultResume:
		return "resume"
	case FaultClockSkew:
		return "clock-skew"
	case FaultCrash:
		return "crash"
	case FaultRestart:
		return "restart"
	default:
		return fmt.Sprintf("faultkind(%d)", int(k))
	}
}

// minClockMultiplier and maxClockMultiplier bound FaultClockSkew, per the
// phase-4 spec's 0.5x-2x per-node tick-rate multiplier.
const (
	minClockMultiplier = 0.5
	maxClockMultiplier = 2.0
)

// FaultEvent is one entry in FaultSchedule.Events: a fault of Kind, to take
// effect at virtual time Time. Only the fields relevant to Kind are read;
// the rest are the zero value. FaultEvent is plain data (no methods that
// touch a *Sim), so a schedule can be built, serialized, and replayed
// entirely independently of any particular simulator run.
type FaultEvent struct {
	Time VirtualTime
	Kind FaultKind

	// Node targets FaultPause, FaultResume, FaultClockSkew, FaultCrash, and
	// FaultRestart.
	Node raft.NodeID
	// From and To target the directed FaultPartition and FaultHeal.
	From raft.NodeID
	To   raft.NodeID
	// Groups targets the symmetric FaultPartitionGroups and FaultHealGroups.
	Groups [][]raft.NodeID `json:",omitempty"`
	// Rate targets FaultDropRate and FaultDuplicateRate: a probability in
	// [0,1].
	Rate float64 `json:",omitempty"`
	// Multiplier targets FaultClockSkew: a tick-rate multiplier in
	// [minClockMultiplier, maxClockMultiplier].
	Multiplier float64 `json:",omitempty"`
}

// FaultSchedule is the deterministic fault plan for one simulated run: every
// scheduled storage crash point (Crashes, unchanged since Phase 3, keyed by
// per-node Save ordinal) plus every scheduled network/pause/skew/host-crash
// fault (Events, keyed by virtual time). Both halves are plain data, so a
// schedule replays byte-identically and serializes without ambiguity.
// Version records which GenerateFaultSchedule algorithm produced it (0 for a
// hand-written scripted schedule that predates versioning); see
// FaultScheduleGeneratorVersion.
type FaultSchedule struct {
	Version int
	Crashes []CrashDirective
	Events  []FaultEvent
}

// Validate reports a structural problem in the schedule: an out-of-range
// rate or clock-skew multiplier, a degenerate partition, or an unknown fault
// kind. It does not know the target Sim's node set, so it cannot catch a
// Node/From/To that names a node absent from that run; callers that apply
// events against unknown nodes skip them defensively instead of panicking.
func (s FaultSchedule) Validate() error {
	for i, e := range s.Events {
		switch e.Kind {
		case FaultDropRate, FaultDuplicateRate:
			if e.Rate < 0 || e.Rate > 1 {
				return fmt.Errorf("sim: fault event %d (%s): rate %v outside [0,1]", i, e.Kind, e.Rate)
			}
		case FaultClockSkew:
			if e.Multiplier < minClockMultiplier || e.Multiplier > maxClockMultiplier {
				return fmt.Errorf("sim: fault event %d (%s): multiplier %v outside [%v,%v]", i, e.Kind, e.Multiplier, minClockMultiplier, maxClockMultiplier)
			}
		case FaultPartition, FaultHeal:
			if e.From == e.To {
				return fmt.Errorf("sim: fault event %d (%s): From and To are both %d", i, e.Kind, e.From)
			}
		case FaultPartitionGroups, FaultHealGroups:
			if len(e.Groups) < 2 {
				return fmt.Errorf("sim: fault event %d (%s): need at least 2 groups, got %d", i, e.Kind, len(e.Groups))
			}
		case FaultPause, FaultResume, FaultCrash, FaultRestart:
			// Node-only; nothing further to check structurally.
		default:
			return fmt.Errorf("sim: fault event %d: unknown fault kind %d", i, int(e.Kind))
		}
	}
	return nil
}

// EncodeFaultSchedule serializes schedule as indented JSON: deterministic
// byte output for identical input (struct field order is fixed; Events and
// Crashes are ordinary slices, never a map), suitable for committing as a
// regression-corpus file.
func EncodeFaultSchedule(schedule FaultSchedule) ([]byte, error) {
	return json.MarshalIndent(schedule, "", "  ")
}

// DecodeFaultSchedule parses a schedule previously written by
// EncodeFaultSchedule, or a hand-authored scripted schedule in the same
// shape.
func DecodeFaultSchedule(data []byte) (FaultSchedule, error) {
	var schedule FaultSchedule
	if err := json.Unmarshal(data, &schedule); err != nil {
		return FaultSchedule{}, fmt.Errorf("sim: decode fault schedule: %w", err)
	}
	if err := schedule.Validate(); err != nil {
		return FaultSchedule{}, err
	}
	return schedule, nil
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
