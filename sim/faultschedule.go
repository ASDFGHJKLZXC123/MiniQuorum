package sim

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

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
// RestartAfterCrash is host policy used by NewFaultSim. When true, the sim
// enqueues a restart at the same virtual time only after this directive is
// observed firing. The default false keeps Phase 3's explicit/manual
// recovery semantics and lets scripted schedules choose a later
// FaultRestart event when they need a deliberate down interval.
//
// A directive is pure data, so a schedule replays byte-identically: same
// directives + same Save sequence = same survivors and recovery policy.
// Phase 3C's crash-point matrix and the Phase 4/5 fault harnesses reuse this
// struct verbatim.
type CrashDirective struct {
	Node              raft.NodeID
	Save              uint64
	Point             CrashPoint
	RetainUnsynced    int
	RestartAfterCrash bool `json:",omitempty"`
}

// FaultScheduleGeneratorVersion is bumped whenever GenerateFaultSchedule's
// algorithm changes in a way that would remap seed -> schedule (new fault
// kinds, different draw order, different parameter ranges, ...). Every
// generated schedule is stamped with the version that produced it so a
// committed corpus file is self-describing: a generator change never
// silently reinterprets an old file's meaning, per the phase-4 spec.
//
// v2 replaced v1's independently-drawn pause/resume, partition/heal, and
// partition-groups/heal-groups events (which could draw a lone Resume/Heal
// with no preceding Pause/Partition for the same target -- a guaranteed
// no-op) with matched (open, close) pairs on the same target, and paired
// every generated Save-ordinal crash directive with a later FaultRestart.
//
// v3 makes all generated stateful pair intervals globally non-overlapping,
// preventing group and directed partitions from redundantly owning the same
// edge. It also adds serialized CrashDirective.RestartAfterCrash and uses it
// for causal same-time recovery after a generated Save-ordinal crash fires,
// replacing v2's unrelated late-run restart heuristic.
//
// v4 appends a deterministic recovery tail at the generation horizon: message
// drop/duplication rates return to zero and every node's skew returns to 1x.
// Stateful pairs already close structurally. The recovery tail keeps a short
// fault campaign from turning the rest of a workload into a vacuous retry
// storm while preserving every injected fault in the serialized artifact.
//
// v5 varies the surviving-unsynced-prefix retention of generated before-sync
// storage crashes across none (0), a positive partial/torn prefix, and all
// (RetainAllUnsynced) instead of pinning every generated crash at
// RetainAllUnsynced, so the closed seed campaign finally combines Phase 3's
// before-sync loss and torn-write survivor states with the network/pause/skew
// faults (guide decision #16; Phase 3 -> Phase 4 hand-off "drive combinations").
// Post-sync crash points keep RetainAllUnsynced, where the drained buffer makes
// retention a no-op. Because the retention draws consume additional RNG values
// inside the crash loop, every generated schedule's later draws remap — hence
// the version bump and full corpus regeneration.
const FaultScheduleGeneratorVersion = 5

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
	// [0.5, 2.0]: 2.0 ticks twice as often as the cluster default (half the
	// interval), 0.5 ticks half as often (double the interval).
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

// Validate reports a structural problem in the schedule: a negative event
// time, an unsupported generator version, a non-finite or out-of-range rate
// or clock-skew multiplier, a degenerate or malformed partition group split,
// an unknown fault kind, or a malformed crash directive (bad Save ordinal,
// non-schedulable Point, out-of-range RetainUnsynced, or a duplicate
// (Node,Save) pair). It does not know the target Sim's node set, so it
// cannot catch a Node/From/To/group-member/crash-directive that names a node
// absent from that run — construct with NewFaultSim, which layers
// ValidateForCluster's cluster-membership checks on top of Validate once the
// node set is known, instead of silently skipping an unreachable target.
func (s FaultSchedule) Validate() error {
	if s.Version != 0 && s.Version != FaultScheduleGeneratorVersion {
		return fmt.Errorf("sim: fault schedule version %d unsupported (want 0 for a hand-written scripted schedule, or %d)", s.Version, FaultScheduleGeneratorVersion)
	}
	for i, e := range s.Events {
		if e.Time < 0 {
			return fmt.Errorf("sim: fault event %d (%s): negative time %d", i, e.Kind, e.Time)
		}
		switch e.Kind {
		case FaultDropRate, FaultDuplicateRate:
			if err := validateUnitRate(e.Rate); err != nil {
				return fmt.Errorf("sim: fault event %d (%s): %w", i, e.Kind, err)
			}
		case FaultClockSkew:
			if math.IsNaN(e.Multiplier) || math.IsInf(e.Multiplier, 0) {
				return fmt.Errorf("sim: fault event %d (%s): multiplier %v is not finite", i, e.Kind, e.Multiplier)
			}
			if e.Multiplier < minClockMultiplier || e.Multiplier > maxClockMultiplier {
				return fmt.Errorf("sim: fault event %d (%s): multiplier %v outside [%v,%v]", i, e.Kind, e.Multiplier, minClockMultiplier, maxClockMultiplier)
			}
		case FaultPartition, FaultHeal:
			if e.From == e.To {
				return fmt.Errorf("sim: fault event %d (%s): From and To are both %d", i, e.Kind, e.From)
			}
		case FaultPartitionGroups, FaultHealGroups:
			if err := validatePartitionGroups(e.Groups); err != nil {
				return fmt.Errorf("sim: fault event %d (%s): %w", i, e.Kind, err)
			}
		case FaultPause, FaultResume, FaultCrash, FaultRestart:
			// Node-only; nothing further to check structurally.
		default:
			return fmt.Errorf("sim: fault event %d: unknown fault kind %d", i, int(e.Kind))
		}
	}
	if err := validateCrashDirectives(s.Crashes); err != nil {
		return err
	}
	return nil
}

// validateUnitRate rejects a NaN/±Inf rate before it could otherwise slip
// past a plain range comparison: NaN compares false against every bound, so
// `rate < 0 || rate > 1` alone silently accepts it.
func validateUnitRate(rate float64) error {
	if math.IsNaN(rate) || math.IsInf(rate, 0) {
		return fmt.Errorf("rate %v is not finite", rate)
	}
	if rate < 0 || rate > 1 {
		return fmt.Errorf("rate %v outside [0,1]", rate)
	}
	return nil
}

// validatePartitionGroups rejects a degenerate group split: fewer than 2
// groups, an empty group, a node repeated within one group, or a node that
// appears in more than one group (the groups must be disjoint for
// PartitionGroups/HealGroups's "every cross-group pair" semantics to be
// well-defined).
func validatePartitionGroups(groups [][]raft.NodeID) error {
	if len(groups) < 2 {
		return fmt.Errorf("need at least 2 groups, got %d", len(groups))
	}
	seen := make(map[raft.NodeID]int, len(groups))
	for gi, group := range groups {
		if len(group) == 0 {
			return fmt.Errorf("group %d is empty", gi)
		}
		local := make(map[raft.NodeID]struct{}, len(group))
		for _, id := range group {
			if _, dup := local[id]; dup {
				return fmt.Errorf("group %d contains duplicate node %d", gi, id)
			}
			local[id] = struct{}{}
			if prior, dup := seen[id]; dup {
				return fmt.Errorf("node %d appears in both group %d and group %d, groups must be disjoint", id, prior, gi)
			}
			seen[id] = gi
		}
	}
	return nil
}

type crashDirectiveKey struct {
	node raft.NodeID
	save uint64
}

// validateCrashDirectives structurally validates every Crashes entry: the
// same checks NewCrashStorage applies per-node once construction reaches it
// (Save is 1-based, Point is schedulable, RetainUnsynced is at least the
// RetainAllUnsynced sentinel), plus a duplicate-(Node,Save) check across the
// whole schedule. Running these at Validate() time means a malformed
// directive naming a node absent from the run's cluster is still caught
// (schedule.For(id) would otherwise never surface it to any real node's
// NewCrashStorage call, silently dropping the directive instead of
// rejecting it).
func validateCrashDirectives(directives []CrashDirective) error {
	seen := make(map[crashDirectiveKey]struct{}, len(directives))
	for i, d := range directives {
		if d.Save == 0 {
			return fmt.Errorf("sim: crash directive %d (node %d): Save 0; ordinals are 1-based", i, d.Node)
		}
		if d.Point == CrashHostInitiated || d.Point > CrashAfterSend {
			return fmt.Errorf("sim: crash directive %d (node %d): non-schedulable point %s", i, d.Node, d.Point)
		}
		if d.RetainUnsynced < RetainAllUnsynced {
			return fmt.Errorf("sim: crash directive %d (node %d): RetainUnsynced %d below minimum %d", i, d.Node, d.RetainUnsynced, RetainAllUnsynced)
		}
		key := crashDirectiveKey{d.Node, d.Save}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("sim: crash directive %d: duplicate directive for node %d at save %d", i, d.Node, d.Save)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// ValidateForCluster runs Validate and additionally rejects any
// Node/From/To/group-member/crash-directive that names a node outside
// nodeIDs. NewFaultSim calls this once the run's cluster membership is
// known, so a schedule that targets a nonexistent node is a construction-time
// error — never a fault that silently no-ops because no real node's id
// matched it.
func (s FaultSchedule) ValidateForCluster(nodeIDs []raft.NodeID) error {
	if err := s.Validate(); err != nil {
		return err
	}
	known := make(map[raft.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		known[id] = struct{}{}
	}
	member := func(id raft.NodeID) error {
		if _, ok := known[id]; !ok {
			return fmt.Errorf("node %d is not a member of the cluster", id)
		}
		return nil
	}
	for i, e := range s.Events {
		switch e.Kind {
		case FaultPause, FaultResume, FaultClockSkew, FaultCrash, FaultRestart:
			if err := member(e.Node); err != nil {
				return fmt.Errorf("sim: fault event %d (%s): %w", i, e.Kind, err)
			}
		case FaultPartition, FaultHeal:
			if err := member(e.From); err != nil {
				return fmt.Errorf("sim: fault event %d (%s): From: %w", i, e.Kind, err)
			}
			if err := member(e.To); err != nil {
				return fmt.Errorf("sim: fault event %d (%s): To: %w", i, e.Kind, err)
			}
		case FaultPartitionGroups, FaultHealGroups:
			for _, group := range e.Groups {
				for _, id := range group {
					if err := member(id); err != nil {
						return fmt.Errorf("sim: fault event %d (%s): group member: %w", i, e.Kind, err)
					}
				}
			}
		}
	}
	for i, d := range s.Crashes {
		if err := member(d.Node); err != nil {
			return fmt.Errorf("sim: crash directive %d: %w", i, err)
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
