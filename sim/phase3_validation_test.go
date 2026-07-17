package sim

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	raftpb "miniquorum/proto"
)

const phase3SentinelKey = "phase3-committed"

func newPhase3Sim(t *testing.T, seed int64, schedule FaultSchedule) *Sim {
	t.Helper()
	s, err := newCrashSim(Config{Seed: seed, NodeIDs: []raft.NodeID{1, 2, 3}}, schedule)
	if err != nil {
		t.Fatalf("newCrashSim(seed=%d) error = %v", seed, err)
	}
	s.RegisterInvariant(SingleLeaderPerTerm)
	s.setStateMachineFactory(func() statemachine.StateMachine { return mapsm.New() })
	return s
}

func phase3Store(t *testing.T, s *Sim, id raft.NodeID) *CrashStorage {
	t.Helper()
	store, ok := s.nodes[id].storage.(*CrashStorage)
	if !ok {
		t.Fatalf("node %d storage = %T, want *CrashStorage", id, s.nodes[id].storage)
	}
	return store
}

func phase3Prelude(t *testing.T, seed int64, schedule FaultSchedule) (*Sim, raft.NodeID, uint64, uint64) {
	t.Helper()
	s := newPhase3Sim(t, seed, schedule)
	_, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	index, term := proposeCommand(t, s, leader, &raftpb.Command{
		ClientId: 30_000, Seq: 1, Op: raftpb.Op_PUT,
		Key: []byte(phase3SentinelKey), Value: []byte("durable"),
	})
	awaitAppliedEverywhere(t, s, index, term, 3*phase1Window)
	return s, leader, index, term
}

type matrixRole string

const (
	matrixCandidate      matrixRole = "candidate-self-vote"
	matrixVoteFollower   matrixRole = "follower-vote-response"
	matrixLeader         matrixRole = "leader-entry-broadcast"
	matrixAppendFollower matrixRole = "follower-append-response"
)

type matrixPlan struct {
	role   matrixRole
	target raft.NodeID
	save   uint64
}

func planMatrixCrash(t *testing.T, seed int64, role matrixRole) matrixPlan {
	t.Helper()
	s, leader, _, _ := phase3Prelude(t, seed, FaultSchedule{})
	switch role {
	case matrixLeader:
		return matrixPlan{role: role, target: leader, save: phase3Store(t, s, leader).SaveCount() + 1}

	case matrixAppendFollower:
		target := without(s.order, leader)[0]
		index, _ := proposeCommand(t, s, leader, matrixCommand(matrixAppendFollower))
		deadline := s.Now() + 3*phase1Window
		for phase3Store(t, s, target).LastIndex() < index && s.Now() <= deadline {
			if err := phase3StepOne(s); err != nil {
				t.Fatalf("seed %d append plan invariant: %v", seed, err)
			}
		}
		if phase3Store(t, s, target).LastIndex() < index {
			t.Fatalf("seed %d follower %d never persisted planned index %d", seed, target, index)
		}
		return matrixPlan{role: role, target: target, save: phase3Store(t, s, target).SaveCount()}

	case matrixCandidate:
		target := without(s.order, leader)[0]
		isolateNode(s, target)
		old, _ := phase3Store(t, s, target).HardState()
		deadline := s.Now() + 10*phase1Window
		for s.Now() <= deadline {
			if err := phase3StepOne(s); err != nil {
				t.Fatalf("seed %d candidate plan invariant: %v", seed, err)
			}
			hard, _ := phase3Store(t, s, target).HardState()
			if hard.Term > old.Term && hard.VotedFor == target {
				return matrixPlan{role: role, target: target, save: phase3Store(t, s, target).SaveCount()}
			}
		}
		t.Fatalf("seed %d node %d never persisted a candidate self-vote", seed, target)

	case matrixVoteFollower:
		followers := without(s.order, leader)
		partitionGroups(s, []raft.NodeID{leader}, followers)
		oldTerm := s.HighestTerm()
		deadline := s.Now() + 10*phase1Window
		for s.Now() <= deadline {
			if err := phase3StepOne(s); err != nil {
				t.Fatalf("seed %d voter plan invariant: %v", seed, err)
			}
			for _, voter := range followers {
				hard, _ := phase3Store(t, s, voter).HardState()
				if hard.Term > oldTerm && hard.VotedFor != 0 && hard.VotedFor != voter {
					return matrixPlan{role: role, target: voter, save: phase3Store(t, s, voter).SaveCount()}
				}
			}
		}
		t.Fatalf("seed %d majority followers never persisted a granted vote", seed)
	}
	t.Fatalf("unknown matrix role %q", role)
	return matrixPlan{}
}

func executeMatrixPlan(t *testing.T, seed int64, plan matrixPlan, point CrashPoint) string {
	t.Helper()
	schedule := FaultSchedule{Crashes: []CrashDirective{{
		Node: plan.target, Save: plan.save, Point: point, RetainUnsynced: 0,
	}}}
	s, leader, sentinelIndex, sentinelTerm := phase3Prelude(t, seed, schedule)
	if current := phase3Store(t, s, plan.target).SaveCount(); current >= plan.save {
		t.Fatalf("seed %d %s target %d reached save %d during prelude; planned crash save=%d", seed, plan.role, plan.target, current, plan.save)
	}

	switch plan.role {
	case matrixLeader:
		if leader != plan.target {
			t.Fatalf("seed %d replay leader = %d, planned target = %d", seed, leader, plan.target)
		}
		proposeCommand(t, s, leader, matrixCommand(matrixLeader))
	case matrixAppendFollower:
		proposeCommand(t, s, leader, matrixCommand(matrixAppendFollower))
		phase3RunUntilCrash(t, s, plan.target, 3*phase1Window)
	case matrixCandidate:
		isolateNode(s, plan.target)
		phase3RunUntilCrash(t, s, plan.target, 10*phase1Window)
	case matrixVoteFollower:
		partitionGroups(s, []raft.NodeID{leader}, without(s.order, leader))
		phase3RunUntilCrash(t, s, plan.target, 10*phase1Window)
	default:
		t.Fatalf("unknown matrix role %q", plan.role)
	}

	store := phase3Store(t, s, plan.target)
	if !store.Crashed() || s.nodes[plan.target].node != nil {
		t.Fatalf("seed %d %s %s target %d did not crash", seed, plan.role, point, plan.target)
	}
	info, ok := store.LastCrash()
	if !ok || info.Save != plan.save || info.Point != point {
		t.Fatalf("seed %d %s LastCrash = %+v, %t, want save %d at %s", seed, plan.role, info, ok, plan.save, point)
	}
	if point == CrashBeforeSync && info.UnsyncedBytes == 0 {
		t.Fatalf("seed %d %s crashed on an empty batch; want a persistence-bearing Ready", seed, plan.role)
	}

	s.handleRestart(plan.target)
	healAll(s)
	awaitPhase3Convergence(t, s, sentinelIndex, sentinelTerm, 12*phase1Window)
	for _, id := range s.order {
		value, err := s.nodes[id].sm.Read([]byte(phase3SentinelKey))
		if err != nil || !value.Found || !bytes.Equal(value.Value, []byte("durable")) {
			t.Fatalf("seed %d %s node %d lost committed sentinel: value=%#v err=%v", seed, plan.role, id, value, err)
		}
	}

	hashes := make([]string, 0, len(s.order))
	for _, id := range s.order {
		hashes = append(hashes, fmt.Sprintf("%d:%016x", id, s.nodes[id].sm.Hash()))
	}
	return fmt.Sprintf("seed=%d role=%s node=%d save=%d point=%s info=%+v hashes=%s trace=%s",
		seed, plan.role, plan.target, plan.save, point, info, strings.Join(hashes, ","), strings.Join(s.Trace(), "\n"))
}

func matrixCommand(role matrixRole) *raftpb.Command {
	clientID := uint64(30_101)
	if role == matrixAppendFollower {
		clientID = 30_102
	}
	return &raftpb.Command{
		ClientId: clientID, Seq: 1, Op: raftpb.Op_PUT,
		Key: []byte("matrix-" + string(role)), Value: []byte("durable-if-committed"),
	}
}

func executeCrashMatrix(t *testing.T, seed int64) []string {
	t.Helper()
	roles := []matrixRole{matrixCandidate, matrixVoteFollower, matrixLeader, matrixAppendFollower}
	points := []CrashPoint{CrashBeforeSync, CrashAfterSyncBeforeSend, CrashAfterSend}
	digest := make([]string, 0, len(roles)*len(points))
	for _, role := range roles {
		plan := planMatrixCrash(t, seed, role)
		for _, point := range points {
			t.Logf("seed=%d role=%s target=%d save=%d point=%s", seed, role, plan.target, plan.save, point)
			digest = append(digest, executeMatrixPlan(t, seed, plan, point))
		}
	}
	return digest
}

func TestPhase3CrashPointMatrix(t *testing.T) {
	for _, seed := range []int64{20260717031, 20260717032, 20260717033} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			if digest := executeCrashMatrix(t, seed); len(digest) != 12 {
				t.Fatalf("matrix outcomes = %d, want 4 roles x 3 crash points", len(digest))
			}
		})
	}
}

func TestPhase3WholeMatrixSameSeedReproducible(t *testing.T) {
	const seed = int64(20260717999)
	first := executeCrashMatrix(t, seed)
	second := executeCrashMatrix(t, seed)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same-seed crash matrices diverged\nfirst=%v\nsecond=%v", first, second)
	}
}

// TestVotePersistencePreventsForgottenVote is also the disposable mutation
// detector for Packet 3C. With the canonical persist-before-send host, node 3
// crashes before its vote for node 1 is durable, so node 1 never receives the
// grant; after recovery node 3 may vote for node 2 and term 1 has only leader
// 2. Temporarily moving send before Save makes the first grant escape, the
// crash forgets it, and this exact test reports term 1 leaders [1 2].
func TestVotePersistencePreventsForgottenVote(t *testing.T) {
	schedule := FaultSchedule{Crashes: []CrashDirective{{
		Node: 3, Save: 1, Point: CrashBeforeSync, RetainUnsynced: 0,
	}}}
	s, err := newCrashSim(Config{
		Seed: 3, NodeIDs: []raft.NodeID{1, 2, 3}, ElectionTickMin: 1, ElectionTickMax: 1,
		MinDelayMS: 1, MaxDelayMS: 1,
	}, schedule)
	if err != nil {
		t.Fatalf("newCrashSim() error = %v", err)
	}
	s.RegisterInvariant(SingleLeaderPerTerm)
	s.Partition(1, 2)
	s.Partition(2, 1)

	s.handleTick(1)
	if err := s.Run(3); err != nil {
		t.Fatalf("first campaign: %v", err)
	}
	if !phase3Store(t, s, 3).Crashed() {
		t.Fatal("node 3 did not crash while trying to persist its first granted vote")
	}
	s.handleRestart(3)
	s.handleTick(2)
	if err := s.Run(s.Now() + 3); err != nil {
		t.Fatalf("second campaign: %v", err)
	}
	if got := s.Leaderships()[1]; !reflect.DeepEqual(got, []raft.NodeID{2}) {
		t.Fatalf("term 1 leaders = %v, want only node 2; a second leader proves the first vote escaped before persistence", got)
	}
}

func TestPhase3EveryUnsyncedFinalFramePrefixRecoversAndRejoins(t *testing.T) {
	command := &raftpb.Command{ClientId: 41, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("torn"), Value: []byte("rejoined")}
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	base := []raftpb.Entry{{Index: 1, Term: 1, Type: raftpb.EntryType_NOOP}}
	final := raftpb.Entry{Index: 2, Term: 1, Type: raftpb.EntryType_NORMAL, Data: data}
	finalRecordBytes := recordBytesFor(t, nil, []raftpb.Entry{final})

	for cut := 0; cut <= finalRecordBytes; cut++ {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			schedule := FaultSchedule{Crashes: []CrashDirective{{
				Node: 3, Save: 2, Point: CrashBeforeSync, RetainUnsynced: cut,
			}}}
			s := newPhase3Sim(t, int64(20260718000+cut), schedule)
			hard := raft.HardState{Term: 1}
			full := append(cloneSimEntries(base), final)
			for _, id := range []raft.NodeID{1, 2} {
				if err := s.nodes[id].storage.Save(&hard, full); err != nil {
					t.Fatalf("node %d preload Save() error = %v", id, err)
				}
			}
			torn := phase3Store(t, s, 3)
			if err := torn.Save(&hard, base); err != nil {
				t.Fatalf("node 3 base Save() error = %v", err)
			}
			synced := torn.DurableBytes()
			if err := torn.Save(nil, []raftpb.Entry{final}); !errors.Is(err, ErrCrashed) {
				t.Fatalf("node 3 final Save() error = %v, want ErrCrashed", err)
			}
			platter := torn.DurableBytes()
			if len(platter) < len(synced) || !bytes.Equal(platter[:len(synced)], synced) {
				t.Fatal("a final-frame tear changed already-synced bytes")
			}

			for _, id := range []raft.NodeID{1, 2} {
				s.handleCrash(id)
				s.handleRestart(id)
			}
			s.markProcessCrashed(s.nodes[3])
			s.handleRestart(3)
			awaitPhase3ValueConvergence(t, s, []byte("torn"), []byte("rejoined"), 12*phase1Window)
			for _, id := range s.order {
				if !simHasExactEntry(s.Log(id), &final) {
					t.Fatalf("cut %d node %d log = %s, want re-replicated final entry", cut, id, simEntries(s.Log(id)))
				}
			}
		})
	}
}

func TestPhase3AllNodeRestartStormConvergesHashes(t *testing.T) {
	s := newPhase3Sim(t, 20260717041, FaultSchedule{})
	_, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	var lastIndex, lastTerm uint64
	for seq := uint64(1); seq <= 4; seq++ {
		lastIndex, lastTerm = proposeCommand(t, s, leader, &raftpb.Command{
			ClientId: 51, Seq: seq, Op: raftpb.Op_PUT,
			Key: []byte(fmt.Sprintf("storm-%d", seq)), Value: []byte(fmt.Sprintf("value-%d", seq)),
		})
		awaitAppliedEverywhere(t, s, lastIndex, lastTerm, 3*phase1Window)
	}

	crashAt := s.Now()
	for _, id := range s.order {
		s.ScheduleCrash(id, crashAt)
		s.ScheduleRestart(id, crashAt+2*phase1TickInterval)
	}
	awaitPhase3Convergence(t, s, lastIndex, lastTerm, 15*phase1Window)
	for _, id := range s.order {
		value, err := s.nodes[id].sm.Read([]byte("storm-4"))
		if err != nil || !value.Found || !bytes.Equal(value.Value, []byte("value-4")) {
			t.Fatalf("node %d storm-4 = %#v, %v, want value-4", id, value, err)
		}
	}
}

func TestPhase3RecoveryRebuildsDedupAndRetryMutatesOnce(t *testing.T) {
	s := newPhase3Sim(t, 20260717042, FaultSchedule{})
	_, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)
	original := &raftpb.Command{ClientId: 77, Seq: 9, Op: raftpb.Op_PUT, Key: []byte("dedup-recovery"), Value: []byte("v1")}
	originalIndex, originalTerm := proposeCommand(t, s, leader, original)
	awaitAppliedEverywhere(t, s, originalIndex, originalTerm, 3*phase1Window)

	crashAt := s.Now()
	for _, id := range s.order {
		s.ScheduleCrash(id, crashAt)
		s.ScheduleRestart(id, crashAt+2*phase1TickInterval)
	}
		awaitPhase3Convergence(t, s, originalIndex, originalTerm, 15*phase1Window)

	interveningIndex, interveningTerm, _ := proposePhase3CommandToLeader(t, s, &raftpb.Command{
		ClientId: 88, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("dedup-recovery"), Value: []byte("v2"),
	}, 3*phase1Window)
	awaitAppliedEverywhere(t, s, interveningIndex, interveningTerm, 3*phase1Window)
	retry := proto.Clone(original).(*raftpb.Command)
	retryIndex, retryTerm, retryLeader := proposePhase3CommandToLeader(t, s, retry, 3*phase1Window)
	awaitAppliedEverywhere(t, s, retryIndex, retryTerm, 3*phase1Window)

	for _, id := range s.order {
		got, err := s.nodes[id].sm.Read([]byte("dedup-recovery"))
		if err != nil || !got.Found || !bytes.Equal(got.Value, []byte("v2")) {
			t.Fatalf("node %d value after same-sequence retry = %#v, %v, want intervening v2 (v1 would prove double mutation)", id, got, err)
		}
	}
	matching := 0
	for i := range s.nodes[retryLeader].applied {
		entry := &s.nodes[retryLeader].applied[i]
		if entry.Type != raftpb.EntryType_NORMAL {
			continue
		}
		var command raftpb.Command
		if err := proto.Unmarshal(entry.Data, &command); err == nil && command.ClientId == original.ClientId && command.Seq == original.Seq {
			matching++
		}
	}
	if matching != 2 {
		t.Fatalf("recovered leader applied-log occurrences for client=%d seq=%d = %d, want recovered original plus retry", original.ClientId, original.Seq, matching)
	}
}

func proposePhase3CommandToLeader(t *testing.T, s *Sim, command *raftpb.Command, within VirtualTime) (uint64, uint64, raft.NodeID) {
	t.Helper()
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	deadline := s.Now() + within
	for s.Now() <= deadline {
		for _, id := range s.order {
			if index, term, ok := s.Propose(id, data); ok {
				return index, term, id
			}
		}
		if s.queue.Len() == 0 {
			break
		}
		if err := phase3StepOne(s); err != nil {
			t.Fatalf("find leader invariant: %v", err)
		}
	}
	t.Fatalf("no node accepted command before t=%d", deadline)
	return 0, 0, 0
}

func TestPhase3FiveHundredSeedsWithCrashFaults(t *testing.T) {
	for run := 0; run < 500; run++ {
		seed := int64(202607175000 + run)
		target := raft.NodeID(run%3 + 1)
		point := []CrashPoint{CrashBeforeSync, CrashAfterSyncBeforeSend, CrashAfterSend}[run%3]

		// Derive the target's first persistence-bearing candidate Save from a
		// same-seed baseline. Direct Tick/Ready steps do not enqueue duplicate
		// scheduler ticks; all later activity remains scheduler-driven.
		baseline, err := newCrashSim(Config{Seed: seed, NodeIDs: []raft.NodeID{1, 2, 3}}, FaultSchedule{})
		if err != nil {
			t.Fatalf("seed %d baseline: %v", seed, err)
		}
		baselineStore := phase3Store(t, baseline, target)
		for {
			baseline.nodes[target].node.Tick()
			baseline.processReady(baseline.nodes[target], baseline.nodes[target].node.Ready())
			hard, _ := baselineStore.HardState()
			if hard.Term > 0 {
				break
			}
		}
		crashSave := baselineStore.SaveCount()
		retain := 0
		if point == CrashBeforeSync {
			retain = run % 64
		}
		schedule := FaultSchedule{Crashes: []CrashDirective{{
			Node: target, Save: crashSave, Point: point, RetainUnsynced: retain,
		}}}
		s, err := newCrashSim(Config{Seed: seed, NodeIDs: []raft.NodeID{1, 2, 3}}, schedule)
		if err != nil {
			t.Fatalf("seed %d NewSim: %v", seed, err)
		}
		s.RegisterInvariant(SingleLeaderPerTerm)
		store := phase3Store(t, s, target)
		for !store.Crashed() {
			s.nodes[target].node.Tick()
			s.processReady(s.nodes[target], s.nodes[target].node.Ready())
		}
		info, _ := store.LastCrash()
		if info.Save != crashSave || info.Point != point || info.UnsyncedBytes == 0 && point == CrashBeforeSync {
			t.Fatalf("seed %d crash = %+v, want persistence-bearing save %d at %s", seed, info, crashSave, point)
		}
		s.handleRestart(target)
		if err := s.Run(6 * phase1Window); err != nil {
			t.Fatalf("seed %d election after crash: %v", seed, err)
		}
		term := s.HighestTerm()
		leaders := s.Leaderships()[term]
		if len(leaders) != 1 {
			t.Fatalf("seed %d leaders in highest term %d = %v, want one", seed, term, leaders)
		}
		index, proposalTerm, ok := s.Propose(leaders[0], []byte(fmt.Sprintf("phase3-seed-%d", seed)))
		if !ok {
			t.Fatalf("seed %d leader %d rejected proposal", seed, leaders[0])
		}
		if err := s.Run(s.Now() + 3*phase1Window); err != nil {
			t.Fatalf("seed %d replication after crash: %v", seed, err)
		}
		for _, id := range s.order {
			if !simContainsEntry(s.AppliedEntries(id), index, proposalTerm, raftpb.EntryType_NORMAL, []byte(fmt.Sprintf("phase3-seed-%d", seed))) {
				t.Fatalf("seed %d node %d did not apply post-crash proposal (%d,%d)", seed, id, index, proposalTerm)
			}
		}
	}
}

func phase3StepOne(s *Sim) error {
	if s.queue.Len() == 0 {
		return errors.New("sim: event queue empty")
	}
	ev := heap.Pop(&s.queue).(*event)
	s.now = ev.time
	s.handleEvent(ev)
	for _, invariant := range s.invariants {
		if err := invariant(s); err != nil {
			return err
		}
	}
	return nil
}

func phase3RunUntilCrash(t *testing.T, s *Sim, target raft.NodeID, within VirtualTime) {
	t.Helper()
	deadline := s.Now() + within
	store := phase3Store(t, s, target)
	for !store.Crashed() && s.Now() <= deadline {
		if err := phase3StepOne(s); err != nil {
			t.Fatalf("run until node %d crash: %v", target, err)
		}
	}
	if !store.Crashed() {
		t.Fatalf("node %d did not crash by t=%d", target, deadline)
	}
}

func isolateNode(s *Sim, target raft.NodeID) {
	partitionGroups(s, []raft.NodeID{target}, without(s.order, target))
}

func healAll(s *Sim) {
	for _, from := range s.order {
		for _, to := range s.order {
			if from != to {
				s.Heal(from, to)
			}
		}
	}
}

func awaitPhase3Convergence(t *testing.T, s *Sim, committedIndex, committedTerm uint64, within VirtualTime) {
	t.Helper()
	deadline := s.Now() + within
	for s.Now() <= deadline {
		converged := true
		var hash uint64
		for position, id := range s.order {
			sn := s.nodes[id]
			if sn.node == nil || sn.halted || sn.sm == nil || !phase3HasIndexTerm(s.Log(id), committedIndex, committedTerm) {
				converged = false
				break
			}
			got := sn.sm.Hash()
			if position == 0 {
				hash = got
			} else if got != hash {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		if s.queue.Len() == 0 {
			break
		}
		if err := phase3StepOne(s); err != nil {
			t.Fatalf("convergence invariant: %v", err)
		}
	}
	t.Fatalf("cluster did not converge committed entry (%d,%d) and state hashes by t=%d", committedIndex, committedTerm, deadline)
}

func awaitPhase3ValueConvergence(t *testing.T, s *Sim, key, want []byte, within VirtualTime) {
	t.Helper()
	deadline := s.Now() + within
	for s.Now() <= deadline {
		converged := true
		var hash uint64
		for position, id := range s.order {
			sn := s.nodes[id]
			if sn.node == nil || sn.halted || sn.sm == nil {
				converged = false
				break
			}
			value, err := sn.sm.Read(key)
			if err != nil || !value.Found || !bytes.Equal(value.Value, want) {
				converged = false
				break
			}
			got := sn.sm.Hash()
			if position == 0 {
				hash = got
			} else if got != hash {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		if s.queue.Len() == 0 {
			break
		}
		if err := phase3StepOne(s); err != nil {
			t.Fatalf("value convergence invariant: %v", err)
		}
	}
	t.Fatalf("cluster did not converge key %q to %q by t=%d", key, want, deadline)
}

func phase3HasIndexTerm(entries []raftpb.Entry, index, term uint64) bool {
	for i := range entries {
		if entries[i].Index == index && entries[i].Term == term {
			return true
		}
	}
	return false
}
