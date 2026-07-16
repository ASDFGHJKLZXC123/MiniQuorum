package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// --- KVApplier: waiter exact-key behavior -----------------------------------

func TestKVApplierFulfillsExactIndexTermMatch(t *testing.T) {
	applier := NewKVApplier(mapsm.New())
	ch := applier.register(5, 3)

	data := marshalCommand(t, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")})
	if err := applier.Apply(&raftpb.Entry{Index: 5, Term: 3, Type: raftpb.EntryType_NORMAL, Data: data}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	select {
	case outcome := <-ch:
		if outcome.stale {
			t.Fatal("outcome.stale = true, want a successful match")
		}
	default:
		t.Fatal("waiter channel empty, want fulfilled outcome")
	}
}

func TestKVApplierFailsStaleWaiterWhenADifferentEntryCommitsAtItsIndex(t *testing.T) {
	applier := NewKVApplier(mapsm.New())
	// This node proposed at (index=5, term=3) while leader...
	ch := applier.register(5, 3)

	// ...but lost leadership before it committed, and a different leader's
	// entry (term 4) ended up committed at that same index.
	data := marshalCommand(t, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("other"), Value: []byte("v2")})
	if err := applier.Apply(&raftpb.Entry{Index: 5, Term: 4, Type: raftpb.EntryType_NORMAL, Data: data}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	select {
	case outcome := <-ch:
		if !outcome.stale {
			t.Fatal("outcome.stale = false, want the stale waiter to be failed, never handed the foreign result")
		}
	default:
		t.Fatal("waiter channel empty, want a stale outcome")
	}
}

func TestKVApplierNOOPAdvancesIndexWithoutTouchingAnyWaiter(t *testing.T) {
	applier := NewKVApplier(mapsm.New())
	ch := applier.register(7, 2)

	if err := applier.Apply(&raftpb.Entry{Index: 6, Term: 2, Type: raftpb.EntryType_NOOP}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	select {
	case outcome := <-ch:
		t.Fatalf("waiter at index 7 fulfilled by unrelated NOOP at index 6: %+v", outcome)
	default:
	}
}

// TestKVApplierResolvesEveryWaiterAtACommittedIndexRegardlessOfRegistrationOrder
// is the adversarial case byIndex used to lose: two waiters registered at the
// same index for different terms (this node proposed at (5,3), lost and
// regained leadership, and proposed again at (5,4) before the first waiter's
// entry was ever applied or cancelled). Whichever registered second used to
// silently overwrite byIndex's single slot, stranding the other forever. Both
// registration orders must resolve: the exact (index,term) match gets the
// applied result, and the other term gets a stale failure — never neither.
func TestKVApplierResolvesEveryWaiterAtACommittedIndexRegardlessOfRegistrationOrder(t *testing.T) {
	for _, order := range [][2]uint64{{3, 4}, {4, 3}} {
		t.Run(fmt.Sprintf("register_%d_then_%d", order[0], order[1]), func(t *testing.T) {
			applier := NewKVApplier(mapsm.New())
			channels := map[uint64]<-chan waiterOutcome{
				order[0]: applier.register(5, order[0]),
				order[1]: applier.register(5, order[1]),
			}

			data := marshalCommand(t, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")})
			if err := applier.Apply(&raftpb.Entry{Index: 5, Term: 3, Type: raftpb.EntryType_NORMAL, Data: data}); err != nil {
				t.Fatalf("Apply() error = %v", err)
			}

			for term, ch := range channels {
				select {
				case outcome := <-ch:
					wantStale := term != 3
					if outcome.stale != wantStale {
						t.Fatalf("term %d waiter outcome.stale = %v, want %v", term, outcome.stale, wantStale)
					}
				default:
					t.Fatalf("waiter registered at term %d left unresolved (stranded)", term)
				}
			}

			if len(applier.waiters) != 0 {
				t.Fatalf("waiters not fully drained: %v", applier.waiters)
			}
			if len(applier.byIndex) != 0 {
				t.Fatalf("byIndex not fully cleaned up: %v", applier.byIndex)
			}
		})
	}
}

func TestKVApplierCancelIsIdempotentAfterFulfillment(t *testing.T) {
	applier := NewKVApplier(mapsm.New())
	_ = applier.register(9, 1)
	data := marshalCommand(t, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	if err := applier.Apply(&raftpb.Entry{Index: 9, Term: 1, Type: raftpb.EntryType_NORMAL, Data: data}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	applier.cancel(9, 1) // must not panic or double-deliver
}

func marshalCommand(t *testing.T, cmd *raftpb.Command) []byte {
	t.Helper()
	data, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return data
}

// --- KVService: non-leader and leader-through-the-log paths ----------------

func TestKVServiceExecuteReturnsNotLeaderHintFromFollower(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}}
	applier := NewKVApplier(mapsm.New())
	host.Applier = applier
	peers := map[raft.NodeID]string{1: "n1:1", 2: "n2:1", 3: "n3:1"}
	svc := NewKVService(host, applier, peers)

	resp, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if resp.GetNotLeader() == nil {
		t.Fatal("resp.NotLeader = nil, want a NotLeader response from a follower")
	}
	if resp.GetNotLeader().GetLeaderId() != 0 || resp.GetNotLeader().GetLeaderAddr() != "" {
		t.Fatalf("resp.NotLeader = %+v, want an empty hint before any leader is observed", resp.GetNotLeader())
	}
}

func TestKVServiceExecuteFollowsLearnedLeaderHint(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 3, Peers: []raft.NodeID{1, 2, 3}}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}}
	applier := NewKVApplier(mapsm.New())
	host.Applier = applier
	peers := map[raft.NodeID]string{1: "n1:1", 2: "n2:1", 3: "n3:1"}
	svc := NewKVService(host, applier, peers)

	if err := host.Step(&raftpb.Message{
		From: 2, To: 3, Term: 5,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{Term: 5, LeaderId: 2}},
	}); err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	resp, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	hint := resp.GetNotLeader()
	if hint == nil || hint.GetLeaderId() != 2 || hint.GetLeaderAddr() != "n2:1" {
		t.Fatalf("resp.NotLeader = %+v, want leader_id=2 leader_addr=n2:1", hint)
	}
}

// --- Host: LeaderHint tracking -----------------------------------------
//
// internal/raft carries no leader-hint state (phases/phase-0-scaffold.md §4
// pins its exported API exactly); the host tracks its own ID and the
// best-known leader entirely itself, observing inbound AppendEntries and
// recording itself after a successful leader Propose.

func TestHostLeaderHintUnknownBeforeAnyObservation(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}, SelfID: 1}

	if id, ok := host.LeaderHint(); ok {
		t.Fatalf("LeaderHint() = (%d,true), want unknown before any leader is observed", id)
	}
}

func TestHostLeaderHintLearnedFromInboundAppendEntries(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 3, Peers: []raft.NodeID{1, 2, 3}}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}, SelfID: 3}

	if err := host.Step(&raftpb.Message{
		From: 2, To: 3, Term: 5,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{Term: 5, LeaderId: 2}},
	}); err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	if id, ok := host.LeaderHint(); !ok || id != 2 {
		t.Fatalf("LeaderHint() = (%d,%v), want (2,true) after AppendEntries from node 2", id, ok)
	}
}

func TestHostLeaderHintRecordsSelfAfterSuccessfulLeaderPropose(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1}, ElectionTickMin: 1, ElectionTickMax: 2}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}, SelfID: 1}

	if err := host.Tick(); err != nil { // election timeout -> immediate single-node leadership
		t.Fatalf("Tick() error = %v", err)
	}
	if id, ok := host.LeaderHint(); ok {
		t.Fatalf("LeaderHint() = (%d,true) merely from becoming leader, want still unknown until a Propose records self", id)
	}

	if _, _, isLeader, err := host.Propose([]byte("data"), nil); err != nil || !isLeader {
		t.Fatalf("Propose() = (isLeader=%v, err=%v), want isLeader=true, err=nil", isLeader, err)
	}

	if id, ok := host.LeaderHint(); !ok || id != 1 {
		t.Fatalf("LeaderHint() = (%d,%v), want (1,true) after a successful leader Propose", id, ok)
	}
}

func TestHostLeaderHintIgnoresZeroLeaderIDAndKeepsPriorHint(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 3, Peers: []raft.NodeID{1, 2, 3}}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}, SelfID: 3}

	if err := host.Step(&raftpb.Message{
		From: 2, To: 3, Term: 5,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{Term: 5, LeaderId: 2}},
	}); err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	// A same-term AppendEntries with an unset LeaderId (0) must never erase
	// an already-known hint; stale/empty hints are allowed, but this is
	// neither — it is a case that must not overwrite good data with none.
	if err := host.Step(&raftpb.Message{
		From: 2, To: 3, Term: 5,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{Term: 5, LeaderId: 0}},
	}); err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	if id, ok := host.LeaderHint(); !ok || id != 2 {
		t.Fatalf("LeaderHint() = (%d,%v), want (2,true) unchanged by a zero LeaderId message", id, ok)
	}
}

func TestKVServiceExecuteRejectsUnspecifiedOp(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1}}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}}
	applier := NewKVApplier(mapsm.New())
	host.Applier = applier
	svc := NewKVService(host, applier, map[raft.NodeID]string{1: "n1:1"})

	if _, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 1, Seq: 1}}); err == nil {
		t.Fatal("Execute() error = nil, want rejection of an unspecified op")
	}
}

// TestKVServiceExecutePutThenGetGoesThroughApply drives a single-node cluster
// (so Propose commits within the same call) through the leader path twice,
// exercising the exact register-before-drain ordering that a same-call
// commit requires and confirming GET is answered from Apply, never a direct
// map read.
func TestKVServiceExecutePutThenGetGoesThroughApply(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1}, ElectionTickMin: 1, ElectionTickMax: 2}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: storage.NewMemStorage(), Transport: noopTransport{}}
	applier := NewKVApplier(mapsm.New())
	host.Applier = applier
	svc := NewKVService(host, applier, map[raft.NodeID]string{1: "n1:1"})

	if err := host.Tick(); err != nil { // election timeout -> immediate single-node leadership + NOOP commit
		t.Fatalf("Tick() error = %v", err)
	}

	putResp, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 42, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")}})
	if err != nil {
		t.Fatalf("Execute(put) error = %v", err)
	}
	if !putResp.GetOk() || putResp.GetNotLeader() != nil {
		t.Fatalf("Execute(put) = %+v, want ok with no NotLeader", putResp)
	}

	getResp, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 42, Seq: 2, Op: raftpb.Op_GET, Key: []byte("k")}})
	if err != nil {
		t.Fatalf("Execute(get) error = %v", err)
	}
	if !getResp.GetOk() || !getResp.GetFound() || string(getResp.GetValue()) != "v" {
		t.Fatalf("Execute(get) = %+v, want ok found value=v", getResp)
	}
}

// --- KVService: waiter cleanup on a fail-stopping Propose -------------------
//
// packet 2C correction: Execute registers a waiter before the Ready
// containing its own entry is drained (see the register-before-drain
// comment on KVApplier.register). If that Ready's Storage.Save or
// state-machine Apply fails, Host.Propose returns an error and the host is
// now permanently fail-stopped — nothing will ever fulfill that waiter. Both
// tests below drive a single-node leader through Execute with a failure
// injected on the Propose's own Ready, then assert the waiter maps are fully
// drained and the fail-stop/Ready-ordering behavior is otherwise unchanged.

// singleNodeLeaderHost builds a one-node cluster and ticks it into
// leadership (electing itself and committing the term-start NOOP) before any
// failure is injected, so the failure under test is isolated to the
// Execute-triggered Propose's own Ready rather than the election's.
func singleNodeLeaderHost(t *testing.T, store storage.Storage, applier *KVApplier) *Host {
	t.Helper()
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1}, ElectionTickMin: 1, ElectionTickMax: 2}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: store, Transport: noopTransport{}, Applier: applier, SelfID: 1}
	if err := host.Tick(); err != nil { // election timeout -> immediate single-node leadership + NOOP commit
		t.Fatalf("Tick() (election) error = %v", err)
	}
	return host
}

func TestKVServiceExecuteCancelsWaiterWhenProposeReadySaveFails(t *testing.T) {
	store := &failAfterNSavesStorage{inner: storage.NewMemStorage(), okSaves: 1, err: errors.New("save boom")}
	applier := NewKVApplier(mapsm.New())
	host := singleNodeLeaderHost(t, store, applier)
	svc := NewKVService(host, applier, map[raft.NodeID]string{1: "n1:1"})

	_, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")}})
	if err == nil {
		t.Fatal("Execute() error = nil, want the storage save failure surfaced")
	}
	assertNoStrandedWaiters(t, applier)

	// Fail-stop must still hold: the host must not be touched again, and the
	// Ready-processing order (Save before anything else) must be unchanged.
	savesAfterFailure := store.saves
	if err := host.Tick(); !errors.Is(err, host.stopped) || err == nil {
		t.Fatalf("Tick() after fail-stop = %v, want the same fail-stop error", err)
	}
	if store.saves != savesAfterFailure {
		t.Fatalf("storage saves after fail-stopped Tick = %d, want still %d (node must not be touched again)", store.saves, savesAfterFailure)
	}
}

func TestKVServiceExecuteCancelsWaiterWhenProposeReadyApplyFails(t *testing.T) {
	applier := NewKVApplier(&failingOnNormalSM{StateMachine: mapsm.New(), err: errors.New("apply boom")})
	host := singleNodeLeaderHost(t, storage.NewMemStorage(), applier)
	svc := NewKVService(host, applier, map[raft.NodeID]string{1: "n1:1"})

	_, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")}})
	if err == nil {
		t.Fatal("Execute() error = nil, want the state-machine apply failure surfaced")
	}
	assertNoStrandedWaiters(t, applier)

	if err := host.Tick(); !errors.Is(err, host.stopped) || err == nil {
		t.Fatalf("Tick() after fail-stop = %v, want the same fail-stop error", err)
	}
}

func assertNoStrandedWaiters(t *testing.T, applier *KVApplier) {
	t.Helper()
	applier.mu.Lock()
	defer applier.mu.Unlock()
	if len(applier.waiters) != 0 {
		t.Fatalf("waiters not cleaned up after a fail-stopping Propose: %v", applier.waiters)
	}
	if len(applier.byIndex) != 0 {
		t.Fatalf("byIndex not cleaned up after a fail-stopping Propose: %v", applier.byIndex)
	}
}

// failAfterNSavesStorage lets the first okSaves calls through to inner, then
// fails every subsequent Save — isolating the failure to a specific Ready
// (here, the Execute-triggered Propose's Ready, not the prior election's).
type failAfterNSavesStorage struct {
	inner   storage.Storage
	okSaves int
	saves   int
	err     error
}

func (s *failAfterNSavesStorage) Save(hs *raft.HardState, entries []raftpb.Entry) error {
	s.saves++
	if s.saves > s.okSaves {
		return s.err
	}
	return s.inner.Save(hs, entries)
}

func (s *failAfterNSavesStorage) HardState() (raft.HardState, error) { return s.inner.HardState() }
func (s *failAfterNSavesStorage) Entries(lo, hi uint64) ([]raftpb.Entry, error) {
	return s.inner.Entries(lo, hi)
}
func (s *failAfterNSavesStorage) FirstIndex() uint64 { return s.inner.FirstIndex() }
func (s *failAfterNSavesStorage) LastIndex() uint64  { return s.inner.LastIndex() }

// failingOnNormalSM wraps a real StateMachine but fails Apply for NORMAL
// entries, while letting NOOP entries (the election's term-start no-op)
// succeed normally — isolating the failure to the client command under test.
type failingOnNormalSM struct {
	statemachine.StateMachine
	err error
}

func (f *failingOnNormalSM) Apply(entry *raftpb.Entry) (statemachine.Result, error) {
	if entry.GetType() == raftpb.EntryType_NORMAL {
		return statemachine.Result{}, f.err
	}
	return f.StateMachine.Apply(entry)
}

// --- KVApplier + KVService: fail-stop drains EVERY outstanding waiter -------
//
// packet 2C correction (verifier finding, MEDIUM): a fail-stopping proposal
// used to clean up only its own waiter. On a multi-node leader, an earlier
// proposal still awaiting quorum stayed registered in both KVApplier maps
// forever once the host fail-stopped — nothing could ever commit again, so
// its Execute stayed blocked for as long as its context lived (forever, with
// context.Background). The Host now notifies its Applier exactly once on the
// stopped transition (FailStopNotifier), and KVApplier.FailStop resolves
// every registered waiter with a retryable error. Save and Apply failures
// funnel through that same single transition, which the two three-node
// scenarios below exercise end to end.

func TestKVApplierFailStopDrainsEveryWaiterExactlyOnce(t *testing.T) {
	applier := NewKVApplier(mapsm.New())
	chA := applier.register(2, 1)
	chB := applier.register(3, 1)

	failure := errors.New("fail-stop boom")
	applier.FailStop(failure)

	for name, ch := range map[string]<-chan waiterOutcome{"A": chA, "B": chB} {
		select {
		case outcome := <-ch:
			if !errors.Is(outcome.err, failure) {
				t.Fatalf("waiter %s outcome.err = %v, want the fail-stop error", name, outcome.err)
			}
		default:
			t.Fatalf("waiter %s left unresolved by FailStop", name)
		}
	}
	assertNoStrandedWaiters(t, applier)

	// A second FailStop cannot happen (the stopped transition is unique) but
	// must be harmless, as must a context cancel arriving after the drain.
	applier.FailStop(failure)
	applier.cancel(2, 1)
	for name, ch := range map[string]<-chan waiterOutcome{"A": chA, "B": chB} {
		select {
		case outcome := <-ch:
			t.Fatalf("waiter %s delivered a second outcome: %+v", name, outcome)
		default:
		}
	}
}

// TestKVServiceExecuteFailStopDrainsEarlierOutstandingWaiterOnSaveFailure is
// the multi-waiter regression itself: a real three-node leader has proposal A
// registered and awaiting quorum (its AppendEntries are never acked) when a
// later proposal B's Storage.Save fails. The host permanently fail-stops, so
// A can never commit: both Execute calls — not only B's — must come back
// retryable and both maps must empty, even though neither call's context ever
// expires.
func TestKVServiceExecuteFailStopDrainsEarlierOutstandingWaiterOnSaveFailure(t *testing.T) {
	store := &armedFailStorage{inner: storage.NewMemStorage(), err: errors.New("save boom")}
	applier := NewKVApplier(mapsm.New())
	host := threeNodeLeaderHost(t, store, applier)
	svc := NewKVService(host, applier, map[raft.NodeID]string{1: "n1:1", 2: "n2:1", 3: "n3:1"})

	doneA := executeAsync(svc, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("a"), Value: []byte("va")})
	waitForWaiterCount(t, applier, 1)
	assertLeaderHintBarrier(t, host)
	// A's Propose has fully drained its own Ready (registration, Save, and
	// the leader hint all happen inside one host.mu critical section), so the
	// next Save the host performs belongs to B's proposal. Arm the failure.
	store.arm()

	respB, errB := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("b"), Value: []byte("vb")}})
	assertUnavailable(t, executeResult{resp: respB, err: errB}, "B")

	assertUnavailable(t, awaitExecute(t, doneA), "A")
	assertNoStrandedWaiters(t, applier)

	if err := host.Tick(); !errors.Is(err, host.stopped) || err == nil {
		t.Fatalf("Tick() after fail-stop = %v, want the same fail-stop error", err)
	}
}

// TestKVServiceExecuteFailStopDrainsOutstandingWaitersOnApplyFailure proves a
// state-machine Apply failure rides the same drain. Proposals A, B, C are all
// outstanding on a real three-node leader; the follower then acks the probe
// and the suffix, committing indices 1..4 in one batch. The apply loop runs
// NOOP, then A (applies for real and is fulfilled with its exact result),
// then B — whose apply fails, fail-stopping the host with C never reached.
// B and C must drain retryable while A's committed success is preserved.
func TestKVServiceExecuteFailStopDrainsOutstandingWaitersOnApplyFailure(t *testing.T) {
	sm := &failOnNthNormalSM{StateMachine: mapsm.New(), n: 2, err: errors.New("apply boom")}
	applier := NewKVApplier(sm)
	host := threeNodeLeaderHost(t, storage.NewMemStorage(), applier)
	svc := NewKVService(host, applier, map[raft.NodeID]string{1: "n1:1", 2: "n2:1", 3: "n3:1"})

	doneA := executeAsync(svc, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("a"), Value: []byte("va")})
	waitForWaiterCount(t, applier, 1)
	doneB := executeAsync(svc, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("b"), Value: []byte("vb")})
	waitForWaiterCount(t, applier, 2)
	doneC := executeAsync(svc, &raftpb.Command{ClientId: 3, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("c"), Value: []byte("vc")})
	waitForWaiterCount(t, applier, 3)
	assertLeaderHintBarrier(t, host)

	// Follower 2 acks the leader's initial probe (the in-flight request
	// covering index 0), which releases the suffix request carrying entries
	// 1..4: NOOP, A, B, C.
	if err := host.Step(ackAppend(2, 1, 0)); err != nil {
		t.Fatalf("Step(probe ack) error = %v", err)
	}
	// Acking that suffix gives index 4 a quorum (leader + follower 2), so the
	// same Ready commits 1..4 and applies until B fails.
	if err := host.Step(ackAppend(2, 1, 4)); err == nil {
		t.Fatal("Step(suffix ack) error = nil, want the state-machine apply failure surfaced")
	}

	resultA := awaitExecute(t, doneA)
	if resultA.err != nil || !resultA.resp.GetOk() || resultA.resp.GetNotLeader() != nil {
		t.Fatalf("A Execute = (%+v, %v), want its exact committed-and-applied Ok result before the fail-stop", resultA.resp, resultA.err)
	}
	assertUnavailable(t, awaitExecute(t, doneB), "B")
	assertUnavailable(t, awaitExecute(t, doneC), "C")
	assertNoStrandedWaiters(t, applier)

	if err := host.Tick(); !errors.Is(err, host.stopped) || err == nil {
		t.Fatalf("Tick() after fail-stop = %v, want the same fail-stop error", err)
	}
}

// threeNodeLeaderHost builds a {1,2,3} cluster host for node 1 and elects it
// by granting node 2's vote. The term-start NOOP (index 1, term 1) and every
// later proposal stay uncommitted until the test itself acks AppendEntries,
// which is what lets a proposal sit registered "awaiting quorum".
func threeNodeLeaderHost(t *testing.T, store storage.Storage, applier *KVApplier) *Host {
	t.Helper()
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}, ElectionTickMin: 1, ElectionTickMax: 2}, raft.InitialState{}, fixedTestRand{})
	host := &Host{Node: node, Storage: store, Transport: noopTransport{}, Applier: applier, SelfID: 1}
	if err := host.Tick(); err != nil { // election timeout -> candidate at term 1
		t.Fatalf("Tick() (candidacy) error = %v", err)
	}
	if err := host.Step(&raftpb.Message{
		From: 2, To: 1, Term: 1,
		Body: &raftpb.Message_RequestVoteResp{RequestVoteResp: &raftpb.RequestVoteResp{Term: 1, VoteGranted: true}},
	}); err != nil { // vote quorum (self + node 2) -> leader, NOOP appended
		t.Fatalf("Step(vote grant) error = %v", err)
	}
	return host
}

func ackAppend(from, term, matchIndex uint64) *raftpb.Message {
	return &raftpb.Message{
		From: from, To: 1, Term: term,
		Body: &raftpb.Message_AppendEntriesResp{AppendEntriesResp: &raftpb.AppendEntriesResp{
			Term: term, Success: true, MatchIndex: matchIndex,
		}},
	}
}

type executeResult struct {
	resp *raftpb.ExecuteResponse
	err  error
}

// executeAsync runs one Execute with a context that never expires — exactly
// the caller the fail-stop drain must be able to release — and reports its
// result without asserting from the goroutine.
func executeAsync(svc *KVService, cmd *raftpb.Command) <-chan executeResult {
	done := make(chan executeResult, 1)
	go func() {
		resp, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: cmd})
		done <- executeResult{resp: resp, err: err}
	}()
	return done
}

func awaitExecute(t *testing.T, done <-chan executeResult) executeResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("Execute call still blocked; fail-stop drain never released its waiter")
		return executeResult{}
	}
}

// waitForWaiterCount observes registrations made by concurrent Execute
// goroutines; it only ever proceeds once the expected state is visible, so
// test ordering never depends on scheduler timing.
func waitForWaiterCount(t *testing.T, applier *KVApplier, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		applier.mu.Lock()
		got := len(applier.waiters)
		applier.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters = %d, want %d before deadline", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// assertLeaderHintBarrier doubles as a synchronization barrier: the hint is
// recorded inside Propose's host.mu critical section (before the waiter
// registers and the Ready drains), so once this acquires host.mu and reads
// it, every Execute whose registration was already observed has fully
// finished its Propose — including its Save — and is blocked on its channel.
func assertLeaderHintBarrier(t *testing.T, host *Host) {
	t.Helper()
	if id, ok := host.LeaderHint(); !ok || id != 1 {
		t.Fatalf("LeaderHint() = (%d,%v), want (1,true) after a successful leader Propose", id, ok)
	}
}

func assertUnavailable(t *testing.T, result executeResult, who string) {
	t.Helper()
	if result.err == nil {
		t.Fatalf("%s Execute error = nil (resp=%+v), want a retryable error after fail-stop", who, result.resp)
	}
	if status.Code(result.err) != codes.Unavailable {
		t.Fatalf("%s Execute error = %v, want gRPC code Unavailable (mqctl retries it against another peer with the same client_id/seq)", who, result.err)
	}
}

// armedFailStorage forwards to inner until armed, then fails every Save —
// placing the failure on exactly the next proposal's Ready without hardcoding
// how many Saves earlier host steps performed.
type armedFailStorage struct {
	inner storage.Storage
	err   error

	mu    sync.Mutex
	armed bool
}

func (s *armedFailStorage) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
}

func (s *armedFailStorage) Save(hs *raft.HardState, entries []raftpb.Entry) error {
	s.mu.Lock()
	armed := s.armed
	s.mu.Unlock()
	if armed {
		return s.err
	}
	return s.inner.Save(hs, entries)
}

func (s *armedFailStorage) HardState() (raft.HardState, error) { return s.inner.HardState() }
func (s *armedFailStorage) Entries(lo, hi uint64) ([]raftpb.Entry, error) {
	return s.inner.Entries(lo, hi)
}
func (s *armedFailStorage) FirstIndex() uint64 { return s.inner.FirstIndex() }
func (s *armedFailStorage) LastIndex() uint64  { return s.inner.LastIndex() }

// failOnNthNormalSM wraps a real StateMachine but fails the nth NORMAL apply
// (1-based), letting NOOPs and every other client entry apply for real — so
// one commit batch can fulfill an earlier proposal and then fail a later one.
// Apply only ever runs on the single Ready-processing path (under Host.mu),
// so the counter needs no lock of its own.
type failOnNthNormalSM struct {
	statemachine.StateMachine
	n       int
	applied int
	err     error
}

func (f *failOnNthNormalSM) Apply(entry *raftpb.Entry) (statemachine.Result, error) {
	if entry.GetType() == raftpb.EntryType_NORMAL {
		f.applied++
		if f.applied == f.n {
			return statemachine.Result{}, f.err
		}
	}
	return f.StateMachine.Apply(entry)
}

// --- Host: mutex-serialized fail-stop --------------------------------------

func TestHostFailStopsAndShortCircuitsFurtherCalls(t *testing.T) {
	node := raft.NewNode(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}, ElectionTickMin: 1, ElectionTickMax: 2}, raft.InitialState{}, fixedTestRand{})
	store := &countingErrStorage{err: errors.New("boom")}
	host := &Host{Node: node, Storage: store, Transport: noopTransport{}}

	err1 := host.Tick() // election timeout dirties HardState, forcing a Save
	if err1 == nil {
		t.Fatal("Tick() error = nil, want the storage save failure")
	}
	if store.saves != 1 {
		t.Fatalf("storage saves = %d, want 1", store.saves)
	}

	err2 := host.Tick()
	if !errors.Is(err2, err1) {
		t.Fatalf("Tick() after fail-stop = %v, want the same fail-stop error %v", err2, err1)
	}
	if store.saves != 1 {
		t.Fatalf("storage saves after fail-stopped Tick = %d, want still 1 (node must not be touched again)", store.saves)
	}
}

// --- test doubles ------------------------------------------------------------

type fixedTestRand struct{}

func (fixedTestRand) IntN(int) int { return 0 }

type noopTransport struct{}

func (noopTransport) Send(raft.NodeID, *raftpb.Message) {}

type countingErrStorage struct {
	err   error
	saves int
}

func (s *countingErrStorage) Save(*raft.HardState, []raftpb.Entry) error {
	s.saves++
	return s.err
}

func (s *countingErrStorage) HardState() (raft.HardState, error) { return raft.HardState{}, nil }
func (s *countingErrStorage) Entries(uint64, uint64) ([]raftpb.Entry, error) {
	return nil, storage.ErrOutOfBounds
}
func (s *countingErrStorage) FirstIndex() uint64 { return 1 }
func (s *countingErrStorage) LastIndex() uint64  { return 0 }
