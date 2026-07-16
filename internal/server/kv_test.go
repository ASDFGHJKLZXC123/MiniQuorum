package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"

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
