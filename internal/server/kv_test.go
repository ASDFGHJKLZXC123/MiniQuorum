package server

import (
	"context"
	"errors"
	"testing"

	"miniquorum/internal/raft"
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
