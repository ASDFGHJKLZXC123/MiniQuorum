package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

type fakeKV struct {
	raftpb.UnimplementedKVServer

	mu      sync.Mutex
	calls   []*raftpb.Command
	respond func(call int, cmd *raftpb.Command) (*raftpb.ExecuteResponse, error)
}

func (f *fakeKV) Execute(_ context.Context, req *raftpb.ExecuteRequest) (*raftpb.ExecuteResponse, error) {
	f.mu.Lock()
	call := len(f.calls)
	f.calls = append(f.calls, req.GetCmd())
	f.mu.Unlock()
	return f.respond(call, req.GetCmd())
}

func (f *fakeKV) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func startFakeKV(t *testing.T, kv *fakeKV) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	raftpb.RegisterKVServer(s, kv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func TestExecuteFollowsNotLeaderAddressHint(t *testing.T) {
	var leaderAddr string
	follower := &fakeKV{respond: func(int, *raftpb.Command) (*raftpb.ExecuteResponse, error) {
		return &raftpb.ExecuteResponse{NotLeader: &raftpb.NotLeader{LeaderId: 2, LeaderAddr: leaderAddr}}, nil
	}}
	leader := &fakeKV{respond: func(int, *raftpb.Command) (*raftpb.ExecuteResponse, error) {
		return &raftpb.ExecuteResponse{Ok: true}, nil
	}}
	followerAddr := startFakeKV(t, follower)
	leaderAddr = startFakeKV(t, leader)

	peers := map[raft.NodeID]string{1: followerAddr, 2: leaderAddr}
	order := []raft.NodeID{1, 2}
	cmd := &raftpb.Command{ClientId: 7, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")}

	resp, err := execute(context.Background(), peers, order, cmd, time.Second)
	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("execute() = %+v, want ok", resp)
	}
	if follower.callCount() != 1 || leader.callCount() != 1 {
		t.Fatalf("call counts = follower:%d leader:%d, want exactly one hop via the hint", follower.callCount(), leader.callCount())
	}
}

func TestExecuteRoundRobinsWhenNotLeaderHintIsEmpty(t *testing.T) {
	first := &fakeKV{respond: func(int, *raftpb.Command) (*raftpb.ExecuteResponse, error) {
		return &raftpb.ExecuteResponse{NotLeader: &raftpb.NotLeader{}}, nil
	}}
	second := &fakeKV{respond: func(int, *raftpb.Command) (*raftpb.ExecuteResponse, error) {
		return &raftpb.ExecuteResponse{Ok: true}, nil
	}}
	peers := map[raft.NodeID]string{1: startFakeKV(t, first), 2: startFakeKV(t, second)}
	order := []raft.NodeID{1, 2}
	cmd := &raftpb.Command{ClientId: 9, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")}

	resp, err := execute(context.Background(), peers, order, cmd, time.Second)
	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("execute() = %+v, want ok after round-robin", resp)
	}
}

func TestExecuteRoundRobinsPastAnUnreachablePeer(t *testing.T) {
	up := &fakeKV{respond: func(int, *raftpb.Command) (*raftpb.ExecuteResponse, error) {
		return &raftpb.ExecuteResponse{Ok: true}, nil
	}}
	peers := map[raft.NodeID]string{1: "127.0.0.1:1", 2: startFakeKV(t, up)}
	order := []raft.NodeID{1, 2}
	cmd := &raftpb.Command{ClientId: 3, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")}

	resp, err := execute(context.Background(), peers, order, cmd, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("execute() = %+v, want ok after skipping the unreachable peer", resp)
	}
}

func TestExecuteRetriesReuseTheSameClientIDAndSeq(t *testing.T) {
	var seen []*raftpb.Command
	var mu sync.Mutex
	kv := &fakeKV{respond: func(call int, cmd *raftpb.Command) (*raftpb.ExecuteResponse, error) {
		mu.Lock()
		seen = append(seen, cmd)
		mu.Unlock()
		if call < 2 {
			return &raftpb.ExecuteResponse{NotLeader: &raftpb.NotLeader{}}, nil
		}
		return &raftpb.ExecuteResponse{Ok: true}, nil
	}}
	addr := startFakeKV(t, kv)
	peers := map[raft.NodeID]string{1: addr}
	order := []raft.NodeID{1}
	cmd := &raftpb.Command{ClientId: 55, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")}

	if _, err := execute(context.Background(), peers, order, cmd, time.Second); err != nil {
		t.Fatalf("execute() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("attempts = %d, want 3 (two not_leader retries then success)", len(seen))
	}
	for i, c := range seen {
		if c.GetClientId() != 55 || c.GetSeq() != 1 {
			t.Fatalf("attempt %d command = client_id=%d seq=%d, want client_id=55 seq=1 unchanged", i, c.GetClientId(), c.GetSeq())
		}
	}
}

func TestRandomClientIDIsNonZero(t *testing.T) {
	if id := randomClientID(); id == 0 {
		t.Fatal("randomClientID() = 0, want a non-zero client identity")
	}
}

func TestBuildCommandAssignsSeqOne(t *testing.T) {
	cmd, err := buildCommand("put", []string{"k", "v"})
	if err != nil {
		t.Fatalf("buildCommand() error = %v", err)
	}
	if cmd.GetSeq() != 1 {
		t.Fatalf("Seq = %d, want 1", cmd.GetSeq())
	}
	if cmd.GetOp() != raftpb.Op_PUT || string(cmd.GetKey()) != "k" || string(cmd.GetValue()) != "v" {
		t.Fatalf("buildCommand(put) = %+v, want PUT k=v", cmd)
	}
}
