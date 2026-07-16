package server

import (
	"bytes"
	"context"
	"testing"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine/mapsm"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

// TestWaiterLeadershipChangeCollisionReturnsRetryableNeverForeignResult drives
// the full Host/KVService waiter path. A term-1 leader has a GET waiting at
// index 3 when a term-2 leader replaces that position with a different GET
// whose applied Result is found=v1. The old caller must receive NotLeader and
// never that recognizable foreign Result.
func TestWaiterLeadershipChangeCollisionReturnsRetryableNeverForeignResult(t *testing.T) {
	sm := mapsm.New()
	applier := NewKVApplier(sm)
	host := threeNodeLeaderHost(t, storage.NewMemStorage(), applier)
	peers := map[raft.NodeID]string{1: "n1:1", 2: "n2:1", 3: "n3:1"}
	svc := NewKVService(host, applier, peers)

	// First commit k=v1 in term 1 so the foreign GET below has a distinctive
	// non-empty result that an index-only waiter bug would wrongly expose.
	putDone := executeAsync(svc, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v1")})
	waitForWaiterCount(t, applier, 1)
	assertLeaderHintBarrier(t, host)
	if err := host.Step(ackAppend(2, 1, 0)); err != nil {
		t.Fatalf("Step(initial probe ack): %v", err)
	}
	if err := host.Step(ackAppend(2, 1, 2)); err != nil {
		t.Fatalf("Step(initial suffix ack): %v", err)
	}
	putResult := awaitExecute(t, putDone)
	if putResult.err != nil || !putResult.resp.GetOk() {
		t.Fatalf("initial PUT = (%+v, %v), want committed success", putResult.resp, putResult.err)
	}

	// The old leader's next proposal occupies (index=3, term=1) and waits
	// without a follower acknowledgement.
	oldDone := executeAsync(svc, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("missing")})
	waitForWaiterCount(t, applier, 1)
	assertLeaderHintBarrier(t, host)

	// A successor leader in term 2 has the committed prefix through index 2,
	// but commits a foreign GET(k) at index 3. Applying it yields found=v1 and
	// must stale the term-1 waiter before that result reaches the client.
	foreignData := marshalCommand(t, &raftpb.Command{ClientId: 3, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	if err := host.Step(&raftpb.Message{
		From: 2, To: 1, Term: 2,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term:         2,
			LeaderId:     2,
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			Entries: []*raftpb.Entry{{
				Index: 3, Term: 2, Type: raftpb.EntryType_NORMAL, Data: foreignData,
			}},
			LeaderCommit: 3,
		}},
	}); err != nil {
		t.Fatalf("Step(successor committed collision): %v", err)
	}

	oldResult := awaitExecute(t, oldDone)
	if oldResult.err != nil {
		t.Fatalf("old Execute error = %v, want retryable NotLeader response", oldResult.err)
	}
	if oldResult.resp.GetOk() || oldResult.resp.GetNotLeader() == nil {
		t.Fatalf("old Execute response = %+v, want NotLeader and never a successful foreign result", oldResult.resp)
	}
	if hint := oldResult.resp.GetNotLeader(); hint.GetLeaderId() != 2 || hint.GetLeaderAddr() != "n2:1" {
		t.Fatalf("old Execute NotLeader = %+v, want successor node 2 hint", hint)
	}
	if oldResult.resp.GetFound() || len(oldResult.resp.GetValue()) != 0 {
		t.Fatalf("old Execute leaked foreign result found=%v value=%q", oldResult.resp.GetFound(), oldResult.resp.GetValue())
	}
	assertNoStrandedWaiters(t, applier)

	state, err := sm.Read([]byte("k"))
	if err != nil || !state.Found || !bytes.Equal(state.Value, []byte("v1")) {
		t.Fatalf("state-machine Read(k) = %#v, %v; want v1 after foreign GET applied", state, err)
	}

	// The retryable response shape is usable by the same public Execute API,
	// not an internal channel-only assertion.
	if _, err := svc.Execute(context.Background(), &raftpb.ExecuteRequest{Cmd: &raftpb.Command{ClientId: 4, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")}}); err != nil {
		t.Fatalf("follower Execute after leadership loss: %v", err)
	}
}
