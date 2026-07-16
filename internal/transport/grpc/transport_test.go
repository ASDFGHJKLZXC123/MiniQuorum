package grpc

import (
	"net"
	"testing"
	"time"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"

	"google.golang.org/protobuf/proto"
)

func TestTransportUnaryRoundTrip(t *testing.T) {
	received := make(chan *raftpb.Message, 1)
	b := New(nil, func(m *raftpb.Message) { received <- m })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	go func() { _ = b.Serve(listener) }()
	defer b.Stop()

	a := New(map[raft.NodeID]string{2: listener.Addr().String()}, nil)
	message := &raftpb.Message{
		From: 1,
		To:   2,
		Term: 4,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term: 4, LeaderId: 1, PrevLogIndex: 2, PrevLogTerm: 3,
			Entries:      []*raftpb.Entry{{Index: 3, Term: 4, Type: raftpb.EntryType_NORMAL, Data: []byte("value")}},
			LeaderCommit: 2,
		}},
	}
	a.Send(2, message)
	select {
	case got := <-received:
		if !proto.Equal(got, message) {
			t.Fatalf("received message = %v, want identical %v", got, message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for unary Raft.Send")
	}
}
