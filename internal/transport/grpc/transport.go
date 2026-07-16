// Package grpc is the production unary gRPC Raft transport wrapper.
package grpc

import (
	"context"
	"net"
	"sync"
	"time"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"

	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Handler receives an inbound Raft transport message.
type Handler func(*raftpb.Message)

// Transport sends every Raft message, including snapshot chunks, through the
// generated unary Raft.Send RPC.
type Transport struct {
	peers   map[raft.NodeID]string
	handler Handler
	server  *grpcgo.Server
	mu      sync.Mutex
}

// New constructs a transport using the supplied peer address map and handler.
func New(peers map[raft.NodeID]string, handler Handler) *Transport {
	copyPeers := make(map[raft.NodeID]string, len(peers))
	for id, addr := range peers {
		copyPeers[id] = addr
	}
	t := &Transport{peers: copyPeers, handler: handler, server: grpcgo.NewServer()}
	raftpb.RegisterRaftServer(t.server, inboundService{handler: handler})
	return t
}

// Serve registers this transport on listener and blocks until stopped.
func (t *Transport) Serve(listener net.Listener) error {
	t.mu.Lock()
	server := t.server
	t.mu.Unlock()
	return server.Serve(listener)
}

// Stop stops accepting inbound RPCs and unblocks Serve.
func (t *Transport) Stop() {
	t.mu.Lock()
	t.server.Stop()
	t.mu.Unlock()
}

// Send asynchronously performs a best-effort unary RPC. Failures are dropped.
func (t *Transport) Send(to raft.NodeID, m *raftpb.Message) {
	addr, ok := t.peers[to]
	if !ok || m == nil {
		return
	}
	message := proto.Clone(m).(*raftpb.Message)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, err := grpcgo.NewClient(addr, grpcgo.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = raftpb.NewRaftClient(conn).Send(ctx, message)
	}()
}

type inboundService struct {
	raftpb.UnimplementedRaftServer
	handler Handler
}

// Send handles the inbound unary Raft RPC.
func (s inboundService) Send(ctx context.Context, m *raftpb.Message) (*raftpb.SendResp, error) {
	if s.handler != nil {
		s.handler(proto.Clone(m).(*raftpb.Message))
	}
	return &raftpb.SendResp{}, nil
}
