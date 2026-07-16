package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// KVService implements the KV gRPC service on top of a Host and the
// KVApplier registered as that Host's Applier. Every node runs it: a leader
// proposes and waits for its own entry to apply, a non-leader answers
// NotLeader with its best-known hint.
type KVService struct {
	raftpb.UnimplementedKVServer

	host    *Host
	applier *KVApplier
	peers   map[raft.NodeID]string
}

var _ raftpb.KVServer = (*KVService)(nil)

// NewKVService constructs a KVService. peers maps every cluster member
// (including this node) to its client-reachable address, used to fill in
// NotLeader hints.
func NewKVService(host *Host, applier *KVApplier, peers map[raft.NodeID]string) *KVService {
	peerCopy := make(map[raft.NodeID]string, len(peers))
	for id, addr := range peers {
		peerCopy[id] = addr
	}
	return &KVService{host: host, applier: applier, peers: peerCopy}
}

// Execute proposes cmd through the Raft log (GETs included) and returns once
// the matching entry has been applied, or answers NotLeader if this node
// cannot propose it.
func (s *KVService) Execute(ctx context.Context, req *raftpb.ExecuteRequest) (*raftpb.ExecuteResponse, error) {
	cmd := req.GetCmd()
	if err := validateCommand(cmd); err != nil {
		return nil, err
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal command: %v", err)
	}

	var outcomeCh <-chan waiterOutcome
	index, term, isLeader, err := s.host.Propose(data, func(index, term uint64) {
		outcomeCh = s.applier.register(index, term)
	})
	if err != nil {
		// A Propose error is always the fail-stop transition (a stopped host
		// short-circuits with isLeader=false before onProposed can run), and
		// that transition already drained every registered waiter — this
		// call's own included — via KVApplier.FailStop. Nothing to clean up
		// here; the buffered outcome this handler never reads is discarded.
		return nil, status.Errorf(codes.Unavailable, "raft host stopped: %v", err)
	}
	if !isLeader {
		return s.notLeaderResponse(), nil
	}

	select {
	case outcome := <-outcomeCh:
		if outcome.err != nil {
			// The host fail-stopped (a later Ready's Save or Apply failed)
			// before this entry could commit and apply. Same retryable shape
			// as the Propose-error path above: mqctl treats any RPC error as
			// a transport failure and retries against another peer with the
			// unchanged (client_id, seq), which dedup makes safe.
			return nil, status.Errorf(codes.Unavailable, "raft host stopped: %v", outcome.err)
		}
		if outcome.stale {
			return s.notLeaderResponse(), nil
		}
		return &raftpb.ExecuteResponse{Ok: true, Value: outcome.result.Value, Found: outcome.result.Found}, nil
	case <-ctx.Done():
		s.applier.cancel(index, term)
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (s *KVService) notLeaderResponse() *raftpb.ExecuteResponse {
	hint := &raftpb.NotLeader{}
	if id, ok := s.host.LeaderHint(); ok {
		hint.LeaderId = uint64(id)
		hint.LeaderAddr = s.peers[id]
	}
	return &raftpb.ExecuteResponse{NotLeader: hint}
}

func validateCommand(cmd *raftpb.Command) error {
	if cmd == nil {
		return status.Error(codes.InvalidArgument, "cmd is required")
	}
	switch cmd.GetOp() {
	case raftpb.Op_PUT, raftpb.Op_DELETE, raftpb.Op_GET:
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported op %s", cmd.GetOp())
	}
	return nil
}
