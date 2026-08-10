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
	reads   ReadMode
}

// ReadMode selects the GET implementation. Writes always use the Raft log.
type ReadMode string

const (
	ReadModeLog       ReadMode = "log"
	ReadModeReadIndex ReadMode = "readindex"
)

var _ raftpb.KVServer = (*KVService)(nil)

// NewKVService constructs a KVService. peers maps every cluster member
// (including this node) to its client-reachable address, used to fill in
// NotLeader hints.
func NewKVService(host *Host, applier *KVApplier, peers map[raft.NodeID]string) *KVService {
	return NewKVServiceWithReadMode(host, applier, peers, ReadModeLog)
}

// NewKVServiceWithReadMode constructs a KVService with the Phase 6 A/B read
// lever. ReadModeLog preserves the pre-Phase-6 path exactly.
func NewKVServiceWithReadMode(host *Host, applier *KVApplier, peers map[raft.NodeID]string, reads ReadMode) *KVService {
	peerCopy := make(map[raft.NodeID]string, len(peers))
	for id, addr := range peers {
		peerCopy[id] = addr
	}
	return &KVService{host: host, applier: applier, peers: peerCopy, reads: reads}
}

// Execute routes GET through the configured read path; every write, and every
// GET in log mode, keeps the pre-Phase-6 propose-and-apply path. The daemon's
// Phase-6 default is ReadIndex; NewKVService intentionally remains the
// programmatic compatibility constructor for callers that need log reads.
func (s *KVService) Execute(ctx context.Context, req *raftpb.ExecuteRequest) (*raftpb.ExecuteResponse, error) {
	cmd := req.GetCmd()
	if err := validateCommand(cmd); err != nil {
		return nil, err
	}
	if cmd.GetOp() == raftpb.Op_GET && s.reads == ReadModeReadIndex {
		return s.executeReadIndex(ctx, cmd.GetKey())
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

func (s *KVService) executeReadIndex(ctx context.Context, key []byte) (*raftpb.ExecuteResponse, error) {
	token, outcomeCh := s.applier.registerRead()
	if err := s.host.RequestRead(token); err != nil {
		s.applier.cancelRead(token)
		return nil, status.Errorf(codes.Unavailable, "raft host stopped: %v", err)
	}

	select {
	case outcome := <-outcomeCh:
		if outcome.err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft host stopped: %v", outcome.err)
		}
		if outcome.rejected {
			return s.notLeaderResponse(), nil
		}
		if outcome.canceled {
			return nil, status.Error(codes.Unavailable, "read index canceled after leadership change")
		}
		result, err := s.applier.read(key)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "state-machine read: %v", err)
		}
		return &raftpb.ExecuteResponse{Ok: true, Value: result.Value, Found: result.Found}, nil
	case <-ctx.Done():
		s.applier.cancelRead(token)
		// Remove the core-owned token too. Host serialization gives cancellation
		// a total order with a concurrent quorum response; either outcome may win,
		// but an abandoned queued/active token can never be retained indefinitely.
		_ = s.host.CancelRead(token)
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
