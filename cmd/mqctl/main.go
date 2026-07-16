// mqctl is the Phase 2 client CLI: put/get/delete against a MiniQuorum
// cluster, retrying on NotLeader (follow the hint, else round-robin) with a
// stable per-process client session.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// maxAttempts bounds the retry loop so a misconfigured or fully partitioned
// cluster fails the CLI invocation instead of hanging forever.
const maxAttempts = 50

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	op := os.Args[1]

	fs := flag.NewFlagSet(op, flag.ExitOnError)
	peersFlag := fs.String("peers", "", "comma-separated id=address peers")
	timeout := fs.Duration("timeout", 2*time.Second, "per-attempt RPC timeout")
	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}

	peers, order, err := parsePeers(*peersFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqctl:", err)
		os.Exit(2)
	}
	if len(order) == 0 {
		fmt.Fprintln(os.Stderr, "mqctl: --peers is required")
		os.Exit(2)
	}

	cmd, err := buildCommand(op, fs.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqctl:", err)
		os.Exit(2)
	}

	resp, err := execute(context.Background(), peers, order, cmd, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqctl:", err)
		os.Exit(1)
	}

	if op == "get" {
		if resp.GetFound() {
			fmt.Println(string(resp.GetValue()))
		} else {
			fmt.Println("(not found)")
		}
		return
	}
	fmt.Println("OK")
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mqctl <put|get|delete> --peers=id=addr[,id=addr...] [--timeout=2s] <key> [value]")
}

func buildCommand(op string, args []string) (*raftpb.Command, error) {
	cmd := &raftpb.Command{ClientId: randomClientID(), Seq: 1}
	switch op {
	case "put":
		if len(args) != 2 {
			return nil, fmt.Errorf("put requires <key> <value>")
		}
		cmd.Op, cmd.Key, cmd.Value = raftpb.Op_PUT, []byte(args[0]), []byte(args[1])
	case "get":
		if len(args) != 1 {
			return nil, fmt.Errorf("get requires <key>")
		}
		cmd.Op, cmd.Key = raftpb.Op_GET, []byte(args[0])
	case "delete":
		if len(args) != 1 {
			return nil, fmt.Errorf("delete requires <key>")
		}
		cmd.Op, cmd.Key = raftpb.Op_DELETE, []byte(args[0])
	default:
		return nil, fmt.Errorf("unknown command %q (want put|get|delete)", op)
	}
	return cmd, nil
}

// randomClientID is generated once per CLI process and reused, unchanged,
// across every retry of this invocation's single logical operation.
func randomClientID() uint64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("mqctl: reading random client ID: %v", err))
	}
	if id := binary.BigEndian.Uint64(buf[:]); id != 0 {
		return id
	}
	return 1
}

func parsePeers(value string) (map[raft.NodeID]string, []raft.NodeID, error) {
	peers := make(map[raft.NodeID]string)
	if value == "" {
		return peers, nil, nil
	}
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return nil, nil, fmt.Errorf("invalid --peers entry %q, want id=address", item)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, nil, fmt.Errorf("invalid peer ID %q", parts[0])
		}
		peers[raft.NodeID(id)] = parts[1]
	}
	order := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		order = append(order, id)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	return peers, order, nil
}

// execute sends cmd to the cluster, retrying on NotLeader (following its
// hint when present, else round-robining the configured peers) and on
// transport errors (round-robin). cmd's client_id/seq never change across
// attempts, so a retried write or read applies at most once.
func execute(ctx context.Context, peers map[raft.NodeID]string, order []raft.NodeID, cmd *raftpb.Command, timeout time.Duration) (*raftpb.ExecuteResponse, error) {
	addrs := make([]string, len(order))
	for i, id := range order {
		addrs[i] = peers[id]
	}
	next := 0
	target := addrs[0]

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := tryExecute(ctx, target, cmd, timeout)
		if err != nil {
			lastErr = err
			next = (next + 1) % len(addrs)
			target = addrs[next]
			continue
		}
		if hint := resp.GetNotLeader(); hint != nil {
			lastErr = fmt.Errorf("not leader (hint id=%d addr=%q)", hint.GetLeaderId(), hint.GetLeaderAddr())
			if hint.GetLeaderAddr() != "" {
				target = hint.GetLeaderAddr()
			} else {
				next = (next + 1) % len(addrs)
				target = addrs[next]
			}
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("exhausted %d attempts, last error: %w", maxAttempts, lastErr)
}

func tryExecute(ctx context.Context, addr string, cmd *raftpb.Command, timeout time.Duration) (*raftpb.ExecuteResponse, error) {
	conn, err := grpcgo.NewClient(addr, grpcgo.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return raftpb.NewKVClient(conn).Execute(callCtx, &raftpb.ExecuteRequest{Cmd: cmd})
}
