// miniquorumd hosts one MiniQuorum Raft node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"miniquorum/internal/clock"
	"miniquorum/internal/raft"
	"miniquorum/internal/server"
	"miniquorum/internal/statemachine/mapsm"
	"miniquorum/internal/storage"
	transportgrpc "miniquorum/internal/transport/grpc"
	raftpb "miniquorum/proto"
)

func main() {
	var id uint64
	var peersFlag string
	var dataDir string
	flag.Uint64Var(&id, "id", 0, "this node ID")
	flag.StringVar(&peersFlag, "peers", "", "comma-separated id=address peers")
	flag.StringVar(&dataDir, "data-dir", "", "data directory (disk storage starts in Phase 3)")
	flag.Parse()
	if id == 0 {
		log.Print("--id is required")
		return
	}
	peers, err := parsePeers(peersFlag)
	if err != nil {
		log.Printf("invalid --peers: %v", err)
		return
	}
	addr, ok := peers[raft.NodeID(id)]
	if !ok {
		log.Printf("--peers must include this node ID %d", id)
		return
	}

	node := raft.NewNode(raft.Config{ID: raft.NodeID(id), Peers: peerIDs(peers)}, raft.InitialState{}, fixedRand{})
	host := &server.Host{Node: node, Storage: storage.NewMemStorage()}
	transport := transportgrpc.New(peers, func(m *raftpb.Message) {
		if err := host.Step(m); err != nil {
			log.Printf("miniquorumd node=%d fail-stop (step): %v", id, err)
		}
	})
	host.Transport = transport
	applier := server.NewKVApplier(mapsm.New())
	host.Applier = applier
	raftpb.RegisterKVServer(transport.Server(), server.NewKVService(host, applier, peers))

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("listen %s: %v", addr, err)
		return
	}
	defer func() { _ = listener.Close() }()
	go func() {
		if err := transport.Serve(listener); err != nil {
			log.Printf("transport serve: %v", err)
		}
	}()

	ticker := clock.NewRealClock().NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("miniquorumd node=%d listening=%s data-dir=%s started", id, addr, dataDir)
	for {
		select {
		case <-ctx.Done():
			transport.Stop()
			log.Printf("miniquorumd node=%d stopped", id)
			return
		case <-ticker.C():
			if err := host.Tick(); err != nil {
				log.Printf("miniquorumd node=%d fail-stop: %v", id, err)
				transport.Stop()
				return
			}
			log.Printf("miniquorumd node=%d tick", id)
		}
	}
}

func parsePeers(value string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)
	if value == "" {
		return peers, nil
	}
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return nil, fmt.Errorf("want id=address, got %q", item)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid peer ID %q", parts[0])
		}
		peers[raft.NodeID(id)] = parts[1]
	}
	return peers, nil
}

func peerIDs(peers map[raft.NodeID]string) []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

type fixedRand struct{}

func (fixedRand) IntN(n int) int { return 0 }
