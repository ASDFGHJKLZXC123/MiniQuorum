// miniquorumd hosts one MiniQuorum Raft node.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand/v2"
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
	"miniquorum/internal/storage/disklog"
	transportgrpc "miniquorum/internal/transport/grpc"
	raftpb "miniquorum/proto"
)

func main() {
	var id uint64
	var peersFlag string
	var dataDir string
	flag.Uint64Var(&id, "id", 0, "this node ID")
	flag.StringVar(&peersFlag, "peers", "", "comma-separated id=address peers")
	flag.StringVar(&dataDir, "data-dir", "", "directory for this node's durable Raft log")
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
	if dataDir == "" {
		log.Print("--data-dir is required")
		return
	}
	store, err := openDataDir(dataDir)
	if err != nil {
		log.Printf("open --data-dir: %v", err)
		return
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close data-dir: %v", err)
		}
	}()

	rnd, err := newProdRand()
	if err != nil {
		log.Printf("seed election jitter rand: %v", err)
		return
	}
	node, err := server.NewRecoveredNode(raft.Config{ID: raft.NodeID(id), Peers: peerIDs(peers)}, store, rnd)
	if err != nil {
		log.Printf("recover node: %v", err)
		return
	}
	host := &server.Host{Node: node, Storage: store, SelfID: raft.NodeID(id)}
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

func openDataDir(path string) (*disklog.DiskLog, error) {
	if path == "" {
		return nil, fmt.Errorf("data directory is empty")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	store, err := disklog.Open(path, disklog.Options{})
	if err != nil {
		return nil, fmt.Errorf("open disklog in %s: %w", path, err)
	}
	return store, nil
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

// prodRand implements raft.Rand for the production daemon: a PRNG seeded
// once from a cryptographic source at startup, rather than a fixed sequence.
// A fixed or shared jitter sequence across real, independently started
// processes can synchronize their election timeouts and prevent the cluster
// from ever electing a leader; crypto seeding decorrelates them.
type prodRand struct {
	r *mathrand.Rand
}

// newProdRand seeds a prodRand from crypto/rand. Seed acquisition failure is
// reported rather than silently falling back to a weak or fixed seed.
func newProdRand() (*prodRand, error) {
	return newProdRandFromEntropy(cryptorand.Read)
}

// newProdRandFromEntropy seeds a prodRand from 16 bytes produced by read
// (crypto/rand.Read in production). The entropy source is injectable so
// startup seeding and failure propagation can be tested deterministically,
// rather than by asserting two crypto-seeded sequences never collide.
func newProdRandFromEntropy(read func(b []byte) (int, error)) (*prodRand, error) {
	var seed [16]byte
	if _, err := read(seed[:]); err != nil {
		return nil, fmt.Errorf("read crypto seed: %w", err)
	}
	s1 := binary.BigEndian.Uint64(seed[:8])
	s2 := binary.BigEndian.Uint64(seed[8:])
	return &prodRand{r: mathrand.New(mathrand.NewPCG(s1, s2))}, nil
}

func (p *prodRand) IntN(n int) int { return p.r.IntN(n) }
