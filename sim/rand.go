package sim

import (
	"math/rand"

	"miniquorum/internal/raft"
)

// nodeRand adapts a *rand.Rand to the raft.Rand interface so each node gets
// its own deterministic jitter stream, derived from the run seed.
type nodeRand struct {
	r *rand.Rand
}

// IntN implements raft.Rand.
func (n *nodeRand) IntN(bound int) int { return n.r.Intn(bound) }

// newRandStreams builds one seeded delay stream for the network, one seeded
// jitter stream per node, and one seeded fault-decision stream (message
// drop/duplicate draws), all derived from a single run seed. Streams are
// independent *rand.Rand instances (not shared) so that adding or removing
// nodes never perturbs another node's draw sequence. order fixes the
// stream-assignment order deterministically; callers must never derive it
// from Go map iteration. fault is drawn last precisely so that a run with no
// drop/duplicate faults active (every Phase 1-3 caller) consumes src
// identically to before this stream existed: delay and perNode are
// unaffected by fault's addition.
func newRandStreams(seed int64, order []raft.NodeID) (delay *rand.Rand, perNode map[raft.NodeID]*nodeRand, fault *rand.Rand) {
	src := rand.New(rand.NewSource(seed))
	delay = rand.New(rand.NewSource(src.Int63()))
	perNode = make(map[raft.NodeID]*nodeRand, len(order))
	for _, id := range order {
		perNode[id] = &nodeRand{r: rand.New(rand.NewSource(src.Int63()))}
	}
	fault = rand.New(rand.NewSource(src.Int63()))
	return delay, perNode, fault
}

// lsmRandSeed derives an independent deterministic stream for a node's LSM
// skip list. It deliberately does not consume any existing Raft, network, or
// fault stream, preserving map-engine simulation traces byte-for-byte.
func lsmRandSeed(seed int64, id raft.NodeID) int64 {
	mixed := uint64(seed) ^ (uint64(id) * 0x9e3779b97f4a7c15)
	mixed ^= mixed >> 30
	mixed *= 0xbf58476d1ce4e5b9
	mixed ^= mixed >> 27
	mixed *= 0x94d049bb133111eb
	mixed ^= mixed >> 31
	return int64(mixed)
}

func newSeededLSMRand(seed int64) *nodeRand {
	return &nodeRand{r: rand.New(rand.NewSource(seed))}
}
