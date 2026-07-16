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

// newRandStreams builds one seeded delay stream for the network and one
// seeded jitter stream per node, all derived from a single run seed. Streams
// are independent *rand.Rand instances (not shared) so that adding or
// removing nodes never perturbs another node's draw sequence. order fixes
// the stream-assignment order deterministically; callers must never derive
// it from Go map iteration.
func newRandStreams(seed int64, order []raft.NodeID) (delay *rand.Rand, perNode map[raft.NodeID]*nodeRand) {
	src := rand.New(rand.NewSource(seed))
	delay = rand.New(rand.NewSource(src.Int63()))
	perNode = make(map[raft.NodeID]*nodeRand, len(order))
	for _, id := range order {
		perNode[id] = &nodeRand{r: rand.New(rand.NewSource(src.Int63()))}
	}
	return delay, perNode
}
