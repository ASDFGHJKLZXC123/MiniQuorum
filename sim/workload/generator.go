package workload

import (
	"math/rand"
	"strconv"

	"miniquorum/checker"
	raftpb "miniquorum/proto"
)

// Generator deterministically chooses uniform operations and keys for one
// stable client session. A runner calls Next exactly once per logical sequence,
// so retry timing never perturbs future workload choices.
type Generator struct {
	rnd      *rand.Rand
	clientID uint64
	keyCount int
}

// NewGenerator constructs a per-client stream derived solely from seed and
// clientID.
func NewGenerator(seed int64, clientID uint64, keyCount int) (*Generator, error) {
	if keyCount <= 0 {
		return nil, &generatorConfigError{keyCount: keyCount}
	}
	derived := uint64(seed) ^ (clientID * 0x9e3779b97f4a7c15)
	return &Generator{
		rnd:      rand.New(rand.NewSource(int64(derived))),
		clientID: clientID,
		keyCount: keyCount,
	}, nil
}

// Next generates one logical invocation. PUT, DELETE, and GET are selected
// uniformly; keys are selected uniformly from k0 through k{keyCount-1}.
func (g *Generator) Next(seq uint64) checker.Input {
	op := raftpb.Op(1 + g.rnd.Intn(3))
	key := []byte("k" + strconv.Itoa(g.rnd.Intn(g.keyCount)))
	input := checker.Input{Op: op, Key: key}
	if op == raftpb.Op_PUT {
		input.Value = []byte("v" + strconv.FormatUint(g.clientID, 36) + "-" + strconv.FormatUint(seq, 36))
	}
	return input
}

type generatorConfigError struct {
	keyCount int
}

func (e *generatorConfigError) Error() string {
	return "workload: key count must be positive, got " + strconv.Itoa(e.keyCount)
}
