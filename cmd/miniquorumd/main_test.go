package main

import (
	"reflect"
	"testing"

	"miniquorum/internal/raft"
)

func TestPeerIDsAreSorted(t *testing.T) {
	peers := map[raft.NodeID]string{3: "three", 1: "one", 2: "two"}
	if got, want := peerIDs(peers), []raft.NodeID{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("peerIDs() = %v, want %v", got, want)
	}
}

// TestNewProdRandProducesIndependentSequences guards against the
// synchronized-jitter failure this Rand replaces: two independently seeded
// instances (standing in for two real processes) must not draw the same
// sequence. A probabilistic false failure is astronomically unlikely (each
// draw is one of 1000 values), so this is not a flaky assertion.
func TestNewProdRandProducesIndependentSequences(t *testing.T) {
	a, err := newProdRand()
	if err != nil {
		t.Fatalf("newProdRand() error = %v", err)
	}
	b, err := newProdRand()
	if err != nil {
		t.Fatalf("newProdRand() error = %v", err)
	}

	var seqA, seqB [8]int
	for i := range seqA {
		seqA[i] = a.IntN(1000)
		seqB[i] = b.IntN(1000)
	}
	if seqA == seqB {
		t.Fatalf("two independently crypto-seeded prodRand instances produced the same sequence %v; jitter would synchronize across real processes", seqA)
	}
}

// TestProdRandIntNStaysInBounds exercises the raft.Rand contract (bounded,
// non-negative) across many draws instead of asserting an exact sequence.
func TestProdRandIntNStaysInBounds(t *testing.T) {
	r, err := newProdRand()
	if err != nil {
		t.Fatalf("newProdRand() error = %v", err)
	}
	const bound = 7
	for i := 0; i < 1000; i++ {
		if v := r.IntN(bound); v < 0 || v >= bound {
			t.Fatalf("IntN(%d) = %d, want [0,%d)", bound, v, bound)
		}
	}
}

var _ raft.Rand = (*prodRand)(nil)
