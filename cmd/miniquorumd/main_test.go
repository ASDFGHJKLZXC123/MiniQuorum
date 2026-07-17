package main

import (
	"errors"
	"reflect"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestPeerIDsAreSorted(t *testing.T) {
	peers := map[raft.NodeID]string{3: "three", 1: "one", 2: "two"}
	if got, want := peerIDs(peers), []raft.NodeID{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("peerIDs() = %v, want %v", got, want)
	}
}

// fixedEntropy returns an entropy-read func that fills every requested byte
// slice with a repeated pattern byte, so two calls with different patterns
// are guaranteed (not merely overwhelmingly likely) to seed different PCG
// states.
func fixedEntropy(pattern byte) func([]byte) (int, error) {
	return func(b []byte) (int, error) {
		for i := range b {
			b[i] = pattern
		}
		return len(b), nil
	}
}

// TestNewProdRandFromEntropySameSeedIsDeterministic pins that seeding is a
// pure function of the entropy bytes: replaying the same bytes must replay
// the same draw sequence.
func TestNewProdRandFromEntropySameSeedIsDeterministic(t *testing.T) {
	a, err := newProdRandFromEntropy(fixedEntropy(0x11))
	if err != nil {
		t.Fatalf("newProdRandFromEntropy() error = %v", err)
	}
	b, err := newProdRandFromEntropy(fixedEntropy(0x11))
	if err != nil {
		t.Fatalf("newProdRandFromEntropy() error = %v", err)
	}

	for i := 0; i < 8; i++ {
		if va, vb := a.IntN(1000), b.IntN(1000); va != vb {
			t.Fatalf("draw %d: got %d and %d from identical seed bytes, want equal", i, va, vb)
		}
	}
}

// TestNewProdRandFromEntropyDistinctSeedsProduceDistinctSequences guards
// against the synchronized-jitter failure this Rand replaces: two
// independently seeded instances (standing in for two real processes) must
// not draw the same sequence. Unlike comparing two crypto-seeded instances,
// the two seeds here are explicitly constructed to differ, so this is a
// deterministic assertion, not a probabilistic one.
func TestNewProdRandFromEntropyDistinctSeedsProduceDistinctSequences(t *testing.T) {
	a, err := newProdRandFromEntropy(fixedEntropy(0x11))
	if err != nil {
		t.Fatalf("newProdRandFromEntropy() error = %v", err)
	}
	b, err := newProdRandFromEntropy(fixedEntropy(0x22))
	if err != nil {
		t.Fatalf("newProdRandFromEntropy() error = %v", err)
	}

	var seqA, seqB [8]int
	for i := range seqA {
		seqA[i] = a.IntN(1000)
		seqB[i] = b.IntN(1000)
	}
	if seqA == seqB {
		t.Fatalf("two distinctly seeded prodRand instances produced the same sequence %v; jitter would synchronize across real processes", seqA)
	}
}

// TestNewProdRandFromEntropyPropagatesReadError pins the explicit startup
// failure requirement: if entropy acquisition fails, seeding must fail
// rather than silently falling back to a weak or fixed seed.
func TestNewProdRandFromEntropyPropagatesReadError(t *testing.T) {
	wantErr := errors.New("entropy source exhausted")
	if _, err := newProdRandFromEntropy(func([]byte) (int, error) { return 0, wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("newProdRandFromEntropy() error = %v, want it to wrap %v", err, wantErr)
	}
}

// TestNewProdRandUsesRealCryptoRand is the one place production wiring is
// exercised end-to-end: newProdRand must succeed using the real crypto/rand
// source. It asserts success only, never a sequence, so it carries no
// collision risk.
func TestNewProdRandUsesRealCryptoRand(t *testing.T) {
	if _, err := newProdRand(); err != nil {
		t.Fatalf("newProdRand() error = %v", err)
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

func TestOpenDataDirCreatesAndRecoversDiskLog(t *testing.T) {
	dir := t.TempDir() + "/node-1/raft"
	store, err := openDataDir(dir)
	if err != nil {
		t.Fatalf("openDataDir() error = %v", err)
	}
	hard := raft.HardState{Term: 4, VotedFor: 2}
	entries := []raftpb.Entry{{Index: 1, Term: 4, Type: raftpb.EntryType_NOOP}}
	if err := store.Save(&hard, entries); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := openDataDir(dir)
	if err != nil {
		t.Fatalf("openDataDir(reopen) error = %v", err)
	}
	defer func() { _ = reopened.Close() }()
	gotHard, err := reopened.HardState()
	if err != nil || gotHard != hard {
		t.Fatalf("HardState() = %+v, %v, want %+v, nil", gotHard, err, hard)
	}
	gotEntries, err := reopened.Entries(1, 2)
	if err != nil || len(gotEntries) != 1 || gotEntries[0].Index != 1 || gotEntries[0].Term != 4 {
		t.Fatalf("Entries(1,2) = %+v, %v, want recovered index 1 term 4", gotEntries, err)
	}
}
