package lsm

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// seededRand adapts a *rand.Rand to the Rand interface, giving tests a
// reproducible, single-goroutine tower-height stream instead of reading
// global, auto-seeded entropy.
type seededRand struct{ r *rand.Rand }

func (s *seededRand) IntN(n int) int { return s.r.Intn(n) }

func newSeededRand(seed int64) *seededRand {
	return &seededRand{r: rand.New(rand.NewSource(seed))}
}

// scriptedRand always returns the same fixed value, letting a test drive
// randomHeight down one specific, chosen path instead of relying on a
// statistical sample to happen to reach it.
type scriptedRand struct{ next int }

func (s *scriptedRand) IntN(int) int { return s.next }

// TestSkipListOrderedIterationProperty checks, across many randomized
// insert/update/delete sequences, that forEach always yields keys in
// strictly ascending byte order and that every yielded (value, tombstone,
// seq) matches a reference model applying the same seq-monotonic
// accept/reject rule as skipList.put: a write only takes effect if its seq
// is strictly greater than the seq already recorded for that key. Drawing
// seq from a range that straddles each key's prior seq means lower, equal,
// and higher-seq writes are all exercised across the trial — the same mix
// full-log replay can produce.
func TestSkipListOrderedIterationProperty(t *testing.T) {
	const trials = 200
	const opsPerTrial = 300
	const poolSize = 40

	keyPool := make([][]byte, poolSize)
	for i := range keyPool {
		keyPool[i] = []byte(fmt.Sprintf("key-%02d", i))
	}

	for trial := 0; trial < trials; trial++ {
		rnd := newSeededRand(int64(trial) + 1)
		list := newSkipList(rnd)
		model := make(map[string]Entry, poolSize)

		for op := 0; op < opsPerTrial; op++ {
			key := keyPool[rnd.IntN(poolSize)]
			seq := uint64(rnd.IntN(opsPerTrial*2) + 1)
			tombstone := rnd.IntN(4) == 0

			var value []byte
			if !tombstone {
				value = []byte(fmt.Sprintf("v-%d-%d", trial, op))
			}

			list.put(append([]byte(nil), key...), value, tombstone, seq)

			if existing, ok := model[string(key)]; !ok || seq > existing.Seq {
				model[string(key)] = Entry{Value: value, Tombstone: tombstone, Seq: seq}
			}
		}

		var want []string
		for k := range model {
			want = append(want, k)
		}
		sort.Strings(want)

		var got []string
		var prev []byte
		first := true
		list.forEach(func(key, value []byte, tombstone bool, seq uint64) bool {
			if !first && bytes.Compare(prev, key) >= 0 {
				t.Fatalf("trial %d: forEach not strictly ascending: %q then %q", trial, prev, key)
			}
			first = false
			prev = key
			got = append(got, string(key))

			wantEntry := model[string(key)]
			if !bytes.Equal(value, wantEntry.Value) || tombstone != wantEntry.Tombstone || seq != wantEntry.Seq {
				t.Fatalf("trial %d: key %q = (%q,%v,%d), want (%q,%v,%d)", trial, key, value, tombstone, seq, wantEntry.Value, wantEntry.Tombstone, wantEntry.Seq)
			}
			return true
		})

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d: forEach keys = %v, want %v", trial, got, want)
		}
	}
}

// TestRandomHeightTowerDistributionSanity checks the tower-height generator
// against the theoretical geometric(p=1/4) shape: strictly bounded to
// [1, maxHeight], non-increasing level frequencies, and the first few
// levels' empirical probabilities close to theory.
func TestRandomHeightTowerDistributionSanity(t *testing.T) {
	const samples = 300000
	rnd := newSeededRand(42)

	counts := make(map[int]int, maxHeight)
	for i := 0; i < samples; i++ {
		h := randomHeight(rnd, maxHeight, towerInverseP)
		if h < 1 || h > maxHeight {
			t.Fatalf("randomHeight() = %d, want in [1,%d]", h, maxHeight)
		}
		counts[h]++
	}

	for h := 1; h < maxHeight; h++ {
		if counts[h] < counts[h+1] {
			t.Fatalf("counts[%d]=%d < counts[%d]=%d, want non-increasing frequency by level", h, counts[h], h+1, counts[h+1])
		}
	}

	// P(height==h) = (1/4)^(h-1) * (3/4) for the untruncated geometric tail;
	// truncation only perturbs the top level, negligible at h<=3. Tolerance
	// is far wider than sampling noise at 300k draws to avoid flakes.
	wantP := map[int]float64{1: 0.75, 2: 0.1875, 3: 0.046875}
	for h, want := range wantP {
		got := float64(counts[h]) / float64(samples)
		if diff := math.Abs(got - want); diff > 0.02 {
			t.Fatalf("P(height=%d) = %.4f, want ~%.4f (+/-0.02)", h, got, want)
		}
	}
}

// TestRandomHeightCapsAtMaxHeight proves the tower-height generator's upper
// bound directly rather than relying on a statistical sample to reach it:
// at p=1/4, P(height==maxHeight) = (1/4)^15 ≈ 9e-10, so
// TestRandomHeightTowerDistributionSanity's 300k draws essentially never
// land there. A scripted Rand that always signals "grow the tower" forces
// every draw down the longest possible path, proving randomHeight both
// reaches maxHeight and never exceeds it.
func TestRandomHeightCapsAtMaxHeight(t *testing.T) {
	rnd := &scriptedRand{next: 0}
	for i := 0; i < 100; i++ {
		if h := randomHeight(rnd, maxHeight, towerInverseP); h != maxHeight {
			t.Fatalf("randomHeight() = %d, want %d (Rand always signals growth, so height must cap exactly at maxHeight)", h, maxHeight)
		}
	}
}

// TestSkipListSequenceMonotonicity directly exercises the recovery-critical
// rule from phases/phase-5-lsm.md: a write only takes effect if its seq is
// strictly greater than the seq already stored for that key. A lower seq
// must never overwrite a higher one (protects the memtable during full-log
// replay); an equal seq re-applies the same Raft entry and must be a no-op
// (idempotent replay).
func TestSkipListSequenceMonotonicity(t *testing.T) {
	rnd := newSeededRand(101)
	list := newSkipList(rnd)

	list.put([]byte("k"), []byte("v10"), false, 10)
	if value, tombstone, seq, found := list.get([]byte("k")); !found || tombstone || seq != 10 || !bytes.Equal(value, []byte("v10")) {
		t.Fatalf("get(k) after seq=10 put = (%q,%v,%d,%v), want (v10,false,10,true)", value, tombstone, seq, found)
	}

	// Lower seq: rejected. The higher-seq value, tombstone, and seq survive
	// untouched.
	list.put([]byte("k"), []byte("v-lower"), false, 5)
	if value, tombstone, seq, found := list.get([]byte("k")); !found || tombstone || seq != 10 || !bytes.Equal(value, []byte("v10")) {
		t.Fatalf("get(k) after rejected seq=5 put = (%q,%v,%d,%v), want (v10,false,10,true) unchanged", value, tombstone, seq, found)
	}

	// Equal seq: idempotent no-op. A real replay would resend an identical
	// write for the same seq, but the rule holds unconditionally on seq
	// alone, so a differing value/tombstone here still must not apply.
	list.put([]byte("k"), []byte("v-equal"), true, 10)
	if value, tombstone, seq, found := list.get([]byte("k")); !found || tombstone || seq != 10 || !bytes.Equal(value, []byte("v10")) {
		t.Fatalf("get(k) after idempotent seq=10 put = (%q,%v,%d,%v), want (v10,false,10,true) unchanged", value, tombstone, seq, found)
	}

	// Higher seq: applied, replacing value, tombstone, and seq.
	list.put([]byte("k"), []byte("v20"), false, 20)
	if value, tombstone, seq, found := list.get([]byte("k")); !found || tombstone || seq != 20 || !bytes.Equal(value, []byte("v20")) {
		t.Fatalf("get(k) after seq=20 put = (%q,%v,%d,%v), want (v20,false,20,true)", value, tombstone, seq, found)
	}

	// A lower-seq delete must not shadow the higher live value.
	list.put([]byte("k"), nil, true, 15)
	if value, tombstone, seq, found := list.get([]byte("k")); !found || tombstone || seq != 20 || !bytes.Equal(value, []byte("v20")) {
		t.Fatalf("get(k) after rejected seq=15 delete = (%q,%v,%d,%v), want (v20,false,20,true) unchanged", value, tombstone, seq, found)
	}

	// A higher-seq delete applies normally.
	list.put([]byte("k"), nil, true, 30)
	if _, tombstone, seq, found := list.get([]byte("k")); !found || !tombstone || seq != 30 {
		t.Fatalf("get(k) after seq=30 delete = (tombstone=%v,seq=%d,found=%v), want (true,30,true)", tombstone, seq, found)
	}

	// A lower-seq put must not resurrect a value over a higher tombstone —
	// the classic LSM resurrection bug, at the memtable layer.
	list.put([]byte("k"), []byte("resurrected"), false, 25)
	if _, tombstone, seq, found := list.get([]byte("k")); !found || !tombstone || seq != 30 {
		t.Fatalf("get(k) after rejected seq=25 put over tombstone = (tombstone=%v,seq=%d,found=%v), want (true,30,true) unchanged", tombstone, seq, found)
	}

	// A key absent from the list accepts any seq: nothing to compare
	// against yet.
	list.put([]byte("fresh"), []byte("v1"), false, 1)
	if value, tombstone, seq, found := list.get([]byte("fresh")); !found || tombstone || seq != 1 || !bytes.Equal(value, []byte("v1")) {
		t.Fatalf("get(fresh) after first seq=1 put = (%q,%v,%d,%v), want (v1,false,1,true)", value, tombstone, seq, found)
	}
}

// TestSkipListTombstoneAndUpdateBehavior directly exercises insert, update,
// delete, undelete, and delete-of-absent-key semantics against the raw
// skip list, plus forEach's obligation to surface tombstones rather than
// hide them.
func TestSkipListTombstoneAndUpdateBehavior(t *testing.T) {
	rnd := newSeededRand(7)
	list := newSkipList(rnd)

	if _, _, _, found := list.get([]byte("a")); found {
		t.Fatalf("get(a) found = true before any write, want false")
	}

	list.put([]byte("a"), []byte("v1"), false, 1)
	if value, tombstone, seq, found := list.get([]byte("a")); !found || tombstone || seq != 1 || !bytes.Equal(value, []byte("v1")) {
		t.Fatalf("get(a) = (%q,%v,%d,%v), want (v1,false,1,true)", value, tombstone, seq, found)
	}

	list.put([]byte("a"), []byte("v2"), false, 2)
	if value, tombstone, seq, found := list.get([]byte("a")); !found || tombstone || seq != 2 || !bytes.Equal(value, []byte("v2")) {
		t.Fatalf("get(a) after update = (%q,%v,%d,%v), want (v2,false,2,true)", value, tombstone, seq, found)
	}

	list.put([]byte("a"), nil, true, 3)
	if _, tombstone, seq, found := list.get([]byte("a")); !found || !tombstone || seq != 3 {
		t.Fatalf("get(a) after delete = tombstone=%v seq=%d found=%v, want true,3,true", tombstone, seq, found)
	}

	list.put([]byte("never-put"), nil, true, 1)
	if _, tombstone, seq, found := list.get([]byte("never-put")); !found || !tombstone || seq != 1 {
		t.Fatalf("get(never-put) = tombstone=%v seq=%d found=%v, want true,1,true", tombstone, seq, found)
	}

	list.put([]byte("a"), []byte("v3"), false, 4)
	if value, tombstone, seq, found := list.get([]byte("a")); !found || tombstone || seq != 4 || !bytes.Equal(value, []byte("v3")) {
		t.Fatalf("get(a) after undelete = (%q,%v,%d,%v), want (v3,false,4,true)", value, tombstone, seq, found)
	}

	seen := map[string]bool{}
	list.forEach(func(key, _ []byte, tombstone bool, _ uint64) bool {
		seen[string(key)] = tombstone
		return true
	})
	if tomb, ok := seen["never-put"]; !ok || !tomb {
		t.Fatalf("forEach missing/wrong tombstoned key never-put: ok=%v tombstone=%v", ok, tomb)
	}
	if tomb, ok := seen["a"]; !ok || tomb {
		t.Fatalf("forEach: a = (present=%v, tombstone=%v), want (true,false)", ok, tomb)
	}
}
