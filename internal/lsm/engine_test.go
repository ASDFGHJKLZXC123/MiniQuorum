package lsm

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestEnginePutGetDeleteAndDefensiveCopies(t *testing.T) {
	e := NewEngine(newSeededRand(11))

	key := []byte("k")
	value := []byte("v1")
	e.Put(key, value, 1)
	// Put defensively copies key and value into engine-owned storage, so
	// mutating the caller's slices afterward must not affect stored state.
	key[0] = 'z'
	value[0] = 'z'

	got, tombstone, seq, found := e.Get([]byte("k"))
	if !found || tombstone || seq != 1 || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get(k) = (%q,%v,%d,%v), want (v1,false,1,true)", got, tombstone, seq, found)
	}

	// Mutating a returned value must not alias engine-internal storage.
	got[0] = 'X'
	if got2, _, _, _ := e.Get([]byte("k")); !bytes.Equal(got2, []byte("v1")) {
		t.Fatalf("Get(k) after mutating prior result = %q, want v1 (aliasing detected)", got2)
	}

	e.Delete([]byte("k"), 2)
	if _, tombstone, seq, found := e.Get([]byte("k")); !found || !tombstone || seq != 2 {
		t.Fatalf("Get(k) after Delete = tombstone=%v seq=%d found=%v, want true,2,true", tombstone, seq, found)
	}

	if _, _, _, found := e.Get([]byte("missing")); found {
		t.Fatalf("Get(missing) found = true, want false")
	}
}

// TestEngineSeqMonotonicity is the Engine-level counterpart to
// TestSkipListSequenceMonotonicity: it proves Put/Delete actually plumb seq
// through to the skip list's monotonic-per-key rule (lower/equal rejected,
// higher applied) rather than, say, silently dropping it or applying every
// write unconditionally.
func TestEngineSeqMonotonicity(t *testing.T) {
	e := NewEngine(newSeededRand(17))

	e.Put([]byte("k"), []byte("v10"), 10)
	e.Put([]byte("k"), []byte("v-lower"), 5) // lower seq: rejected
	if value, tombstone, seq, found := e.Get([]byte("k")); !found || tombstone || seq != 10 || !bytes.Equal(value, []byte("v10")) {
		t.Fatalf("Get(k) after rejected lower-seq Put = (%q,%v,%d,%v), want (v10,false,10,true)", value, tombstone, seq, found)
	}

	e.Put([]byte("k"), []byte("v-equal"), 10) // equal seq: idempotent no-op
	if value, tombstone, seq, found := e.Get([]byte("k")); !found || tombstone || seq != 10 || !bytes.Equal(value, []byte("v10")) {
		t.Fatalf("Get(k) after idempotent equal-seq Put = (%q,%v,%d,%v), want (v10,false,10,true)", value, tombstone, seq, found)
	}

	e.Delete([]byte("k"), 8) // lower seq: rejected
	if value, tombstone, seq, found := e.Get([]byte("k")); !found || tombstone || seq != 10 || !bytes.Equal(value, []byte("v10")) {
		t.Fatalf("Get(k) after rejected lower-seq Delete = (%q,%v,%d,%v), want (v10,false,10,true)", value, tombstone, seq, found)
	}

	e.Put([]byte("k"), []byte("v20"), 20) // higher seq: applied
	if value, tombstone, seq, found := e.Get([]byte("k")); !found || tombstone || seq != 20 || !bytes.Equal(value, []byte("v20")) {
		t.Fatalf("Get(k) after higher-seq Put = (%q,%v,%d,%v), want (v20,false,20,true)", value, tombstone, seq, found)
	}

	e.Delete([]byte("k"), 30) // higher seq: applied
	if _, tombstone, seq, found := e.Get([]byte("k")); !found || !tombstone || seq != 30 {
		t.Fatalf("Get(k) after higher-seq Delete = tombstone=%v seq=%d found=%v, want true,30,true", tombstone, seq, found)
	}
}

// TestNewEngineRejectsNilRand proves NewEngine(nil) fails immediately, at
// construction, instead of leaving a nil Rand to surface later as an
// opaque nil-pointer panic on the engine's first write.
func TestNewEngineRejectsNilRand(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("NewEngine(nil) did not panic; want an immediate, explicit rejection of a nil Rand")
		}
	}()
	NewEngine(nil)
}

func TestEngineEntriesOrderedSnapshot(t *testing.T) {
	e := NewEngine(newSeededRand(13))
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for i, k := range keys {
		e.Put([]byte(k), []byte("v-"+k), uint64(i+1))
	}
	deleteSeq := uint64(len(keys) + 1)
	e.Delete([]byte("charlie"), deleteSeq)

	entries := e.Entries()
	if len(entries) != len(keys) {
		t.Fatalf("Entries() len = %d, want %d (tombstones must still appear)", len(entries), len(keys))
	}
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("Entries() not strictly ascending at %d: %q then %q", i, entries[i-1].Key, entries[i].Key)
		}
	}
	for _, entry := range entries {
		if string(entry.Key) == "charlie" {
			if !entry.Tombstone {
				t.Fatalf("Entries(): charlie tombstone = false, want true")
			}
			if entry.Seq != deleteSeq {
				t.Fatalf("Entries(): charlie seq = %d, want %d", entry.Seq, deleteSeq)
			}
		}
	}
}

// TestEngineConcurrentReadersDuringSustainedWrites is the packet-5A
// concurrency-foundation gate: a single writer applying a sustained stream
// of Put/Delete calls (the future apply-loop's role) runs concurrently with
// several readers hammering Get and Entries (the future ReadIndex role),
// all coordinated through the one engine-level sync.RWMutex.
//
// The writer and readers are released together through a ready/start
// barrier: every goroutine signals it is spawned and blocked, then main
// releases them all at once. That alone does not guarantee a real overlap:
// 1000 fast, mutex-guarded writes can run to completion inside a single
// scheduling quantum before any reader is ever scheduled (reproducible with
// GOMAXPROCS=1). So readers do not run a fixed iteration budget that could
// exhaust on its own — an earlier version of this test did, and that let
// every reader finish and exit before the writer goroutine ran even once,
// which then deadlocked once the writer waited for an overlap witness that
// no reader remained alive to provide. Instead, readers loop until told to
// stop, and main only sends that stop signal after writerDone confirms the
// writer finished — so a reader is always still alive whenever the writer
// needs one to witness it. The writer itself does not report done — and
// does not flip writerActive false — until it receives overlapAck, closed
// by whichever reader is first to observe writerActive true; since that
// receive blocks, the writer goroutine parks and forces the runtime to
// schedule a reader instead (the only other runnable goroutine at that
// point), making the overlap structural rather than raced for, on any
// GOMAXPROCS. Every reader also asserts every Entries() snapshot it
// observes mid-write is strictly ordered, proving the read lock is held for
// the whole scan and never observes a partially-linked skip list. Run under
// -race.
func TestEngineConcurrentReadersDuringSustainedWrites(t *testing.T) {
	e := NewEngine(newSeededRand(29))

	const keyCount = 1000
	const deleteEvery = 5
	const readers = 6

	keys := make([][]byte, keyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%05d", i))
	}

	var ready sync.WaitGroup
	ready.Add(1 + readers)
	start := make(chan struct{})
	stop := make(chan struct{})

	var writerActive atomic.Bool
	var overlapWitnessed atomic.Bool
	overlapAck := make(chan struct{})
	var ackOnce sync.Once
	signalOverlap := func() {
		ackOnce.Do(func() {
			overlapWitnessed.Store(true)
			close(overlapAck)
		})
	}

	var writerDone sync.WaitGroup
	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		ready.Done()
		<-start

		writerActive.Store(true)

		seq := uint64(0)
		for i, key := range keys {
			seq++
			e.Put(append([]byte(nil), key...), []byte(fmt.Sprintf("value-%d", i)), seq)
			if i%deleteEvery == 0 {
				seq++
				e.Delete(append([]byte(nil), key...), seq)
			}
		}

		// All writes are issued, but the writer is not "done" — and
		// writerActive stays true — until a reader has actually witnessed
		// it active. See the func doc for why this can't just be a
		// best-effort check.
		<-overlapAck
		writerActive.Store(false)
	}()

	var readersDone sync.WaitGroup
	for r := 0; r < readers; r++ {
		readersDone.Add(1)
		go func(seed int64) {
			defer readersDone.Done()
			rnd := rand.New(rand.NewSource(seed))
			ready.Done()
			<-start

			for {
				select {
				case <-stop:
					return
				default:
				}

				if writerActive.Load() {
					signalOverlap()
				}

				_, _, _, _ = e.Get(keys[rnd.Intn(keyCount)])

				entries := e.Entries()
				for j := 1; j < len(entries); j++ {
					if bytes.Compare(entries[j-1].Key, entries[j].Key) >= 0 {
						t.Errorf("Entries() not strictly ascending mid-write at %d: %q then %q", j, entries[j-1].Key, entries[j].Key)
						return
					}
				}

				if writerActive.Load() {
					signalOverlap()
				}
			}
		}(int64(100 + r))
	}

	ready.Wait()
	close(start)

	writerDone.Wait()
	close(stop)
	readersDone.Wait()

	if !overlapWitnessed.Load() {
		t.Fatalf("no reader ever observed the writer active: writer and readers did not overlap, so this run did not exercise concurrent read/write access")
	}

	entries := e.Entries()
	if len(entries) != keyCount {
		t.Fatalf("final Entries() len = %d, want %d", len(entries), keyCount)
	}
	for i, key := range keys {
		value, tombstone, _, found := e.Get(key)
		if !found {
			t.Fatalf("Get(%s) found = false, want true", key)
		}
		if i%deleteEvery == 0 {
			if !tombstone {
				t.Fatalf("Get(%s) tombstone = false, want true (deleted)", key)
			}
			continue
		}
		if tombstone {
			t.Fatalf("Get(%s) tombstone = true, want false", key)
		}
		if want := fmt.Sprintf("value-%d", i); !bytes.Equal(value, []byte(want)) {
			t.Fatalf("Get(%s) = %q, want %q", key, value, want)
		}
	}
}
