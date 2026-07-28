// Package lsm implements the hand-built LSM storage engine for Phase 5.
//
// Packet 5A scope: the skip-list memtable and its concurrency foundation
// only. Engine owns the single engine-level sync.RWMutex the rest of Phase 5
// builds on (bloom filters, SSTables, manifest, compaction, StateMachine
// integration are later packets): Put/Delete take the write lock (single
// writer), Get/Entries take the read lock (concurrent readers).
//
// Every Put/Delete carries seq, the Raft applied index of the write
// (phases/phase-5-lsm.md's binding recovery contract). The memtable is
// monotonic per key: a seq no greater than the one already stored for a key
// is a no-op, so a full-log replay from index 0 can never let a replayed
// lower seq clobber a higher one already present. See skipList.put for the
// exact rule, including the equal-seq idempotence case.
package lsm

import "sync"

// Entry is one ordered key/value record returned by Engine.Entries.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Seq       uint64
}

// Engine owns the memtable and the engine-level RWMutex that guards it.
// Engine itself performs no I/O and reads no wall clock or free entropy: all
// randomness (skip-list tower heights) flows through the injected Rand.
type Engine struct {
	mu   sync.RWMutex
	list *skipList
}

// NewEngine constructs an empty engine whose memtable draws tower heights
// from rnd. rnd must not be nil: NewEngine panics immediately rather than
// leaving a nil Rand to surface later as an opaque nil-pointer panic on the
// engine's first write. There is no fallback entropy source — the host
// (production server or simulator) must always supply one, since simulator
// determinism depends on every draw coming from the injected Rand.
func NewEngine(rnd Rand) *Engine {
	if rnd == nil {
		panic("lsm: NewEngine: rnd must not be nil")
	}
	return &Engine{list: newSkipList(rnd)}
}

// Put inserts or updates key with value at seq. Put defensively copies key
// and value into engine-owned storage; the caller keeps ownership of the
// slices it passed in and may reuse or mutate them freely once Put returns.
// See skipList.put for the seq-monotonic accept/reject rule.
func (e *Engine) Put(key, value []byte, seq uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list.put(cloneBytes(key), cloneBytes(value), false, seq)
}

// Delete records a tombstone for key at seq, shadowing any earlier value
// regardless of whether key was previously present. Delete defensively
// copies key into engine-owned storage; the caller keeps ownership of the
// slice it passed in and may reuse or mutate it freely once Delete returns.
// See skipList.put for the seq-monotonic accept/reject rule.
func (e *Engine) Delete(key []byte, seq uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list.put(cloneBytes(key), nil, true, seq)
}

// Get looks up key. found is false if key has never been written;
// tombstone is true if the highest-seq write for key was a delete; seq is
// the Raft applied index that write was made at. The returned value is an
// owned copy safe for the caller to mutate.
func (e *Engine) Get(key []byte) (value []byte, tombstone bool, seq uint64, found bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, tombstone, seq, found = e.list.get(key)
	return cloneBytes(value), tombstone, seq, found
}

// Entries returns every key currently in the memtable, in ascending byte
// order, live entries and tombstones alike, each with the seq it was
// written at. The engine read lock is held for the entire scan so Entries
// observes one consistent snapshot; the returned slice owns its own copies
// of every key and value.
func (e *Engine) Entries() []Entry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var out []Entry
	e.list.forEach(func(key, value []byte, tombstone bool, seq uint64) bool {
		out = append(out, Entry{Key: cloneBytes(key), Value: cloneBytes(value), Tombstone: tombstone, Seq: seq})
		return true
	})
	return out
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
