package lsm

import "bytes"

// maxHeight and towerInverseP implement the Phase 5 binding decision: a
// probabilistic skip list with max height 16 and p = 1/4 for the tower
// (level) distribution.
const (
	maxHeight     = 16
	towerInverseP = 4 // p = 1/towerInverseP = 1/4
)

// Rand is the deterministic randomness source the skip list draws tower
// heights from. The host (production server or simulator) supplies a seeded
// implementation; internal/lsm never reads math/rand or any other entropy
// source directly, keeping simulator-visible behavior deterministic.
type Rand interface {
	IntN(n int) int
}

// skipNode is one key's tower. forward[i] is the next node at level i. seq
// is the Raft applied index the stored value or tombstone was written at
// (phases/phase-5-lsm.md's SSTable data-block record carries the same
// field for the same reason: recovery must be able to tell which of two
// versions of a key is newer).
type skipNode struct {
	key       []byte
	value     []byte
	tombstone bool
	seq       uint64
	forward   []*skipNode
}

// skipList is a probabilistic, byte-ordered skip list keyed on raw bytes.
// It is the memtable's backing data structure. skipList itself performs no
// synchronization: callers (Engine) are responsible for single-writer,
// concurrent-reader access via an external lock.
type skipList struct {
	head  *skipNode
	level int // number of levels currently in use, 1..maxHeight
	rnd   Rand
}

// newSkipList constructs an empty skip list whose tower heights are drawn
// from rnd.
func newSkipList(rnd Rand) *skipList {
	return &skipList{
		head:  &skipNode{forward: make([]*skipNode, maxHeight)},
		level: 1,
		rnd:   rnd,
	}
}

// randomHeight draws a tower height in [1, maxHeight] with P(height >= h) =
// (1/towerInverseP)^(h-1), truncated at maxHeight.
func randomHeight(rnd Rand, maxH, inverseP int) int {
	height := 1
	for height < maxH && rnd.IntN(inverseP) == 0 {
		height++
	}
	return height
}

// put inserts key at seq if absent. If key is present, put applies the
// write only when seq is strictly greater than the seq already stored for
// key; otherwise it is a no-op. This is the monotonic-per-key rule the
// recovery contract in phases/phase-5-lsm.md depends on: full-log replay
// from index 0 can re-deliver an already-applied write, or (while the
// replay view and the durable engine are reconciled) deliver writes out of
// order relative to what the memtable already holds.
//
//   - seq > stored seq: applied — replaces value, tombstone, and seq.
//   - seq == stored seq: no-op. The same Raft entry produces the same write
//     by construction, so re-applying it must leave state unchanged
//     (idempotent replay) rather than, say, re-copying an identical value.
//   - seq < stored seq: no-op. A higher memtable seq must never be
//     overwritten by a replayed lower one.
//
// A key with no existing entry accepts any seq — there is nothing to
// compare against yet. A tombstone put still occupies the key's slot in the
// ordered sequence so iteration and lookups observe the delete.
func (s *skipList) put(key, value []byte, tombstone bool, seq uint64) {
	var update [maxHeight]*skipNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	next := x.forward[0]
	if next != nil && bytes.Equal(next.key, key) {
		if seq <= next.seq {
			return
		}
		next.value = value
		next.tombstone = tombstone
		next.seq = seq
		return
	}

	height := randomHeight(s.rnd, maxHeight, towerInverseP)
	if height > s.level {
		for i := s.level; i < height; i++ {
			update[i] = s.head
		}
		s.level = height
	}

	node := &skipNode{
		key:       key,
		value:     value,
		tombstone: tombstone,
		seq:       seq,
		forward:   make([]*skipNode, height),
	}
	for i := 0; i < height; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}
}

// get performs a byte-ordered lookup. found is false if key has never been
// put; tombstone is true if the highest-seq put for key was a delete; seq
// is the Raft applied index that write was made at.
func (s *skipList) get(key []byte) (value []byte, tombstone bool, seq uint64, found bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && bytes.Equal(x.key, key) {
		return x.value, x.tombstone, x.seq, true
	}
	return nil, false, 0, false
}

// forEach walks every key in ascending byte order, live entries and
// tombstones alike, calling fn with each key's stored seq. Iteration stops
// early if fn returns false.
func (s *skipList) forEach(fn func(key, value []byte, tombstone bool, seq uint64) bool) {
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		if !fn(x.key, x.value, x.tombstone, x.seq) {
			return
		}
	}
}
