// Package storage defines the durable Raft storage boundary.
package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// Storage persists Raft state. Save is one atomic durable operation per Ready.
type Storage interface {
	Save(hs *raft.HardState, entries []raftpb.Entry) error
	SaveSnapshot(meta raft.SnapshotMeta) error
	HardState() (raft.HardState, error)
	Snapshot() (raft.SnapshotMeta, error)
	Compacted() (raft.SnapshotMeta, error)
	Entries(lo, hi uint64) ([]raftpb.Entry, error)
	FirstIndex() uint64
	LastIndex() uint64
	Compact(uptoIndex uint64) error
}

// ErrOutOfBounds indicates an Entries request outside the retained log range.
var ErrOutOfBounds = errors.New("storage: entries out of bounds")

// ErrCompactionBeyondSnapshot rejects deletion of log state not covered by
// the persisted state-machine snapshot. The server also checks the engine's
// durable watermark before it reaches this boundary.
var ErrCompactionBeyondSnapshot = errors.New("storage: compaction exceeds persisted snapshot")

// MemStorage is a volatile, concurrency-safe implementation for early tests.
type MemStorage struct {
	mu        sync.RWMutex
	hard      raft.HardState
	snapshot  raft.SnapshotMeta
	compacted raft.SnapshotMeta
	entries   []raftpb.Entry
}

// SaveSnapshot persists the newest state-machine snapshot position.
func (s *MemStorage) SaveSnapshot(meta raft.SnapshotMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta.Index == math.MaxUint64 {
		return errors.New("storage: snapshot index overflows first index")
	}
	if meta.Index == 0 && meta.Term != 0 || meta.Index != 0 && meta.Term == 0 {
		return fmt.Errorf("storage: invalid snapshot meta index=%d term=%d", meta.Index, meta.Term)
	}
	if meta.Index < s.snapshot.Index {
		return fmt.Errorf("storage: snapshot index regresses from %d to %d", s.snapshot.Index, meta.Index)
	}
	if meta.Index == s.snapshot.Index && s.snapshot != (raft.SnapshotMeta{}) && meta.Term != s.snapshot.Term {
		return fmt.Errorf("storage: snapshot term changed at index %d from %d to %d", meta.Index, s.snapshot.Term, meta.Term)
	}
	if meta == s.snapshot {
		return nil
	}
	compacted := s.compacted
	installed := false
	if meta.Index != 0 {
		if term, ok := entryTerm(s.entries, meta.Index); !ok || term != meta.Term {
			// InstallSnapshot may publish X on a lagging follower which has no
			// matching entry X. In that case there is no tail overlap: X itself
			// becomes the compacted base atomically with the snapshot metadata.
			compacted = meta
			installed = true
		}
	}
	s.snapshot = meta
	s.compacted = compacted
	if installed {
		// A suffix is reusable after InstallSnapshot only when entry X has the
		// snapshot term. A missing or conflicting boundary invalidates every
		// later entry, even though B=X hides the old prefix.
		clear(s.entries)
		s.entries = nil
	} else if compacted.Index == meta.Index {
		cut := 0
		for cut < len(s.entries) && s.entries[cut].Index <= compacted.Index {
			cut++
		}
		clear(s.entries[:cut])
		s.entries = append([]raftpb.Entry(nil), s.entries[cut:]...)
	}
	return nil
}

// NewMemStorage returns empty volatile Raft storage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// Save atomically updates hard state and appends entries, truncating suffixes
// beginning at the first supplied entry index.
func (s *MemStorage) Save(hs *raft.HardState, entries []raftpb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newHard := s.hard
	if hs != nil {
		newHard = *hs
	}
	newEntries := cloneEntries(s.entries)
	if len(entries) > 0 {
		cut := len(newEntries)
		for i := range newEntries {
			if newEntries[i].Index >= entries[0].Index {
				cut = i
				break
			}
		}
		newEntries = append(newEntries[:cut], cloneEntries(entries)...)
	}
	s.hard = newHard
	s.entries = newEntries
	return nil
}

// TruncateSuffix applies the standalone durable truncate record used by the
// crash-model storage when an entries record tears away. It is intentionally
// outside Storage: production callers truncate suffixes through Save.
func (s *MemStorage) TruncateSuffix(fromIndex uint64) error {
	if fromIndex == 0 {
		return errors.New("storage: truncate suffix from index 0")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cut := len(s.entries)
	for i := range s.entries {
		if s.entries[i].Index >= fromIndex {
			cut = i
			break
		}
	}
	clear(s.entries[cut:])
	s.entries = s.entries[:cut]
	return nil
}

// HardState returns a copy of the persisted hard state.
func (s *MemStorage) HardState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hard, nil
}

// Snapshot returns the persisted state-machine snapshot position.
func (s *MemStorage) Snapshot() (raft.SnapshotMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, nil
}

// Compacted returns the persisted Raft log base. It can trail Snapshot: the
// entries between the two positions are the retained overlap available to
// slightly lagging followers after restart.
func (s *MemStorage) Compacted() (raft.SnapshotMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compacted, nil
}

// Entries returns entries in the half-open interval [lo, hi).
func (s *MemStorage) Entries(lo, hi uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	first, last := firstLast(s.entries, s.snapshot, s.compacted)
	if lo > hi || lo < first || hi > last+1 {
		return nil, ErrOutOfBounds
	}
	start := lo - first
	end := hi - first
	return cloneEntries(s.entries[start:end]), nil
}

// FirstIndex returns the first available index, or 1 for an empty log.
func (s *MemStorage) FirstIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	first, _ := firstLast(s.entries, s.snapshot, s.compacted)
	return first
}

// LastIndex returns the last available index, or 0 for an empty log.
func (s *MemStorage) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, last := firstLast(s.entries, s.snapshot, s.compacted)
	return last
}

// Compact discards the in-memory prefix through uptoIndex. MemStorage has no
// physical segments; the durable disk implementation applies the same API by
// deleting only whole segment files.
func (s *MemStorage) Compact(uptoIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if uptoIndex > s.snapshot.Index {
		return fmt.Errorf("%w: compact through %d, snapshot index %d", ErrCompactionBeyondSnapshot, uptoIndex, s.snapshot.Index)
	}
	if uptoIndex < s.compacted.Index {
		return nil
	}
	if uptoIndex > s.compacted.Index {
		term, ok := entryTerm(s.entries, uptoIndex)
		if uptoIndex == s.snapshot.Index {
			term, ok = s.snapshot.Term, true
		}
		if !ok {
			return fmt.Errorf("storage: compact index %d has no retained term", uptoIndex)
		}
		s.compacted = raft.SnapshotMeta{Index: uptoIndex, Term: term}
	}
	cut := 0
	for cut < len(s.entries) && s.entries[cut].Index <= uptoIndex {
		cut++
	}
	if cut == 0 {
		return nil
	}
	clear(s.entries[:cut])
	s.entries = append([]raftpb.Entry(nil), s.entries[cut:]...)
	return nil
}

func firstLast(entries []raftpb.Entry, snapshot, compacted raft.SnapshotMeta) (uint64, uint64) {
	if len(entries) == 0 {
		return compacted.Index + 1, snapshot.Index
	}
	return entries[0].Index, max(entries[len(entries)-1].Index, snapshot.Index)
}

func entryTerm(entries []raftpb.Entry, index uint64) (uint64, bool) {
	for i := range entries {
		if entries[i].Index == index {
			return entries[i].Term, true
		}
	}
	return 0, false
}

func cloneEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]raftpb.Entry, len(entries))
	for i := range entries {
		cloned[i] = raftpb.Entry{
			Index: entries[i].Index,
			Term:  entries[i].Term,
			Type:  entries[i].Type,
			Data:  append([]byte(nil), entries[i].Data...),
		}
	}
	return cloned
}
