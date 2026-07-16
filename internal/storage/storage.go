// Package storage defines the durable Raft storage boundary.
package storage

import (
	"errors"
	"sync"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// Storage persists Raft state. Save is one atomic durable operation per Ready.
type Storage interface {
	Save(hs *raft.HardState, entries []raftpb.Entry) error
	HardState() (raft.HardState, error)
	Entries(lo, hi uint64) ([]raftpb.Entry, error)
	FirstIndex() uint64
	LastIndex() uint64
}

// ErrOutOfBounds indicates an Entries request outside the retained log range.
var ErrOutOfBounds = errors.New("storage: entries out of bounds")

// MemStorage is a volatile, concurrency-safe implementation for early tests.
type MemStorage struct {
	mu      sync.RWMutex
	hard    raft.HardState
	entries []raftpb.Entry
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

// HardState returns a copy of the persisted hard state.
func (s *MemStorage) HardState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hard, nil
}

// Entries returns entries in the half-open interval [lo, hi).
func (s *MemStorage) Entries(lo, hi uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	first, last := firstLast(s.entries)
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
	first, _ := firstLast(s.entries)
	return first
}

// LastIndex returns the last available index, or 0 for an empty log.
func (s *MemStorage) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, last := firstLast(s.entries)
	return last
}

func firstLast(entries []raftpb.Entry) (uint64, uint64) {
	if len(entries) == 0 {
		return 1, 0
	}
	return entries[0].Index, entries[len(entries)-1].Index
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
