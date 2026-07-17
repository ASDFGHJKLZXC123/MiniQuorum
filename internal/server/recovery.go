package server

import (
	"errors"
	"fmt"
	"math"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

// NewRecoveredNode constructs a Raft node from the durable state exposed by
// store. Recovery deliberately loads only HardState and the retained log:
// commit index is not persisted, and no entry is applied here. Once ordinary
// Raft traffic re-establishes commitIndex, the node emits those entries in
// Ready.CommittedEntries and Host processes them through the normal apply
// path.
func NewRecoveredNode(cfg raft.Config, store storage.Storage, rnd raft.Rand) (*raft.Node, error) {
	if store == nil {
		return nil, errors.New("server: recover node: nil storage")
	}
	hard, err := store.HardState()
	if err != nil {
		return nil, fmt.Errorf("server: recover hard state: %w", err)
	}

	first, last := store.FirstIndex(), store.LastIndex()
	var entries []raftpb.Entry
	if last >= first {
		if last == math.MaxUint64 {
			return nil, errors.New("server: recover entries: last index overflows half-open range")
		}
		entries, err = store.Entries(first, last+1)
		if err != nil {
			return nil, fmt.Errorf("server: recover entries [%d,%d): %w", first, last+1, err)
		}
	}

	return raft.NewNode(cfg, raft.InitialState{HardState: hard, Entries: entries}, rnd), nil
}
