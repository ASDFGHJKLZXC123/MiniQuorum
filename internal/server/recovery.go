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
	snapshot, err := store.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("server: recover snapshot metadata: %w", err)
	}
	compacted, err := store.Compacted()
	if err != nil {
		return nil, fmt.Errorf("server: recover compacted metadata: %w", err)
	}
	if err := validateRecoveredMetadata(snapshot, compacted); err != nil {
		return nil, fmt.Errorf("server: recover metadata: %w", err)
	}

	first, last := store.FirstIndex(), store.LastIndex()
	if compacted.Index == math.MaxUint64 {
		return nil, errors.New("server: recover compacted index overflows half-open range")
	}
	wantFirst := compacted.Index + 1
	if first != wantFirst {
		return nil, fmt.Errorf("server: recover first index %d, want compacted index + 1 = %d", first, wantFirst)
	}
	if last < snapshot.Index {
		return nil, fmt.Errorf("server: recover last index %d precedes snapshot index %d", last, snapshot.Index)
	}
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
	if err := validateRecoveredEntries(snapshot, compacted, first, last, entries); err != nil {
		return nil, fmt.Errorf("server: recover log: %w", err)
	}

	return raft.NewNode(cfg, raft.InitialState{HardState: hard, Entries: entries, Snapshot: compacted, Applied: snapshot.Index}, rnd), nil
}

func validateRecoveredMetadata(snapshot, compacted raft.SnapshotMeta) error {
	for _, item := range []struct {
		name string
		meta raft.SnapshotMeta
	}{{"snapshot", snapshot}, {"compacted", compacted}} {
		name, meta := item.name, item.meta
		if meta.Index == 0 && meta.Term != 0 || meta.Index != 0 && meta.Term == 0 {
			return fmt.Errorf("%s metadata has invalid index/term %d/%d", name, meta.Index, meta.Term)
		}
	}
	if compacted.Index > snapshot.Index {
		return fmt.Errorf("compacted index %d exceeds snapshot index %d", compacted.Index, snapshot.Index)
	}
	if compacted.Index == snapshot.Index && compacted.Term != snapshot.Term {
		return fmt.Errorf("compacted term %d differs from snapshot term %d at index %d", compacted.Term, snapshot.Term, snapshot.Index)
	}
	return nil
}

func validateRecoveredEntries(snapshot, compacted raft.SnapshotMeta, first, last uint64, entries []raftpb.Entry) error {
	wantCount := uint64(0)
	if last >= first {
		wantCount = last - first + 1
	}
	if uint64(len(entries)) != wantCount {
		return fmt.Errorf("loaded %d entries for contiguous range [%d,%d], want %d", len(entries), first, last, wantCount)
	}
	for offset := range entries {
		want := first + uint64(offset)
		if entries[offset].Index != want {
			return fmt.Errorf("entry offset %d has index %d, want %d", offset, entries[offset].Index, want)
		}
	}
	if compacted.Index < snapshot.Index {
		offset := snapshot.Index - first
		if offset >= uint64(len(entries)) {
			return fmt.Errorf("retained overlap is missing snapshot index %d", snapshot.Index)
		}
		if entries[offset].Term != snapshot.Term {
			return fmt.Errorf("snapshot term %d at index %d differs from retained log term %d", snapshot.Term, snapshot.Index, entries[offset].Term)
		}
	}
	return nil
}
