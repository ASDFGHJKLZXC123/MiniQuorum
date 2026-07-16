// Package statemachine defines the deterministic replicated-state-machine
// contract used by the Raft host.
package statemachine

import (
	"errors"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

// Result is the outcome of a state-machine command.
//
// Implementations return Value as an owned copy. Callers may modify it.
type Result struct {
	Value []byte
	Found bool
}

// ErrSnapshotUnsupported is returned by the Phase 2 snapshot stubs. Snapshot
// creation and restoration are implemented in Phase 6.
var ErrSnapshotUnsupported = errors.New("state machine snapshots are not implemented until phase 6")

// StateMachine applies committed Raft entries in log order.
type StateMachine interface {
	// Apply decodes NORMAL entries and handles deduplication. NOOP entries
	// mutate no user state but advance AppliedIndex. The caller retains entry;
	// implementations must neither retain nor mutate it.
	Apply(entry *raftpb.Entry) (Result, error)
	// Read is the direct, concurrent-safe read path used by Phase 6 ReadIndex.
	Read(key []byte) (Result, error)
	AppliedIndex() uint64
	Hash() uint64
	CreateSnapshot(dir string, meta raft.SnapshotMeta) error
	RestoreSnapshot(dir string) (raft.SnapshotMeta, error)
}
