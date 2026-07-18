package server

import (
	"errors"
	"reflect"
	"testing"

	"miniquorum/internal/raft"
	"miniquorum/internal/storage"
	raftpb "miniquorum/proto"
)

type recoveryRand struct{}

func (recoveryRand) IntN(int) int { return 0 }

func TestNewRecoveredNodeWaitsForOrdinaryCommitBeforeReplay(t *testing.T) {
	store := storage.NewMemStorage()
	hard := raft.HardState{Term: 3, VotedFor: 1}
	entries := []raftpb.Entry{
		{Index: 1, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("old")},
		{Index: 2, Term: 3, Type: raftpb.EntryType_NOOP},
	}
	if err := store.Save(&hard, entries); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	node, err := NewRecoveredNode(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}}, store, recoveryRand{})
	if err != nil {
		t.Fatalf("NewRecoveredNode() error = %v", err)
	}
	before := node.Ready()
	if before.HardState != nil || len(before.Entries) != 0 || len(before.CommittedEntries) != 0 {
		t.Fatalf("startup Ready = %+v, want no persistence or special replay before commit is re-established", before)
	}
	node.Advance()

	// A normal AppendEntries heartbeat re-establishes commitIndex. The loaded
	// entries must then appear through the same Ready.CommittedEntries field as
	// entries committed without a restart.
	node.Step(&raftpb.Message{
		From: 2, To: 1, Term: 3,
		Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
			Term: 3, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 3, LeaderCommit: 2,
		}},
	})
	after := node.Ready()
	if !reflect.DeepEqual(after.CommittedEntries, entries) {
		t.Fatalf("CommittedEntries after ordinary heartbeat = %#v, want recovered entries %#v", after.CommittedEntries, entries)
	}
}

type recoveryErrorStorage struct {
	hardErr    error
	entriesErr error
}

func (*recoveryErrorStorage) Save(*raft.HardState, []raftpb.Entry) error { return nil }
func (s *recoveryErrorStorage) HardState() (raft.HardState, error) {
	return raft.HardState{}, s.hardErr
}
func (s *recoveryErrorStorage) Entries(uint64, uint64) ([]raftpb.Entry, error) {
	return nil, s.entriesErr
}
func (*recoveryErrorStorage) FirstIndex() uint64 { return 1 }
func (*recoveryErrorStorage) LastIndex() uint64  { return 1 }

func TestNewRecoveredNodePropagatesStorageErrors(t *testing.T) {
	wantHard := errors.New("hard read failed")
	if _, err := NewRecoveredNode(raft.Config{}, &recoveryErrorStorage{hardErr: wantHard}, recoveryRand{}); !errors.Is(err, wantHard) {
		t.Fatalf("hard-state error = %v, want it to wrap %v", err, wantHard)
	}

	wantEntries := errors.New("entries read failed")
	if _, err := NewRecoveredNode(raft.Config{}, &recoveryErrorStorage{entriesErr: wantEntries}, recoveryRand{}); !errors.Is(err, wantEntries) {
		t.Fatalf("entries error = %v, want it to wrap %v", err, wantEntries)
	}
	if _, err := NewRecoveredNode(raft.Config{}, nil, recoveryRand{}); err == nil {
		t.Fatal("nil storage error = nil, want startup refusal")
	}
}
