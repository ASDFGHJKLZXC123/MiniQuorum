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

func TestNewRecoveredNodeRetainsStorageOverlapButNeverReappliesSnapshotPrefix(t *testing.T) {
	store := storage.NewMemStorage()
	entries := []raftpb.Entry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 2}, {Index: 5, Term: 2},
	}
	if err := store.Save(&raft.HardState{Term: 2}, entries); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(raft.SnapshotMeta{Index: 5, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(3); err != nil {
		t.Fatal(err)
	}
	node, err := NewRecoveredNode(raft.Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}}, store, recoveryRand{})
	if err != nil {
		t.Fatal(err)
	}
	node.Advance()

	// PrevLogIndex 4 is behind state snapshot X=5 but inside retained overlap
	// (B,X]. Success proves startup kept that tail in the Raft core. The lower
	// leaderCommit must not regress commit/applied or emit any replay <=X.
	node.Step(&raftpb.Message{From: 2, To: 1, Term: 2, Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
		Term: 2, LeaderId: 2, PrevLogIndex: 4, PrevLogTerm: 2, LeaderCommit: 4,
	}}})
	rd := node.Ready()
	if len(rd.CommittedEntries) != 0 {
		t.Fatalf("overlap heartbeat reapplied entries: %#v", rd.CommittedEntries)
	}
	if len(rd.Messages) != 1 || !rd.Messages[0].GetAppendEntriesResp().GetSuccess() {
		t.Fatalf("overlap heartbeat response = %#v, want success", rd.Messages)
	}
	node.Advance()

	entry6 := &raftpb.Entry{Index: 6, Term: 2, Type: raftpb.EntryType_NORMAL, Data: []byte("new")}
	node.Step(&raftpb.Message{From: 2, To: 1, Term: 2, Body: &raftpb.Message_AppendEntries{AppendEntries: &raftpb.AppendEntriesReq{
		Term: 2, LeaderId: 2, PrevLogIndex: 5, PrevLogTerm: 2, Entries: []*raftpb.Entry{entry6}, LeaderCommit: 6,
	}}})
	rd = node.Ready()
	if len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != entry6.Index || rd.CommittedEntries[0].Term != entry6.Term || !reflect.DeepEqual(rd.CommittedEntries[0].Data, entry6.Data) {
		t.Fatalf("committed after snapshot = %#v, want only index 6", rd.CommittedEntries)
	}
}

type malformedRecoveryStorage struct {
	*storage.MemStorage
	first uint64
	last  uint64
	read  []raftpb.Entry
	meta  raft.SnapshotMeta
	base  raft.SnapshotMeta
}

func (s *malformedRecoveryStorage) Snapshot() (raft.SnapshotMeta, error)  { return s.meta, nil }
func (s *malformedRecoveryStorage) Compacted() (raft.SnapshotMeta, error) { return s.base, nil }
func (s *malformedRecoveryStorage) FirstIndex() uint64                    { return s.first }
func (s *malformedRecoveryStorage) LastIndex() uint64                     { return s.last }
func (s *malformedRecoveryStorage) Entries(uint64, uint64) ([]raftpb.Entry, error) {
	return append([]raftpb.Entry(nil), s.read...), nil
}

func TestNewRecoveredNodeRejectsMalformedSnapshotOverlap(t *testing.T) {
	tests := []struct {
		name  string
		store *malformedRecoveryStorage
	}{
		{
			name: "missing first overlap entry",
			store: &malformedRecoveryStorage{MemStorage: storage.NewMemStorage(), first: 4, last: 5,
				meta: raft.SnapshotMeta{Index: 5, Term: 2}, base: raft.SnapshotMeta{Index: 2, Term: 1},
				read: []raftpb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}}},
		},
		{
			name: "snapshot term mismatch",
			store: &malformedRecoveryStorage{MemStorage: storage.NewMemStorage(), first: 3, last: 5,
				meta: raft.SnapshotMeta{Index: 5, Term: 2}, base: raft.SnapshotMeta{Index: 2, Term: 1},
				read: []raftpb.Entry{{Index: 3, Term: 1}, {Index: 4, Term: 1}, {Index: 5, Term: 3}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRecoveredNode(raft.Config{}, test.store, recoveryRand{}); err == nil {
				t.Fatal("NewRecoveredNode() error = nil, want startup refusal")
			}
		})
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
func (*recoveryErrorStorage) SaveSnapshot(raft.SnapshotMeta) error { return nil }
func (*recoveryErrorStorage) Snapshot() (raft.SnapshotMeta, error) {
	return raft.SnapshotMeta{}, nil
}
func (*recoveryErrorStorage) Compacted() (raft.SnapshotMeta, error) {
	return raft.SnapshotMeta{}, nil
}
func (s *recoveryErrorStorage) Entries(uint64, uint64) ([]raftpb.Entry, error) {
	return nil, s.entriesErr
}
func (*recoveryErrorStorage) FirstIndex() uint64   { return 1 }
func (*recoveryErrorStorage) LastIndex() uint64    { return 1 }
func (*recoveryErrorStorage) Compact(uint64) error { return nil }

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
