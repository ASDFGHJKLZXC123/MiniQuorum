package storage

import (
	"errors"
	"reflect"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestMemStorageSnapshotOverlapAndInstalledSnapshotRules(t *testing.T) {
	t.Run("matching boundary retains overlap", func(t *testing.T) {
		store := NewMemStorage()
		entries := []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2}, {Index: 4, Term: 2}}
		if err := store.Save(nil, entries); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveSnapshot(raft.SnapshotMeta{Index: 4, Term: 2}); err != nil {
			t.Fatal(err)
		}
		if err := store.Compact(2); err != nil {
			t.Fatal(err)
		}
		got, err := store.Entries(3, 5)
		if err != nil || !reflect.DeepEqual(got, entries[2:]) {
			t.Fatalf("overlap = %#v,%v, want %#v", got, err, entries[2:])
		}
	})

	t.Run("conflicting boundary discards suffix", func(t *testing.T) {
		store := NewMemStorage()
		if err := store.Save(nil, []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}}); err != nil {
			t.Fatal(err)
		}
		meta := raft.SnapshotMeta{Index: 2, Term: 2}
		if err := store.SaveSnapshot(meta); err != nil {
			t.Fatal(err)
		}
		if got, err := store.Compacted(); err != nil || got != meta {
			t.Fatalf("Compacted() = %+v,%v, want %+v", got, err, meta)
		}
		if store.FirstIndex() != 3 || store.LastIndex() != 2 {
			t.Fatalf("indexes = %d..%d, want empty tail 3..2", store.FirstIndex(), store.LastIndex())
		}
		if _, err := store.Entries(3, 5); !errors.Is(err, ErrOutOfBounds) {
			t.Fatalf("old suffix read error = %v, want ErrOutOfBounds", err)
		}
		newSuffix := []raftpb.Entry{{Index: 3, Term: 2}}
		if err := store.Save(nil, newSuffix); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveSnapshot(meta); err != nil {
			t.Fatal(err)
		}
		if got, err := store.Entries(3, 4); err != nil || !reflect.DeepEqual(got, newSuffix) {
			t.Fatalf("idempotent snapshot lost valid suffix: %#v,%v", got, err)
		}
	})
}
