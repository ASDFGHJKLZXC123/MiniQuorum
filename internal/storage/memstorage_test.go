package storage

import (
	"errors"
	"reflect"
	"testing"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestMemStorageSaveRoundTripIsAtomic(t *testing.T) {
	s := NewMemStorage()
	hard := raft.HardState{Term: 7, VotedFor: 3}
	entries := []raftpb.Entry{
		{Index: 1, Term: 5, Type: raftpb.EntryType_NORMAL, Data: []byte("one")},
		{Index: 2, Term: 7, Type: raftpb.EntryType_NOOP},
	}
	if err := s.Save(&hard, entries); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	gotHard, err := s.HardState()
	if err != nil {
		t.Fatalf("HardState() error: %v", err)
	}
	if gotHard != hard {
		t.Fatalf("HardState() = %#v, want %#v", gotHard, hard)
	}
	gotEntries, err := s.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries() error: %v", err)
	}
	if !reflect.DeepEqual(gotEntries, entries) {
		t.Fatalf("Entries() = %#v, want %#v", gotEntries, entries)
	}
	entries[0].Data[0] = 'X'
	gotEntries, _ = s.Entries(1, 2)
	if string(gotEntries[0].Data) != "one" {
		t.Fatalf("Save retained caller data: %q", gotEntries[0].Data)
	}
}

func TestMemStorageSaveTruncatesSuffixAtExistingIndex(t *testing.T) {
	s := NewMemStorage()
	if err := s.Save(nil, []raftpb.Entry{{Index: 1}, {Index: 2}, {Index: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(nil, []raftpb.Entry{{Index: 2, Term: 9}, {Index: 3, Term: 9}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Entries(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []raftpb.Entry{{Index: 1}, {Index: 2, Term: 9}, {Index: 3, Term: 9}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries after suffix replacement = %#v, want %#v", got, want)
	}
}

func TestMemStorageEntriesBounds(t *testing.T) {
	s := NewMemStorage()
	if err := s.Save(nil, []raftpb.Entry{{Index: 4}, {Index: 5}, {Index: 6}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Entries(5, 7)
	if err != nil {
		t.Fatalf("Entries(5, 7) error: %v", err)
	}
	if want := []raftpb.Entry{{Index: 5}, {Index: 6}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries(5, 7) = %#v, want %#v", got, want)
	}
	for _, bounds := range [][2]uint64{{3, 4}, {4, 8}, {6, 5}} {
		if _, err := s.Entries(bounds[0], bounds[1]); !errors.Is(err, ErrOutOfBounds) {
			t.Errorf("Entries(%d, %d) error = %v, want ErrOutOfBounds", bounds[0], bounds[1], err)
		}
	}
}
