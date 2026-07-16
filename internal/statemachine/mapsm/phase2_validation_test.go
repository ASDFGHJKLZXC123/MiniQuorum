package mapsm

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	raftpb "miniquorum/proto"
)

func TestHashDeterministicAcrossNodesAndRepeatedReplays(t *testing.T) {
	log := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryType_NOOP},
		phase2CommandEntry(t, 2, 1, &raftpb.Command{ClientId: 11, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("zeta"), Value: []byte("last")}),
		phase2CommandEntry(t, 3, 1, &raftpb.Command{ClientId: 12, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("alpha"), Value: []byte("first")}),
		phase2CommandEntry(t, 4, 2, &raftpb.Command{ClientId: 13, Seq: 1, Op: raftpb.Op_GET, Key: []byte("zeta")}),
		phase2CommandEntry(t, 5, 2, &raftpb.Command{ClientId: 11, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("zeta"), Value: []byte("ignored-duplicate")}),
		phase2CommandEntry(t, 6, 2, &raftpb.Command{ClientId: 14, Seq: 1, Op: raftpb.Op_DELETE, Key: []byte("alpha")}),
	}

	var reference uint64
	for replay := 0; replay < 100; replay++ {
		left, right := New(), New()
		for i := range log {
			leftEntry := proto.Clone(&log[i]).(*raftpb.Entry)
			rightEntry := proto.Clone(&log[i]).(*raftpb.Entry)
			leftResult, err := left.Apply(leftEntry)
			if err != nil {
				t.Fatalf("replay %d left Apply index %d: %v", replay, log[i].Index, err)
			}
			rightResult, err := right.Apply(rightEntry)
			if err != nil {
				t.Fatalf("replay %d right Apply index %d: %v", replay, log[i].Index, err)
			}
			if !reflect.DeepEqual(leftResult, rightResult) {
				t.Fatalf("replay %d index %d results differ: left=%#v right=%#v", replay, log[i].Index, leftResult, rightResult)
			}
			if left.Hash() != right.Hash() {
				t.Fatalf("replay %d index %d hashes differ: left=%x right=%x", replay, log[i].Index, left.Hash(), right.Hash())
			}
		}

		got := left.Hash()
		if replay == 0 {
			reference = got
		} else if got != reference {
			t.Fatalf("replay %d final hash = %x, want stable reference %x", replay, got, reference)
		}
		for call := 0; call < 32; call++ {
			if left.Hash() != reference || right.Hash() != reference {
				t.Fatalf("replay %d repeated Hash call %d changed: left=%x right=%x reference=%x", replay, call, left.Hash(), right.Hash(), reference)
			}
		}
	}
}

func phase2CommandEntry(t *testing.T, index, term uint64, command *raftpb.Command) raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return raftpb.Entry{Index: index, Term: term, Type: raftpb.EntryType_NORMAL, Data: data}
}
