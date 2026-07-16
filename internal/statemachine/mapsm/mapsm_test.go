package mapsm

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/raft"
	"miniquorum/internal/statemachine"
	raftpb "miniquorum/proto"
)

func TestApplyPutGetDeleteAndNoop(t *testing.T) {
	sm := New()

	if got := applyCommand(t, sm, 1, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")}); got.Found || got.Value != nil {
		t.Fatalf("PUT result = %#v, want empty result", got)
	}
	got := applyCommand(t, sm, 2, 1, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	if !got.Found || !bytes.Equal(got.Value, []byte("v")) {
		t.Fatalf("GET result = %#v, want found v", got)
	}

	if got, err := sm.Read([]byte("k")); err != nil || !got.Found || !bytes.Equal(got.Value, []byte("v")) {
		t.Fatalf("Read(k) = %#v, %v; want found v, nil", got, err)
	}
	applyCommand(t, sm, 3, 1, &raftpb.Command{ClientId: 3, Seq: 1, Op: raftpb.Op_DELETE, Key: []byte("k")})
	if got, err := sm.Read([]byte("k")); err != nil || got.Found || got.Value != nil {
		t.Fatalf("Read(k) after DELETE = %#v, %v; want not found, nil", got, err)
	}

	hashBeforeNoop := sm.Hash()
	if _, err := sm.Apply(&raftpb.Entry{Index: 4, Type: raftpb.EntryType_NOOP}); err != nil {
		t.Fatalf("Apply(NOOP): %v", err)
	}
	if got := sm.AppliedIndex(); got != 4 {
		t.Fatalf("AppliedIndex() = %d, want 4", got)
	}
	if got, err := sm.Read([]byte("k")); err != nil || got.Found {
		t.Fatalf("Read(k) after NOOP = %#v, %v; want unchanged not found", got, err)
	}
	if sm.Hash() == hashBeforeNoop {
		t.Fatal("Hash() did not include the applied index after NOOP")
	}
}

func TestApplyDeduplicatesRepeatedGetAndReturnsCachedResult(t *testing.T) {
	sm := New()
	applyCommand(t, sm, 1, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v1")})
	firstGet := applyCommand(t, sm, 2, 1, &raftpb.Command{ClientId: 22, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	applyCommand(t, sm, 3, 1, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v2")})

	duplicateGet := applyCommand(t, sm, 4, 1, &raftpb.Command{ClientId: 22, Seq: 1, Op: raftpb.Op_GET, Key: []byte("k")})
	if !duplicateGet.Found || !bytes.Equal(duplicateGet.Value, []byte("v1")) {
		t.Fatalf("duplicate GET result = %#v, want cached v1", duplicateGet)
	}
	if !bytes.Equal(firstGet.Value, duplicateGet.Value) {
		t.Fatalf("duplicate GET result = %q, first result = %q", duplicateGet.Value, firstGet.Value)
	}
	if got, err := sm.Read([]byte("k")); err != nil || !got.Found || !bytes.Equal(got.Value, []byte("v2")) {
		t.Fatalf("Read(k) = %#v, %v; want current v2, nil", got, err)
	}
	if got := sm.AppliedIndex(); got != 4 {
		t.Fatalf("AppliedIndex() = %d, want 4 after duplicate entry", got)
	}
}

func TestApplyDefensivelyCopiesInputsAndResults(t *testing.T) {
	sm := New()
	key := []byte("key")
	value := []byte("value")
	command := &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: key, Value: value}
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	entry := &raftpb.Entry{Index: 1, Type: raftpb.EntryType_NORMAL, Data: data}
	before := proto.Clone(entry)
	if _, err := sm.Apply(entry); err != nil {
		t.Fatalf("Apply(PUT): %v", err)
	}
	if !proto.Equal(entry, before) {
		t.Fatalf("Apply mutated entry: got %#v, want %#v", entry, before)
	}

	key[0] = 'X'
	value[0] = 'X'
	entry.Data[0] ^= 0xff
	got, err := sm.Read([]byte("key"))
	if err != nil || !got.Found || !bytes.Equal(got.Value, []byte("value")) {
		t.Fatalf("Read(key) after caller mutation = %#v, %v; want value, nil", got, err)
	}
	got.Value[0] = 'X'
	again, err := sm.Read([]byte("key"))
	if err != nil || !again.Found || !bytes.Equal(again.Value, []byte("value")) {
		t.Fatalf("Read(key) after result mutation = %#v, %v; want value, nil", again, err)
	}

	firstGet := applyCommand(t, sm, 2, 1, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")})
	firstGet.Value[0] = 'X'
	cachedGet := applyCommand(t, sm, 3, 1, &raftpb.Command{ClientId: 2, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")})
	if !cachedGet.Found || !bytes.Equal(cachedGet.Value, []byte("value")) {
		t.Fatalf("cached GET after caller result mutation = %#v, want value", cachedGet)
	}
}

func TestConcurrentApplyReadAndHash(t *testing.T) {
	sm := New()
	const writers = 8
	const writesPerWriter = 100

	errs := make(chan error, writers+2)
	var writersWG sync.WaitGroup
	for writer := range writers {
		writersWG.Add(1)
		go func(writer int) {
			defer writersWG.Done()
			for sequence := range writesPerWriter {
				command := &raftpb.Command{
					ClientId: uint64(writer*writesPerWriter + sequence + 1),
					Seq:      1,
					Op:       raftpb.Op_PUT,
					Key:      []byte(fmt.Sprintf("key-%d-%d", writer, sequence)),
					Value:    []byte("value"),
				}
				if _, err := sm.Apply(commandEntry(t, uint64(writer*writesPerWriter+sequence+1), command)); err != nil {
					errs <- err
					return
				}
			}
		}(writer)
	}

	var readersWG sync.WaitGroup
	for range 2 {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for range 500 {
				if _, err := sm.Read([]byte("key-0-0")); err != nil {
					errs <- err
					return
				}
				_ = sm.Hash()
				_ = sm.AppliedIndex()
			}
		}()
	}
	writersWG.Wait()
	readersWG.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent state-machine operation: %v", err)
	}
	if got, want := sm.AppliedIndex(), uint64(writers*writesPerWriter); got != want {
		t.Fatalf("AppliedIndex() = %d, want %d", got, want)
	}
}

func TestHashIsOrderIndependentAndCoversFullState(t *testing.T) {
	first, second := New(), New()
	commands := []*raftpb.Command{
		{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("gamma"), Value: []byte("3")},
		{ClientId: 2, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("alpha"), Value: []byte("1")},
		{ClientId: 3, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("beta"), Value: []byte("2")},
	}
	for index, command := range commands {
		applyCommand(t, first, uint64(index+1), 1, command)
	}
	for index, commandIndex := range []int{2, 0, 1} {
		applyCommand(t, second, uint64(index+1), 1, commands[commandIndex])
	}
	if got, want := first.Hash(), second.Hash(); got != want {
		t.Fatalf("same logical state hash differs by insertion order: %x != %x", got, want)
	}
	for range 20 {
		if got, want := first.Hash(), second.Hash(); got != want {
			t.Fatalf("repeated hash differs: %x != %x", got, want)
		}
	}

	sameKVFirst, sameKVSecond := New(), New()
	applyCommand(t, sameKVFirst, 1, 1, &raftpb.Command{ClientId: 10, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")})
	applyCommand(t, sameKVFirst, 2, 1, &raftpb.Command{ClientId: 11, Seq: 1, Op: raftpb.Op_GET, Key: []byte("missing")})
	applyCommand(t, sameKVSecond, 1, 1, &raftpb.Command{ClientId: 20, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")})
	applyCommand(t, sameKVSecond, 2, 1, &raftpb.Command{ClientId: 21, Seq: 1, Op: raftpb.Op_GET, Key: []byte("missing")})
	if sameKVFirst.Hash() == sameKVSecond.Hash() {
		t.Fatal("Hash() omitted deduplication state")
	}

	differentIndex := New()
	applyCommand(t, differentIndex, 3, 1, &raftpb.Command{ClientId: 10, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")})
	applyCommand(t, differentIndex, 4, 1, &raftpb.Command{ClientId: 11, Seq: 1, Op: raftpb.Op_GET, Key: []byte("missing")})
	if sameKVFirst.Hash() == differentIndex.Hash() {
		t.Fatal("Hash() omitted the applied index")
	}
}

func TestSnapshotStubs(t *testing.T) {
	sm := New()
	applyCommand(t, sm, 1, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("k"), Value: []byte("v")})
	before := sm.Hash()
	if err := sm.CreateSnapshot(t.TempDir(), raft.SnapshotMeta{Index: 1, Term: 2}); !errors.Is(err, statemachine.ErrSnapshotUnsupported) {
		t.Fatalf("CreateSnapshot() error = %v, want ErrSnapshotUnsupported", err)
	}
	if _, err := sm.RestoreSnapshot(t.TempDir()); !errors.Is(err, statemachine.ErrSnapshotUnsupported) {
		t.Fatalf("RestoreSnapshot() error = %v, want ErrSnapshotUnsupported", err)
	}
	if got := sm.Hash(); got != before {
		t.Fatalf("snapshot stubs mutated state: hash = %x, want %x", got, before)
	}
}

func TestApplyRejectsInvalidEntries(t *testing.T) {
	sm := New()
	if _, err := sm.Apply(nil); err == nil {
		t.Fatal("Apply(nil) error = nil, want error")
	}
	if _, err := sm.Apply(&raftpb.Entry{Index: 1, Type: raftpb.EntryType_NORMAL, Data: []byte("not protobuf")}); err == nil {
		t.Fatal("Apply(malformed command) error = nil, want error")
	}
	if _, err := sm.Apply(&raftpb.Entry{Index: 1, Type: raftpb.EntryType(99)}); err == nil {
		t.Fatal("Apply(unknown entry type) error = nil, want error")
	}
}

func applyCommand(t *testing.T, sm *MapStateMachine, index, _ uint64, command *raftpb.Command) statemachine.Result {
	t.Helper()
	result, err := sm.Apply(commandEntry(t, index, command))
	if err != nil {
		t.Fatalf("Apply(%s): %v", command.GetOp(), err)
	}
	return result
}

func commandEntry(t *testing.T, index uint64, command *raftpb.Command) *raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return &raftpb.Entry{Index: index, Type: raftpb.EntryType_NORMAL, Data: data}
}
