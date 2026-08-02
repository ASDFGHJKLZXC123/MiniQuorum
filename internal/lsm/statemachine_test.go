package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/internal/statemachine"
	"miniquorum/internal/statemachine/mapsm"
	raftpb "miniquorum/proto"
)

func TestStateMachineMatchesMapOnRandomizedCommands(t *testing.T) {
	const (
		seeds      = 24
		operations = 120
		clients    = 6
		keys       = 11
	)
	for seed := int64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			fs := NewSimFS()
			lsmStateMachine, err := OpenStateMachine("/property", Options{
				FS:             fs,
				Rand:           newSeededRand(10_000 + seed),
				FlushThreshold: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := lsmStateMachine.Close(); err != nil {
					t.Fatalf("Close(): %v", err)
				}
			}()

			mapStateMachine := mapsm.New()
			random := rand.New(rand.NewSource(seed))
			nextSeq := make([]uint64, clients)
			lastSeq := make([]uint64, clients)
			index := uint64(0)

			for operation := 0; operation < operations; operation++ {
				index++
				if random.Intn(9) == 0 {
					entry := &raftpb.Entry{Index: index, Type: raftpb.EntryType_NOOP}
					assertSameApply(t, mapStateMachine, lsmStateMachine, entry)
					assertSameHash(t, mapStateMachine, lsmStateMachine)
					continue
				}

				client := random.Intn(clients)
				seq := uint64(0)
				if lastSeq[client] != 0 && random.Intn(4) == 0 {
					seq = lastSeq[client]
				} else {
					nextSeq[client]++
					seq = nextSeq[client]
					lastSeq[client] = seq
				}
				command := &raftpb.Command{
					ClientId: uint64(client + 1),
					Seq:      seq,
					Key:      []byte(fmt.Sprintf("key-%02d", random.Intn(keys))),
				}
				switch random.Intn(3) {
				case 0:
					command.Op = raftpb.Op_PUT
					command.Value = []byte(fmt.Sprintf("value-%d-%d", seed, random.Intn(1000)))
				case 1:
					command.Op = raftpb.Op_GET
				case 2:
					command.Op = raftpb.Op_DELETE
				}
				assertSameApply(t, mapStateMachine, lsmStateMachine, stateMachineEntry(t, index, command))
				assertSameHash(t, mapStateMachine, lsmStateMachine)
			}

			for key := 0; key < keys; key++ {
				name := []byte(fmt.Sprintf("key-%02d", key))
				mapResult, mapErr := mapStateMachine.Read(name)
				lsmResult, lsmErr := lsmStateMachine.Read(name)
				if mapErr != nil || lsmErr != nil || !reflect.DeepEqual(mapResult, lsmResult) {
					t.Fatalf("Read(%q): map=(%#v,%v) lsm=(%#v,%v)", name, mapResult, mapErr, lsmResult, lsmErr)
				}
			}
		})
	}
}

func TestStateMachineFullLogRecoveryKeepsHistoricalGet(t *testing.T) {
	fs := NewSimFS()
	const dir = "/historical"
	first, err := OpenStateMachine(dir, Options{FS: fs, Rand: newSeededRand(20_001), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}

	oldGet := stateMachineEntry(t, 1, &raftpb.Command{ClientId: 7, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")})
	if got, err := first.Apply(oldGet); err != nil || got.Found || got.Value != nil {
		t.Fatalf("old GET = (%#v,%v), want not found", got, err)
	}
	putOld := stateMachineEntry(t, 2, &raftpb.Command{ClientId: 8, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("durable-old")})
	if _, err := first.Apply(putOld); err != nil {
		t.Fatal(err)
	}
	if err := first.Engine().ForceFlush(); err != nil {
		t.Fatalf("ForceFlush(): %v", err)
	}
	if got := first.Engine().FlushedIndex(); got != 2 {
		t.Fatalf("FlushedIndex() = %d, want 2", got)
	}
	putNew := stateMachineEntry(t, 3, &raftpb.Command{ClientId: 9, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("replay-must-restore")})
	if _, err := first.Apply(putNew); err != nil {
		t.Fatal(err)
	}
	beforeCrashHash := first.Hash()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Crash(RetainAllUnsynced); err != nil {
		t.Fatal(err)
	}
	if err := fs.Recover(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenStateMachine(dir, Options{FS: fs, Rand: newSeededRand(20_002), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.BeginReplay(1, 3); err != nil {
		t.Fatalf("BeginReplay(): %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("Close recovered: %v", err)
		}
	}()
	// This proves the negative control is load-bearing: normal direct reads
	// already see the later durable value. Routing the replayed GET below
	// through Engine.Read would therefore make this test fail immediately.
	if latest, err := recovered.Read([]byte("key")); err != nil || !latest.Found || !bytes.Equal(latest.Value, []byte("durable-old")) {
		t.Fatalf("normal recovered Read(key) = (%#v,%v), want durable later value", latest, err)
	}

	// Replay starts at zero, not at FlushedIndex. The old GET must observe the
	// initially empty historical view, never the already-open durable table.
	if got, err := recovered.Apply(oldGet); err != nil || got.Found || got.Value != nil {
		t.Fatalf("replayed old GET = (%#v,%v), want original not-found", got, err)
	}
	if _, err := recovered.Apply(putOld); err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Apply(putNew); err != nil {
		t.Fatal(err)
	}
	if got := recovered.Hash(); got != beforeCrashHash {
		t.Fatalf("recovered Hash() = %x, want pre-crash %x", got, beforeCrashHash)
	}
	if got, err := recovered.Read([]byte("key")); err != nil || !got.Found || !bytes.Equal(got.Value, []byte("replay-must-restore")) {
		t.Fatalf("Read(key) after replay = (%#v,%v), want replay-restored value", got, err)
	}

	// The retry is a new Raft entry but the same client sequence. It must
	// return precisely the cached historical not-found result.
	retryOldGet := stateMachineEntry(t, 4, &raftpb.Command{ClientId: 7, Seq: 1, Op: raftpb.Op_GET, Key: []byte("key")})
	if got, err := recovered.Apply(retryOldGet); err != nil || got.Found || got.Value != nil {
		t.Fatalf("retry old GET = (%#v,%v), want cached not-found", got, err)
	}
	if names, err := fs.List(dir); err != nil || len(names) != 2 {
		t.Fatalf("recovery files = %v, %v; want MANIFEST and one referenced SSTable", names, err)
	}
}

func TestStateMachineReplayLifecycleRejectsPreSnapshotPartialLog(t *testing.T) {
	stateMachine, err := OpenStateMachine("/replay-lifecycle", Options{FS: NewSimFS(), Rand: newSeededRand(20_003)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stateMachine.Close() }()
	if err := stateMachine.BeginReplay(2, 4); err == nil {
		t.Fatal("BeginReplay(2,4) error = nil, want pre-snapshot partial-replay rejection")
	}
}

func TestStateMachineConcurrentApplyReadHashAndIterators(t *testing.T) {
	stateMachine, err := OpenStateMachine("/concurrent", Options{FS: NewSimFS(), Rand: newSeededRand(30_001), FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stateMachine.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	}()

	const (
		writes     = 400
		checkpoint = writes / 2
		readersN   = 4
	)
	start := make(chan struct{})
	done := make(chan struct{})
	checkpointReached := make(chan struct{})
	checkpointWitnessed := make(chan struct{})
	witnessClaim := make(chan struct{}, 1)
	witnessClaim <- struct{}{}
	errs := make(chan error, 16)
	var readers sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(readersN)
	for range readersN {
		readers.Add(1)
		go func() {
			defer readers.Done()
			ready.Done()
			<-start
			select {
			case <-checkpointReached:
				select {
				case <-witnessClaim:
					// The writer is blocked below until this reader completes a
					// Read, Hash, and non-empty ordered iterator snapshot.
					result, err := stateMachine.Read([]byte("key-000"))
					if err != nil || !result.Found {
						errs <- fmt.Errorf("checkpoint Read(key-000) = (%#v,%v), want found", result, err)
						return
					}
					tail, err := stateMachine.Read([]byte("key-399"))
					if err != nil || tail.Found {
						errs <- fmt.Errorf("checkpoint Read(key-399) = (%#v,%v), want not found before remaining Applies", tail, err)
						return
					}
					_ = stateMachine.Hash()
					entries := stateMachine.Engine().Entries()
					if len(entries) != checkpoint {
						errs <- fmt.Errorf("checkpoint iterator entries = %d, want %d", len(entries), checkpoint)
						return
					}
					for index := 1; index < len(entries); index++ {
						if bytes.Compare(entries[index-1].Key, entries[index].Key) >= 0 {
							errs <- fmt.Errorf("checkpoint iterator unordered keys %q then %q", entries[index-1].Key, entries[index].Key)
							return
						}
					}
					close(checkpointWitnessed)
				default:
				}
			case <-done:
				return
			}
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := stateMachine.Read([]byte("key-000")); err != nil {
					errs <- err
					return
				}
				_ = stateMachine.Hash()
				entries := stateMachine.Engine().Entries()
				for index := 1; index < len(entries); index++ {
					if bytes.Compare(entries[index-1].Key, entries[index].Key) >= 0 {
						errs <- fmt.Errorf("iterator returned unordered keys %q then %q", entries[index-1].Key, entries[index].Key)
						return
					}
				}
			}
		}()
	}

	ready.Wait()
	close(start)
	for index := 1; index <= writes; index++ {
		command := &raftpb.Command{ClientId: uint64(index), Seq: 1, Op: raftpb.Op_PUT, Key: []byte(fmt.Sprintf("key-%03d", index-1)), Value: []byte(fmt.Sprintf("value-%d", index))}
		if _, err := stateMachine.Apply(stateMachineEntry(t, uint64(index), command)); err != nil {
			errs <- err
			break
		}
		if index == checkpoint {
			close(checkpointReached)
			<-checkpointWitnessed
		}
	}
	close(done)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestStateMachineReturnsFlushFailureToHost(t *testing.T) {
	fs := NewSimFS()
	stateMachine, err := OpenStateMachine("/apply-error", Options{FS: fs, Rand: newSeededRand(40_001), FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stateMachine.Close() }()

	want := errors.New("injected create failure")
	if err := fs.SetErrorSchedule([]SimErrorDirective{{Op: FSOpCreate, Occurrence: 1, Point: SimBeforeOperation, Err: want}}); err != nil {
		t.Fatal(err)
	}
	entry := stateMachineEntry(t, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("value")})
	if _, err := stateMachine.Apply(entry); !errors.Is(err, want) {
		t.Fatalf("Apply() error = %v, want injected filesystem error %v", err, want)
	}
	if got := stateMachine.AppliedIndex(); got != 0 {
		t.Fatalf("AppliedIndex() = %d after failed Apply, want 0", got)
	}
	if got, wantHash := stateMachine.Hash(), mapsm.New().Hash(); got != wantHash {
		t.Fatalf("failed Apply published logical dedup/KV state: Hash() = %x, want empty %x", got, wantHash)
	}
}

func TestStateMachineCrashDoesNotGracefullyCleanUpSimFS(t *testing.T) {
	fs := NewSimFS()
	stateMachine, err := OpenStateMachine("/abandon", Options{FS: fs, Rand: newSeededRand(40_002), FlushThreshold: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateMachine.Apply(stateMachineEntry(t, 1, &raftpb.Command{ClientId: 1, Seq: 1, Op: raftpb.Op_PUT, Key: []byte("key"), Value: []byte("value")})); err != nil {
		t.Fatal(err)
	}
	fs.ResetEvents()
	if err := stateMachine.Crash(); err != nil {
		t.Fatal(err)
	}
	if events := fs.Events(); len(events) != 0 {
		t.Fatalf("crash performed filesystem work instead of abandoning process: %#v", events)
	}
	if !fs.Crashed() {
		t.Fatal("Crash left SimFS live")
	}
}

func assertSameApply(t *testing.T, mapStateMachine statemachine.StateMachine, lsmStateMachine statemachine.StateMachine, entry *raftpb.Entry) {
	t.Helper()
	before := proto.Clone(entry).(*raftpb.Entry)
	mapResult, mapErr := mapStateMachine.Apply(entry)
	lsmResult, lsmErr := lsmStateMachine.Apply(entry)
	if mapErr != nil || lsmErr != nil || !reflect.DeepEqual(mapResult, lsmResult) {
		t.Fatalf("Apply(%+v): map=(%#v,%v) lsm=(%#v,%v)", entry, mapResult, mapErr, lsmResult, lsmErr)
	}
	if !proto.Equal(entry, before) {
		t.Fatalf("Apply mutated entry: got %#v want %#v", entry, before)
	}
}

func assertSameHash(t *testing.T, mapStateMachine statemachine.StateMachine, lsmStateMachine statemachine.StateMachine) {
	t.Helper()
	if got, want := lsmStateMachine.Hash(), mapStateMachine.Hash(); got != want {
		t.Fatalf("Hash() LSM=%x map=%x", got, want)
	}
}

func stateMachineEntry(t *testing.T, index uint64, command *raftpb.Command) *raftpb.Entry {
	t.Helper()
	data, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return &raftpb.Entry{Index: index, Type: raftpb.EntryType_NORMAL, Data: data}
}
