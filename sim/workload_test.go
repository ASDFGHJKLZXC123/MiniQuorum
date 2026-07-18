package sim

import (
	"bytes"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"miniquorum/checker"
	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
	workloadpkg "miniquorum/sim/workload"
)

func TestSimWorkloadCompletesThroughRaftAndProducesLinearizableHistory(t *testing.T) {
	s := newWorkloadSim(t, 20260716, 6)
	if err := s.Run(6000); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !s.WorkloadDone() {
		t.Fatalf("workload did not finish: clients=%#v", s.WorkloadClients())
	}
	history := s.WorkloadHistory()
	if len(history) != workloadpkg.DefaultClientCount*6 {
		t.Fatalf("history length = %d, want %d", len(history), workloadpkg.DefaultClientCount*6)
	}
	if !checker.Check(history) {
		t.Fatal("sim workload history is not linearizable")
	}
	assertStableSequentialSessions(t, history, 6)
	assertInitialClientInterleaving(t, history)
	assertAllKVOperationsWentThroughRaft(t, s)
	assertRetryCommandsKeepIdentity(t, s.WorkloadAttempts())
}

func TestSimWorkloadSameSeedIsDeterministic(t *testing.T) {
	run := func() (checker.History, []workloadpkg.AttemptRecord, []string) {
		s := newWorkloadSim(t, 424244, 4)
		if err := s.Run(5000); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !s.WorkloadDone() {
			t.Fatalf("workload did not finish: clients=%#v", s.WorkloadClients())
		}
		return s.WorkloadHistory(), s.WorkloadAttempts(), s.Trace()
	}
	firstHistory, firstAttempts, firstTrace := run()
	secondHistory, secondAttempts, secondTrace := run()
	if !reflect.DeepEqual(firstHistory, secondHistory) {
		t.Fatalf("same-seed histories differ:\nfirst=%#v\nsecond=%#v", firstHistory, secondHistory)
	}
	if !reflect.DeepEqual(firstAttempts, secondAttempts) {
		t.Fatalf("same-seed attempts differ:\nfirst=%#v\nsecond=%#v", firstAttempts, secondAttempts)
	}
	if !reflect.DeepEqual(firstTrace, secondTrace) {
		t.Fatal("same-seed workload traces differ")
	}
}

func TestSimWorkloadTimeoutRemainsOpenAndRetryKeepsSequence(t *testing.T) {
	s, err := NewSim(Config{Seed: 88, NodeIDs: []raft.NodeID{1, 2, 3}})
	if err != nil {
		t.Fatalf("NewSim: %v", err)
	}
	if err := s.StartWorkload(workloadpkg.Config{
		ClientCount:         2,
		OperationsPerClient: 1,
		OperationTimeout:    100,
		RetryDelay:          10,
	}); err != nil {
		t.Fatalf("StartWorkload: %v", err)
	}
	if err := s.Run(100); err != nil {
		t.Fatalf("Run(timeout): %v", err)
	}
	history := s.WorkloadHistory()
	if len(history) != 2 {
		t.Fatalf("history length = %d, want 2", len(history))
	}
	for _, operation := range history {
		if operation.ReturnTime != nil {
			t.Fatalf("timed-out operation recorded completion: %#v", operation)
		}
	}
	if !checker.Check(history) {
		t.Fatal("Porcupine rejected logically open timeout history")
	}
	clients := s.WorkloadClients()
	for _, client := range clients {
		if client.OutstandingSeq != 1 || !client.TimedOut || client.NextSeq != 1 {
			t.Fatalf("client after timeout = %#v, want seq 1 still outstanding", client)
		}
	}
	before := len(s.WorkloadAttempts())
	if err := s.ScheduleWorkloadRetry(clients[0].ClientID, 101); err != nil {
		t.Fatalf("ScheduleWorkloadRetry: %v", err)
	}
	if err := s.Run(150); err != nil {
		t.Fatalf("Run(retry): %v", err)
	}
	after := s.WorkloadAttempts()
	if len(after) <= before {
		t.Fatalf("attempt count after explicit retry = %d, want > %d", len(after), before)
	}
	for _, attempt := range after[before:] {
		if attempt.AttemptID.ClientID == clients[0].ClientID && attempt.AttemptID.Seq != 1 {
			t.Fatalf("timeout retry advanced sequence: %#v", attempt.AttemptID)
		}
	}
	if got := s.WorkloadHistory(); len(got) != 2 || got[0].ReturnTime != nil {
		t.Fatalf("retry created or completed a logical record unexpectedly: %#v", got)
	}
}

func TestSingleNodeWorkloadRegistersBeforeSynchronousApply(t *testing.T) {
	s, err := NewSim(Config{
		Seed:            9,
		NodeIDs:         []raft.NodeID{1},
		ElectionTickMin: 2,
		ElectionTickMax: 3,
	})
	if err != nil {
		t.Fatalf("NewSim: %v", err)
	}
	if err := s.StartWorkload(workloadpkg.Config{
		ClientCount:         1,
		OperationsPerClient: 1,
		OperationTimeout:    500,
		StartTime:           200,
	}); err != nil {
		t.Fatalf("StartWorkload: %v", err)
	}
	if err := s.Run(500); err != nil {
		t.Fatalf("Run: %v", err)
	}
	history := s.WorkloadHistory()
	if len(history) != 1 || history[0].ReturnTime == nil {
		t.Fatalf("single-node synchronous apply was missed: %#v", history)
	}
}

func newWorkloadSim(t *testing.T, seed int64, operationsPerClient int) *Sim {
	t.Helper()
	s, err := NewSim(Config{Seed: seed, NodeIDs: []raft.NodeID{1, 2, 3}})
	if err != nil {
		t.Fatalf("NewSim: %v", err)
	}
	s.RegisterInvariant(SingleLeaderPerTerm)
	if err := s.StartWorkload(workloadpkg.Config{
		ClientCount:         workloadpkg.DefaultClientCount,
		KeyCount:            workloadpkg.DefaultKeyCount,
		OperationsPerClient: operationsPerClient,
		OperationTimeout:    3000,
		RetryDelay:          10,
		ThinkTime:           1,
	}); err != nil {
		t.Fatalf("StartWorkload: %v", err)
	}
	return s
}

func assertStableSequentialSessions(t *testing.T, history checker.History, operationsPerClient int) {
	t.Helper()
	byClient := make(map[uint64][]checker.Operation)
	for _, operation := range history {
		byClient[operation.ClientID] = append(byClient[operation.ClientID], operation)
	}
	if len(byClient) != workloadpkg.DefaultClientCount {
		t.Fatalf("client count = %d, want %d", len(byClient), workloadpkg.DefaultClientCount)
	}
	for clientID := uint64(1); clientID <= workloadpkg.DefaultClientCount; clientID++ {
		operations := byClient[clientID]
		if len(operations) != operationsPerClient {
			t.Fatalf("client %d operations = %d, want %d", clientID, len(operations), operationsPerClient)
		}
		for i, operation := range operations {
			wantSeq := uint64(i + 1)
			if operation.Seq != wantSeq || operation.ReturnTime == nil {
				t.Fatalf("client %d operation %d = %#v, want completed seq %d", clientID, i, operation, wantSeq)
			}
			if i > 0 && operation.InvokeTime <= *operations[i-1].ReturnTime {
				t.Fatalf("client %d overlaps its own operations: previous=%#v current=%#v", clientID, operations[i-1], operation)
			}
		}
	}
}

func assertInitialClientInterleaving(t *testing.T, history checker.History) {
	t.Helper()
	initial := make(map[uint64]checker.Operation)
	for _, operation := range history {
		if operation.Seq == 1 {
			initial[operation.ClientID] = operation
		}
	}
	if len(initial) < 2 {
		t.Fatalf("initial operations = %#v, want at least two clients", initial)
	}
	var first *checker.Operation
	for clientID := uint64(1); clientID <= workloadpkg.DefaultClientCount; clientID++ {
		operation, ok := initial[clientID]
		if !ok {
			continue
		}
		if first == nil {
			copy := operation
			first = &copy
			continue
		}
		if first.ReturnTime != nil && operation.ReturnTime != nil && first.InvokeTime <= *operation.ReturnTime && operation.InvokeTime <= *first.ReturnTime {
			return
		}
	}
	t.Fatalf("initial operations did not overlap: %#v", initial)
}

func assertAllKVOperationsWentThroughRaft(t *testing.T, s *Sim) {
	t.Helper()
	seen := make(map[raftpb.Op]bool)
	for _, id := range s.order {
		entries := s.AppliedEntries(id)
		for i := range entries {
			entry := &entries[i]
			if entry.GetType() != raftpb.EntryType_NORMAL {
				continue
			}
			var command raftpb.Command
			if err := proto.Unmarshal(entry.GetData(), &command); err != nil {
				continue
			}
			seen[command.GetOp()] = true
		}
	}
	for _, op := range []raftpb.Op{raftpb.Op_PUT, raftpb.Op_GET, raftpb.Op_DELETE} {
		if !seen[op] {
			t.Fatalf("no applied %s command found; GET/PUT/DELETE must all traverse the Raft log", op)
		}
	}
}

func assertRetryCommandsKeepIdentity(t *testing.T, attempts []workloadpkg.AttemptRecord) {
	t.Helper()
	type logicalKey struct {
		clientID uint64
		seq      uint64
	}
	commands := make(map[logicalKey]*raftpb.Command)
	for _, attempt := range attempts {
		key := logicalKey{clientID: attempt.AttemptID.ClientID, seq: attempt.AttemptID.Seq}
		first, ok := commands[key]
		if !ok {
			commands[key] = attempt.Command
			continue
		}
		if first.GetClientId() != attempt.Command.GetClientId() || first.GetSeq() != attempt.Command.GetSeq() || first.GetOp() != attempt.Command.GetOp() || !bytes.Equal(first.GetKey(), attempt.Command.GetKey()) || !bytes.Equal(first.GetValue(), attempt.Command.GetValue()) {
			t.Fatalf("logical operation %+v changed across retry: first=%+v retry=%+v", key, first, attempt.Command)
		}
	}
}
