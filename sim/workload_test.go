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
	selectedClient := clients[0].ClientID
	selectedBefore := historyOperationForClient(t, history, selectedClient)
	if err := s.Run(1000); err != nil {
		t.Fatalf("Run(elect leader after timeout): %v", err)
	}
	if err := s.ScheduleWorkloadRetry(selectedClient, s.Now()+1); err != nil {
		t.Fatalf("ScheduleWorkloadRetry: %v", err)
	}
	if err := s.Run(2500); err != nil {
		t.Fatalf("Run(retry): %v", err)
	}
	after := s.WorkloadAttempts()
	if len(after) <= before {
		t.Fatalf("attempt count after explicit retry = %d, want > %d", len(after), before)
	}
	acceptedRetry := false
	for _, attempt := range after[before:] {
		if attempt.AttemptID.ClientID != selectedClient {
			continue
		}
		if attempt.AttemptID.Seq != selectedBefore.Seq {
			t.Fatalf("timeout retry advanced sequence: %#v", attempt.AttemptID)
		}
		if attempt.Status == workloadpkg.SubmitAccepted {
			acceptedRetry = true
		}
	}
	if !acceptedRetry {
		t.Fatalf("no same-sequence retry was accepted: attempts=%#v", after[before:])
	}
	completedHistory := s.WorkloadHistory()
	if len(completedHistory) != len(history) {
		t.Fatalf("retry changed logical history length from %d to %d: %#v", len(history), len(completedHistory), completedHistory)
	}
	selectedAfter := historyOperationForClient(t, completedHistory, selectedClient)
	if selectedAfter.Seq != selectedBefore.Seq || selectedAfter.InvokeTime != selectedBefore.InvokeTime ||
		!reflect.DeepEqual(selectedAfter.Input, selectedBefore.Input) || selectedAfter.ReturnTime == nil {
		t.Fatalf("retry did not complete the original logical record: before=%#v after=%#v", selectedBefore, selectedAfter)
	}
	for _, client := range s.WorkloadClients() {
		if client.ClientID == selectedClient && (client.NextSeq != 2 || client.Completed != 1 || client.OutstandingSeq != 0 || client.TimedOut) {
			t.Fatalf("selected client did not advance after retry completion: %#v", client)
		}
	}
	if !checker.Check(completedHistory) {
		t.Fatal("Porcupine rejected timeout/retry/completion history")
	}
}

func TestRaftAppliedHistoryDistinguishesMissingKeyFromPresentEmptyValue(t *testing.T) {
	s := phase2StateMachineSim(t, 2026071642)
	_, leader := requireLeaderAtHighestTerm(t, s, 3*phase1Window)

	putInput := checker.Input{Op: raftpb.Op_PUT, Key: []byte("empty"), Value: []byte{}}
	putInvoke := s.Now()
	putIndex, putTerm := proposeCommand(t, s, leader, &raftpb.Command{
		ClientId: 7001,
		Seq:      1,
		Op:       putInput.Op,
		Key:      putInput.Key,
		Value:    putInput.Value,
	})
	putResult := awaitAppliedResult(t, s, leader, putIndex, putTerm, 3*phase1Window)
	putReturn := s.Now()
	runFor(t, s, phase1TickInterval)

	presentInput := checker.Input{Op: raftpb.Op_GET, Key: []byte("empty")}
	presentInvoke := s.Now()
	presentIndex, presentTerm := proposeCommand(t, s, leader, &raftpb.Command{
		ClientId: 7002,
		Seq:      1,
		Op:       presentInput.Op,
		Key:      presentInput.Key,
	})
	presentResult := awaitAppliedResult(t, s, leader, presentIndex, presentTerm, 3*phase1Window)
	presentReturn := s.Now()
	runFor(t, s, phase1TickInterval)

	missingInput := checker.Input{Op: raftpb.Op_GET, Key: []byte("missing")}
	missingInvoke := s.Now()
	missingIndex, missingTerm := proposeCommand(t, s, leader, &raftpb.Command{
		ClientId: 7003,
		Seq:      1,
		Op:       missingInput.Op,
		Key:      missingInput.Key,
	})
	missingResult := awaitAppliedResult(t, s, leader, missingIndex, missingTerm, 3*phase1Window)
	missingReturn := s.Now()

	presentOutput := successfulWorkloadOutput(presentResult)
	missingOutput := successfulWorkloadOutput(missingResult)
	if !presentOutput.Found || len(presentOutput.Value) != 0 {
		t.Fatalf("Raft-applied GET(empty) output = %#v, want found empty value", presentOutput)
	}
	if missingOutput.Found || len(missingOutput.Value) != 0 {
		t.Fatalf("Raft-applied GET(missing) output = %#v, want absent", missingOutput)
	}

	history := checker.History{
		completedHistoryOperation(7001, 1, putInvoke, putReturn, putInput, successfulWorkloadOutput(putResult)),
		completedHistoryOperation(7002, 1, presentInvoke, presentReturn, presentInput, presentOutput),
		completedHistoryOperation(7003, 1, missingInvoke, missingReturn, missingInput, missingOutput),
	}
	if !checker.Check(history) {
		t.Fatalf("checker rejected actual Raft-applied empty/missing history: %#v", history)
	}

	presentConflatedWithMissing := append(checker.History(nil), history...)
	presentConflatedWithMissing[1].Output.Found = false
	if checker.Check(presentConflatedWithMissing) {
		t.Fatal("checker accepted a present empty value reported as missing")
	}
	missingConflatedWithEmpty := append(checker.History(nil), history...)
	missingConflatedWithEmpty[2].Output.Found = true
	if checker.Check(missingConflatedWithEmpty) {
		t.Fatal("checker accepted a missing key reported as a present empty value")
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
	firstAttempts := make(map[logicalKey]workloadpkg.AttemptRecord)
	retries := 0
	for _, attempt := range attempts {
		key := logicalKey{clientID: attempt.AttemptID.ClientID, seq: attempt.AttemptID.Seq}
		first, ok := firstAttempts[key]
		if !ok {
			firstAttempts[key] = attempt
			continue
		}
		retries++
		if attempt.AttemptID.Attempt <= first.AttemptID.Attempt {
			t.Fatalf("logical operation %+v retry attempt = %d, want > first attempt %d", key, attempt.AttemptID.Attempt, first.AttemptID.Attempt)
		}
		if first.Command.GetClientId() != attempt.Command.GetClientId() || first.Command.GetSeq() != attempt.Command.GetSeq() || first.Command.GetOp() != attempt.Command.GetOp() || !bytes.Equal(first.Command.GetKey(), attempt.Command.GetKey()) || !bytes.Equal(first.Command.GetValue(), attempt.Command.GetValue()) {
			t.Fatalf("logical operation %+v changed across retry: first=%+v retry=%+v", key, first.Command, attempt.Command)
		}
	}
	if retries == 0 {
		t.Fatal("retry identity assertion was vacuous: workload produced no repeated logical operation")
	}
}

func historyOperationForClient(t *testing.T, history checker.History, clientID uint64) checker.Operation {
	t.Helper()
	var matches []checker.Operation
	for _, operation := range history {
		if operation.ClientID == clientID {
			matches = append(matches, operation)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("client %d history records = %d, want exactly one: %#v", clientID, len(matches), matches)
	}
	return matches[0]
}

func completedHistoryOperation(clientID, seq uint64, invoke, returnAt VirtualTime, input checker.Input, output checker.Output) checker.Operation {
	returnTime := int64(returnAt)
	return checker.Operation{
		ClientID:   clientID,
		Seq:        seq,
		InvokeTime: int64(invoke),
		ReturnTime: &returnTime,
		Input:      input,
		Output:     output,
	}
}
