//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"miniquorum/internal/raft"
)

// TestRealLeaderSIGKILLDuringMQCTLLoopLosesNoAckedWrite is the Phase 3
// real-process spot check. A background mqctl put loop runs against a live
// three-node cluster started with --data-dir disklogs; the current leader is
// SIGKILLed while that loop is in flight; the loop must resume acknowledging
// writes against the surviving majority; the killed node restarts from its
// unchanged data directory and rejoins; and every write acknowledged at any
// point — before, during, or after the kill — must still read back. A put in
// flight at the kill that never acknowledged is indeterminate and is
// deliberately not asserted either way.
func TestRealLeaderSIGKILLDuringMQCTLLoopLosesNoAckedWrite(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("repository root: %v", err)
	}
	temp := t.TempDir()
	daemonPath := filepath.Join(temp, "miniquorumd")
	ctlPath := filepath.Join(temp, "mqctl")
	buildSmokeBinary(t, root, daemonPath, "./cmd/miniquorumd")
	buildSmokeBinary(t, root, ctlPath, "./cmd/mqctl")

	peers := map[raft.NodeID]string{
		1: reserveLocalAddress(t),
		2: reserveLocalAddress(t),
		3: reserveLocalAddress(t),
	}
	peerFlag := fmt.Sprintf("1=%s,2=%s,3=%s", peers[1], peers[2], peers[3])

	processes := make(map[raft.NodeID]*smokeProcess, len(peers))
	t.Cleanup(func() {
		for _, id := range []raft.NodeID{1, 2, 3} {
			if process := processes[id]; process != nil {
				process.stop(t)
			}
		}
		if t.Failed() {
			for _, id := range []raft.NodeID{1, 2, 3} {
				if process := processes[id]; process != nil {
					contents, readErr := os.ReadFile(process.logPath)
					if readErr == nil {
						t.Logf("node %d log:\n%s", id, contents)
					}
				}
			}
		}
	})
	for _, id := range []raft.NodeID{1, 2, 3} {
		processes[id] = startSmokeNode(t, daemonPath, temp, id, peerFlag)
	}

	probe := uint64(910_000) // client-id space disjoint from the failover smoke test
	if _, ok := waitForRealLeader(peers, 0, &probe, 20*time.Second); !ok {
		t.Fatal("real cluster did not elect an initial leader")
	}

	type ackedWrite struct{ key, value string }
	var (
		ackedMu sync.Mutex
		acked   []ackedWrite
	)
	ackCount := func() int {
		ackedMu.Lock()
		defer ackedMu.Unlock()
		return len(acked)
	}
	awaitAcks := func(want int, within time.Duration) {
		t.Helper()
		deadline := time.Now().Add(within)
		for ackCount() < want {
			if time.Now().After(deadline) {
				t.Fatalf("only %d acknowledged writes by deadline, want at least %d", ackCount(), want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	loopCtx, stopLoop := context.WithCancel(context.Background())
	defer stopLoop()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		for i := 1; loopCtx.Err() == nil; i++ {
			key := fmt.Sprintf("sigkill-%03d", i)
			value := fmt.Sprintf("value-%03d", i)
			// A failed or interrupted put is indeterminate: it is retried under
			// a fresh key next iteration and never recorded as acknowledged.
			if runMQCTL(loopCtx, ctlPath, peerFlag, "OK", "put", key, value) != nil {
				continue
			}
			ackedMu.Lock()
			acked = append(acked, ackedWrite{key: key, value: value})
			ackedMu.Unlock()
		}
	}()

	awaitAcks(3, 30*time.Second)
	leader, ok := waitForRealLeader(peers, 0, &probe, 10*time.Second)
	if !ok {
		t.Fatal("could not identify the leader while the mqctl loop was running")
	}
	preKill := ackCount()
	t.Logf("SIGKILL leader node %d after %d acknowledged writes", leader, preKill)
	killed := processes[leader]
	if err := killed.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL node %d: %v", leader, err)
	}
	killed.stopped = true
	select {
	case <-killed.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("killed leader %d did not exit", leader)
	}

	// The loop must keep acknowledging against the surviving majority — this
	// both proves post-kill write availability and pins that the kill really
	// landed in the middle of a live acknowledgement stream.
	awaitAcks(preKill+5, 60*time.Second)

	t.Logf("restarting node %d from its original data directory", leader)
	processes[leader] = startSmokeNode(t, daemonPath, temp, leader, peerFlag)
	if !awaitNodeResponds(peers[leader], &probe, 20*time.Second) {
		t.Fatalf("restarted node %d never answered its KV service after recovery", leader)
	}
	awaitAcks(ackCount()+2, 60*time.Second)

	stopLoop()
	select {
	case <-loopDone:
	case <-time.After(60 * time.Second):
		t.Fatal("mqctl put loop did not stop")
	}

	ackedMu.Lock()
	finalAcked := append([]ackedWrite(nil), acked...)
	ackedMu.Unlock()
	if len(finalAcked) < preKill+7 {
		t.Fatalf("acknowledged %d writes, want at least %d", len(finalAcked), preKill+7)
	}
	for _, write := range finalAcked {
		assertMQCTL(t, ctlPath, peerFlag, write.value, "get", write.key)
	}
	t.Logf("all %d acknowledged writes survived the leader SIGKILL and restart", len(finalAcked))
}

// runMQCTL runs one mqctl invocation and reports whether it succeeded with
// exactly the expected output. Unlike assertMQCTL it never fails the test:
// callers in the kill window expect and tolerate failures.
func runMQCTL(ctx context.Context, binary, peers, expected, operation string, args ...string) error {
	commandArgs := []string{operation, "--peers=" + peers, "--timeout=500ms"}
	commandArgs = append(commandArgs, args...)
	callCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	output, err := exec.CommandContext(callCtx, binary, commandArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mqctl %s %v: %w\n%s", operation, args, err, output)
	}
	if got := strings.TrimSpace(string(output)); got != expected {
		return fmt.Errorf("mqctl %s %v output = %q, want %q", operation, args, got, expected)
	}
	return nil
}

// awaitNodeResponds probes one node's KV endpoint until it answers anything
// at all — Ok or NotLeader — which a freshly restarted process can only do
// after its disklog recovery and Raft node construction succeeded.
func awaitNodeResponds(address string, probe *uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		*probe++
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		response, err := executeRealProbe(ctx, address, *probe)
		cancel()
		if err == nil && (response.GetOk() || response.GetNotLeader() != nil) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
