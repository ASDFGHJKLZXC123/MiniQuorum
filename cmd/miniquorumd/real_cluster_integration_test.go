//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"miniquorum/internal/raft"
	raftpb "miniquorum/proto"
)

func TestRealThreeProcessMQCTLRoundTripAndLeaderFailover(t *testing.T) {
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

	probe := uint64(900_000)
	if _, ok := waitForRealLeader(peers, 0, &probe, 20*time.Second); !ok {
		t.Fatal("real cluster did not elect an initial leader")
	}

	assertMQCTL(t, ctlPath, peerFlag, "OK", "put", "delete-me", "temporary")
	assertMQCTL(t, ctlPath, peerFlag, "temporary", "get", "delete-me")
	assertMQCTL(t, ctlPath, peerFlag, "OK", "delete", "delete-me")
	assertMQCTL(t, ctlPath, peerFlag, "(not found)", "get", "delete-me")

	// This is the acknowledged write whose survival is checked after killing
	// the exact leader identified immediately afterward through the public KV
	// service.
	assertMQCTL(t, ctlPath, peerFlag, "OK", "put", "failover-key", "survives")
	assertMQCTL(t, ctlPath, peerFlag, "survives", "get", "failover-key")
	leader, ok := waitForRealLeader(peers, 0, &probe, 10*time.Second)
	if !ok {
		t.Fatal("could not identify the leader after the acknowledged write")
	}
	t.Logf("terminating acknowledged-write leader node %d", leader)
	processes[leader].stop(t)

	replacement, ok := waitForRealLeader(peers, leader, &probe, 20*time.Second)
	if !ok || replacement == leader {
		t.Fatalf("replacement leader = %d, ok=%v; killed leader was %d", replacement, ok, leader)
	}
	t.Logf("replacement leader node %d", replacement)
	assertMQCTL(t, ctlPath, peerFlag, "survives", "get", "failover-key")
}

type smokeProcess struct {
	cmd     *exec.Cmd
	done    chan error
	logPath string
	stopped bool
}

func buildSmokeBinary(t *testing.T, root, output, pkg string) {
	t.Helper()
	command := exec.Command("go", "build", "-race", "-o", output, pkg)
	command.Dir = root
	if buildOutput, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, buildOutput)
	}
}

func reserveLocalAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve local address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved address: %v", err)
	}
	return address
}

func startSmokeNode(t *testing.T, daemonPath, temp string, id raft.NodeID, peers string) *smokeProcess {
	t.Helper()
	logPath := filepath.Join(temp, "node-"+strconv.FormatUint(uint64(id), 10)+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create node %d log: %v", id, err)
	}
	command := exec.Command(daemonPath,
		"--id="+strconv.FormatUint(uint64(id), 10),
		"--peers="+peers,
		"--data-dir="+filepath.Join(temp, "node-"+strconv.FormatUint(uint64(id), 10)),
	)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start node %d: %v", id, err)
	}
	done := make(chan error, 1)
	go func() {
		done <- command.Wait()
		_ = logFile.Close()
	}()
	return &smokeProcess{cmd: command, done: done, logPath: logPath}
}

func (p *smokeProcess) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.stopped {
		return
	}
	p.stopped = true
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !strings.Contains(err.Error(), "process already finished") {
		t.Logf("signal process %d: %v", p.cmd.Process.Pid, err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		if err := p.cmd.Process.Kill(); err != nil {
			t.Logf("kill process %d after shutdown timeout: %v", p.cmd.Process.Pid, err)
		}
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Logf("process %d did not exit after kill", p.cmd.Process.Pid)
		}
	}
}

func waitForRealLeader(peers map[raft.NodeID]string, excluded raft.NodeID, probe *uint64, timeout time.Duration) (raft.NodeID, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range []raft.NodeID{1, 2, 3} {
			if id == excluded {
				continue
			}
			*probe++
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			response, err := executeRealProbe(ctx, peers[id], *probe)
			cancel()
			if err == nil && response.GetOk() && response.GetNotLeader() == nil {
				return id, true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return 0, false
}

func executeRealProbe(ctx context.Context, address string, clientID uint64) (*raftpb.ExecuteResponse, error) {
	connection, err := grpcgo.NewClient(address, grpcgo.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = connection.Close() }()
	return raftpb.NewKVClient(connection).Execute(ctx, &raftpb.ExecuteRequest{Cmd: &raftpb.Command{
		ClientId: clientID,
		Seq:      1,
		Op:       raftpb.Op_GET,
		Key:      []byte("leader-probe"),
	}})
}

func assertMQCTL(t *testing.T, binary, peers, expected, operation string, args ...string) {
	t.Helper()
	commandArgs := []string{operation, "--peers=" + peers, "--timeout=500ms"}
	commandArgs = append(commandArgs, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("mqctl %s %v: %v\n%s", operation, args, err, output)
	}
	if got := strings.TrimSpace(string(output)); got != expected {
		t.Fatalf("mqctl %s %v output = %q, want %q", operation, args, got, expected)
	}
}
