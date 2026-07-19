package node_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/cli"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

const processTestDeadline = 30 * time.Second

func TestThreeProcessesSetThroughCLIAndElectReplacementLeader(t *testing.T) {
	members := make(map[string]config.Member, 3)
	for index := 1; index <= 3; index++ {
		members[fmt.Sprintf("node-%d", index)] = config.Member{
			PeerAddress:   unusedAddress(t),
			ClientAddress: unusedAddress(t),
		}
	}

	processes := make(map[string]*nodeProcess, 3)
	for index := 1; index <= 3; index++ {
		id := fmt.Sprintf("node-%d", index)
		cfg := config.Config{
			Version:            1,
			ClusterID:          "process-test-cluster",
			ActiveSessionLimit: 1,
			Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
			Members:            members,
		}
		processes[id] = startNodeProcess(t, cfg)
	}
	defer func() {
		for _, process := range processes {
			process.stop()
		}
	}()

	first := waitForSingleLeader(t, members, nil, processTestDeadline)
	initialFollower := memberOtherThan(t, members, map[string]bool{first: true})
	sessionClient := client.New(initialFollower.ClientAddress)
	requestCtx, cancel := context.WithTimeout(context.Background(), processTestDeadline)
	sessionID, err := sessionClient.OpenSession(requestCtx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session through Follower: %v", err)
	}
	if sessionID == ([16]byte{}) {
		t.Fatal("OpenSession() returned the zero identity")
	}
	setLeader := waitForStableLeader(t, members, nil, processTestDeadline)
	if output, err := runCLISet(members[setLeader].ClientAddress, sessionID, 1, "opaque", string([]byte{1, 2, 3}), 5*time.Second); err != nil {
		for _, process := range processes {
			process.stop()
		}
		t.Fatalf("SET through CLI: %v\n%s\nNode output: %#v", err, output, nodeProcessOutputs(processes))
	} else if !strings.Contains(output, `"stored":true`) {
		t.Fatalf("SET CLI output = %q, want stored success", output)
	}
	first = waitForSingleLeader(t, members, nil, processTestDeadline)

	processes[first].stop()
	delete(processes, first)

	replacement := waitForSingleLeader(t, members, map[string]bool{first: true}, processTestDeadline)
	if replacement == first {
		t.Fatalf("replacement Leader = stopped Node %q", first)
	}
	replacementFollower := memberOtherThan(t, members, map[string]bool{first: true, replacement: true})
	if output, err := runCLISet(replacementFollower.ClientAddress, sessionID, 2, "empty", "", 5*time.Second); err != nil {
		t.Fatalf("SET empty Value through replacement Leader: %v\n%s", err, output)
	}
	sessionClient = client.New(replacementFollower.ClientAddress)
	requestCtx, cancel = context.WithTimeout(context.Background(), processTestDeadline)
	err = sessionClient.CloseSession(requestCtx, sessionID)
	cancel()
	if err != nil {
		t.Fatalf("close replicated Client Session after Leader change: %v", err)
	}

	requestCtx, cancel = context.WithTimeout(context.Background(), processTestDeadline)
	isolationSession, err := sessionClient.OpenSession(requestCtx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session for Quorum-loss check: %v", err)
	}
	isolate := memberIDForAddress(t, members, replacementFollower.ClientAddress)
	processes[isolate].stop()
	delete(processes, isolate)
	output, err := runCLISet(members[replacement].ClientAddress, isolationSession, 1, "must-not-commit", "value", 1500*time.Millisecond)
	if err == nil || strings.Contains(output, `"stored":true`) {
		t.Fatalf("SET without Quorum output = %q, error = %v; want timeout or Unavailable without success", output, err)
	}

	for id, process := range processes {
		select {
		case err := <-process.done:
			process.stopped = true
			t.Fatalf("surviving Node %q exited early (%v): %s", id, err, process.output.String())
		default:
		}
	}
}

func nodeProcessOutputs(processes map[string]*nodeProcess) map[string]string {
	outputs := make(map[string]string, len(processes))
	for id, process := range processes {
		outputs[id] = process.output.String()
	}
	return outputs
}

func runCLISet(address string, sessionID [16]byte, sequence uint64, key, value string, timeout time.Duration) (string, error) {
	var output bytes.Buffer
	err := cli.Run([]string{
		"--address", address,
		"--timeout", timeout.String(),
		"set", hex.EncodeToString(sessionID[:]), strconv.FormatUint(sequence, 10), key, value,
	}, &output)
	return output.String(), err
}

func memberIDForAddress(t *testing.T, members map[string]config.Member, address string) string {
	t.Helper()
	for id, member := range members {
		if member.ClientAddress == address {
			return id
		}
	}
	t.Fatalf("no Cluster member has client address %q", address)
	return ""
}

func memberOtherThan(t *testing.T, members map[string]config.Member, excluded map[string]bool) config.Member {
	t.Helper()
	for id, member := range members {
		if !excluded[id] {
			return member
		}
	}
	t.Fatal("no eligible Cluster member")
	return config.Member{}
}

func TestNodeProcessHelper(t *testing.T) {
	path := os.Getenv("QUORUMKV_NODE_PROCESS_CONFIG")
	if path == "" {
		return
	}
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := node.New(cfg).Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

type nodeProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
	stopped bool
}

func startNodeProcess(t *testing.T, cfg config.Config) *nodeProcess {
	t.Helper()
	contents, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config for %q: %v", cfg.Node.ID, err)
	}
	path := filepath.Join(t.TempDir(), cfg.Node.ID+".yaml")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write config for %q: %v", cfg.Node.ID, err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test executable: %v", err)
	}
	process := &nodeProcess{done: make(chan error, 1)}
	process.command = exec.Command(executable, "-test.run=^TestNodeProcessHelper$")
	process.command.Env = append(os.Environ(), "QUORUMKV_NODE_PROCESS_CONFIG="+path)
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start Node %q: %v", cfg.Node.ID, err)
	}
	go func() { process.done <- process.command.Wait() }()
	return process
}

func (p *nodeProcess) stop() {
	if p == nil || p.stopped {
		return
	}
	p.stopped = true
	_ = p.command.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
	}
}

func waitForSingleLeader(t *testing.T, members map[string]config.Member, excluded map[string]bool, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var observed map[string]*quorumkvv1.GetStatusResponse
	for time.Now().Before(deadline) {
		observed = make(map[string]*quorumkvv1.GetStatusResponse)
		for id, member := range members {
			if excluded[id] {
				continue
			}
			if status := fetchStatus(member.ClientAddress); status != nil {
				observed[id] = status
			}
		}
		var leader string
		leaders := 0
		for id, status := range observed {
			if status.Role == quorumkvv1.RaftRole_RAFT_ROLE_LEADER {
				leader = id
				leaders++
			}
		}
		if len(observed) == 3-len(excluded) && leaders == 1 {
			allAgree := true
			for _, status := range observed {
				if status.LeaderId != leader {
					allAgree = false
				}
			}
			if allAgree {
				return leader
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	details := make(map[string]string, len(observed))
	for id, state := range observed {
		details[id] = fmt.Sprintf("role=%s leader=%q term=%d", state.Role, state.LeaderId, state.Term)
	}
	t.Fatalf("did not observe one agreed Leader within %v; last status: %#v", timeout, details)
	return ""
}

func waitForStableLeader(t *testing.T, members map[string]config.Member, excluded map[string]bool, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		first := waitForSingleLeader(t, members, excluded, time.Until(deadline))
		time.Sleep(checkQuorumObservationWindow)
		second := waitForSingleLeader(t, members, excluded, time.Until(deadline))
		if first == second {
			return first
		}
	}
	t.Fatalf("did not observe stable Leader within %v", timeout)
	return ""
}

const checkQuorumObservationWindow = 1200 * time.Millisecond

func fetchStatus(address string) *quorumkvv1.GetStatusResponse {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	status, err := quorumkvv1.NewNodeServiceClient(connection).GetStatus(ctx, &quorumkvv1.GetStatusRequest{})
	if err != nil {
		return nil
	}
	return status
}
