package node_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

func TestThreeProcessesElectReplacementLeader(t *testing.T) {
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

	first := waitForSingleLeader(t, members, nil, 12*time.Second)
	initialFollower := memberOtherThan(t, members, map[string]bool{first: true})
	sessionClient := client.New(initialFollower.ClientAddress)
	requestCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	sessionID, err := sessionClient.OpenSession(requestCtx)
	cancel()
	if err != nil {
		t.Fatalf("open Client Session through Follower: %v", err)
	}
	if sessionID == ([16]byte{}) {
		t.Fatal("OpenSession() returned the zero identity")
	}

	processes[first].stop()
	delete(processes, first)

	replacement := waitForSingleLeader(t, members, map[string]bool{first: true}, 12*time.Second)
	if replacement == first {
		t.Fatalf("replacement Leader = stopped Node %q", first)
	}
	replacementFollower := memberOtherThan(t, members, map[string]bool{first: true, replacement: true})
	sessionClient = client.New(replacementFollower.ClientAddress)
	requestCtx, cancel = context.WithTimeout(context.Background(), 12*time.Second)
	err = sessionClient.CloseSession(requestCtx, sessionID)
	cancel()
	if err != nil {
		t.Fatalf("close replicated Client Session after Leader change: %v", err)
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
	t.Fatalf("did not observe one agreed Leader within %v; last status: %#v", timeout, observed)
	return ""
}

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
