package node

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
)

func TestProcessRestartDoesNotGrantSecondVoteInTerm(t *testing.T) {
	directory := t.TempDir()
	first := runVoteProcess(t, directory, "candidate-a")
	if first != "granted" {
		t.Fatalf("first vote = %q, want granted", first)
	}

	second := runVoteProcess(t, directory, "candidate-b")
	if second != "rejected" {
		t.Fatalf("vote after process restart = %q, want rejected", second)
	}
}

func runVoteProcess(t *testing.T, directory, candidate string) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test executable: %v", err)
	}
	command := exec.Command(executable, "-test.run=^TestVoteProcessHelper$")
	command.Env = append(os.Environ(),
		"QUORUMKV_VOTE_HELPER=1",
		"QUORUMKV_DATA_DIR="+directory,
		"QUORUMKV_CANDIDATE="+candidate,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run vote process for %q: %v\n%s", candidate, err, output)
	}
	return strings.TrimSpace(string(output))
}

func TestVoteProcessHelper(t *testing.T) {
	if os.Getenv("QUORUMKV_VOTE_HELPER") != "1" {
		return
	}
	runtime, err := openRaftRuntime(config.Config{
		ClusterID: "cluster-1",
		Node: config.Node{
			ID:      "node-1",
			DataDir: os.Getenv("QUORUMKV_DATA_DIR"),
		},
	}, []raft.NodeID{"candidate-a", "candidate-b"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	actions, err := runtime.step(raft.VoteRequest{
		From: raft.NodeID(os.Getenv("QUORUMKV_CANDIDATE")),
		Term: 7,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for _, action := range actions {
		response, ok := action.(raft.SendVoteResponse)
		if !ok {
			continue
		}
		if response.Response.Granted {
			fmt.Println("granted")
		} else {
			fmt.Println("rejected")
		}
		// os.Exit deliberately skips Close to model abrupt process loss after
		// the response becomes externally visible.
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "vote request produced no response")
	os.Exit(2)
}
