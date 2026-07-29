package cli_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Het-Jethva/quorumkv/internal/cli"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/node"
)

const clusterDeadline = 30 * time.Second

// TestRunRejectsMalformedArgumentsWithoutACluster covers the argument handling
// that must fail before any Node is contacted, so a mistyped command reports
// the mistake instead of a connection timeout.
func TestRunRejectsMalformedArgumentsWithoutACluster(t *testing.T) {
	t.Parallel()
	session := strings.Repeat("ab", 16)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "no command", args: nil, want: "usage:"},
		{name: "unknown command", args: []string{"compact"}, want: "usage:"},
		{name: "status with extra argument", args: []string{"status", "extra"}, want: "usage:"},
		{name: "session without subcommand", args: []string{"session"}, want: "usage:"},
		{name: "session unknown subcommand", args: []string{"session", "restart"}, want: "usage:"},
		{name: "session close without identity", args: []string{"session", "close"}, want: "usage:"},
		{name: "session close short identity", args: []string{"session", "close", "abcd"}, want: "32 hexadecimal"},
		{name: "session close non-hex identity", args: []string{"session", "close", strings.Repeat("zz", 16)}, want: "32 hexadecimal"},
		{name: "set with too few arguments", args: []string{"set", session, "1", "key"}, want: "usage:"},
		{name: "set with bad identity", args: []string{"set", "beef", "1", "key", "value"}, want: "32 hexadecimal"},
		{name: "set with zero sequence", args: []string{"set", session, "0", "key", "value"}, want: "positive base-10"},
		{name: "set with negative sequence", args: []string{"set", session, "-1", "key", "value"}, want: "positive base-10"},
		{name: "set with non-numeric sequence", args: []string{"set", session, "first", "key", "value"}, want: "positive base-10"},
		{name: "get without a Key", args: []string{"get"}, want: "usage:"},
		{name: "get with extra argument", args: []string{"get", "key", "extra"}, want: "usage:"},
		{name: "delete with too few arguments", args: []string{"delete", session, "1"}, want: "usage:"},
		{name: "delete with zero sequence", args: []string{"delete", session, "0", "key"}, want: "positive base-10"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			args := append([]string{"-address", "127.0.0.1:1", "-timeout", "100ms"}, test.args...)
			err := cli.Run(args, &output)
			if err == nil {
				t.Fatalf("Run(%q) succeeded, want an argument error", test.args)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run(%q) error = %v, want it to mention %q", test.args, err, test.want)
			}
			if output.Len() != 0 {
				t.Fatalf("Run(%q) wrote %q on an argument error, want nothing", test.args, output.String())
			}
		})
	}
}

// TestRunDrivesAClusterThroughEveryCommand exercises the whole command surface
// against a real three-Node Cluster. Every command is issued to a Follower, so
// the typed Leader redirect is on the path for all of them: this is the
// binary an operator actually runs, and until now nothing tested it.
func TestRunDrivesAClusterThroughEveryCommand(t *testing.T) {
	members := startCluster(t)
	leader := waitForLeaderAddress(t, members)
	follower := followerAddress(t, members, leader)

	var status struct {
		ClusterID string `json:"cluster_id"`
		NodeID    string `json:"node_id"`
		State     string `json:"state"`
		Role      string `json:"role"`
		LeaderID  string `json:"leader_id"`
		Term      uint64 `json:"term"`
	}
	decodeCommand(t, follower, &status, "status")
	if status.ClusterID != "cli-test-cluster" || status.State != "NODE_STATE_READY" {
		t.Fatalf("status = %+v, want the configured ready Cluster", status)
	}
	if status.LeaderID == "" || status.Term == 0 {
		t.Fatalf("status = %+v, want an observed Leader and Term", status)
	}

	var opened struct {
		SessionID string `json:"session_id"`
	}
	decodeCommand(t, follower, &opened, "session", "open")
	if _, err := hex.DecodeString(opened.SessionID); err != nil || len(opened.SessionID) != 32 {
		t.Fatalf("session open returned %q, want 32 hexadecimal characters", opened.SessionID)
	}

	var stored struct {
		Key      string `json:"key"`
		Sequence uint64 `json:"sequence"`
		Stored   bool   `json:"stored"`
	}
	decodeCommand(t, follower, &stored, "set", opened.SessionID, "1", "greeting", "hello")
	if !stored.Stored || stored.Key != "greeting" || stored.Sequence != 1 {
		t.Fatalf("set = %+v, want a stored greeting at sequence 1", stored)
	}

	var fetched struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}
	decodeCommand(t, follower, &fetched, "get", "greeting")
	if !fetched.Found || string(fetched.Value) != "hello" {
		t.Fatalf("get = %+v, want the Value written through the redirect", fetched)
	}

	// Retrying the same sequence is one logical mutation, so it reports the
	// cached result rather than storing a second time.
	decodeCommand(t, follower, &stored, "set", opened.SessionID, "1", "greeting", "hello")
	if !stored.Stored {
		t.Fatalf("retried set = %+v, want the cached success", stored)
	}

	var removed struct {
		Key     string `json:"key"`
		Existed bool   `json:"existed"`
	}
	decodeCommand(t, follower, &removed, "delete", opened.SessionID, "2", "greeting")
	if !removed.Existed {
		t.Fatalf("delete = %+v, want existed for a Key that was present", removed)
	}
	decodeCommand(t, follower, &removed, "delete", opened.SessionID, "3", "greeting")
	if removed.Existed {
		t.Fatalf("second delete = %+v, want existed=false", removed)
	}

	if _, err := runCommand(follower, "get", "greeting"); err == nil {
		t.Fatal("get after delete succeeded, want a missing-Key error")
	}

	var closed struct {
		Closed bool `json:"closed"`
	}
	decodeCommand(t, follower, &closed, "session", "close", opened.SessionID)
	if !closed.Closed {
		t.Fatalf("session close = %+v, want closed", closed)
	}
	if _, err := runCommand(follower, "set", opened.SessionID, "4", "greeting", "again"); err == nil {
		t.Fatal("set through a closed Client Session succeeded, want a rejection")
	}
}

func runCommand(address string, args ...string) (string, error) {
	var output bytes.Buffer
	full := append([]string{"-address", address, "-timeout", "15s"}, args...)
	err := cli.Run(full, &output)
	return output.String(), err
}

func decodeCommand(t *testing.T, address string, into any, args ...string) {
	t.Helper()
	output, err := runCommand(address, args...)
	if err != nil {
		t.Fatalf("run %q: %v\noutput: %s", args, err, output)
	}
	if err := json.Unmarshal([]byte(output), into); err != nil {
		t.Fatalf("decode %q output %q: %v", args, output, err)
	}
}

func startCluster(t *testing.T) map[string]config.Member {
	t.Helper()
	members := make(map[string]config.Member, 3)
	for index := 1; index <= 3; index++ {
		members[fmt.Sprintf("node-%d", index)] = config.Member{
			PeerAddress:   unusedAddress(t),
			ClientAddress: unusedAddress(t),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var running sync.WaitGroup
	for id := range members {
		cfg := config.Config{
			Version:            1,
			ClusterID:          "cli-test-cluster",
			ActiveSessionLimit: 8,
			Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
			Members:            members,
		}
		running.Add(1)
		go func() {
			defer running.Done()
			if err := node.New(cfg).Run(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("run Node %q: %v", cfg.Node.ID, err)
			}
		}()
	}
	t.Cleanup(func() {
		cancel()
		running.Wait()
	})
	return members
}

// waitForLeaderAddress polls through the status command itself, so the wait is
// also a check that status reports something usable during an election.
func waitForLeaderAddress(t *testing.T, members map[string]config.Member) string {
	t.Helper()
	deadline := time.Now().Add(clusterDeadline)
	for time.Now().Before(deadline) {
		for _, member := range members {
			var status struct {
				Role string `json:"role"`
			}
			output, err := runCommand(member.ClientAddress, "status")
			if err != nil || json.Unmarshal([]byte(output), &status) != nil {
				continue
			}
			if status.Role == "RAFT_ROLE_LEADER" {
				return member.ClientAddress
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no Leader reported through the status command within %v", clusterDeadline)
	return ""
}

func followerAddress(t *testing.T, members map[string]config.Member, leader string) string {
	t.Helper()
	for _, member := range members {
		if member.ClientAddress != leader {
			return member.ClientAddress
		}
	}
	t.Fatal("no Follower in the configured member map")
	return ""
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve an unused address: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}
