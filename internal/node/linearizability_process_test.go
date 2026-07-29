package node_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/linearizability"
	"google.golang.org/grpc/status"
)

const (
	// The workload is deliberately small. A history the Cluster can actually
	// produce is matched in about one search step per operation, so a few
	// hundred operations check in milliseconds; see the linearizability
	// package for why the opposite outcome is the expensive one.
	linearizabilityWriters      = 3
	linearizabilityReaders      = 2
	linearizabilityOpsPerWorker = 25
	linearizabilityKeys         = 3

	// Several writer Sessions share every Key, so conflicting concurrent
	// mutations are the common case rather than an edge case.
	linearizabilityOperationPause = 15 * time.Millisecond
	linearizabilityOperationLimit = 10 * time.Second
	linearizabilityRetryLimit     = 5

	// The Leader is killed once this many operations have finished, leaving
	// the rest of the workload to span the election.
	linearizabilityKillAfter = 25
)

// TestConcurrentHistoryIsLinearizableAcrossALeaderKill records what real
// Clients observed from three real Node processes while the Leader was killed
// mid-workload, then checks that the recording admits a legal sequential
// explanation. It is the check that turns linearizability from a property the
// design argues for into one an actual run is tested against.
func TestConcurrentHistoryIsLinearizableAcrossALeaderKill(t *testing.T) {
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
		processes[id] = startNodeProcess(t, config.Config{
			Version:            1,
			ClusterID:          "linearizability-cluster",
			ActiveSessionLimit: linearizabilityWriters,
			Node:               config.Node{ID: id, DataDir: filepath.Join(t.TempDir(), id)},
			Members:            members,
		})
	}
	defer func() {
		for _, process := range processes {
			process.stop()
		}
	}()

	waitForStableLeader(t, members, nil, processTestDeadline)
	addresses := make([]string, 0, len(members))
	for _, member := range members {
		addresses = append(addresses, member.ClientAddress)
	}
	cluster := newClient(t, addresses...)

	sessions := make([][16]byte, linearizabilityWriters)
	for index := range sessions {
		ctx, cancel := context.WithTimeout(context.Background(), processTestDeadline)
		session, err := cluster.OpenSession(ctx)
		cancel()
		if err != nil {
			t.Fatalf("open Client Session %d: %v", index, err)
		}
		sessions[index] = session
	}

	var history linearizability.History
	var finished atomic.Int64
	var workers sync.WaitGroup

	for index := range linearizabilityWriters {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runLinearizabilityWriter(&history, cluster, sessions[index], index, &finished)
		}()
	}
	for index := range linearizabilityReaders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runLinearizabilityReader(&history, cluster, index, &finished)
		}()
	}

	killed := waitAndKillLeader(t, members, processes, &finished)
	finishedAtKill := finished.Load()
	replacement := waitForSingleLeader(t, members, map[string]bool{killed: true}, processTestDeadline)
	if replacement == killed {
		t.Fatalf("replacement Leader = killed Node %q", killed)
	}
	workers.Wait()

	// The recording only means something if work actually continued across the
	// Leader change rather than finishing before it.
	if remaining := finished.Load() - finishedAtKill; remaining == 0 {
		t.Fatalf("no operations ran after Leader %q was killed; the workload did not span the election", killed)
	}

	operations := history.Operations()
	total := linearizabilityWriters*linearizabilityOpsPerWorker + linearizabilityReaders*linearizabilityOpsPerWorker
	if len(operations) < total {
		t.Fatalf("recorded %d operations, want at least %d", len(operations), total)
	}
	pending := 0
	for _, operation := range operations {
		if operation.Complete.IsZero() {
			pending++
		}
	}
	t.Logf("checked %d recorded operations (%d with unknown results) across the killed Leader %q; %d of %d finished after the kill",
		len(operations), pending, killed, finished.Load()-finishedAtKill, finished.Load())
	if err := linearizability.Check(operations); err != nil {
		t.Fatalf("recorded history over %d operations has no legal sequential explanation: %v\nrecorded history: %s",
			len(operations), err, writeHistoryArtifact(t, operations))
	}
}

// runLinearizabilityWriter issues one Session's ordered mutations. An
// ambiguous failure leaves the operation pending in the history, which is
// exactly what the model means by an unknown result, and retries the same
// sequence so the Cluster deduplicates it into one logical mutation.
func runLinearizabilityWriter(history *linearizability.History, cluster clientAPI, session [16]byte, writer int, finished *atomic.Int64) {
	for operation := 1; operation <= linearizabilityOpsPerWorker; operation++ {
		sequence := uint64(operation)
		key := fmt.Sprintf("key-%d", (writer+operation)%linearizabilityKeys)
		value := []byte(fmt.Sprintf("writer-%d-sequence-%d", writer, sequence))
		removal := operation%5 == 0

		for attempt := 0; attempt < linearizabilityRetryLimit; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), linearizabilityOperationLimit)
			var complete func(linearizability.Outcome)
			var outcome linearizability.Outcome
			var err error
			if removal {
				complete = history.Start(linearizability.Delete, session, sequence, key, nil)
				outcome.Existed, err = cluster.Delete(ctx, session, sequence, key)
			} else {
				complete = history.Start(linearizability.Set, session, sequence, key, value)
				err = cluster.Set(ctx, session, sequence, key, value)
			}
			cancel()
			if err == nil {
				complete(outcome)
				break
			}
			if reason, ok := semanticFailure(err); ok {
				complete(linearizability.Outcome{Error: reason})
				break
			}
		}
		finished.Add(1)
		time.Sleep(linearizabilityOperationPause)
	}
}

func runLinearizabilityReader(history *linearizability.History, cluster clientAPI, reader int, finished *atomic.Int64) {
	for operation := 1; operation <= linearizabilityOpsPerWorker; operation++ {
		key := fmt.Sprintf("key-%d", (reader+operation)%linearizabilityKeys)
		ctx, cancel := context.WithTimeout(context.Background(), linearizabilityOperationLimit)
		complete := history.Start(linearizability.Get, [16]byte{}, 0, key, nil)
		value, err := cluster.Get(ctx, key)
		cancel()
		switch reason, ok := semanticFailure(err); {
		case err == nil:
			complete(linearizability.Outcome{Found: true, Value: value})
		case ok:
			complete(linearizability.Outcome{Error: reason})
		}
		finished.Add(1)
		time.Sleep(linearizabilityOperationPause)
	}
}

// clientAPI is the subset of the public Client the workload uses.
type clientAPI interface {
	Set(context.Context, [16]byte, uint64, string, []byte) error
	Delete(context.Context, [16]byte, uint64, string) (bool, error)
	Get(context.Context, string) ([]byte, error)
}

// semanticFailure maps a typed contract error to the stable category the model
// uses. An untyped failure is ambiguous rather than semantic: the command may
// still have taken effect, so it must stay pending in the history.
func semanticFailure(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	for _, detail := range status.Convert(err).Details() {
		switch detail.(type) {
		case *quorumkvv1.KeyNotFound:
			return "not_found", true
		case *quorumkvv1.StaleSequence:
			return "stale_sequence", true
		case *quorumkvv1.OutOfOrderSequence:
			return "out_of_order", true
		}
	}
	return "", false
}

// waitAndKillLeader lets the workload get under way before killing the Leader,
// so the kill lands while mutations are in flight rather than on an idle
// Cluster. That is the moment an entry can be replicated but not yet
// committed, which is where a failover would be able to lose or reorder work.
func waitAndKillLeader(t *testing.T, members map[string]config.Member, processes map[string]*nodeProcess, finished *atomic.Int64) string {
	t.Helper()
	deadline := time.Now().Add(processTestDeadline)
	for time.Now().Before(deadline) {
		if finished.Load() >= linearizabilityKillAfter {
			if leader := currentLeader(members, nil); leader != "" {
				processes[leader].stop()
				delete(processes, leader)
				return leader
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no Leader to kill within %v", processTestDeadline)
	return ""
}

func currentLeader(members map[string]config.Member, excluded map[string]bool) string {
	for id, member := range members {
		if excluded[id] {
			continue
		}
		if state := fetchStatus(member.ClientAddress); state != nil && state.Role == quorumkvv1.RaftRole_RAFT_ROLE_LEADER {
			return id
		}
	}
	return ""
}

// writeHistoryArtifact saves the recording so a failure can be replayed and
// studied offline instead of only existing in a CI log.
func writeHistoryArtifact(t *testing.T, operations []linearizability.Operation) string {
	t.Helper()
	directory := os.Getenv("QUORUMKV_ARTIFACT_DIR")
	if directory == "" {
		directory = t.TempDir()
	} else if err := os.MkdirAll(directory, 0o750); err != nil {
		t.Logf("create artifact directory %q: %v", directory, err)
		directory = t.TempDir()
	}
	path := filepath.Join(directory, "linearizability-history.json")
	contents, err := json.MarshalIndent(operations, "", "  ")
	if err != nil {
		t.Logf("encode recorded history: %v", err)
		return "not recorded"
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Logf("write recorded history: %v", err)
		return "not recorded"
	}
	return path
}
