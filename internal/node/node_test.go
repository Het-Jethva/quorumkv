package node_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/node"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNodeReportsStatusAndHealthThenStops(t *testing.T) {
	peerAddress := unusedAddress(t)
	clientAddress := unusedAddress(t)
	cfg := config.Config{
		Version:   1,
		ClusterID: "test-cluster",
		Node: config.Node{
			ID:      "node-1",
			DataDir: t.TempDir(),
		},
		Members: testMembers(peerAddress, clientAddress),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- node.New(cfg).Run(ctx)
	}()

	connection := dialEventually(t, clientAddress)
	defer connection.Close()

	status, err := quorumkvv1.NewNodeServiceClient(connection).GetStatus(
		context.Background(),
		&quorumkvv1.GetStatusRequest{},
	)
	if err != nil {
		t.Fatalf("GetStatus() error = %v", err)
	}
	if status.ClusterId != cfg.ClusterID || status.NodeId != cfg.Node.ID {
		t.Fatalf("GetStatus() identity = %q/%q, want %q/%q", status.ClusterId, status.NodeId, cfg.ClusterID, cfg.Node.ID)
	}
	if status.State != quorumkvv1.NodeState_NODE_STATE_READY {
		t.Fatalf("GetStatus() state = %v, want READY", status.State)
	}
	if status.PeerAddress != peerAddress || status.ClientAddress != clientAddress {
		t.Fatalf("GetStatus() addresses = %q/%q, want %q/%q", status.PeerAddress, status.ClientAddress, peerAddress, clientAddress)
	}

	healthClient := grpc_health_v1.NewHealthClient(connection)
	for _, service := range []string{node.LivenessService, node.ReadinessService} {
		response, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("health Check(%q) error = %v", service, err)
		}
		if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("health Check(%q) = %v, want SERVING", service, response.Status)
		}
	}

	peerConnection, err := net.DialTimeout("tcp", peerAddress, time.Second)
	if err != nil {
		t.Fatalf("dial peer endpoint: %v", err)
	}
	peerConnection.Close()

	cancel()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error after cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
}

func TestNodeStartupRejectsConflictingDurableIdentity(t *testing.T) {
	directory := t.TempDir()
	base := config.Config{
		Version:   1,
		ClusterID: "cluster-1",
		Node: config.Node{
			ID:      "node-1",
			DataDir: directory,
		},
		Members: testMembers(unusedAddress(t), unusedAddress(t)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := node.New(base).Run(ctx); err != nil {
		t.Fatalf("initialize Node identity: %v", err)
	}

	conflict := base
	conflict.Node.ID = "node-2"
	err := node.New(conflict).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "durable identity mismatch") {
		t.Fatalf("Run() error = %v, want durable identity mismatch", err)
	}
}

func TestNodeStopsOnPeerClusterMismatchWithDiagnostic(t *testing.T) {
	members := map[string]config.Member{
		"node-1": {PeerAddress: unusedAddress(t), ClientAddress: unusedAddress(t)},
		"node-2": {PeerAddress: unusedAddress(t), ClientAddress: unusedAddress(t)},
		"node-3": {PeerAddress: unusedAddress(t), ClientAddress: unusedAddress(t)},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 2)
	for index, clusterID := range []string{"cluster-1", "cluster-2"} {
		id := fmt.Sprintf("node-%d", index+1)
		cfg := config.Config{
			Version:   1,
			ClusterID: clusterID,
			Node:      config.Node{ID: id, DataDir: t.TempDir()},
			Members:   members,
		}
		go func() { results <- node.New(cfg).Run(ctx) }()
	}

	select {
	case err := <-results:
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("Run() error = %v, want actionable Cluster Identity mismatch", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Nodes did not fail after incompatible peer handshake")
	}
	cancel()
	select {
	case <-results:
	case <-time.After(2 * time.Second):
		t.Fatal("second Node did not stop after cancellation")
	}
}

func testMembers(peerAddress, clientAddress string) map[string]config.Member {
	return map[string]config.Member{
		"node-1": {PeerAddress: peerAddress, ClientAddress: clientAddress},
		"node-2": {PeerAddress: "127.0.0.1:17302", ClientAddress: "127.0.0.1:17402"},
		"node-3": {PeerAddress: "127.0.0.1:17303", ClientAddress: "127.0.0.1:17403"},
	}
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	listener.Close()
	return address
}

func dialEventually(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		connection, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("create client for %q: %v", address, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err = quorumkvv1.NewNodeServiceClient(connection).GetStatus(ctx, &quorumkvv1.GetStatusRequest{})
		cancel()
		if err == nil {
			return connection
		}
		connection.Close()
		if time.Now().After(deadline) {
			t.Fatalf("dial client endpoint %q: %v", address, err)
		}
	}
}
