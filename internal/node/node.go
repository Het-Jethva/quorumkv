package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	// LivenessService and ReadinessService are separate health-check names so
	// operators do not mistake local process health for readiness to accept RPCs.
	LivenessService  = "quorumkv.v1.Liveness"
	ReadinessService = "quorumkv.v1.Readiness"

	shutdownGracePeriod = 5 * time.Second
)

// Node owns the local listeners and gRPC servers for one configured process.
// Run must be called at most once.
type Node struct {
	quorumkvv1.UnimplementedNodeServiceServer

	config config.Config
	ready  atomic.Bool
}

// New creates a node from an already validated configuration.
func New(cfg config.Config) *Node {
	return &Node{config: cfg}
}

// Run serves the peer and client endpoints until cancellation or a server error.
func (n *Node) Run(ctx context.Context) (runErr error) {
	runtime, err := openRaftRuntime(n.config, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := runtime.close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close Node %q consensus state: %w", n.config.Node.ID, err)
		}
	}()

	peerListener, err := net.Listen("tcp", n.config.Node.PeerAddress)
	if err != nil {
		return fmt.Errorf("listen on peer address %q: %w", n.config.Node.PeerAddress, err)
	}
	defer peerListener.Close()

	clientListener, err := net.Listen("tcp", n.config.Node.ClientAddress)
	if err != nil {
		return fmt.Errorf("listen on client address %q: %w", n.config.Node.ClientAddress, err)
	}
	defer clientListener.Close()

	peerServer := grpc.NewServer()
	clientServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(LivenessService, grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(ReadinessService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	grpc_health_v1.RegisterHealthServer(clientServer, healthServer)
	quorumkvv1.RegisterNodeServiceServer(clientServer, n)

	n.ready.Store(true)
	healthServer.SetServingStatus(ReadinessService, grpc_health_v1.HealthCheckResponse_SERVING)

	serveErrors := make(chan error, 2)
	var servers sync.WaitGroup
	serve := func(name string, server *grpc.Server, listener net.Listener) {
		defer servers.Done()
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrors <- fmt.Errorf("serve %s endpoint: %w", name, err)
		}
	}
	servers.Add(2)
	go serve("peer", peerServer, peerListener)
	go serve("client", clientServer, clientListener)

	select {
	case <-ctx.Done():
	case runErr = <-serveErrors:
	}

	n.ready.Store(false)
	healthServer.SetServingStatus(ReadinessService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	stopServers(peerServer, clientServer)
	servers.Wait()
	return runErr
}

func stopServers(servers ...*grpc.Server) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, server := range servers {
			server.GracefulStop()
		}
	}()

	select {
	case <-done:
	case <-time.After(shutdownGracePeriod):
		for _, server := range servers {
			server.Stop()
		}
		<-done
	}
}

// GetStatus reports only local process state; it makes no Cluster health claim.
func (n *Node) GetStatus(context.Context, *quorumkvv1.GetStatusRequest) (*quorumkvv1.GetStatusResponse, error) {
	state := quorumkvv1.NodeState_NODE_STATE_STARTING
	if n.ready.Load() {
		state = quorumkvv1.NodeState_NODE_STATE_READY
	}
	return &quorumkvv1.GetStatusResponse{
		ClusterId:     n.config.ClusterID,
		NodeId:        n.config.Node.ID,
		State:         state,
		PeerAddress:   n.config.Node.PeerAddress,
		ClientAddress: n.config.Node.ClientAddress,
	}, nil
}
