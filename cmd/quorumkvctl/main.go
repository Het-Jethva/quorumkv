package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("quorumkvctl", flag.ContinueOnError)
	address := flags.String("address", "127.0.0.1:7400", "node client address")
	timeout := flags.Duration("timeout", 5*time.Second, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() == 0 {
		return fmt.Errorf("usage: quorumkvctl [flags] status | session open | session close <session-id>")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if flags.Arg(0) == "session" {
		return runSession(ctx, *address, flags.Args()[1:])
	}
	if flags.NArg() != 1 || flags.Arg(0) != "status" {
		return fmt.Errorf("usage: quorumkvctl [flags] status | session open | session close <session-id>")
	}
	connection, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to node at %q: %w", *address, err)
	}
	defer connection.Close()

	status, err := quorumkvv1.NewNodeServiceClient(connection).GetStatus(ctx, &quorumkvv1.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("get node status from %q: %w", *address, err)
	}
	output := struct {
		ClusterID     string `json:"cluster_id"`
		NodeID        string `json:"node_id"`
		State         string `json:"state"`
		PeerAddress   string `json:"peer_address"`
		ClientAddress string `json:"client_address"`
		Role          string `json:"role"`
		LeaderID      string `json:"leader_id,omitempty"`
		Term          uint64 `json:"term"`
	}{status.ClusterId, status.NodeId, status.State.String(), status.PeerAddress, status.ClientAddress, status.Role.String(), status.LeaderId, status.Term}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func runSession(ctx context.Context, address string, args []string) error {
	if len(args) == 1 && args[0] == "open" {
		sessionID, err := client.New(address).OpenSession(ctx)
		if err != nil {
			return fmt.Errorf("open Client Session: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			SessionID string `json:"session_id"`
		}{SessionID: hex.EncodeToString(sessionID[:])})
	}
	if len(args) == 2 && args[0] == "close" {
		decoded, err := hex.DecodeString(args[1])
		if err != nil || len(decoded) != 16 {
			return fmt.Errorf("session-id must be exactly 32 hexadecimal characters")
		}
		var sessionID [16]byte
		copy(sessionID[:], decoded)
		if err := client.New(address).CloseSession(ctx, sessionID); err != nil {
			return fmt.Errorf("close Client Session %q: %w", args[1], err)
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			SessionID string `json:"session_id"`
			Closed    bool   `json:"closed"`
		}{SessionID: args[1], Closed: true})
	}
	return fmt.Errorf("usage: quorumkvctl [flags] session open | session close <session-id>")
}
