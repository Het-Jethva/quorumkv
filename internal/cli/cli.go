// Package cli implements the quorumkvctl command surface independently of
// process startup so integration tests can exercise it against real Nodes.
package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/Het-Jethva/quorumkv/client"
	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Run parses and executes one quorumkvctl command, writing JSON results to output.
func Run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("quorumkvctl", flag.ContinueOnError)
	address := flags.String("address", "127.0.0.1:7400", "node client address")
	timeout := flags.Duration("timeout", 5*time.Second, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() == 0 {
		return usageError()
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if flags.Arg(0) == "session" {
		return runSession(ctx, output, *address, flags.Args()[1:])
	}
	if flags.Arg(0) == "set" {
		return runSet(ctx, output, *address, flags.Args()[1:])
	}
	if flags.NArg() != 1 || flags.Arg(0) != "status" {
		return usageError()
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
	result := struct {
		ClusterID     string `json:"cluster_id"`
		NodeID        string `json:"node_id"`
		State         string `json:"state"`
		PeerAddress   string `json:"peer_address"`
		ClientAddress string `json:"client_address"`
		Role          string `json:"role"`
		LeaderID      string `json:"leader_id,omitempty"`
		Term          uint64 `json:"term"`
	}{status.ClusterId, status.NodeId, status.State.String(), status.PeerAddress, status.ClientAddress, status.Role.String(), status.LeaderId, status.Term}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func runSet(ctx context.Context, output io.Writer, address string, args []string) error {
	if len(args) != 4 {
		return usageError()
	}
	sessionID, err := parseSessionID(args[0])
	if err != nil {
		return err
	}
	sequence, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil || sequence == 0 {
		return fmt.Errorf("sequence must be a positive base-10 integer")
	}
	if err := client.New(address).Set(ctx, sessionID, sequence, args[2], []byte(args[3])); err != nil {
		return fmt.Errorf("SET Key %q: %w", args[2], err)
	}
	return json.NewEncoder(output).Encode(struct {
		Key      string `json:"key"`
		Sequence uint64 `json:"sequence"`
		Stored   bool   `json:"stored"`
	}{Key: args[2], Sequence: sequence, Stored: true})
}

func runSession(ctx context.Context, output io.Writer, address string, args []string) error {
	if len(args) == 1 && args[0] == "open" {
		sessionID, err := client.New(address).OpenSession(ctx)
		if err != nil {
			return fmt.Errorf("open Client Session: %w", err)
		}
		return json.NewEncoder(output).Encode(struct {
			SessionID string `json:"session_id"`
		}{SessionID: hex.EncodeToString(sessionID[:])})
	}
	if len(args) == 2 && args[0] == "close" {
		sessionID, err := parseSessionID(args[1])
		if err != nil {
			return err
		}
		if err := client.New(address).CloseSession(ctx, sessionID); err != nil {
			return fmt.Errorf("close Client Session %q: %w", args[1], err)
		}
		return json.NewEncoder(output).Encode(struct {
			SessionID string `json:"session_id"`
			Closed    bool   `json:"closed"`
		}{SessionID: args[1], Closed: true})
	}
	return fmt.Errorf("usage: quorumkvctl [flags] session open | session close <session-id>")
}

func parseSessionID(encoded string) ([16]byte, error) {
	var sessionID [16]byte
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != len(sessionID) {
		return sessionID, fmt.Errorf("session-id must be exactly 32 hexadecimal characters")
	}
	copy(sessionID[:], decoded)
	return sessionID, nil
}

func usageError() error {
	return fmt.Errorf("usage: quorumkvctl [flags] status | session open | session close <session-id> | set <session-id> <sequence> <key> <value>")
}
