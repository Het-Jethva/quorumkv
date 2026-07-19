// Package client implements the official QuorumKV v1 client behavior.
package client

import (
	"context"
	"fmt"
	"time"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	maxAttempts    = 128
	initialBackoff = 25 * time.Millisecond
	maximumBackoff = 200 * time.Millisecond
)

// Client starts at one configured Node and follows typed Leader hints directly.
type Client struct {
	address string
}

// New creates a Client whose first request is sent to address.
func New(address string) *Client { return &Client{address: address} }

// OpenSession creates a replicated Client Session and returns its 128-bit identity.
func (c *Client) OpenSession(ctx context.Context) ([16]byte, error) {
	var sessionID [16]byte
	err := c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		response, err := client.OpenSession(ctx, &quorumkvv1.OpenSessionRequest{})
		if err != nil {
			return err
		}
		if len(response.SessionId) != len(sessionID) {
			return fmt.Errorf("leader returned a %d-byte Client Session identity, want 16", len(response.SessionId))
		}
		copy(sessionID[:], response.SessionId)
		return nil
	})
	return sessionID, err
}

// CloseSession permanently closes sessionID through consensus.
func (c *Client) CloseSession(ctx context.Context, sessionID [16]byte) error {
	return c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		_, err := client.CloseSession(ctx, &quorumkvv1.CloseSessionRequest{SessionId: sessionID[:]})
		return err
	})
}

func (c *Client) withLeader(ctx context.Context, call func(quorumkvv1.ClientServiceClient) error) error {
	address := c.address
	backoff := initialBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to Node at %q: %w", address, err)
		}
		err = call(quorumkvv1.NewClientServiceClient(connection))
		closeErr := connection.Close()
		if err == nil {
			if closeErr != nil {
				return fmt.Errorf("close connection to Node at %q: %w", address, closeErr)
			}
			return nil
		}
		hint, ok := leaderHint(err)
		if ok {
			address = hint
			continue
		}
		if status.Code(err) != codes.Unavailable {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, maximumBackoff)
	}
	return fmt.Errorf("Client Session command did not reach a stable Leader after %d attempts", maxAttempts)
}

func leaderHint(err error) (string, bool) {
	for _, detail := range status.Convert(err).Details() {
		notLeader, ok := detail.(*quorumkvv1.NotLeader)
		if ok && notLeader.LeaderAddress != "" {
			return notLeader.LeaderAddress, true
		}
	}
	return "", false
}
