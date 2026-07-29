// Package client implements the official QuorumKV v1 client behavior.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
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
// It keeps one connection per Node address it has reached, so a command does
// not pay connection setup. A Client is safe for concurrent use and must be
// closed to release those connections.
type Client struct {
	addresses []string

	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
	closed      bool
}

// New creates a Client that starts at the first address and falls back across
// the remaining configured Node addresses.
func New(addresses ...string) *Client {
	return &Client{
		addresses:   append([]string(nil), addresses...),
		connections: make(map[string]*grpc.ClientConn),
	}
}

// Close releases every connection this Client opened. A closed Client cannot
// be reused.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var first error
	for address, connection := range c.connections {
		if err := connection.Close(); err != nil && first == nil {
			first = fmt.Errorf("close connection to Node at %q: %w", address, err)
		}
		delete(c.connections, address)
	}
	return first
}

// connection returns the Client's connection to address, dialing once. A
// Leader hint may name a Node that is not in the configured address list, so
// connections are keyed by address rather than by configured position.
func (c *Client) connection(address string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("Client is closed")
	}
	if connection, ok := c.connections[address]; ok {
		return connection, nil
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to Node at %q: %w", address, err)
	}
	c.connections[address] = connection
	return connection, nil
}

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

// Set stores Value under key using the next sequence in sessionID.
func (c *Client) Set(ctx context.Context, sessionID [16]byte, sequence uint64, key string, value []byte) error {
	return c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		_, err := client.Set(ctx, &quorumkvv1.SetRequest{
			SessionId: sessionID[:],
			Sequence:  sequence,
			Key:       key,
			Value:     value,
		})
		return err
	})
}

// Delete removes key and reports whether a Value existed before the mutation.
func (c *Client) Delete(ctx context.Context, sessionID [16]byte, sequence uint64, key string) (bool, error) {
	var existed bool
	err := c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		response, err := client.Delete(ctx, &quorumkvv1.DeleteRequest{
			SessionId: sessionID[:],
			Sequence:  sequence,
			Key:       key,
		})
		if err != nil {
			return err
		}
		existed = response.Existed
		return nil
	})
	return existed, err
}

// Get returns the latest linearizable Value stored under key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := c.withLeader(ctx, func(client quorumkvv1.ClientServiceClient) error {
		response, err := client.Get(ctx, &quorumkvv1.GetRequest{Key: key})
		if err != nil {
			return err
		}
		value = append([]byte(nil), response.Value...)
		return nil
	})
	return value, err
}

func (c *Client) withLeader(ctx context.Context, call func(quorumkvv1.ClientServiceClient) error) error {
	if len(c.addresses) == 0 {
		return fmt.Errorf("at least one Node address is required")
	}
	configuredIndex := 0
	address := c.addresses[configuredIndex]
	backoff := initialBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		connection, err := c.connection(address)
		if err != nil {
			return err
		}
		err = call(quorumkvv1.NewClientServiceClient(connection))
		if err == nil {
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
		configuredIndex = (configuredIndex + 1) % len(c.addresses)
		address = c.addresses[configuredIndex]
		jitter := time.Duration(rand.Int64N(int64(backoff)/2 + 1))
		timer := time.NewTimer(backoff/2 + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, maximumBackoff)
	}
	return fmt.Errorf("command did not reach a stable Leader after %d attempts", maxAttempts)
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
