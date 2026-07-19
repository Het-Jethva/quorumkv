package node

import (
	"context"
	"crypto/rand"
	"fmt"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sessionFailure uint8

const (
	sessionSucceeded sessionFailure = iota
	sessionLimitReached
	sessionUnknown
	sessionClosed
	sessionAlreadyExists
)

type proposalResult struct {
	sessionID raft.SessionID
	failure   sessionFailure
	leaderID  raft.NodeID
	rejected  bool
}

type sessionState uint8

const (
	sessionActive sessionState = iota
	sessionPermanentlyClosed
)

// sessionMachine is owned exclusively by the Raft runtime loop. Applying the
// same committed entries therefore produces the same state on every Node.
type sessionMachine struct {
	limit    int
	active   int
	sessions map[raft.SessionID]sessionState
}

func newSessionMachine(limit int) *sessionMachine {
	return &sessionMachine{limit: limit, sessions: make(map[raft.SessionID]sessionState)}
}

func (m *sessionMachine) apply(entry raft.LogEntry) proposalResult {
	result := proposalResult{sessionID: entry.SessionID}
	switch entry.Type {
	case raft.EntryNoOp:
		return result
	case raft.EntryOpenSession:
		if _, exists := m.sessions[entry.SessionID]; exists {
			result.failure = sessionAlreadyExists
			return result
		}
		if m.active >= m.limit {
			result.failure = sessionLimitReached
			return result
		}
		m.sessions[entry.SessionID] = sessionActive
		m.active++
		return result
	case raft.EntryCloseSession:
		state, exists := m.sessions[entry.SessionID]
		if !exists {
			result.failure = sessionUnknown
			return result
		}
		if state == sessionPermanentlyClosed {
			result.failure = sessionClosed
			return result
		}
		m.sessions[entry.SessionID] = sessionPermanentlyClosed
		m.active--
		return result
	default:
		panic(fmt.Sprintf("apply unsupported Raft entry type %d", entry.Type))
	}
}

func (n *Node) OpenSession(ctx context.Context, _ *quorumkvv1.OpenSessionRequest) (*quorumkvv1.OpenSessionResponse, error) {
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}
	var sessionID raft.SessionID
	if _, err := rand.Read(sessionID[:]); err != nil {
		return nil, status.Errorf(codes.Internal, "generate Client Session identity: %v", err)
	}
	result, err := n.proposeSession(ctx, raft.EntryOpenSession, sessionID)
	if err != nil {
		return nil, err
	}
	return &quorumkvv1.OpenSessionResponse{SessionId: result.sessionID[:]}, nil
}

func (n *Node) CloseSession(ctx context.Context, request *quorumkvv1.CloseSessionRequest) (*quorumkvv1.CloseSessionResponse, error) {
	if len(request.SessionId) != len(raft.SessionID{}) {
		return nil, status.Errorf(codes.InvalidArgument, "Client Session identity is %d bytes, want 16", len(request.SessionId))
	}
	var sessionID raft.SessionID
	copy(sessionID[:], request.SessionId)
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}
	if _, err := n.proposeSession(ctx, raft.EntryCloseSession, sessionID); err != nil {
		return nil, err
	}
	return &quorumkvv1.CloseSessionResponse{}, nil
}

func (n *Node) rejectIfNotLeader() (proposalResult, bool) {
	state := n.raftState.Load().(raft.State)
	if state.Role == raft.Leader {
		return proposalResult{}, false
	}
	return proposalResult{leaderID: state.LeaderID, rejected: true}, true
}

func (n *Node) proposeSession(ctx context.Context, entryType raft.EntryType, sessionID raft.SessionID) (proposalResult, error) {
	proposalID := raft.ProposalID(n.nextProposal.Add(1))
	results := make(chan proposalResult, 1)
	input := raftInput{
		event:          raft.ProposeSession{ProposalID: proposalID, Type: entryType, SessionID: sessionID},
		result:         results,
		requestContext: ctx,
	}
	select {
	case n.events <- input:
	case <-n.runtimeDone:
		return proposalResult{}, status.Error(codes.Unavailable, "Node is stopping")
	case <-ctx.Done():
		return proposalResult{}, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case result := <-results:
		return result, n.proposalError(result)
	case <-n.runtimeDone:
		return proposalResult{}, status.Error(codes.Unavailable, "Node stopped before the Client Session command completed")
	case <-ctx.Done():
		return proposalResult{}, status.FromContextError(ctx.Err()).Err()
	}
}

func (n *Node) proposalError(result proposalResult) error {
	if result.rejected {
		if result.leaderID == "" {
			return status.Error(codes.Unavailable, "Leader is unknown")
		}
		leader, ok := n.config.Members[string(result.leaderID)]
		if !ok {
			return status.Errorf(codes.Unavailable, "Leader %q is not in the configured member map", result.leaderID)
		}
		base := status.New(codes.FailedPrecondition, fmt.Sprintf("Node %q is not the Leader; retry Node %q", n.config.Node.ID, result.leaderID))
		withDetails, err := base.WithDetails(&quorumkvv1.NotLeader{LeaderId: string(result.leaderID), LeaderAddress: leader.ClientAddress})
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	}
	switch result.failure {
	case sessionSucceeded:
		return nil
	case sessionLimitReached:
		return status.Errorf(codes.ResourceExhausted, "active Client Session limit %d reached", n.config.ActiveSessionLimit)
	case sessionUnknown:
		return status.Error(codes.NotFound, "Client Session is unknown")
	case sessionClosed:
		return status.Error(codes.FailedPrecondition, "Client Session is closed and cannot be reused")
	case sessionAlreadyExists:
		return status.Error(codes.AlreadyExists, "Client Session identity was already used and cannot be reopened")
	default:
		return status.Error(codes.Internal, "Client Session command returned an unknown result")
	}
}
