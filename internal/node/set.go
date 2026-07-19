package node

import (
	"context"
	"unicode/utf8"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxKeyBytes   = 1024
	maxValueBytes = 1 << 20
)

// Set stores an opaque Value after its command is durably committed and
// applied. Cancellation only stops waiting; it cannot retract a proposal.
func (n *Node) Set(ctx context.Context, request *quorumkvv1.SetRequest) (*quorumkvv1.SetResponse, error) {
	if len(request.SessionId) != len(raft.SessionID{}) {
		return nil, status.Errorf(codes.InvalidArgument, "Client Session identity is %d bytes, want 16", len(request.SessionId))
	}
	if request.Sequence == 0 {
		return nil, status.Error(codes.InvalidArgument, "mutation sequence must begin at one")
	}
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key must not be empty")
	}
	if !utf8.ValidString(request.Key) {
		return nil, status.Error(codes.InvalidArgument, "Key must be valid UTF-8")
	}
	if len(request.Key) > maxKeyBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Key is %d bytes, limit is %d", len(request.Key), maxKeyBytes)
	}
	if len(request.Value) > maxValueBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Value is %d bytes, limit is %d", len(request.Value), maxValueBytes)
	}
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}
	var sessionID raft.SessionID
	copy(sessionID[:], request.SessionId)
	result, err := n.propose(ctx, raft.ProposeSet{
		ProposalID: raft.ProposalID(n.nextProposal.Add(1)),
		SessionID:  sessionID,
		Sequence:   request.Sequence,
		Key:        request.Key,
		Value:      append([]byte(nil), request.Value...),
	})
	if err != nil {
		return nil, err
	}
	if err := n.proposalError(result); err != nil {
		return nil, err
	}
	return &quorumkvv1.SetResponse{}, nil
}

func (n *Node) propose(ctx context.Context, event raft.Event) (proposalResult, error) {
	results := make(chan proposalResult, 1)
	input := raftInput{event: event, result: results, requestContext: ctx}
	select {
	case n.events <- input:
	case <-n.runtimeDone:
		return proposalResult{}, status.Error(codes.Unavailable, "Node is stopping")
	case <-ctx.Done():
		return proposalResult{}, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case result := <-results:
		return result, nil
	case <-n.runtimeDone:
		return proposalResult{}, status.Error(codes.Unavailable, "Node stopped before the command completed")
	case <-ctx.Done():
		return proposalResult{}, status.FromContextError(ctx.Err()).Err()
	}
}
