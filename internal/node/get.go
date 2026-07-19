package node

import (
	"context"
	"unicode/utf8"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readResult struct {
	value    []byte
	found    bool
	leaderID raft.NodeID
	rejected bool
}

// Get returns a Value only after Raft confirms the Leader's current authority.
func (n *Node) Get(ctx context.Context, request *quorumkvv1.GetRequest) (*quorumkvv1.GetResponse, error) {
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key must not be empty")
	}
	if !utf8.ValidString(request.Key) {
		return nil, status.Error(codes.InvalidArgument, "Key must be valid UTF-8")
	}
	if len(request.Key) > maxKeyBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Key is %d bytes, limit is %d", len(request.Key), maxKeyBytes)
	}
	if result, rejected := n.rejectIfNotLeader(); rejected {
		return nil, n.proposalError(result)
	}

	results := make(chan readResult, 1)
	readID := raft.ReadID(n.nextRead.Add(1))
	input := raftInput{event: raft.ConfirmRead{ReadID: readID}, readResult: results, requestContext: ctx, key: request.Key}
	select {
	case n.events <- input:
	case <-n.runtimeDone:
		return nil, status.Error(codes.Unavailable, "Node is stopping")
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case result := <-results:
		if result.rejected {
			return nil, n.proposalError(proposalResult{leaderID: result.leaderID, rejected: true})
		}
		if !result.found {
			return nil, status.Errorf(codes.NotFound, "Key %q was not found", request.Key)
		}
		return &quorumkvv1.GetResponse{Value: result.value}, nil
	case <-n.runtimeDone:
		return nil, status.Error(codes.Unavailable, "Node stopped before GET completed")
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
