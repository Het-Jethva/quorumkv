package node

import (
	"context"
	"testing"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSessionMachineEnforcesLimitAndPermanentClosure(t *testing.T) {
	machine := newSessionMachine(1)
	first := raft.SessionID{1}
	second := raft.SessionID{2}

	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: first}); result.failure != sessionSucceeded {
		t.Fatalf("open first Client Session = %v, want success", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: second}); result.failure != sessionLimitReached {
		t.Fatalf("open above limit = %v, want limit reached", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryCloseSession, SessionID: first}); result.failure != sessionSucceeded {
		t.Fatalf("close first Client Session = %v, want success", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: first}); result.failure != sessionAlreadyExists {
		t.Fatalf("reopen closed Client Session = %v, want already used", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryCloseSession, SessionID: first}); result.failure != sessionClosed {
		t.Fatalf("close closed Client Session = %v, want closed", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryCloseSession, SessionID: second}); result.failure != sessionUnknown {
		t.Fatalf("close unknown Client Session = %v, want unknown", result.failure)
	}
	if result := machine.apply(raft.LogEntry{Type: raft.EntryOpenSession, SessionID: second}); result.failure != sessionSucceeded {
		t.Fatalf("open after releasing capacity = %v, want success", result.failure)
	}
}

func TestFollowerReturnsTypedLeaderHint(t *testing.T) {
	n := New(config.Config{
		ClusterID:          "cluster-1",
		ActiveSessionLimit: 1,
		Node:               config.Node{ID: "node-1"},
		Members: map[string]config.Member{
			"node-1": {ClientAddress: "127.0.0.1:7401"},
			"node-2": {ClientAddress: "127.0.0.1:7402"},
			"node-3": {ClientAddress: "127.0.0.1:7403"},
		},
	})
	n.publishRaftState(raft.State{ID: "node-1", Role: raft.Follower, LeaderID: "node-2", Term: 3})

	_, err := n.OpenSession(context.Background(), &quorumkvv1.OpenSessionRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("OpenSession() error = %v, want FailedPrecondition", err)
	}
	for _, detail := range status.Convert(err).Details() {
		notLeader, ok := detail.(*quorumkvv1.NotLeader)
		if ok {
			if notLeader.LeaderId != "node-2" || notLeader.LeaderAddress != "127.0.0.1:7402" {
				t.Fatalf("NotLeader detail = %#v, want node-2 address", notLeader)
			}
			return
		}
	}
	t.Fatal("OpenSession() error has no typed NotLeader detail")
}
