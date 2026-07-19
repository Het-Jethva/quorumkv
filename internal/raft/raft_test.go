package raft_test

import (
	"reflect"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

func TestElectionRequestsVotesOnlyAfterSelfVoteIsPersistent(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-3", "node-2"})

	actions := node.Step(raft.ElectionTimeout{})
	wantPersistence := []raft.Action{raft.PersistHardState{
		PersistenceID: 1,
		Term:          1,
		VotedFor:      "node-1",
	}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("timeout actions = %#v, want %#v", actions, wantPersistence)
	}
	if got := node.State().Role; got != raft.Candidate {
		t.Fatalf("role after timeout = %v, want Candidate", got)
	}

	actions = node.Step(raft.HardStatePersisted{PersistenceID: 1})
	wantRequests := []raft.Action{
		raft.SendVoteRequest{To: "node-2", Request: raft.VoteRequest{From: "node-1", Term: 1}},
		raft.SendVoteRequest{To: "node-3", Request: raft.VoteRequest{From: "node-1", Term: 1}},
	}
	if !reflect.DeepEqual(actions, wantRequests) {
		t.Fatalf("persistence actions = %#v, want %#v", actions, wantRequests)
	}
}

func TestGrantedVoteResponseWaitsForPersistence(t *testing.T) {
	node := raft.NewNode("node-2", []raft.NodeID{"node-1", "node-3"})

	actions := node.Step(raft.VoteRequest{From: "node-1", Term: 1})
	wantPersistence := []raft.Action{raft.PersistHardState{
		PersistenceID: 1,
		Term:          1,
		VotedFor:      "node-1",
	}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("vote request actions = %#v, want %#v", actions, wantPersistence)
	}

	actions = node.Step(raft.HardStatePersisted{PersistenceID: 1})
	wantResponse := []raft.Action{raft.SendVoteResponse{
		To:       "node-1",
		Response: raft.VoteResponse{From: "node-2", Term: 1, Granted: true},
	}}
	if !reflect.DeepEqual(actions, wantResponse) {
		t.Fatalf("persistence actions = %#v, want %#v", actions, wantResponse)
	}
}

func TestCandidateBecomesLeaderWithAQuorum(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})

	actions := node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	want := []raft.Action{raft.BecameLeader{Term: 1}}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("vote response actions = %#v, want %#v", actions, want)
	}
	if got := node.State().Role; got != raft.Leader {
		t.Fatalf("role = %v, want Leader", got)
	}
}
