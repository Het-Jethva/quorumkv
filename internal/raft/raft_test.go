package raft_test

import (
	"reflect"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

func TestPreVoteDoesNotChangeTermAndElectionWaitsForPersistentSelfVote(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-3", "node-2"})

	actions := node.Step(raft.ElectionTimeout{})
	wantPreVote := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendPreVoteRequest{To: "node-2", Request: raft.PreVoteRequest{From: "node-1", Term: 1}},
		raft.SendPreVoteRequest{To: "node-3", Request: raft.PreVoteRequest{From: "node-1", Term: 1}},
	}
	if !reflect.DeepEqual(actions, wantPreVote) {
		t.Fatalf("timeout actions = %#v, want %#v", actions, wantPreVote)
	}
	if state := node.State(); state.Role != raft.PreCandidate || state.Term != 0 {
		t.Fatalf("state after timeout = %#v, want PreCandidate in Term 0", state)
	}

	actions = node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, CurrentTerm: 0, Granted: true})
	wantPersistence := []raft.Action{raft.PersistHardState{
		PersistenceID: 1,
		Term:          1,
		VotedFor:      "node-1",
	}}
	if !reflect.DeepEqual(actions, wantPersistence) {
		t.Fatalf("pre-vote response actions = %#v, want %#v", actions, wantPersistence)
	}
	if got := node.State().Role; got != raft.Candidate {
		t.Fatalf("role after pre-vote quorum = %v, want Candidate", got)
	}

	actions = node.Step(raft.HardStatePersisted{PersistenceID: 1})
	wantRequests := []raft.Action{
		raft.ResetElectionTimer{},
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
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})

	actions := node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})
	want := []raft.Action{
		raft.BecameLeader{Term: 1},
		raft.ResetHeartbeatTimer{},
		raft.ResetCheckQuorumTimer{},
		raft.SendHeartbeat{To: "node-2", Heartbeat: raft.Heartbeat{From: "node-1", Term: 1}},
		raft.SendHeartbeat{To: "node-3", Heartbeat: raft.Heartbeat{From: "node-1", Term: 1}},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("vote response actions = %#v, want %#v", actions, want)
	}
	if got := node.State().Role; got != raft.Leader {
		t.Fatalf("role = %v, want Leader", got)
	}
}

func TestFollowerResetsElectionTimerOnlyForCurrentTermLeaderContact(t *testing.T) {
	node := raft.NewNode("node-2", []raft.NodeID{"node-1", "node-3"})
	node.Step(raft.VoteRequest{From: "node-3", Term: 2})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})

	actions := node.Step(raft.Heartbeat{From: "node-1", Term: 1})
	wantStale := []raft.Action{raft.SendHeartbeatResponse{
		To:       "node-1",
		Response: raft.HeartbeatResponse{From: "node-2", Term: 2, Granted: false},
	}}
	if !reflect.DeepEqual(actions, wantStale) {
		t.Fatalf("stale heartbeat actions = %#v, want %#v", actions, wantStale)
	}

	actions = node.Step(raft.Heartbeat{From: "node-1", Term: 2})
	wantCurrent := []raft.Action{
		raft.ResetElectionTimer{},
		raft.SendHeartbeatResponse{To: "node-1", Response: raft.HeartbeatResponse{From: "node-2", Term: 2, Granted: true}},
	}
	if !reflect.DeepEqual(actions, wantCurrent) {
		t.Fatalf("current-Term heartbeat actions = %#v, want %#v", actions, wantCurrent)
	}
	if got := node.State().LeaderID; got != "node-1" {
		t.Fatalf("Leader ID = %q, want node-1", got)
	}
}

func TestLeaderSendsPeriodicHeartbeats(t *testing.T) {
	node := raft.NewNode("node-1", []raft.NodeID{"node-2", "node-3"})
	node.Step(raft.ElectionTimeout{})
	node.Step(raft.PreVoteResponse{From: "node-2", Term: 1, Granted: true})
	node.Step(raft.HardStatePersisted{PersistenceID: 1})
	node.Step(raft.VoteResponse{From: "node-2", Term: 1, Granted: true})

	actions := node.Step(raft.HeartbeatTimeout{})
	want := []raft.Action{
		raft.ResetHeartbeatTimer{},
		raft.SendHeartbeat{To: "node-2", Heartbeat: raft.Heartbeat{From: "node-1", Term: 1}},
		raft.SendHeartbeat{To: "node-3", Heartbeat: raft.Heartbeat{From: "node-1", Term: 1}},
	}
	if !reflect.DeepEqual(actions, want) {
		t.Fatalf("heartbeat timeout actions = %#v, want %#v", actions, want)
	}
}
