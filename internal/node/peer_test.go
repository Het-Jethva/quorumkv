package node

import (
	"context"
	"strings"
	"testing"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
)

func TestHandshakeRejectsIncompatiblePeerIdentity(t *testing.T) {
	n := New(config.Config{
		ClusterID:          "cluster-1",
		ActiveSessionLimit: 10,
		Node:               config.Node{ID: "node-1"},
		Members: map[string]config.Member{
			"node-1": {},
			"node-2": {},
			"node-3": {},
		},
	})
	valid := func() *quorumkvv1.HandshakeRequest {
		return &quorumkvv1.HandshakeRequest{ProtocolVersion: peerProtocolVersion, ClusterId: "cluster-1", NodeId: "node-2", TargetNodeId: "node-1", ActiveSessionLimit: 10}
	}

	tests := []struct {
		name   string
		change func(*quorumkvv1.HandshakeRequest)
		detail string
	}{
		{name: "protocol version", change: func(request *quorumkvv1.HandshakeRequest) { request.ProtocolVersion++ }, detail: "require version 1"},
		{name: "Cluster Identity", change: func(request *quorumkvv1.HandshakeRequest) { request.ClusterId = "other-cluster" }, detail: "does not match"},
		{name: "unknown Node Identity", change: func(request *quorumkvv1.HandshakeRequest) { request.NodeId = "node-4" }, detail: "not a configured Cluster member"},
		{name: "target Node Identity", change: func(request *quorumkvv1.HandshakeRequest) { request.TargetNodeId = "node-3" }, detail: "targeted Node"},
		{name: "active Client Session limit", change: func(request *quorumkvv1.HandshakeRequest) { request.ActiveSessionLimit++ }, detail: "does not match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid()
			test.change(request)
			_, err := n.Handshake(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("Handshake() error = %v, want detail %q", err, test.detail)
			}
		})
	}
}

func TestPeerAdapterRoundTripsInternalRaftMessages(t *testing.T) {
	cfg := config.Config{ClusterID: "cluster-1", Node: config.Node{ID: "node-1"}}
	actions := []raft.Action{
		raft.SendPreVoteRequest{To: "node-2", Request: raft.PreVoteRequest{From: "node-1", Term: 2, LastLogIndex: 3, LastLogTerm: 1}},
		raft.SendPreVoteResponse{To: "node-2", Response: raft.PreVoteResponse{From: "node-1", Term: 2, CurrentTerm: 1, Granted: true}},
		raft.SendVoteRequest{To: "node-2", Request: raft.VoteRequest{From: "node-1", Term: 2, LastLogIndex: 3, LastLogTerm: 1}},
		raft.SendVoteResponse{To: "node-2", Response: raft.VoteResponse{From: "node-1", Term: 2, Granted: true}},
		raft.SendAppendEntries{To: "node-2", Request: raft.AppendEntries{From: "node-1", Term: 2, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []raft.LogEntry{{Index: 3, Term: 2, Type: raft.EntryNoOp}}, LeaderCommit: 2}},
		raft.SendAppendEntriesResponse{To: "node-2", Response: raft.AppendEntriesResponse{From: "node-1", Term: 2, Success: true, MatchIndex: 3}},
	}
	for _, action := range actions {
		to, request, err := encodeRaftAction(cfg, action)
		if err != nil {
			t.Fatalf("encode %T: %v", action, err)
		}
		if to != "node-2" || request.FromNodeId != "node-1" || request.ToNodeId != "node-2" {
			t.Fatalf("encode %T route = %q/%q/%q", action, to, request.FromNodeId, request.ToNodeId)
		}
		if _, err := decodeRaftMessage(request); err != nil {
			t.Fatalf("decode %T: %v", action, err)
		}
	}
}
