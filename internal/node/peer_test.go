package node

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"os"
	"reflect"
	"strings"
	"testing"

	quorumkvv1 "github.com/Het-Jethva/quorumkv/gen/quorumkv/v1"
	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/snapshot"
	"google.golang.org/grpc"
)

type recordingPeerClient struct {
	requests []*quorumkvv1.SendRequest
}

func (c *recordingPeerClient) Handshake(context.Context, *quorumkvv1.HandshakeRequest, ...grpc.CallOption) (*quorumkvv1.HandshakeResponse, error) {
	return nil, errors.New("unexpected Handshake")
}

func (c *recordingPeerClient) Send(_ context.Context, request *quorumkvv1.SendRequest, _ ...grpc.CallOption) (*quorumkvv1.SendResponse, error) {
	c.requests = append(c.requests, request)
	return &quorumkvv1.SendResponse{}, nil
}

type countedSnapshotReader struct {
	snapshotReader
	closes *int
}

func (r *countedSnapshotReader) Close() error {
	*r.closes++
	return r.snapshotReader.Close()
}

func TestPeerTransportStreamsAndSharesValidatedSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	name, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 12, IncludedTerm: 5,
		Values: map[string][]byte{"large": bytes.Repeat([]byte("snapshot-data"), 20<<10)},
	})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	encoded, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read encoded Snapshot: %v", err)
	}
	cfg := config.Config{ClusterID: identity.ClusterID, Node: config.Node{ID: identity.NodeID, DataDir: directory}}
	transport := newPeerTransport(cfg)
	node2 := &recordingPeerClient{}
	node3 := &recordingPeerClient{}
	transport.clients["node-2"] = &peerClient{client: node2}
	transport.clients["node-3"] = &peerClient{client: node3}
	opens, closes := 0, 0
	transport.openSnapshot = func(directory string, index, term uint64) (snapshotReader, error) {
		opens++
		reader, err := snapshot.Open(directory, index, term)
		if err != nil {
			return nil, err
		}
		return &countedSnapshotReader{snapshotReader: reader, closes: &closes}, nil
	}

	var offset uint64
	for offset < uint64(len(encoded)) {
		for _, follower := range []raft.NodeID{"node-2", "node-3"} {
			if err := transport.sendSnapshot(context.Background(), raft.SendInstallSnapshot{
				To: follower, Term: 6, RequestID: 9, SnapshotIndex: 12, SnapshotTerm: 5, Offset: offset,
			}); err != nil {
				t.Fatalf("send chunk at %d to %s: %v", offset, follower, err)
			}
		}
		offset += 64 << 10
	}
	if opens != 1 {
		t.Fatalf("Snapshot opens = %d, want one full validation shared by followers", opens)
	}
	for follower, requests := range map[string][]*quorumkvv1.SendRequest{"node-2": node2.requests, "node-3": node3.requests} {
		var rebuilt []byte
		for index, request := range requests {
			chunk := request.GetInstallSnapshotRequest()
			wantOffset := uint64(index * (64 << 10))
			if chunk.Offset != wantOffset || chunk.SnapshotLength != uint64(len(encoded)) || chunk.SnapshotChecksum != crc32.ChecksumIEEE(encoded) {
				t.Fatalf("%s chunk %d metadata = offset %d length %d checksum %08x", follower, index, chunk.Offset, chunk.SnapshotLength, chunk.SnapshotChecksum)
			}
			wantEnd := wantOffset + uint64(len(chunk.Data))
			if chunk.Done != (wantEnd == uint64(len(encoded))) {
				t.Fatalf("%s chunk %d Done = %t at end %d", follower, index, chunk.Done, wantEnd)
			}
			rebuilt = append(rebuilt, chunk.Data...)
		}
		if !bytes.Equal(rebuilt, encoded) {
			t.Fatalf("%s received bytes differ from encoded Snapshot", follower)
		}
	}
	transport.clients = make(map[raft.NodeID]*peerClient)
	if err := transport.close(); err != nil {
		t.Fatalf("close transport: %v", err)
	}
	if closes != 1 {
		t.Fatalf("Snapshot closes = %d, want one", closes)
	}
}

type memorySnapshotReader struct {
	data     []byte
	checksum uint32
	closes   int
}

func (r *memorySnapshotReader) Length() uint64   { return uint64(len(r.data)) }
func (r *memorySnapshotReader) Checksum() uint32 { return r.checksum }
func (r *memorySnapshotReader) ReadAt(data []byte, offset int64) (int, error) {
	return bytes.NewReader(r.data).ReadAt(data, offset)
}
func (r *memorySnapshotReader) Close() error { r.closes++; return nil }

func TestPeerTransportSnapshotEvictionAndShutdown(t *testing.T) {
	transport := newPeerTransport(config.Config{})
	readers := []*memorySnapshotReader{{data: []byte("first")}, {data: []byte("second")}}
	next := 0
	transport.openSnapshot = func(string, uint64, uint64) (snapshotReader, error) {
		reader := readers[next]
		next++
		return reader, nil
	}
	first, err := transport.acquireSnapshot(1, 1)
	if err != nil {
		t.Fatalf("acquire first Snapshot: %v", err)
	}
	transport.releaseSnapshot(first)
	second, err := transport.acquireSnapshot(2, 2)
	if err != nil {
		t.Fatalf("acquire replacement Snapshot: %v", err)
	}
	transport.releaseSnapshot(second)
	if readers[0].closes != 1 || readers[1].closes != 0 {
		t.Fatalf("closes after eviction = %d/%d, want 1/0", readers[0].closes, readers[1].closes)
	}
	if err := transport.close(); err != nil {
		t.Fatalf("close transport: %v", err)
	}
	if readers[1].closes != 1 {
		t.Fatalf("active Snapshot closes at shutdown = %d, want 1", readers[1].closes)
	}
}

func TestPeerTransportRejectsInvalidSnapshotBeforeSending(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	name, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 4, IncludedTerm: 2, Values: map[string][]byte{"key": []byte("value")},
	})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	transport := newPeerTransport(config.Config{ClusterID: identity.ClusterID, Node: config.Node{ID: identity.NodeID, DataDir: directory}})
	client := &recordingPeerClient{}
	transport.clients["node-2"] = &peerClient{client: client}
	action := raft.SendInstallSnapshot{To: "node-2", SnapshotIndex: 5, SnapshotTerm: 2}
	if err := transport.sendSnapshot(context.Background(), action); err == nil || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("send wrong-position Snapshot error = %v", err)
	}

	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read Snapshot: %v", err)
	}
	contents[len(contents)-1] ^= 0xff
	if err := os.WriteFile(name, contents, 0o600); err != nil {
		t.Fatalf("corrupt Snapshot: %v", err)
	}
	action.SnapshotIndex = 4
	if err := transport.sendSnapshot(context.Background(), action); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("send corrupt Snapshot error = %v", err)
	}
	if len(client.requests) != 0 {
		t.Fatalf("invalid Snapshots emitted %d chunks, want none", len(client.requests))
	}
}

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
		raft.SendAppendEntries{To: "node-2", Request: raft.AppendEntries{From: "node-1", Term: 2, RequestID: 17, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []raft.LogEntry{
			{Index: 3, Term: 2, Type: raft.EntrySet, SessionID: raft.SessionID{1}, Sequence: 7, Key: "opaque", Value: []byte{0, 255}},
			{Index: 4, Term: 2, Type: raft.EntryDelete, SessionID: raft.SessionID{1}, Sequence: 8, Key: "opaque"},
		}, LeaderCommit: 2, ReadID: 11}},
		raft.SendAppendEntriesResponse{To: "node-2", Response: raft.AppendEntriesResponse{From: "node-1", Term: 2, RequestID: 17, MatchIndex: 3, ConflictTerm: 1, ConflictIndex: 2, ReadID: 11}},
	}
	for _, action := range actions {
		to, request, err := encodeRaftAction(cfg, action)
		if err != nil {
			t.Fatalf("encode %T: %v", action, err)
		}
		if to != "node-2" || request.FromNodeId != "node-1" || request.ToNodeId != "node-2" {
			t.Fatalf("encode %T route = %q/%q/%q", action, to, request.FromNodeId, request.ToNodeId)
		}
		decoded, err := decodeRaftMessage(request)
		if err != nil {
			t.Fatalf("decode %T: %v", action, err)
		}
		if appendAction, ok := action.(raft.SendAppendEntries); ok && !reflect.DeepEqual(decoded, appendAction.Request) {
			t.Fatalf("decoded AppendEntries = %#v, want %#v", decoded, appendAction.Request)
		}
		if responseAction, ok := action.(raft.SendAppendEntriesResponse); ok && !reflect.DeepEqual(decoded, responseAction.Response) {
			t.Fatalf("decoded AppendEntries response = %#v, want %#v", decoded, responseAction.Response)
		}
	}
}

func TestPeerSendReturnsAfterQueueingWithoutWaitingForRuntime(t *testing.T) {
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
	request := &quorumkvv1.SendRequest{
		ProtocolVersion: peerProtocolVersion,
		ClusterId:       "cluster-1",
		FromNodeId:      "node-2",
		ToNodeId:        "node-1",
		Message: &quorumkvv1.SendRequest_PreVoteRequest{PreVoteRequest: &quorumkvv1.PreVoteRequest{
			Term: 1,
		}},
	}
	if _, err := n.Send(context.Background(), request); err != nil {
		t.Fatalf("Send() before runtime dequeue: %v", err)
	}
	select {
	case input := <-n.events:
		if _, ok := input.event.(raft.PreVoteRequest); !ok {
			t.Fatalf("queued event = %T, want PreVoteRequest", input.event)
		}
	default:
		t.Fatal("Send() returned without queueing the peer event")
	}
}
