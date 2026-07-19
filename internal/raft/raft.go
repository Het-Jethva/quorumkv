// Package raft implements the deterministic QuorumKV consensus state machine.
package raft

import "sort"

// NodeID identifies one Node in a Cluster.
type NodeID string

// Role is a Node's current participation state.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// Event is one input to the deterministic state machine.
type Event interface {
	isEvent()
}

// Action is one effect for the runtime to perform outside the Raft core.
type Action interface {
	isAction()
}

// ElectionTimeout reports that the runtime's election timer fired.
type ElectionTimeout struct{}

func (ElectionTimeout) isEvent() {}

// HardStatePersisted reports that a previously requested hard-state update is durable.
type HardStatePersisted struct {
	PersistenceID uint64
}

func (HardStatePersisted) isEvent() {}

// VoteRequest asks a Node to vote in an election.
type VoteRequest struct {
	From         NodeID
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (VoteRequest) isEvent() {}

// VoteResponse reports whether a peer voted for this Node.
type VoteResponse struct {
	From    NodeID
	Term    uint64
	Granted bool
}

func (VoteResponse) isEvent() {}

// PersistHardState asks the runtime to durably store the current Term and vote.
type PersistHardState struct {
	PersistenceID uint64
	Term          uint64
	VotedFor      NodeID
}

func (PersistHardState) isAction() {}

// SendVoteRequest sends a vote request to one peer.
type SendVoteRequest struct {
	To      NodeID
	Request VoteRequest
}

func (SendVoteRequest) isAction() {}

// SendVoteResponse sends a vote decision to one peer.
type SendVoteResponse struct {
	To       NodeID
	Response VoteResponse
}

func (SendVoteResponse) isAction() {}

// BecameLeader reports the observable result of winning an election.
type BecameLeader struct {
	Term uint64
}

func (BecameLeader) isAction() {}

// State is a read-only snapshot used by runtimes and deterministic assertions.
type State struct {
	ID           NodeID
	Role         Role
	Term         uint64
	VotedFor     NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type pendingPersistence struct {
	term     uint64
	votedFor NodeID
	after    []Action
}

// Node consumes one event at a time and emits effects without performing I/O.
type Node struct {
	id           NodeID
	peers        []NodeID
	role         Role
	term         uint64
	votedFor     NodeID
	lastLogIndex uint64
	lastLogTerm  uint64
	votes        map[NodeID]struct{}

	durableTerm     uint64
	durableVotedFor NodeID
	nextPersistence uint64
	pending         map[uint64]pendingPersistence
}

// NewNode creates a Follower with an empty log. Peers must exclude id.
func NewNode(id NodeID, peers []NodeID) *Node {
	orderedPeers := append([]NodeID(nil), peers...)
	sort.Slice(orderedPeers, func(i, j int) bool { return orderedPeers[i] < orderedPeers[j] })
	return &Node{
		id:      id,
		peers:   orderedPeers,
		role:    Follower,
		votes:   make(map[NodeID]struct{}),
		pending: make(map[uint64]pendingPersistence),
	}
}

// State returns the Node's current deterministic state.
func (n *Node) State() State {
	return State{
		ID:           n.id,
		Role:         n.role,
		Term:         n.term,
		VotedFor:     n.votedFor,
		LastLogIndex: n.lastLogIndex,
		LastLogTerm:  n.lastLogTerm,
	}
}

// Step applies one event and returns actions in execution order.
func (n *Node) Step(event Event) []Action {
	switch event := event.(type) {
	case ElectionTimeout:
		return n.startElection()
	case HardStatePersisted:
		return n.hardStatePersisted(event)
	case VoteRequest:
		return n.handleVoteRequest(event)
	case VoteResponse:
		return n.handleVoteResponse(event)
	default:
		return nil
	}
}

func (n *Node) startElection() []Action {
	if n.role == Leader {
		return nil
	}

	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.votes = map[NodeID]struct{}{n.id: {}}

	after := make([]Action, 0, len(n.peers))
	for _, peer := range n.peers {
		after = append(after, SendVoteRequest{
			To: peer,
			Request: VoteRequest{
				From:         n.id,
				Term:         n.term,
				LastLogIndex: n.lastLogIndex,
				LastLogTerm:  n.lastLogTerm,
			},
		})
	}
	return n.persist(after)
}

func (n *Node) hardStatePersisted(event HardStatePersisted) []Action {
	pending, ok := n.pending[event.PersistenceID]
	if !ok {
		return nil
	}
	delete(n.pending, event.PersistenceID)
	n.durableTerm = pending.term
	n.durableVotedFor = pending.votedFor

	// A later Term supersedes effects waiting behind an older persistence barrier.
	if pending.term != n.term || pending.votedFor != n.votedFor {
		return nil
	}
	return pending.after
}

func (n *Node) handleVoteRequest(request VoteRequest) []Action {
	if request.Term < n.term {
		return n.voteResponse(request.From, false)
	}

	termChanged := request.Term > n.term
	if termChanged {
		n.term = request.Term
		n.role = Follower
		n.votedFor = ""
		n.votes = make(map[NodeID]struct{})
	}

	canVote := n.votedFor == "" || n.votedFor == request.From
	grant := canVote && n.candidateLogIsUpToDate(request.LastLogTerm, request.LastLogIndex)
	if grant {
		n.votedFor = request.From
	}

	response := n.voteResponse(request.From, grant)
	if termChanged || (grant && !n.voteIsDurable(request.From)) {
		return n.persist(response)
	}
	return response
}

func (n *Node) handleVoteResponse(response VoteResponse) []Action {
	if response.Term > n.term {
		n.term = response.Term
		n.role = Follower
		n.votedFor = ""
		n.votes = make(map[NodeID]struct{})
		return n.persist(nil)
	}
	if n.role != Candidate || response.Term != n.term || !response.Granted {
		return nil
	}

	n.votes[response.From] = struct{}{}
	if len(n.votes) < n.quorum() {
		return nil
	}
	n.role = Leader
	return []Action{BecameLeader{Term: n.term}}
}

func (n *Node) candidateLogIsUpToDate(term, index uint64) bool {
	return logIsAtLeastAsUpToDate(term, index, n.lastLogTerm, n.lastLogIndex)
}

func logIsAtLeastAsUpToDate(candidateTerm, candidateIndex, localTerm, localIndex uint64) bool {
	return candidateTerm > localTerm || (candidateTerm == localTerm && candidateIndex >= localIndex)
}

func (n *Node) voteIsDurable(candidate NodeID) bool {
	return n.durableTerm == n.term && n.durableVotedFor == candidate
}

func (n *Node) voteResponse(to NodeID, granted bool) []Action {
	return []Action{SendVoteResponse{
		To: to,
		Response: VoteResponse{
			From:    n.id,
			Term:    n.term,
			Granted: granted,
		},
	}}
}

func (n *Node) persist(after []Action) []Action {
	n.nextPersistence++
	id := n.nextPersistence
	n.pending[id] = pendingPersistence{
		term:     n.term,
		votedFor: n.votedFor,
		after:    after,
	}
	return []Action{PersistHardState{
		PersistenceID: id,
		Term:          n.term,
		VotedFor:      n.votedFor,
	}}
}

func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}
