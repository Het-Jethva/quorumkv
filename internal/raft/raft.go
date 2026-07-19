// Package raft implements the deterministic QuorumKV consensus state machine.
package raft

import "sort"

// NodeID identifies one Node in a Cluster.
type NodeID string

// Role is a Node's current participation state.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

// Event is one input to the deterministic state machine.
type Event interface{ isEvent() }

// Action is one effect for the runtime to perform outside the Raft core.
type Action interface{ isAction() }

// ElectionTimeout reports that the runtime's election timer fired.
type ElectionTimeout struct{}

func (ElectionTimeout) isEvent() {}

// HeartbeatTimeout asks a Leader to send its next heartbeat round.
type HeartbeatTimeout struct{}

func (HeartbeatTimeout) isEvent() {}

// CheckQuorumTimeout ends the current Leader contact window.
type CheckQuorumTimeout struct{}

func (CheckQuorumTimeout) isEvent() {}

// HardStatePersisted reports that a requested hard-state update is durable.
type HardStatePersisted struct{ PersistenceID uint64 }

func (HardStatePersisted) isEvent() {}

// PreVoteRequest checks whether an election for Term would be viable without
// changing any Node's current Term or durable vote.
type PreVoteRequest struct {
	From         NodeID
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (PreVoteRequest) isEvent() {}

// PreVoteResponse reports a peer's current Term and pre-vote decision.
type PreVoteResponse struct {
	From        NodeID
	Term        uint64
	CurrentTerm uint64
	Granted     bool
}

func (PreVoteResponse) isEvent() {}

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

// Heartbeat is current Leader contact. Log replication will extend this
// message in a later slice.
type Heartbeat struct {
	From NodeID
	Term uint64
}

func (Heartbeat) isEvent() {}

// HeartbeatResponse reports that a peer heard from a Leader in a Term.
type HeartbeatResponse struct {
	From    NodeID
	Term    uint64
	Granted bool
}

func (HeartbeatResponse) isEvent() {}

// PersistHardState asks the runtime to durably store the current Term and vote.
type PersistHardState struct {
	PersistenceID uint64
	Term          uint64
	VotedFor      NodeID
}

func (PersistHardState) isAction() {}

// SendPreVoteRequest sends a pre-vote request to one peer.
type SendPreVoteRequest struct {
	To      NodeID
	Request PreVoteRequest
}

func (SendPreVoteRequest) isAction() {}

// SendPreVoteResponse sends a pre-vote decision to one peer.
type SendPreVoteResponse struct {
	To       NodeID
	Response PreVoteResponse
}

func (SendPreVoteResponse) isAction() {}

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

// SendHeartbeat sends Leader contact to one peer.
type SendHeartbeat struct {
	To        NodeID
	Heartbeat Heartbeat
}

func (SendHeartbeat) isAction() {}

// SendHeartbeatResponse acknowledges or rejects Leader contact.
type SendHeartbeatResponse struct {
	To       NodeID
	Response HeartbeatResponse
}

func (SendHeartbeatResponse) isAction() {}

// ResetElectionTimer asks the runtime to choose and schedule a new randomized
// election timeout.
type ResetElectionTimer struct{}

func (ResetElectionTimer) isAction() {}

// ResetHeartbeatTimer schedules the Leader's next heartbeat round.
type ResetHeartbeatTimer struct{}

func (ResetHeartbeatTimer) isAction() {}

// ResetCheckQuorumTimer starts a fresh Leader contact window.
type ResetCheckQuorumTimer struct{}

func (ResetCheckQuorumTimer) isAction() {}

// BecameLeader reports the observable result of winning an election.
type BecameLeader struct{ Term uint64 }

func (BecameLeader) isAction() {}

// LostLeadership reports that check-quorum removed local authority.
type LostLeadership struct{ Term uint64 }

func (LostLeadership) isAction() {}

// State is a read-only snapshot used by runtimes and deterministic assertions.
type State struct {
	ID           NodeID
	Role         Role
	Term         uint64
	VotedFor     NodeID
	LeaderID     NodeID
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
	leaderID     NodeID
	lastLogIndex uint64
	lastLogTerm  uint64
	votes        map[NodeID]struct{}
	preVotes     map[NodeID]struct{}
	activePeers  map[NodeID]struct{}

	// recentLeaderContact is cleared only when the election timer fires. It
	// prevents a healthy Follower from helping an isolated peer disrupt a Leader.
	recentLeaderContact bool

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
		id:          id,
		peers:       orderedPeers,
		role:        Follower,
		votes:       make(map[NodeID]struct{}),
		preVotes:    make(map[NodeID]struct{}),
		activePeers: make(map[NodeID]struct{}),
		pending:     make(map[uint64]pendingPersistence),
	}
}

// State returns the Node's current deterministic state.
func (n *Node) State() State {
	return State{
		ID:           n.id,
		Role:         n.role,
		Term:         n.term,
		VotedFor:     n.votedFor,
		LeaderID:     n.leaderID,
		LastLogIndex: n.lastLogIndex,
		LastLogTerm:  n.lastLogTerm,
	}
}

// Step applies one event and returns actions in execution order.
func (n *Node) Step(event Event) []Action {
	switch event := event.(type) {
	case ElectionTimeout:
		return n.startPreVote()
	case HeartbeatTimeout:
		return n.heartbeatRound()
	case CheckQuorumTimeout:
		return n.checkQuorum()
	case HardStatePersisted:
		return n.hardStatePersisted(event)
	case PreVoteRequest:
		return n.handlePreVoteRequest(event)
	case PreVoteResponse:
		return n.handlePreVoteResponse(event)
	case VoteRequest:
		return n.handleVoteRequest(event)
	case VoteResponse:
		return n.handleVoteResponse(event)
	case Heartbeat:
		return n.handleHeartbeat(event)
	case HeartbeatResponse:
		return n.handleHeartbeatResponse(event)
	default:
		return nil
	}
}

func (n *Node) startPreVote() []Action {
	if n.role == Leader {
		return nil
	}
	n.recentLeaderContact = false
	n.leaderID = ""
	n.role = PreCandidate
	n.preVotes = map[NodeID]struct{}{n.id: {}}
	term := n.term + 1
	actions := make([]Action, 0, len(n.peers)+1)
	actions = append(actions, ResetElectionTimer{})
	for _, peer := range n.peers {
		actions = append(actions, SendPreVoteRequest{To: peer, Request: PreVoteRequest{From: n.id, Term: term, LastLogIndex: n.lastLogIndex, LastLogTerm: n.lastLogTerm}})
	}
	if n.quorum() == 1 {
		return append(actions, n.startElection()...)
	}
	return actions
}

func (n *Node) handlePreVoteRequest(request PreVoteRequest) []Action {
	grant := request.Term >= n.term+1 && !n.recentLeaderContact && n.candidateLogIsUpToDate(request.LastLogTerm, request.LastLogIndex)
	return []Action{SendPreVoteResponse{To: request.From, Response: PreVoteResponse{From: n.id, Term: request.Term, CurrentTerm: n.term, Granted: grant}}}
}

func (n *Node) handlePreVoteResponse(response PreVoteResponse) []Action {
	if response.CurrentTerm > n.term {
		n.becomeFollower(response.CurrentTerm, "")
		return n.persist(nil)
	}
	if n.role != PreCandidate || response.Term != n.term+1 || !response.Granted {
		return nil
	}
	n.preVotes[response.From] = struct{}{}
	if len(n.preVotes) < n.quorum() {
		return nil
	}
	return n.startElection()
}

func (n *Node) startElection() []Action {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.votes = map[NodeID]struct{}{n.id: {}}

	after := make([]Action, 0, len(n.peers)+1)
	after = append(after, ResetElectionTimer{})
	for _, peer := range n.peers {
		after = append(after, SendVoteRequest{To: peer, Request: VoteRequest{From: n.id, Term: n.term, LastLogIndex: n.lastLogIndex, LastLogTerm: n.lastLogTerm}})
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
		n.becomeFollower(request.Term, "")
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
		n.becomeFollower(response.Term, "")
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
	n.leaderID = n.id
	n.activePeers = make(map[NodeID]struct{})
	actions := []Action{BecameLeader{Term: n.term}, ResetHeartbeatTimer{}, ResetCheckQuorumTimer{}}
	return append(actions, n.sendHeartbeats()...)
}

func (n *Node) heartbeatRound() []Action {
	if n.role != Leader {
		return nil
	}
	return append([]Action{ResetHeartbeatTimer{}}, n.sendHeartbeats()...)
}

func (n *Node) sendHeartbeats() []Action {
	actions := make([]Action, 0, len(n.peers))
	for _, peer := range n.peers {
		actions = append(actions, SendHeartbeat{To: peer, Heartbeat: Heartbeat{From: n.id, Term: n.term}})
	}
	return actions
}

func (n *Node) handleHeartbeat(heartbeat Heartbeat) []Action {
	if heartbeat.Term < n.term {
		return n.heartbeatResponse(heartbeat.From, false)
	}
	termChanged := heartbeat.Term > n.term
	if termChanged {
		n.becomeFollower(heartbeat.Term, heartbeat.From)
	} else {
		n.role = Follower
		n.leaderID = heartbeat.From
	}
	n.recentLeaderContact = true
	response := append([]Action{ResetElectionTimer{}}, n.heartbeatResponse(heartbeat.From, true)...)
	if termChanged {
		return n.persist(response)
	}
	return response
}

func (n *Node) handleHeartbeatResponse(response HeartbeatResponse) []Action {
	if response.Term > n.term {
		n.becomeFollower(response.Term, "")
		return n.persist(nil)
	}
	if n.role == Leader && response.Term == n.term && response.Granted {
		n.activePeers[response.From] = struct{}{}
	}
	return nil
}

func (n *Node) checkQuorum() []Action {
	if n.role != Leader {
		return nil
	}
	if len(n.activePeers)+1 < n.quorum() {
		term := n.term
		n.becomeFollower(term, "")
		return []Action{ResetElectionTimer{}, LostLeadership{Term: term}}
	}
	n.activePeers = make(map[NodeID]struct{})
	return []Action{ResetCheckQuorumTimer{}}
}

func (n *Node) becomeFollower(term uint64, leader NodeID) {
	if term > n.term {
		n.votedFor = ""
	}
	n.role = Follower
	n.term = term
	n.leaderID = leader
	n.recentLeaderContact = leader != ""
	n.votes = make(map[NodeID]struct{})
	n.preVotes = make(map[NodeID]struct{})
	n.activePeers = make(map[NodeID]struct{})
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
	return []Action{SendVoteResponse{To: to, Response: VoteResponse{From: n.id, Term: n.term, Granted: granted}}}
}

func (n *Node) heartbeatResponse(to NodeID, granted bool) []Action {
	return []Action{SendHeartbeatResponse{To: to, Response: HeartbeatResponse{From: n.id, Term: n.term, Granted: granted}}}
}

func (n *Node) persist(after []Action) []Action {
	n.nextPersistence++
	id := n.nextPersistence
	n.pending[id] = pendingPersistence{term: n.term, votedFor: n.votedFor, after: after}
	return []Action{PersistHardState{PersistenceID: id, Term: n.term, VotedFor: n.votedFor}}
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }
