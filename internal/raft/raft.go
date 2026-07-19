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

// EntryType identifies the replicated command carried by a log entry.
type EntryType uint8

const (
	EntryNoOp EntryType = iota
)

// LogEntry is one position in the replicated Raft log.
type LogEntry struct {
	Index uint64
	Term  uint64
	Type  EntryType
}

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

// LogEntriesPersisted reports that appended log entries are durable.
type LogEntriesPersisted struct{ PersistenceID uint64 }

func (LogEntriesPersisted) isEvent() {}

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

// AppendEntries carries ordered log entries and also serves as a heartbeat
// when Entries is empty.
type AppendEntries struct {
	From         NodeID
	Term         uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

func (AppendEntries) isEvent() {}

// AppendEntriesResponse reports the durable replicated prefix of a Follower.
type AppendEntriesResponse struct {
	From       NodeID
	Term       uint64
	Success    bool
	MatchIndex uint64
}

func (AppendEntriesResponse) isEvent() {}

// PersistHardState asks the runtime to durably store the current Term and vote.
type PersistHardState struct {
	PersistenceID uint64
	Term          uint64
	VotedFor      NodeID
}

func (PersistHardState) isAction() {}

// PersistLogEntries asks the runtime to append and sync entries in order.
type PersistLogEntries struct {
	PersistenceID uint64
	Entries       []LogEntry
}

func (PersistLogEntries) isAction() {}

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

// SendAppendEntries sends replication or heartbeat traffic to one peer.
type SendAppendEntries struct {
	To      NodeID
	Request AppendEntries
}

func (SendAppendEntries) isAction() {}

// SendAppendEntriesResponse acknowledges or rejects one append request.
type SendAppendEntriesResponse struct {
	To       NodeID
	Response AppendEntriesResponse
}

func (SendAppendEntriesResponse) isAction() {}

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

// ApplyEntry asks the runtime to apply one newly committed entry.
type ApplyEntry struct{ Entry LogEntry }

func (ApplyEntry) isAction() {}

// BecameReadReady reports that the Leader's current-Term no-op is applied.
type BecameReadReady struct{ Term uint64 }

func (BecameReadReady) isAction() {}

// State is a read-only snapshot used by runtimes and deterministic assertions.
type State struct {
	ID           NodeID
	Role         Role
	Term         uint64
	VotedFor     NodeID
	LeaderID     NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	CommitIndex  uint64
	LastApplied  uint64
	ReadReady    bool
}

// HardState is the election state restored before a Node processes events.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

type pendingPersistence struct {
	term     uint64
	votedFor NodeID
	after    []Action
}

type logPersistencePurpose uint8

const (
	persistLeaderNoOp logPersistencePurpose = iota
	persistFollowerAppend
)

type pendingLogPersistence struct {
	purpose      logPersistencePurpose
	term         uint64
	leader       NodeID
	lastIndex    uint64
	leaderCommit uint64
}

// Node consumes one event at a time and emits effects without performing I/O.
type Node struct {
	id              NodeID
	peers           []NodeID
	role            Role
	term            uint64
	votedFor        NodeID
	leaderID        NodeID
	lastLogIndex    uint64
	lastLogTerm     uint64
	durableLogIndex uint64
	log             []LogEntry
	commitIndex     uint64
	lastApplied     uint64
	readReady       bool
	votes           map[NodeID]struct{}
	preVotes        map[NodeID]struct{}
	activePeers     map[NodeID]struct{}
	matchIndex      map[NodeID]uint64

	// recentLeaderContact is cleared only when the election timer fires. It
	// prevents a healthy Follower from helping an isolated peer disrupt a Leader.
	recentLeaderContact bool

	durableTerm        uint64
	durableVotedFor    NodeID
	nextPersistence    uint64
	pending            map[uint64]pendingPersistence
	nextLogPersistence uint64
	pendingLog         map[uint64]pendingLogPersistence
}

// NewNode creates a Follower with an empty log and no durable election state.
// Peers must exclude id.
func NewNode(id NodeID, peers []NodeID) *Node {
	return NewNodeWithHardState(id, peers, HardState{})
}

// NewNodeWithHardState creates a Follower from state recovered by the runtime.
// Recovered state is durable by definition, so a vote cannot be granted again
// in the same Term after restart.
func NewNodeWithHardState(id NodeID, peers []NodeID, hardState HardState) *Node {
	orderedPeers := append([]NodeID(nil), peers...)
	sort.Slice(orderedPeers, func(i, j int) bool { return orderedPeers[i] < orderedPeers[j] })
	return &Node{
		id:              id,
		peers:           orderedPeers,
		role:            Follower,
		term:            hardState.Term,
		votedFor:        hardState.VotedFor,
		durableTerm:     hardState.Term,
		durableVotedFor: hardState.VotedFor,
		votes:           make(map[NodeID]struct{}),
		preVotes:        make(map[NodeID]struct{}),
		activePeers:     make(map[NodeID]struct{}),
		matchIndex:      make(map[NodeID]uint64),
		pending:         make(map[uint64]pendingPersistence),
		pendingLog:      make(map[uint64]pendingLogPersistence),
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
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		ReadReady:    n.readReady,
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
	case LogEntriesPersisted:
		return n.logEntriesPersisted(event)
	case PreVoteRequest:
		return n.handlePreVoteRequest(event)
	case PreVoteResponse:
		return n.handlePreVoteResponse(event)
	case VoteRequest:
		return n.handleVoteRequest(event)
	case VoteResponse:
		return n.handleVoteResponse(event)
	case AppendEntries:
		return n.handleAppendEntries(event)
	case AppendEntriesResponse:
		return n.handleAppendEntriesResponse(event)
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
	n.readReady = false
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

func (n *Node) logEntriesPersisted(event LogEntriesPersisted) []Action {
	pending, ok := n.pendingLog[event.PersistenceID]
	if !ok {
		return nil
	}
	delete(n.pendingLog, event.PersistenceID)
	if pending.lastIndex > n.durableLogIndex {
		n.durableLogIndex = pending.lastIndex
	}

	switch pending.purpose {
	case persistLeaderNoOp:
		if n.role != Leader || n.term != pending.term || n.lastLogIndex < pending.lastIndex {
			return nil
		}
		return n.sendAppendEntries()
	case persistFollowerAppend:
		if n.term != pending.term || n.role != Follower || n.leaderID != pending.leader {
			return nil
		}
		actions := n.advanceFollowerCommit(pending.leaderCommit)
		return append(actions, n.appendEntriesResponse(pending.leader, true, pending.lastIndex)...)
	default:
		return nil
	}
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
	n.matchIndex = map[NodeID]uint64{n.id: n.lastLogIndex + 1}
	n.readReady = false
	entry := LogEntry{Index: n.lastLogIndex + 1, Term: n.term, Type: EntryNoOp}
	n.appendEntries([]LogEntry{entry})
	actions := []Action{BecameLeader{Term: n.term}, ResetHeartbeatTimer{}, ResetCheckQuorumTimer{}}
	return append(actions, n.persistLog([]LogEntry{entry}, pendingLogPersistence{
		purpose:   persistLeaderNoOp,
		term:      n.term,
		lastIndex: entry.Index,
	})...)
}

func (n *Node) heartbeatRound() []Action {
	if n.role != Leader {
		return nil
	}
	return append([]Action{ResetHeartbeatTimer{}}, n.sendAppendEntries()...)
}

func (n *Node) sendAppendEntries() []Action {
	actions := make([]Action, 0, len(n.peers))
	durableLastIndex := min(n.durableLogIndex, n.lastLogIndex)
	for _, peer := range n.peers {
		nextIndex := n.matchIndex[peer] + 1
		if nextIndex > durableLastIndex+1 {
			nextIndex = durableLastIndex + 1
		}
		prevIndex := nextIndex - 1
		request := AppendEntries{
			From:         n.id,
			Term:         n.term,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  n.termAt(prevIndex),
			Entries:      n.entriesBetween(nextIndex, durableLastIndex),
			LeaderCommit: n.commitIndex,
		}
		actions = append(actions, SendAppendEntries{To: peer, Request: request})
	}
	return actions
}

func (n *Node) handleAppendEntries(request AppendEntries) []Action {
	if request.Term < n.term {
		return n.appendEntriesResponse(request.From, false, n.lastLogIndex)
	}
	termChanged := request.Term > n.term
	if termChanged {
		n.becomeFollower(request.Term, request.From)
	} else {
		n.role = Follower
		n.leaderID = request.From
		n.readReady = false
	}
	n.recentLeaderContact = true
	after := []Action{ResetElectionTimer{}}
	if !n.matches(request.PrevLogIndex, request.PrevLogTerm) {
		after = append(after, n.appendEntriesResponse(request.From, false, n.lastLogIndex)...)
		if termChanged {
			return n.persist(after)
		}
		return after
	}

	newEntries, ok := n.unseenEntries(request.PrevLogIndex, request.Entries)
	if !ok {
		after = append(after, n.appendEntriesResponse(request.From, false, n.lastLogIndex)...)
		if termChanged {
			return n.persist(after)
		}
		return after
	}
	if len(newEntries) > 0 {
		n.appendEntries(newEntries)
		after = append(after, n.persistLog(newEntries, pendingLogPersistence{
			purpose:      persistFollowerAppend,
			term:         request.Term,
			leader:       request.From,
			lastIndex:    n.lastLogIndex,
			leaderCommit: request.LeaderCommit,
		})...)
	} else {
		if n.lastLogIndex > n.durableLogIndex {
			if termChanged {
				return n.persist(after)
			}
			return after
		}
		after = append(after, n.advanceFollowerCommit(request.LeaderCommit)...)
		after = append(after, n.appendEntriesResponse(request.From, true, n.lastLogIndex)...)
	}
	if termChanged {
		return n.persist(after)
	}
	return after
}

func (n *Node) handleAppendEntriesResponse(response AppendEntriesResponse) []Action {
	if response.Term > n.term {
		n.becomeFollower(response.Term, "")
		return n.persist(nil)
	}
	if n.role != Leader || response.Term != n.term {
		return nil
	}
	n.activePeers[response.From] = struct{}{}
	if !response.Success {
		return nil
	}
	if response.MatchIndex > n.matchIndex[response.From] {
		n.matchIndex[response.From] = response.MatchIndex
	}
	oldCommit := n.commitIndex
	actions := n.advanceLeaderCommit()
	if n.commitIndex > oldCommit {
		actions = append(actions, n.sendAppendEntries()...)
	}
	return actions
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
	n.matchIndex = make(map[NodeID]uint64)
	n.readReady = false
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

func (n *Node) appendEntriesResponse(to NodeID, success bool, matchIndex uint64) []Action {
	return []Action{SendAppendEntriesResponse{To: to, Response: AppendEntriesResponse{
		From:       n.id,
		Term:       n.term,
		Success:    success,
		MatchIndex: matchIndex,
	}}}
}

func (n *Node) persist(after []Action) []Action {
	n.nextPersistence++
	id := n.nextPersistence
	n.pending[id] = pendingPersistence{term: n.term, votedFor: n.votedFor, after: after}
	return []Action{PersistHardState{PersistenceID: id, Term: n.term, VotedFor: n.votedFor}}
}

func (n *Node) persistLog(entries []LogEntry, pending pendingLogPersistence) []Action {
	n.nextLogPersistence++
	id := n.nextLogPersistence
	n.pendingLog[id] = pending
	return []Action{PersistLogEntries{PersistenceID: id, Entries: append([]LogEntry(nil), entries...)}}
}

func (n *Node) appendEntries(entries []LogEntry) {
	n.log = append(n.log, entries...)
	n.lastLogIndex = n.log[len(n.log)-1].Index
	n.lastLogTerm = n.log[len(n.log)-1].Term
}

func (n *Node) matches(index, term uint64) bool {
	return index == 0 && term == 0 || index <= n.lastLogIndex && n.termAt(index) == term
}

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 || index > uint64(len(n.log)) {
		return 0
	}
	return n.log[index-1].Term
}

func (n *Node) entriesBetween(first, last uint64) []LogEntry {
	if first == 0 || first > last || last > n.lastLogIndex {
		return nil
	}
	return append([]LogEntry(nil), n.log[first-1:last]...)
}

func (n *Node) unseenEntries(prevIndex uint64, entries []LogEntry) ([]LogEntry, bool) {
	for offset, entry := range entries {
		wantIndex := prevIndex + uint64(offset) + 1
		if entry.Index != wantIndex {
			return nil, false
		}
		if entry.Index <= n.lastLogIndex {
			if n.termAt(entry.Index) != entry.Term {
				return nil, false
			}
			continue
		}
		if entry.Index != n.lastLogIndex+1 {
			return nil, false
		}
		return append([]LogEntry(nil), entries[offset:]...), true
	}
	return nil, true
}

func (n *Node) advanceLeaderCommit() []Action {
	for index := n.lastLogIndex; index > n.commitIndex; index-- {
		if n.termAt(index) != n.term {
			continue
		}
		replicated := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= index {
				replicated++
			}
		}
		if replicated >= n.quorum() {
			n.commitIndex = index
			return n.applyCommitted()
		}
	}
	return nil
}

func (n *Node) advanceFollowerCommit(leaderCommit uint64) []Action {
	if leaderCommit > n.commitIndex {
		n.commitIndex = min(leaderCommit, n.lastLogIndex)
	}
	return n.applyCommitted()
}

func (n *Node) applyCommitted() []Action {
	actions := make([]Action, 0, n.commitIndex-n.lastApplied+1)
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		actions = append(actions, ApplyEntry{Entry: entry})
		if n.role == Leader && !n.readReady && entry.Type == EntryNoOp && entry.Term == n.term {
			n.readReady = true
			actions = append(actions, BecameReadReady{Term: n.term})
		}
	}
	return actions
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }
