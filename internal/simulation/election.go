// Package simulation executes deterministic Raft events without real I/O or clocks.
package simulation

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

// Step records one event and the actions it produced.
type Step struct {
	Node    raft.NodeID
	Event   raft.Event
	Actions []raft.Action
}

// ElectionResult contains the elected Leader and the replayable trace.
type ElectionResult struct {
	Leader raft.NodeID
	Term   uint64
	Trace  []Step
}

type scheduledEvent struct {
	node  raft.NodeID
	event raft.Event
}

// RunElection executes a three-Node election. The seed selects which controlled
// election timeout fires, and therefore completely determines the resulting trace.
func RunElection(seed int64) (ElectionResult, error) {
	ids := []raft.NodeID{"node-1", "node-2", "node-3"}
	nodes := make(map[raft.NodeID]*raft.Node, len(ids))
	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		nodes[id] = raft.NewNode(id, peers)
	}

	random := rand.New(rand.NewSource(seed)) // #nosec G404 -- reproducibility, not security.
	queue := []scheduledEvent{{node: ids[random.Intn(len(ids))], event: raft.ElectionTimeout{}}}
	var trace []Step
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		actions := nodes[next.node].Step(next.event)
		trace = append(trace, Step{Node: next.node, Event: next.event, Actions: actions})

		if err := assertAtMostOneLeaderPerTerm(nodes); err != nil {
			return ElectionResult{Trace: trace}, err
		}
		for _, action := range actions {
			switch action := action.(type) {
			case raft.PersistHardState:
				queue = append(queue, scheduledEvent{
					node:  next.node,
					event: raft.HardStatePersisted{PersistenceID: action.PersistenceID},
				})
			case raft.SendVoteRequest:
				queue = append(queue, scheduledEvent{node: action.To, event: action.Request})
			case raft.SendVoteResponse:
				queue = append(queue, scheduledEvent{node: action.To, event: action.Response})
			case raft.BecameLeader:
				// The role change is already visible in Node.State.
			}
		}
	}

	leaders := leadersByTerm(nodes)
	if len(leaders) != 1 {
		return ElectionResult{Trace: trace}, fmt.Errorf("election finished with %d leaders, want 1", len(leaders))
	}
	for term, leaders := range leaders {
		return ElectionResult{Leader: leaders[0], Term: term, Trace: trace}, nil
	}
	panic("unreachable")
}

func assertAtMostOneLeaderPerTerm(nodes map[raft.NodeID]*raft.Node) error {
	for term, leaders := range leadersByTerm(nodes) {
		if len(leaders) > 1 {
			return fmt.Errorf("term %d has multiple leaders: %v", term, leaders)
		}
	}
	return nil
}

func leadersByTerm(nodes map[raft.NodeID]*raft.Node) map[uint64][]raft.NodeID {
	leaders := make(map[uint64][]raft.NodeID)
	for _, node := range nodes {
		state := node.State()
		if state.Role == raft.Leader {
			leaders[state.Term] = append(leaders[state.Term], state.ID)
		}
	}
	for _, termLeaders := range leaders {
		sort.Slice(termLeaders, func(i, j int) bool { return termLeaders[i] < termLeaders[j] })
	}
	return leaders
}
