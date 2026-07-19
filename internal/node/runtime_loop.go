package node

import (
	"context"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/Het-Jethva/quorumkv/internal/raft"
)

const (
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 500 * time.Millisecond
	electionTimeoutMax = time.Second
	checkQuorumWindow  = time.Second
)

func (n *Node) runRaft(ctx context.Context, runtime *raftRuntime, transport *peerTransport) error {
	random := rand.New(rand.NewSource(time.Now().UnixNano() + nodeSeed(n.config.Node.ID))) // #nosec G404 -- election jitter is not security-sensitive.
	electionTimer := time.NewTimer(randomElectionTimeout(random))
	defer electionTimer.Stop()

	var heartbeatTimer, quorumTimer *time.Timer
	var heartbeatC, quorumC <-chan time.Time
	defer func() {
		stopTimer(heartbeatTimer)
		stopTimer(quorumTimer)
	}()

	n.publishRaftState(runtime.core.State())
	for {
		var event raft.Event
		select {
		case <-ctx.Done():
			return nil
		case input := <-n.events:
			// Acceptance means the single owner has dequeued the event; the peer
			// never receives success for work stranded during shutdown.
			close(input.accepted)
			event = input.event
		case <-electionTimer.C:
			event = raft.ElectionTimeout{}
		case <-heartbeatC:
			event = raft.HeartbeatTimeout{}
		case <-quorumC:
			event = raft.CheckQuorumTimeout{}
		}

		actions, err := runtime.step(event)
		if err != nil {
			return err
		}
		for _, action := range actions {
			switch action := action.(type) {
			case raft.SendPreVoteRequest, raft.SendPreVoteResponse, raft.SendVoteRequest,
				raft.SendVoteResponse, raft.SendAppendEntries, raft.SendAppendEntriesResponse:
				// A missing peer is ordinary during startup, elections, and process loss.
				// The next timer or inbound message retries protocol progress.
				if err := transport.send(ctx, action); isPeerConfigurationError(err) {
					return err
				}
			case raft.ResetElectionTimer:
				resetTimer(electionTimer, randomElectionTimeout(random))
			case raft.ResetHeartbeatTimer:
				heartbeatTimer, heartbeatC = resetOptionalTimer(heartbeatTimer, heartbeatInterval)
			case raft.ResetCheckQuorumTimer:
				quorumTimer, quorumC = resetOptionalTimer(quorumTimer, checkQuorumWindow)
			case raft.ApplyEntry, raft.BecameLeader, raft.BecameReadReady, raft.LostLeadership:
				// Role and progress are read from the core after all actions finish.
			}
		}

		state := runtime.core.State()
		if state.Role != raft.Leader {
			stopTimer(heartbeatTimer)
			stopTimer(quorumTimer)
			heartbeatC, quorumC = nil, nil
		}
		n.publishRaftState(state)
	}
}

func nodeSeed(id string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(id))
	return int64(hash.Sum64())
}

func randomElectionTimeout(random *rand.Rand) time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(random.Int63n(span+1))
}

func resetOptionalTimer(timer *time.Timer, delay time.Duration) (*time.Timer, <-chan time.Time) {
	if timer == nil {
		timer = time.NewTimer(delay)
	} else {
		resetTimer(timer, delay)
	}
	return timer, timer.C
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	stopTimer(timer)
	timer.Reset(delay)
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
