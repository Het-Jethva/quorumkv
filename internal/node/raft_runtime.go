package node

import (
	"fmt"

	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/wal"
)

// raftRuntime owns the boundary between deterministic election decisions and
// durable local effects. It is intentionally synchronous until a later slice
// introduces the Node event loop.
type raftRuntime struct {
	core *raft.Node
	wal  *wal.WAL
}

func openRaftRuntime(cfg config.Config, peers []raft.NodeID) (*raftRuntime, error) {
	store, recovered, err := wal.Open(cfg.Node.DataDir, wal.Identity{
		ClusterID: cfg.ClusterID,
		NodeID:    cfg.Node.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("recover Node %q consensus state: %w", cfg.Node.ID, err)
	}
	core := raft.NewNodeWithHardState(raft.NodeID(cfg.Node.ID), peers, raft.HardState{
		Term:     recovered.HardState.Term,
		VotedFor: raft.NodeID(recovered.HardState.VotedFor),
	})
	return &raftRuntime{core: core, wal: store}, nil
}

// step executes hard-state persistence inline. The completion event is not
// delivered to Raft until SaveHardState has appended and synced the record.
func (r *raftRuntime) step(event raft.Event) ([]raft.Action, error) {
	pending := r.core.Step(event)
	var emitted []raft.Action
	for len(pending) > 0 {
		action := pending[0]
		pending = pending[1:]
		persist, ok := action.(raft.PersistHardState)
		if !ok {
			emitted = append(emitted, action)
			continue
		}
		if err := r.wal.SaveHardState(wal.HardState{
			Term:     persist.Term,
			VotedFor: string(persist.VotedFor),
		}); err != nil {
			return nil, fmt.Errorf("persist Raft hard state: %w", err)
		}
		pending = append(pending, r.core.Step(raft.HardStatePersisted{
			PersistenceID: persist.PersistenceID,
		})...)
	}
	return emitted, nil
}

func (r *raftRuntime) close() error {
	return r.wal.Close()
}
