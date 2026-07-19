package node

import (
	"fmt"

	"github.com/Het-Jethva/quorumkv/internal/config"
	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/wal"
)

// raftRuntime owns the synchronous boundary between deterministic election
// decisions and durable local effects. Its caller owns event serialization.
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
	logEntries := make([]raft.LogEntry, len(recovered.Log))
	for index, entry := range recovered.Log {
		if entry.Type > wal.EntryType(raft.EntryDelete) {
			store.Close()
			return nil, fmt.Errorf("recover Node %q consensus state: unsupported log entry type %d at index %d", cfg.Node.ID, entry.Type, entry.Index)
		}
		logEntries[index] = raft.LogEntry{Index: entry.Index, Term: entry.Term, Type: raft.EntryType(entry.Type), SessionID: raft.SessionID(entry.SessionID), Sequence: entry.Sequence, Key: entry.Key, Value: append([]byte(nil), entry.Value...)}
	}
	core := raft.NewNodeFromRecoveredState(raft.NodeID(cfg.Node.ID), peers, raft.RecoveredState{
		HardState: raft.HardState{
			Term:     recovered.HardState.Term,
			VotedFor: raft.NodeID(recovered.HardState.VotedFor),
		},
		Log:         logEntries,
		CommitIndex: recovered.CommitIndex,
	})
	return &raftRuntime{core: core, wal: store}, nil
}

// step executes durability actions synchronously. A completion event is not
// delivered to Raft until the corresponding WAL records have been synced.
func (r *raftRuntime) step(event raft.Event) ([]raft.Action, error) {
	pending := r.core.Step(event)
	var emitted []raft.Action
	for len(pending) > 0 {
		action := pending[0]
		pending = pending[1:]
		switch persist := action.(type) {
		case raft.PersistHardState:
			if err := r.wal.SaveHardState(wal.HardState{
				Term:     persist.Term,
				VotedFor: string(persist.VotedFor),
			}); err != nil {
				return nil, fmt.Errorf("persist Raft hard state: %w", err)
			}
			pending = append(pending, r.core.Step(raft.HardStatePersisted{
				PersistenceID: persist.PersistenceID,
			})...)
		case raft.PersistLogEntries:
			if persist.TruncateFrom != 0 {
				if err := r.wal.TruncateLog(persist.TruncateFrom); err != nil {
					return nil, fmt.Errorf("persist Raft log truncation: %w", err)
				}
			}
			entries := make([]wal.LogEntry, len(persist.Entries))
			for index, entry := range persist.Entries {
				entries[index] = wal.LogEntry{Index: entry.Index, Term: entry.Term, Type: wal.EntryType(entry.Type), SessionID: [16]byte(entry.SessionID), Sequence: entry.Sequence, Key: entry.Key, Value: append([]byte(nil), entry.Value...)}
			}
			if err := r.wal.SaveLogEntries(entries); err != nil {
				return nil, fmt.Errorf("persist Raft log entries: %w", err)
			}
			pending = append(pending, r.core.Step(raft.LogEntriesPersisted{
				PersistenceID: persist.PersistenceID,
			})...)
		case raft.PersistCommitIndex:
			if err := r.wal.SaveCommitIndex(persist.CommitIndex); err != nil {
				return nil, fmt.Errorf("persist Raft commit index: %w", err)
			}
			pending = append(pending, r.core.Step(raft.CommitIndexPersisted{
				PersistenceID: persist.PersistenceID,
			})...)
		default:
			emitted = append(emitted, action)
		}
	}
	return emitted, nil
}

func (r *raftRuntime) close() error {
	return r.wal.Close()
}
