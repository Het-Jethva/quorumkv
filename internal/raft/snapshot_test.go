package raft

import "testing"

func TestLeaderRequestsSnapshotWhenFollowerNeedsCompactedHistory(t *testing.T) {
	node := NewNodeFromRecoveredState("node-1", []NodeID{"node-2", "node-3"}, RecoveredState{
		HardState:     HardState{Term: 3},
		Log:           []LogEntry{{Index: 4, Term: 3, Type: EntryNoOp}},
		SnapshotIndex: 3, SnapshotTerm: 2,
	})
	electRecoveredLeader(t, node, 4)
	actions := node.Step(AppendEntriesResponse{From: "node-2", Term: 4, RequestID: 1, ConflictIndex: 1})
	var transfer SendInstallSnapshot
	for _, action := range actions {
		if candidate, ok := action.(SendInstallSnapshot); ok {
			transfer = candidate
		}
	}
	if transfer.To != "node-2" || transfer.SnapshotIndex != 3 || transfer.SnapshotTerm != 2 {
		t.Fatalf("snapshot request = %#v, want node-2 at 3/2", transfer)
	}

	actions = node.Step(InstallSnapshotResponse{From: "node-2", Term: 4, RequestID: transfer.RequestID, SnapshotIndex: 3, Success: true, NextOffset: 64})
	var next SendInstallSnapshot
	for _, action := range actions {
		if candidate, ok := action.(SendInstallSnapshot); ok {
			next = candidate
		}
	}
	if next.Offset != 64 {
		t.Fatalf("next snapshot offset = %d, want 64", next.Offset)
	}
}

func TestFollowerAcknowledgesInstalledSnapshotAndRetainsSuffix(t *testing.T) {
	node := NewNodeFromRecoveredState("node-2", []NodeID{"node-1", "node-3"}, RecoveredState{
		HardState:   HardState{Term: 4},
		Log:         []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}, {Index: 4, Term: 4}},
		CommitIndex: 2,
	})
	actions := node.Step(InstallSnapshot{From: "node-1", Term: 4, RequestID: 7, SnapshotIndex: 3, SnapshotTerm: 2, Done: true, Success: true, NextOffset: 100})
	persist := actions[len(actions)-1].(PersistSnapshot)
	if !persist.RetainSuffix {
		t.Fatalf("Snapshot persistence = %#v, want compatible suffix retained", persist)
	}
	actions = node.Step(SnapshotPersisted{PersistenceID: persist.PersistenceID})
	response := actions[len(actions)-1].(SendInstallSnapshotResponse).Response
	if !response.Success || !response.Done || response.SnapshotIndex != 3 {
		t.Fatalf("snapshot response = %#v, want successful completion", response)
	}
	state := node.State()
	if state.LastLogIndex != 4 || state.LastApplied != 3 || state.CommitIndex != 3 {
		t.Fatalf("follower state after Snapshot = %#v, want suffix at 4 and applied/committed 3", state)
	}
}

func TestFollowerDiscardsDivergentSuffixOnlyAfterSnapshotPersistence(t *testing.T) {
	node := NewNodeFromRecoveredState("node-2", []NodeID{"node-1", "node-3"}, RecoveredState{
		HardState:   HardState{Term: 4},
		Log:         []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}},
		CommitIndex: 2,
	})
	actions := node.Step(InstallSnapshot{
		From: "node-1", Term: 4, RequestID: 8, SnapshotIndex: 3, SnapshotTerm: 2,
		Done: true, Success: true, NextOffset: 100,
	})
	persist := actions[len(actions)-1].(PersistSnapshot)
	if persist.RetainSuffix {
		t.Fatalf("Snapshot persistence = %#v, want divergent suffix discarded", persist)
	}
	if state := node.State(); state.LastLogIndex != 4 || state.SnapshotIndex != 0 {
		t.Fatalf("state before persistence = %#v, want existing durable log unchanged", state)
	}

	actions = node.Step(SnapshotPersisted{PersistenceID: persist.PersistenceID})
	if restore, ok := actions[0].(RestoreSnapshot); !ok || restore.SnapshotIndex != 3 || restore.SnapshotTerm != 2 {
		t.Fatalf("first completion action = %#v, want state-machine restore at 3/2", actions[0])
	}
	if state := node.State(); state.LastLogIndex != 3 || state.LastLogTerm != 2 || state.CommitIndex != 3 || state.LastApplied != 3 {
		t.Fatalf("state after persistence = %#v, want divergent suffix discarded at Snapshot 3/2", state)
	}
}
