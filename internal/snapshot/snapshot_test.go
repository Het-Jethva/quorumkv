package snapshot_test

import (
	"bytes"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/snapshot"
)

func TestOpenStreamsValidatedEncodedSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	name, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 7, IncludedTerm: 3,
		Values: map[string][]byte{"large": bytes.Repeat([]byte("x"), 150<<10)},
	})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	want, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read expected encoding: %v", err)
	}

	handle, err := snapshot.Open(directory, 7, 3)
	if err != nil {
		t.Fatalf("open Snapshot: %v", err)
	}
	if handle.Length() != uint64(len(want)) || handle.Checksum() != crc32.ChecksumIEEE(want) {
		t.Fatalf("metadata = %d/%08x, want %d/%08x", handle.Length(), handle.Checksum(), len(want), crc32.ChecksumIEEE(want))
	}
	got := make([]byte, len(want))
	for offset := 0; offset < len(got); offset += 64 << 10 {
		end := offset + 64<<10
		if end > len(got) {
			end = len(got)
		}
		if _, err := handle.ReadAt(got[offset:end], int64(offset)); err != nil {
			t.Fatalf("read chunk at %d: %v", offset, err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatal("streamed Snapshot differs from stored encoding")
	}
	if _, err := handle.ReadAt(make([]byte, 2), int64(len(want)-1)); err == nil {
		t.Fatal("ReadAt() beyond validated length succeeded")
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("close Snapshot: %v", err)
	}
}

func TestOpenRejectsWrongPositionAndCorruption(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	name, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 8, IncludedTerm: 4, Values: map[string][]byte{"key": []byte("value")},
	})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	wrongName := filepath.Join(directory, "snapshot-00000000000000000009-00000000000000000004-wrong.qsnap")
	if err := os.Rename(name, wrongName); err != nil {
		t.Fatalf("rename Snapshot to wrong position: %v", err)
	}
	if _, err := snapshot.Open(directory, 9, 4); err == nil || !strings.Contains(err.Error(), "encoded position 8/4") {
		t.Fatalf("open wrong-position Snapshot error = %v", err)
	}

	contents, err := os.ReadFile(wrongName)
	if err != nil {
		t.Fatalf("read Snapshot: %v", err)
	}
	contents[len(contents)-1] ^= 0xff
	if err := os.WriteFile(wrongName, contents, 0o600); err != nil {
		t.Fatalf("corrupt Snapshot: %v", err)
	}
	if _, err := snapshot.Open(directory, 9, 4); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("open corrupt Snapshot error = %v", err)
	}
}

func TestSaveAndLoadNewestCompatibleSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	first := snapshot.State{
		Identity: identity, IncludedIndex: 2, IncludedTerm: 1,
		Values:   map[string][]byte{"key": {0, 1, 2}},
		Sessions: []snapshot.Session{{ID: [16]byte{1}, LastSequence: 2, LastDeleteExisted: true}},
	}
	firstName, err := snapshot.Save(directory, first)
	if err != nil {
		t.Fatalf("save first Snapshot: %v", err)
	}
	second := first
	second.IncludedIndex = 4
	second.IncludedTerm = 2
	second.Values = map[string][]byte{"newer": {3}}
	secondName, err := snapshot.Save(directory, second)
	if err != nil {
		t.Fatalf("save second Snapshot: %v", err)
	}
	if firstName == secondName || !strings.HasSuffix(firstName, ".qsnap") || !strings.HasSuffix(secondName, ".qsnap") {
		t.Fatalf("Snapshot names = %q and %q, want unique immutable final names", firstName, secondName)
	}
	if err := os.WriteFile(filepath.Join(directory, "snapshot-999.tmp"), []byte("incomplete"), 0o600); err != nil {
		t.Fatalf("write incomplete temporary Snapshot: %v", err)
	}

	loaded, err := snapshot.LoadNewest(directory, snapshot.Compatibility{
		Identity: identity, CommitIndex: 3, LogTerms: []uint64{1, 1, 1, 2},
	})
	if err != nil {
		t.Fatalf("load newest compatible Snapshot: %v", err)
	}
	if loaded == nil || loaded.IncludedIndex != 2 || string(loaded.Values["key"]) != string([]byte{0, 1, 2}) {
		t.Fatalf("loaded Snapshot = %#v, want first compatible state", loaded)
	}

	loaded, err = snapshot.LoadNewest(directory, snapshot.Compatibility{
		Identity: identity, CommitIndex: 4, LogTerms: []uint64{1, 1, 1, 2},
	})
	if err != nil {
		t.Fatalf("load newest Snapshot: %v", err)
	}
	if loaded == nil || loaded.IncludedIndex != 4 || string(loaded.Values["newer"]) != string([]byte{3}) {
		t.Fatalf("loaded Snapshot = %#v, want newest state", loaded)
	}
}

func TestLoadIgnoresSnapshotAheadOfWALWithoutCheckpoint(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	if _, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 2, IncludedTerm: 1, Values: map[string][]byte{},
	}); err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}

	loaded, err := snapshot.LoadNewest(directory, snapshot.Compatibility{Identity: identity, CommitIndex: 1, LogTerms: []uint64{1}})
	if err != nil {
		t.Fatalf("load with uncommitted orphan Snapshot: %v", err)
	}
	if loaded != nil {
		t.Fatalf("loaded Snapshot = %#v, want no Snapshot", loaded)
	}
}

func TestLoadIgnoresSeveralUnmatchedSnapshotsWithoutCheckpoint(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	for _, state := range []snapshot.State{
		{Identity: identity, IncludedIndex: 2, IncludedTerm: 1, Values: map[string][]byte{}},
		{Identity: identity, IncludedIndex: 4, IncludedTerm: 2, Values: map[string][]byte{}},
	} {
		if _, err := snapshot.Save(directory, state); err != nil {
			t.Fatalf("save Snapshot at %d/%d: %v", state.IncludedIndex, state.IncludedTerm, err)
		}
	}

	loaded, err := snapshot.LoadNewest(directory, snapshot.Compatibility{
		Identity: identity, CommitIndex: 1, LogTerms: []uint64{1},
	})
	if err != nil {
		t.Fatalf("load with uncommitted orphan Snapshots: %v", err)
	}
	if loaded != nil {
		t.Fatalf("loaded Snapshot = %#v, want no Snapshot", loaded)
	}
}

func TestLoadRequiresCheckpointSnapshot(t *testing.T) {
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	tests := []struct {
		name      string
		snapshots []snapshot.State
	}{
		{name: "no Snapshot files"},
		{
			name: "wrong index",
			snapshots: []snapshot.State{
				{Identity: identity, IncludedIndex: 4, IncludedTerm: 3, Values: map[string][]byte{}},
			},
		},
		{
			name: "wrong Term",
			snapshots: []snapshot.State{
				{Identity: identity, IncludedIndex: 5, IncludedTerm: 2, Values: map[string][]byte{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			for _, state := range tt.snapshots {
				if _, err := snapshot.Save(directory, state); err != nil {
					t.Fatalf("save Snapshot at %d/%d: %v", state.IncludedIndex, state.IncludedTerm, err)
				}
			}

			_, err := snapshot.LoadNewest(directory, snapshot.Compatibility{
				Identity: identity, CommitIndex: 5, SnapshotIndex: 5, SnapshotTerm: 3,
			})
			if err == nil || !strings.Contains(err.Error(), "required Snapshot at 5/3") {
				t.Fatalf("load without required Snapshot error = %v, want required position diagnostic", err)
			}
		})
	}
}

func TestLoadRejectsCorruptRequiredSnapshotWithoutFallback(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	if _, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 4, IncludedTerm: 2, Values: map[string][]byte{},
	}); err != nil {
		t.Fatalf("save older Snapshot: %v", err)
	}
	requiredName, err := snapshot.Save(directory, snapshot.State{
		Identity: identity, IncludedIndex: 5, IncludedTerm: 3, Values: map[string][]byte{},
	})
	if err != nil {
		t.Fatalf("save required Snapshot: %v", err)
	}
	contents, err := os.ReadFile(requiredName)
	if err != nil {
		t.Fatalf("read required Snapshot: %v", err)
	}
	contents[len(contents)-1] ^= 0xff
	if err := os.WriteFile(requiredName, contents, 0o600); err != nil {
		t.Fatalf("corrupt required Snapshot: %v", err)
	}

	_, err = snapshot.LoadNewest(directory, snapshot.Compatibility{
		Identity: identity, CommitIndex: 5, SnapshotIndex: 5, SnapshotTerm: 3,
	})
	if err == nil || !strings.Contains(err.Error(), "required Snapshot at 5/3") || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("load corrupt required Snapshot error = %v, want required position and corruption diagnostics", err)
	}
}

func TestLoadRejectsCorruptFinalSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := snapshot.Identity{ClusterID: "cluster-1", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	name, err := snapshot.Save(directory, snapshot.State{Identity: identity, Values: map[string][]byte{}})
	if err != nil {
		t.Fatalf("save Snapshot: %v", err)
	}
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read Snapshot: %v", err)
	}
	contents[len(contents)-1] ^= 0xff
	if err := os.WriteFile(name, contents, 0o600); err != nil {
		t.Fatalf("corrupt Snapshot: %v", err)
	}
	_, err = snapshot.LoadNewest(directory, snapshot.Compatibility{Identity: identity})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("load corrupt Snapshot error = %v, want checksum mismatch", err)
	}
}
