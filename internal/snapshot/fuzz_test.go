package snapshot

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// FuzzDecode drives the Snapshot reader with arbitrary bytes. A Snapshot is
// read during startup, before a Node can serve anything, so the reader has to
// reject damage rather than panic on it: the file it is handed may have been
// truncated by a crash or corrupted on disk. ADR-0005 accepts a heavier
// recovery-testing burden as the price of a purpose-built format, and this is
// part of paying it.
func FuzzDecode(f *testing.F) {
	identity := Identity{ClusterID: "fuzz-cluster", NodeID: "node-1", MemberIDs: []string{"node-1", "node-2", "node-3"}}
	seeds := []State{
		{Identity: identity, IncludedIndex: 1, IncludedTerm: 1, Values: map[string][]byte{}, Sessions: nil},
		{
			Identity:      identity,
			IncludedIndex: 42,
			IncludedTerm:  7,
			Values:        map[string][]byte{"key": {1, 2, 3}, "empty": {}},
			Sessions: []Session{
				{ID: [16]byte{1}, LastSequence: 3, LastResult: 0},
				{ID: [16]byte{2}, Closed: true, LastSequence: 9, LastDeleteExisted: true},
			},
		},
	}
	for _, state := range seeds {
		directory := f.TempDir()
		if _, err := Save(directory, state); err != nil {
			f.Fatalf("seed Snapshot: %v", err)
		}
		contents, err := Encoded(directory, state.IncludedIndex, state.IncludedTerm)
		if err != nil {
			f.Fatalf("read seed Snapshot: %v", err)
		}
		f.Add(contents)
	}
	f.Add([]byte{})
	f.Add([]byte(magic))

	f.Fuzz(func(t *testing.T, contents []byte) {
		state, err := Decode(contents)
		if err != nil {
			if state != nil {
				t.Fatalf("Decode() returned a State alongside error %v", err)
			}
			return
		}
		if state == nil {
			t.Fatal("Decode() returned no State and no error")
		}

		// Anything the reader accepts must survive being written back and read
		// again. A format whose writer and reader disagree would install a
		// Snapshot that a later startup reads differently.
		directory := t.TempDir()
		if _, err := Save(directory, *state); err != nil {
			t.Fatalf("Save() rejected an accepted State %+v: %v", state, err)
		}
		encoded, err := Encoded(directory, state.IncludedIndex, state.IncludedTerm)
		if err != nil {
			t.Fatalf("Encoded() after Save(): %v", err)
		}
		round, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode() rejected its own encoding: %v", err)
		}
		if !equalStates(*state, *round) {
			t.Fatalf("round trip changed the State:\n got %+v\nwant %+v", *round, *state)
		}
	})
}

// FuzzLoadNewestIgnoresDamagedCandidates checks the startup path that chooses
// among stored Snapshots rather than the decoder alone. A damaged candidate
// must never be selected, and must never prevent startup from reporting a
// clean absence of Snapshots.
func FuzzLoadNewestIgnoresDamagedCandidates(f *testing.F) {
	f.Add([]byte{}, uint64(1), uint64(1))
	f.Add([]byte(magic), uint64(2), uint64(3))

	f.Fuzz(func(t *testing.T, contents []byte, includedIndex, includedTerm uint64) {
		directory := t.TempDir()
		name := fmt.Sprintf("snapshot-%020d-%020d-fuzz.qsnap", includedIndex%1000, includedTerm%1000)
		if err := os.WriteFile(filepath.Join(directory, name), contents, 0o600); err != nil {
			t.Skip()
		}
		state, err := LoadNewest(directory, Compatibility{
			Identity: Identity{ClusterID: "fuzz-cluster", NodeID: "node-1", MemberIDs: []string{"node-1"}},
		})
		if err == nil && state != nil {
			t.Fatalf("LoadNewest() selected a damaged Snapshot %+v", state)
		}
	})
}

func equalStates(left, right State) bool {
	if !reflect.DeepEqual(left.Identity, right.Identity) ||
		left.IncludedIndex != right.IncludedIndex ||
		left.IncludedTerm != right.IncludedTerm ||
		len(left.Values) != len(right.Values) ||
		!reflect.DeepEqual(left.Sessions, right.Sessions) {
		return false
	}
	for key, value := range left.Values {
		other, ok := right.Values[key]
		if !ok || !bytes.Equal(value, other) {
			return false
		}
	}
	return true
}
