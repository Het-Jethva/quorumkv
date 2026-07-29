package wal

import (
	"encoding/binary"
	"testing"
)

const (
	fuzzClusterID = "fuzz-cluster"
	fuzzNodeID    = "node-1"
)

// FuzzApplyRecord replays arbitrary sequences of WAL records through the
// decoder that recovery uses. Every record body is parsed with explicit offset
// arithmetic over lengths that came off disk, and recovery runs on the crash
// path, so a malformed body must be rejected rather than panic or leave the
// reconstructed state inconsistent. ADR-0005 accepts a heavier
// recovery-testing burden as the price of a purpose-built format, and this is
// part of paying it.
//
// The stream is decoded as repeated one-byte record kinds with big-endian
// length prefixes, so one input drives a whole recovery rather than a single
// record. That is what reaches the ordering rules: identity before hard state,
// and log indexes that must stay contiguous.
func FuzzApplyRecord(f *testing.F) {
	record := func(kind recordType, payload []byte) []byte {
		framed := make([]byte, 5, 5+len(payload))
		framed[0] = byte(kind)
		binary.BigEndian.PutUint32(framed[1:5], uint32(len(payload)))
		return append(framed, payload...)
	}
	logEntryV3 := func(index, term uint64, key string, value []byte) []byte {
		payload := make([]byte, 49, 49+len(key)+len(value))
		binary.BigEndian.PutUint64(payload[0:8], index)
		binary.BigEndian.PutUint64(payload[8:16], term)
		payload[16] = 1
		binary.BigEndian.PutUint32(payload[41:45], uint32(len(key)))
		binary.BigEndian.PutUint32(payload[45:49], uint32(len(value)))
		payload = append(payload, key...)
		return append(payload, value...)
	}

	identities := append(
		record(recordClusterIdentity, []byte(fuzzClusterID)),
		record(recordNodeIdentity, []byte(fuzzNodeID))...,
	)
	hardState := make([]byte, 8)
	binary.BigEndian.PutUint64(hardState, 4)
	commitIndex := make([]byte, 8)
	binary.BigEndian.PutUint64(commitIndex, 1)

	seed := append([]byte(nil), identities...)
	seed = append(seed, record(recordHardState, append(hardState, fuzzNodeID...))...)
	seed = append(seed, record(recordLogEntryV3, logEntryV3(1, 1, "key", []byte{1, 2, 3}))...)
	seed = append(seed, record(recordLogEntryV3, logEntryV3(2, 4, "", nil))...)
	seed = append(seed, record(recordCommitIndex, commitIndex)...)

	f.Add(seed)
	f.Add(identities)
	f.Add(record(recordLogEntryV3, logEntryV3(1, 1, "orphan", nil)))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, stream []byte) {
		var state RecoveredState
		for offset := 0; offset+5 <= len(stream); {
			kind := recordType(stream[offset])
			length := int(binary.BigEndian.Uint32(stream[offset+1 : offset+5]))
			offset += 5
			if length < 0 || length > len(stream)-offset {
				return
			}
			payload := stream[offset : offset+length]
			offset += length

			if err := applyRecord(kind, payload, &state); err != nil {
				return
			}
			// A record the decoder accepted must leave the reconstructed log
			// contiguous from the Snapshot position. Recovery hands this state
			// straight to the Raft core, which assumes exactly that.
			for index, entry := range state.Log {
				if want := state.SnapshotIndex + uint64(index) + 1; entry.Index != want {
					t.Fatalf("accepted records left log position %d holding index %d, want %d\nstate: %+v",
						index, entry.Index, want, state)
				}
			}
			if state.Identity.NodeID != "" && state.Identity.ClusterID == "" {
				t.Fatalf("accepted records produced a Node Identity with no Cluster Identity: %+v", state)
			}
		}
	})
}
