package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestWALRecoversLatestHardStateAcrossSegments(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-2"}
	wal, recovered, err := open(directory, identity, 64)
	if err != nil {
		t.Fatalf("open new WAL: %v", err)
	}
	if !reflect.DeepEqual(recovered, RecoveredState{Identity: identity}) {
		t.Fatalf("new WAL state = %#v, want identity only", recovered)
	}
	for _, state := range []HardState{
		{Term: 1, VotedFor: "node-1"},
		{Term: 2},
		{Term: 3, VotedFor: "node-3"},
	} {
		if err := wal.SaveHardState(state); err != nil {
			t.Fatalf("save hard state %#v: %v", state, err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	segments, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find segments: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("WAL segments = %d, want rollover", len(segments))
	}
	assertSegmentFormat(t, segments[0].path)

	reopened, recovered, err := open(directory, identity, 64)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	want := RecoveredState{Identity: identity, HardState: HardState{Term: 3, VotedFor: "node-3"}}
	if !reflect.DeepEqual(recovered, want) {
		t.Fatalf("recovered state = %#v, want %#v", recovered, want)
	}
}

func TestWALSyncsAndValidatesLogEntryOrder(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	wal, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	sessionID := [16]byte{1, 2, 3, 4}
	setValue := []byte{0, 1, 255}
	if err := wal.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1, Type: 0},
		{Index: 2, Term: 1, Type: 1, SessionID: sessionID},
		{Index: 3, Term: 1, Type: 3, SessionID: sessionID, Sequence: 1, Key: "empty-or-opaque", Value: setValue},
	}); err != nil {
		t.Fatalf("save log entry: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	wantLog := []LogEntry{
		{Index: 1, Term: 1, Type: 0},
		{Index: 2, Term: 1, Type: 1, SessionID: sessionID},
		{Index: 3, Term: 1, Type: 3, SessionID: sessionID, Sequence: 1, Key: "empty-or-opaque", Value: []byte{0, 1, 255}},
	}
	if !reflect.DeepEqual(recovered.Log, wantLog) {
		t.Fatalf("recovered log = %#v, want %#v", recovered.Log, wantLog)
	}
	if err := reopened.SaveLogEntries(nil); err == nil || !strings.Contains(err.Error(), "at least one entry") {
		t.Fatalf("SaveLogEntries(nil) error = %v, want non-empty detail", err)
	}
}

func TestWALRejectsConfiguredIdentityMismatch(t *testing.T) {
	directory := t.TempDir()
	wal, _, err := Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
	if err != nil {
		t.Fatalf("open new WAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	for _, test := range []struct {
		name     string
		identity Identity
		detail   string
	}{
		{name: "Cluster", identity: Identity{ClusterID: "cluster-2", NodeID: "node-1"}, detail: "configured Cluster \"cluster-2\" Node \"node-1\""},
		{name: "Node", identity: Identity{ClusterID: "cluster-1", NodeID: "node-2"}, detail: "configured Cluster \"cluster-1\" Node \"node-2\""},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := Open(directory, test.identity)
			if err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("Open() error = %v, want durable identity mismatch with %q", err, test.detail)
			}
		})
	}
}

func TestWALRejectsUnsupportedVersionAndChecksumMismatch(t *testing.T) {
	t.Run("version", func(t *testing.T) {
		directory := t.TempDir()
		path := createWAL(t, directory)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open segment: %v", err)
		}
		if _, err := file.WriteAt([]byte{0, 0, 0, 2}, int64(len(segmentMagic))); err != nil {
			t.Fatalf("replace version: %v", err)
		}
		file.Close()
		_, _, err = Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
		if err == nil || !strings.Contains(err.Error(), "unsupported format version 2") {
			t.Fatalf("Open() error = %v, want unsupported version", err)
		}
	})

	t.Run("checksum", func(t *testing.T) {
		directory := t.TempDir()
		path := createWAL(t, directory)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("open segment: %v", err)
		}
		if _, err := file.WriteAt([]byte{'X'}, segmentHeaderSize+frameHeaderSize+1); err != nil {
			t.Fatalf("corrupt record: %v", err)
		}
		file.Close()
		_, _, err = Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
		if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("Open() error = %v, want checksum mismatch", err)
		}
	})
}

func createWAL(t *testing.T, directory string) string {
	t.Helper()
	wal, _, err := Open(directory, Identity{ClusterID: "cluster-1", NodeID: "node-1"})
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	return filepath.Join(directory, "wal-0000000000000001.qwal")
}

func assertSegmentFormat(t *testing.T, path string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if string(contents[:len(segmentMagic)]) != segmentMagic {
		t.Fatalf("segment magic = %q, want %q", contents[:len(segmentMagic)], segmentMagic)
	}
	if got := binary.BigEndian.Uint32(contents[len(segmentMagic):segmentHeaderSize]); got != formatVersion {
		t.Fatalf("format version = %d, want %d", got, formatVersion)
	}
	length := binary.BigEndian.Uint32(contents[segmentHeaderSize : segmentHeaderSize+4])
	wantChecksum := binary.BigEndian.Uint32(contents[segmentHeaderSize+4 : segmentHeaderSize+8])
	body := contents[segmentHeaderSize+frameHeaderSize : segmentHeaderSize+frameHeaderSize+int64(length)]
	if got := crc32.ChecksumIEEE(body); got != wantChecksum {
		t.Fatalf("frame checksum = %08x, want %08x", got, wantChecksum)
	}
}
