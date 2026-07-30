package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
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

func TestNewSegmentsSyncHeaderBeforeDirectory(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	var events []string
	durability := segmentDurability{
		syncFile: func(file *os.File) error {
			contents, err := os.ReadFile(file.Name())
			if err != nil {
				return fmt.Errorf("inspect segment before sync: %w", err)
			}
			if len(contents) != int(segmentHeaderSize) {
				return fmt.Errorf("segment size at file sync = %d, want complete header size %d", len(contents), segmentHeaderSize)
			}
			if string(contents[:len(segmentMagic)]) != segmentMagic {
				return fmt.Errorf("segment magic at file sync = %q, want %q", contents[:len(segmentMagic)], segmentMagic)
			}
			if version := binary.BigEndian.Uint32(contents[len(segmentMagic):]); version != formatVersion {
				return fmt.Errorf("segment version at file sync = %d, want %d", version, formatVersion)
			}
			events = append(events, "file")
			return file.Sync()
		},
		syncDirectory: func(path string) error {
			events = append(events, "directory")
			return syncDirectory(path)
		},
	}

	store, _, err := openWithDurability(directory, identity, 64, durability)
	if err != nil {
		t.Fatalf("open new WAL: %v", err)
	}
	defer store.Close()
	if want := []string{"file", "directory"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("initial segment durability events = %v, want %v", events, want)
	}

	events = nil
	if err := store.SaveHardState(HardState{Term: 1, VotedFor: "node-2"}); err != nil {
		t.Fatalf("save hard state with rollover: %v", err)
	}
	if want := []string{"file", "directory"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("rollover segment durability events = %v, want %v", events, want)
	}
}

func TestRolloverSyncFailureMakesWALUnusable(t *testing.T) {
	for _, test := range []struct {
		name       string
		failFile   bool
		wantDetail string
	}{
		{name: "file", failFile: true, wantDetail: "sync WAL segment header"},
		{name: "directory", wantDetail: "sync WAL directory after creating segment"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
			fail := false
			durability := segmentDurability{
				syncFile: func(file *os.File) error {
					if fail && test.failFile {
						return errors.New("injected file sync failure")
					}
					return file.Sync()
				},
				syncDirectory: func(path string) error {
					if fail && !test.failFile {
						return errors.New("injected directory sync failure")
					}
					return syncDirectory(path)
				},
			}
			store, _, err := openWithDurability(directory, identity, 64, durability)
			if err != nil {
				t.Fatalf("open new WAL: %v", err)
			}
			defer store.Close()

			fail = true
			err = store.SaveHardState(HardState{Term: 1, VotedFor: "node-2"})
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("SaveHardState rollover error = %v, want %q detail", err, test.wantDetail)
			}
			if err := store.SaveHardState(HardState{Term: 2}); err == nil || !strings.Contains(err.Error(), "WAL is closed") {
				t.Fatalf("SaveHardState after failed rollover error = %v, want closed WAL", err)
			}
		})
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

func TestWALRecoversCommittedPrefixAcrossTruncationAndReplacement(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}); err != nil {
		t.Fatalf("save initial log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.TruncateLog(2); err == nil || !strings.Contains(err.Error(), "committed index 2") {
		t.Fatalf("truncate committed history error = %v, want rejection", err)
	}
	if err := store.TruncateLog(3); err != nil {
		t.Fatalf("truncate uncommitted suffix: %v", err)
	}
	replacement := LogEntry{Index: 3, Term: 2}
	if err := store.SaveLogEntries([]LogEntry{replacement}); err != nil {
		t.Fatalf("save replacement entry: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.CommitIndex != 2 {
		t.Fatalf("recovered commit index = %d, want 2", recovered.CommitIndex)
	}
	wantLog := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, replacement}
	if !reflect.DeepEqual(recovered.Log, wantLog) {
		t.Fatalf("recovered log = %#v, want %#v", recovered.Log, wantLog)
	}
}

func TestRetainedLogBytesCountsOnlyNewlyAppliedEntries(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer store.Close()

	entries := []LogEntry{
		{Index: 1, Term: 1, Key: "first", Value: []byte("value-1")},
		{Index: 2, Term: 1, Key: "second", Value: []byte("value-2")},
		{Index: 3, Term: 1, Key: "unapplied", Value: []byte("old")},
	}
	if err := store.SaveLogEntries(entries); err != nil {
		t.Fatalf("save log entries: %v", err)
	}

	firstBytes := logEntryFrameBytes(entries[0])
	if got := store.RetainedLogBytes(1); got != firstBytes {
		t.Fatalf("retained bytes through index 1 = %d, want %d", got, firstBytes)
	}
	// Changing the source size after the first query proves that a repeated
	// query returns the cached total instead of recounting the applied prefix.
	store.entryBytes[1]++
	if got := store.RetainedLogBytes(1); got != firstBytes {
		t.Fatalf("repeated retained bytes through index 1 = %d, want cached %d", got, firstBytes)
	}

	throughTwo := firstBytes + logEntryFrameBytes(entries[1])
	if got := store.RetainedLogBytes(2); got != throughTwo {
		t.Fatalf("retained bytes through index 2 = %d, want %d", got, throughTwo)
	}
	if got := store.RetainedLogBytes(2); got != throughTwo {
		t.Fatalf("retained bytes with unapplied entry = %d, want %d", got, throughTwo)
	}
	if err := store.TruncateLog(3); err != nil {
		t.Fatalf("truncate unapplied suffix: %v", err)
	}
	if got := store.RetainedLogBytes(2); got != throughTwo {
		t.Fatalf("retained bytes after unapplied truncation = %d, want %d", got, throughTwo)
	}
}

func TestRetainedLogBytesResetsAcrossCompactionAndReopen(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	entries := []LogEntry{
		{Index: 1, Term: 1, Key: "one"},
		{Index: 2, Term: 1, Key: "two"},
		{Index: 3, Term: 2, Key: "three", Value: []byte("value")},
		{Index: 4, Term: 2, Key: "four", Value: []byte("another value")},
	}
	if err := store.SaveLogEntries(entries); err != nil {
		t.Fatalf("save log entries: %v", err)
	}
	if err := store.SaveCommitIndex(4); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if got, want := store.RetainedLogBytes(4), logEntryFrameBytes(entries...); got != want {
		t.Fatalf("retained bytes before compaction = %d, want %d", got, want)
	}
	if err := store.Compact(2, 1); err != nil {
		t.Fatalf("compact WAL: %v", err)
	}
	if got := store.RetainedLogBytes(2); got != 0 {
		t.Fatalf("retained bytes at compacted Snapshot = %d, want 0", got)
	}
	suffixBytes := logEntryFrameBytes(entries[2:]...)
	if got := store.RetainedLogBytes(4); got != suffixBytes {
		t.Fatalf("retained bytes after compaction = %d, want suffix %d", got, suffixBytes)
	}
	appended := LogEntry{Index: 5, Term: 2, Key: "new", Value: []byte("after compaction")}
	if err := store.SaveLogEntries([]LogEntry{appended}); err != nil {
		t.Fatalf("append after compaction: %v", err)
	}
	suffixBytes += logEntryFrameBytes(appended)
	if got := store.RetainedLogBytes(5); got != suffixBytes {
		t.Fatalf("retained bytes after post-compaction append = %d, want %d", got, suffixBytes)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	if got := reopened.RetainedLogBytes(5); got != suffixBytes {
		t.Fatalf("retained bytes after reopen = %d, want suffix %d", got, suffixBytes)
	}
}

func TestRetainedLogBytesResetsAcrossReceivedSnapshotInstall(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer store.Close()

	entries := []LogEntry{
		{Index: 1, Term: 1, Key: "one"},
		{Index: 2, Term: 2, Key: "two"},
		{Index: 3, Term: 2, Key: "three"},
		{Index: 4, Term: 3, Key: "retained", Value: []byte("suffix")},
	}
	if err := store.SaveLogEntries(entries); err != nil {
		t.Fatalf("save log entries: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if got, want := store.RetainedLogBytes(2), logEntryFrameBytes(entries[:2]...); got != want {
		t.Fatalf("retained bytes before Snapshot install = %d, want %d", got, want)
	}
	if err := store.InstallSnapshot(3, 2, true); err != nil {
		t.Fatalf("install received Snapshot: %v", err)
	}
	if got := store.RetainedLogBytes(3); got != 0 {
		t.Fatalf("retained bytes at installed Snapshot = %d, want 0", got)
	}
	suffixBytes := logEntryFrameBytes(entries[3])
	if got := store.RetainedLogBytes(4); got != suffixBytes {
		t.Fatalf("retained bytes after Snapshot install = %d, want suffix %d", got, suffixBytes)
	}

	appended := LogEntry{Index: 5, Term: 3, Key: "new", Value: []byte("after reset")}
	if err := store.SaveLogEntries([]LogEntry{appended}); err != nil {
		t.Fatalf("append after received Snapshot: %v", err)
	}
	if got, want := store.RetainedLogBytes(5), suffixBytes+logEntryFrameBytes(appended); got != want {
		t.Fatalf("retained bytes after post-Snapshot append = %d, want %d", got, want)
	}
}

func logEntryFrameBytes(entries ...LogEntry) uint64 {
	var total uint64
	for _, entry := range entries {
		total += uint64(frameHeaderSize) + 1 + 49 + uint64(len(entry.Key)+len(entry.Value))
	}
	return total
}

func TestWALCompactionPreservesRecoveryAcrossPartialSegmentDeletion(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := open(directory, identity, 180)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveHardState(HardState{Term: 3, VotedFor: "node-2"}); err != nil {
		t.Fatalf("save hard state: %v", err)
	}
	for index := uint64(1); index <= 7; index++ {
		if err := store.SaveLogEntries([]LogEntry{{Index: index, Term: 3, Key: fmt.Sprintf("key-%d", index), Value: make([]byte, 32)}}); err != nil {
			t.Fatalf("save log entry %d: %v", index, err)
		}
	}
	if err := store.SaveCommitIndex(7); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	before, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find WAL segments before compaction: %v", err)
	}
	backups := make(map[string][]byte, len(before))
	for _, segment := range before {
		backups[filepath.Base(segment.path)] = readFile(t, segment.path)
	}
	if err := store.Compact(5, 3); err != nil {
		t.Fatalf("compact WAL: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close compacted WAL: %v", err)
	}
	after, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find compacted WAL segments: %v", err)
	}
	if len(after) >= len(before) {
		t.Fatalf("WAL segments after compaction = %d, before = %d; want at least one covered segment deleted", len(after), len(before))
	}

	// Restoring one deleted segment models a crash between segment deletions.
	// The synced checkpoint must make both the partial and complete deletion
	// layouts recover to the same retained suffix.
	for name, contents := range backups {
		path := filepath.Join(directory, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if err := os.WriteFile(path, contents, 0o600); err != nil {
				t.Fatalf("restore one segment for crash point: %v", err)
			}
			break
		}
	}
	reopened, recovered, err := open(directory, identity, 180)
	if err != nil {
		t.Fatalf("recover partially deleted compacted WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 5 || recovered.SnapshotTerm != 3 || recovered.CommitIndex != 7 || recovered.HardState != (HardState{Term: 3, VotedFor: "node-2"}) {
		t.Fatalf("recovered compacted metadata = %#v, want Snapshot 5/3, commit 7, and hard state", recovered)
	}
	if len(recovered.Log) != 2 || recovered.Log[0].Index != 6 || recovered.Log[1].Index != 7 {
		t.Fatalf("recovered retained suffix = %#v, want indexes 6 and 7", recovered.Log)
	}
}

func TestInstallSnapshotRetainsCompatibleSuffixAcrossReopen(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}, {Index: 4, Term: 4},
	}); err != nil {
		t.Fatalf("save log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.InstallSnapshot(3, 2, true); err != nil {
		t.Fatalf("install compatible Snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 3 || recovered.SnapshotTerm != 2 || recovered.CommitIndex != 3 {
		t.Fatalf("recovered Snapshot metadata = %#v, want position 3/2 and commit 3", recovered)
	}
	want := []LogEntry{{Index: 4, Term: 4}}
	if !reflect.DeepEqual(recovered.Log, want) {
		t.Fatalf("recovered compatible suffix = %#v, want %#v", recovered.Log, want)
	}
}

func TestInstallSnapshotDiscardsDivergentSuffixAndAcceptsImmediateAppend(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4},
	}); err != nil {
		t.Fatalf("save divergent log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.InstallSnapshot(3, 2, false); err != nil {
		t.Fatalf("install divergent Snapshot: %v", err)
	}
	replacement := LogEntry{Index: 4, Term: 5, Key: "replacement", Value: []byte("kept")}
	if err := store.SaveLogEntries([]LogEntry{replacement}); err != nil {
		t.Fatalf("append immediately after divergent Snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 3 || recovered.SnapshotTerm != 2 || recovered.CommitIndex != 3 {
		t.Fatalf("recovered Snapshot metadata = %#v, want position 3/2 and commit 3", recovered)
	}
	if !reflect.DeepEqual(recovered.Log, []LogEntry{replacement}) {
		t.Fatalf("recovered log = %#v, want only replacement %#v", recovered.Log, replacement)
	}
}

func TestInstallSnapshotSyncBoundaryIsAtomic(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	failInstallSync := false
	trackInstallSync := false
	installSyncCalls := 0
	durability := segmentDurability{
		syncFile: func(file *os.File) error {
			if trackInstallSync {
				installSyncCalls++
			}
			if failInstallSync {
				failInstallSync = false
				return errors.New("injected Snapshot sync failure")
			}
			return file.Sync()
		},
		syncDirectory: syncDirectory,
	}
	store, _, err := openWithDurability(directory, identity, defaultSegmentSize, durability)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	original := []LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4},
	}
	if err := store.SaveLogEntries(original); err != nil {
		t.Fatalf("save divergent log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}

	trackInstallSync = true
	failInstallSync = true
	err = store.InstallSnapshot(3, 2, false)
	trackInstallSync = false
	if err == nil || !strings.Contains(err.Error(), "injected Snapshot sync failure") {
		t.Fatalf("install with failed sync error = %v, want injected failure", err)
	}
	if installSyncCalls != 1 {
		t.Fatalf("failed Snapshot install sync calls = %d, want one combined sync", installSyncCalls)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL after failed install: %v", err)
	}
	store, recovered, err := openWithDurability(directory, identity, defaultSegmentSize, durability)
	if err != nil {
		t.Fatalf("reopen WAL after failed install: %v", err)
	}
	if recovered.SnapshotIndex != 0 || recovered.CommitIndex != 2 || !reflect.DeepEqual(recovered.Log, original) {
		t.Fatalf("state after failed install = %#v, want complete pre-install state", recovered)
	}

	installSyncCalls = 0
	trackInstallSync = true
	err = store.InstallSnapshot(3, 2, false)
	trackInstallSync = false
	if err != nil {
		t.Fatalf("install Snapshot after sync recovers: %v", err)
	}
	if installSyncCalls != 1 {
		t.Fatalf("successful Snapshot install sync calls = %d, want one combined sync", installSyncCalls)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL after successful install: %v", err)
	}
	reopened, recovered, err := openWithDurability(directory, identity, defaultSegmentSize, durability)
	if err != nil {
		t.Fatalf("reopen WAL after successful install: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 3 || recovered.SnapshotTerm != 2 || recovered.CommitIndex != 3 || len(recovered.Log) != 0 {
		t.Fatalf("state after successful install = %#v, want complete truncation and checkpoint", recovered)
	}
}

func TestInstallSnapshotCheckpointSurvivesCrashBeforeTruncationFrame(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4},
	}); err != nil {
		t.Fatalf("save divergent log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	if err := store.InstallSnapshot(3, 2, false); err != nil {
		t.Fatalf("install divergent Snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	segments, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find WAL segments: %v", err)
	}
	active := segments[len(segments)-1].path
	contents := readFile(t, active)
	if err := os.Truncate(active, lastFrameOffset(t, contents)); err != nil {
		t.Fatalf("model crash before truncation frame: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("recover checkpoint without following truncation frame: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 3 || recovered.SnapshotTerm != 2 || recovered.CommitIndex != 3 || len(recovered.Log) != 0 {
		t.Fatalf("recovered state = %#v, want checkpoint's complete discard decision", recovered)
	}
}

func TestInstallSnapshotRecoveryIgnoresTruncationsCoveredByNewerSnapshot(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4},
	}); err != nil {
		t.Fatalf("save initial divergent log: %v", err)
	}
	if err := store.SaveCommitIndex(2); err != nil {
		t.Fatalf("save initial commit index: %v", err)
	}
	if err := store.InstallSnapshot(3, 2, false); err != nil {
		t.Fatalf("install first Snapshot: %v", err)
	}
	if err := store.SaveLogEntries([]LogEntry{{Index: 4, Term: 5}, {Index: 5, Term: 5}}); err != nil {
		t.Fatalf("save suffix after first Snapshot: %v", err)
	}
	if err := store.InstallSnapshot(4, 3, false); err != nil {
		t.Fatalf("install newer divergent Snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen WAL after sequential Snapshot installs: %v", err)
	}
	defer reopened.Close()
	if recovered.SnapshotIndex != 4 || recovered.SnapshotTerm != 3 || recovered.CommitIndex != 4 || len(recovered.Log) != 0 {
		t.Fatalf("recovered state = %#v, want only newer Snapshot at 4/3", recovered)
	}
}

func TestInstallSnapshotRefusesToDiscardCommittedSuffix(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer store.Close()
	entries := []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}}
	if err := store.SaveLogEntries(entries); err != nil {
		t.Fatalf("save log: %v", err)
	}
	if err := store.SaveCommitIndex(4); err != nil {
		t.Fatalf("save commit index: %v", err)
	}
	err = store.InstallSnapshot(3, 2, false)
	if err == nil || !strings.Contains(err.Error(), "would remove committed index 4") {
		t.Fatalf("discard committed suffix error = %v, want committed-history diagnostic", err)
	}
}

func TestWALRejectsASecondOwnerOfTheDataDirectory(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	owner, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("open first WAL: %v", err)
	}

	second, _, err := Open(directory, identity)
	if err == nil {
		second.Close()
		owner.Close()
		t.Fatal("Open() accepted a second owner of one data directory")
	}
	if !strings.Contains(err.Error(), "lock data directory") {
		t.Fatalf("Open() error = %v, want a data directory lock failure", err)
	}

	// Releasing the directory must let a restarted Node take ownership.
	if err := owner.Close(); err != nil {
		t.Fatalf("close first WAL: %v", err)
	}
	reopened, _, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen released data directory: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
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
		for _, detail := range []string{filepath.Base(path), fmt.Sprintf("byte offset %d", segmentHeaderSize), "checksum mismatch"} {
			if err == nil || !strings.Contains(err.Error(), detail) {
				t.Fatalf("Open() error = %v, want interior corruption detail %q", err, detail)
			}
		}
	})
}

func TestWALRecoversInterruptedFinalFrameAtEveryByteBoundary(t *testing.T) {
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	baselineDirectory := t.TempDir()
	baselinePath := createWAL(t, baselineDirectory)
	baseline := readFile(t, baselinePath)

	completeDirectory := t.TempDir()
	store, _, err := Open(completeDirectory, identity)
	if err != nil {
		t.Fatalf("open source WAL: %v", err)
	}
	wantState := HardState{Term: 4, VotedFor: "node-2"}
	if err := store.SaveHardState(wantState); err != nil {
		t.Fatalf("save source hard state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source WAL: %v", err)
	}
	complete := readFile(t, filepath.Join(completeDirectory, filepath.Base(baselinePath)))
	frame := complete[len(baseline):]
	if len(frame) <= int(frameHeaderSize) {
		t.Fatalf("appended frame length = %d, want header and body", len(frame))
	}

	for written := 0; written < len(frame); written++ {
		t.Run(fmt.Sprintf("bytes_%d", written), func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, filepath.Base(baselinePath))
			writeInterruptedSegment(t, path, append(append([]byte(nil), baseline...), frame[:written]...), false)

			reopened, recovered, err := Open(directory, identity)
			if err != nil {
				t.Fatalf("recover after %d frame bytes: %v", written, err)
			}
			if recovered.HardState != (HardState{}) {
				t.Fatalf("recovered hard state = %#v, want no fabricated partial record", recovered.HardState)
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("close recovered WAL: %v", err)
			}
			assertFileSize(t, path, int64(len(baseline)))
		})
	}

	for _, syncComplete := range []bool{false, true} {
		name := "before_sync_completion"
		if syncComplete {
			name = "after_sync_completion"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, filepath.Base(baselinePath))
			writeInterruptedSegment(t, path, complete, syncComplete)

			reopened, recovered, err := Open(directory, identity)
			if err != nil {
				t.Fatalf("recover complete frame: %v", err)
			}
			defer reopened.Close()
			if recovered.HardState != wantState {
				t.Fatalf("recovered hard state = %#v, want %#v", recovered.HardState, wantState)
			}
			assertFileSize(t, path, int64(len(complete)))
		})
	}
}

func TestWALTruncatesChecksumInvalidFinalFrameAndContinues(t *testing.T) {
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	directory := t.TempDir()
	path := createWAL(t, directory)
	baseline := readFile(t, path)

	sourceDirectory := t.TempDir()
	store, _, err := Open(sourceDirectory, identity)
	if err != nil {
		t.Fatalf("open source WAL: %v", err)
	}
	if err := store.SaveHardState(HardState{Term: 1, VotedFor: "node-2"}); err != nil {
		t.Fatalf("save source hard state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source WAL: %v", err)
	}
	contents := readFile(t, filepath.Join(sourceDirectory, filepath.Base(path)))
	contents[len(contents)-1] ^= 0xff
	writeInterruptedSegment(t, path, contents, true)

	reopened, recovered, err := Open(directory, identity)
	if err != nil {
		t.Fatalf("recover checksum-invalid final frame: %v", err)
	}
	if recovered.HardState != (HardState{}) {
		t.Fatalf("recovered hard state = %#v, want invalid tail discarded", recovered.HardState)
	}
	assertFileSize(t, path, int64(len(baseline)))
	want := HardState{Term: 2, VotedFor: "node-3"}
	if err := reopened.SaveHardState(want); err != nil {
		t.Fatalf("append after tail recovery: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close repaired WAL: %v", err)
	}

	reopened, recovered, err = Open(directory, identity)
	if err != nil {
		t.Fatalf("reopen repaired WAL: %v", err)
	}
	defer reopened.Close()
	if recovered.HardState != want {
		t.Fatalf("recovered replacement hard state = %#v, want %#v", recovered.HardState, want)
	}
}

func TestWALRejectsIncompleteFrameOutsideFinalSegment(t *testing.T) {
	directory := t.TempDir()
	identity := Identity{ClusterID: "cluster-1", NodeID: "node-1"}
	store, _, err := open(directory, identity, 64)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	for term := uint64(1); term <= 4; term++ {
		if err := store.SaveHardState(HardState{Term: term, VotedFor: "node-2"}); err != nil {
			t.Fatalf("save hard state for Term %d: %v", term, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	segments, err := findSegments(directory)
	if err != nil {
		t.Fatalf("find WAL segments: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("WAL segments = %d, want at least two", len(segments))
	}
	contents := readFile(t, segments[0].path)
	frameOffset := lastFrameOffset(t, contents)
	if err := os.Truncate(segments[0].path, int64(len(contents)-1)); err != nil {
		t.Fatalf("truncate non-final segment: %v", err)
	}

	_, _, err = open(directory, identity, 64)
	for _, detail := range []string{filepath.Base(segments[0].path), fmt.Sprintf("byte offset %d", frameOffset), "body"} {
		if err == nil || !strings.Contains(err.Error(), detail) {
			t.Fatalf("open WAL error = %v, want non-tail corruption detail %q", err, detail)
		}
	}
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

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	return contents
}

func writeInterruptedSegment(t *testing.T, path string, contents []byte, syncComplete bool) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create interrupted segment: %v", err)
	}
	if _, err := file.Write(contents); err != nil {
		file.Close()
		t.Fatalf("write interrupted segment: %v", err)
	}
	if syncComplete {
		if err := file.Sync(); err != nil {
			file.Close()
			t.Fatalf("sync interrupted segment: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close interrupted segment: %v", err)
	}
}

func assertFileSize(t *testing.T, path string, want int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	if info.Size() != want {
		t.Fatalf("%q size = %d, want %d", path, info.Size(), want)
	}
}

func lastFrameOffset(t *testing.T, contents []byte) int64 {
	t.Helper()
	offset := segmentHeaderSize
	for offset < int64(len(contents)) {
		if offset+frameHeaderSize > int64(len(contents)) {
			t.Fatalf("test segment has incomplete frame header at %d", offset)
		}
		length := int64(binary.BigEndian.Uint32(contents[offset : offset+4]))
		next := offset + frameHeaderSize + length
		if next > int64(len(contents)) {
			t.Fatalf("test segment has incomplete frame body at %d", offset)
		}
		if next == int64(len(contents)) {
			return offset
		}
		offset = next
	}
	t.Fatal("test segment has no frames")
	return 0
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
