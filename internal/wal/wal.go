// Package wal persists the consensus state owned by one QuorumKV Node.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	segmentMagic       = "QKVWAL01"
	formatVersion      = uint32(1)
	defaultSegmentSize = int64(16 << 20)
	segmentHeaderSize  = int64(len(segmentMagic) + 4)
	frameHeaderSize    = int64(8)
	maxRecordSize      = uint32(2 << 20)
)

type recordType byte

const (
	recordClusterIdentity recordType = 1
	recordNodeIdentity    recordType = 2
	recordHardState       recordType = 3
	recordLogEntries      recordType = 4 // Legacy no-op-only entries.
	recordLogEntriesV2    recordType = 5
	recordLogEntryV3      recordType = 6
	recordLogTruncation   recordType = 7
	recordCommitIndex     recordType = 8
)

// Identity binds durable state to one Node in one Cluster.
type Identity struct {
	ClusterID string
	NodeID    string
}

// HardState is the election state that must survive a restart.
type HardState struct {
	Term     uint64
	VotedFor string
}

// EntryType identifies the command stored in a durable log entry.
type EntryType uint8

// LogEntry is the durable representation of one Raft log position.
type LogEntry struct {
	Index     uint64
	Term      uint64
	Type      EntryType
	SessionID [16]byte
	Sequence  uint64
	Key       string
	Value     []byte
}

// RecoveredState is the latest valid state reconstructed from all WAL segments.
type RecoveredState struct {
	Identity    Identity
	HardState   HardState
	Log         []LogEntry
	CommitIndex uint64
}

// SaveLogEntries appends ordered entries and returns only after they are synced.
func (w *WAL) SaveLogEntries(entries []LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("save log entries: WAL is closed")
	}
	if len(entries) == 0 {
		return errors.New("save log entries: at least one entry is required")
	}
	for offset, entry := range entries {
		want := w.logLength + uint64(offset) + 1
		if entry.Index != want {
			return fmt.Errorf("save log entries: index %d follows index %d", entry.Index, want-1)
		}
	}
	for _, entry := range entries {
		payload := make([]byte, 49+len(entry.Key)+len(entry.Value))
		binary.BigEndian.PutUint64(payload[0:8], entry.Index)
		binary.BigEndian.PutUint64(payload[8:16], entry.Term)
		payload[16] = byte(entry.Type)
		copy(payload[17:33], entry.SessionID[:])
		binary.BigEndian.PutUint64(payload[33:41], entry.Sequence)
		binary.BigEndian.PutUint32(payload[41:45], uint32(len(entry.Key)))
		binary.BigEndian.PutUint32(payload[45:49], uint32(len(entry.Value)))
		copy(payload[49:49+len(entry.Key)], entry.Key)
		copy(payload[49+len(entry.Key):], entry.Value)
		if err := w.appendRecord(recordLogEntryV3, payload); err != nil {
			return fmt.Errorf("append log entry at index %d: %w", entry.Index, err)
		}
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync %d log entries: %w", len(entries), err)
	}
	w.logLength += uint64(len(entries))
	return nil
}

// TruncateLog removes the suffix beginning at firstIndex and returns only
// after the truncation record is synced. Committed history cannot be removed.
func (w *WAL) TruncateLog(firstIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("truncate log: WAL is closed")
	}
	if firstIndex == 0 || firstIndex > w.logLength+1 {
		return fmt.Errorf("truncate log: first index %d is outside log ending at %d", firstIndex, w.logLength)
	}
	if firstIndex <= w.commitIndex {
		return fmt.Errorf("truncate log: first index %d would remove committed index %d", firstIndex, w.commitIndex)
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, firstIndex)
	if err := w.appendRecord(recordLogTruncation, payload); err != nil {
		return fmt.Errorf("append log truncation from index %d: %w", firstIndex, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync log truncation from index %d: %w", firstIndex, err)
	}
	w.logLength = firstIndex - 1
	return nil
}

// SaveCommitIndex records the durable committed prefix and returns only after
// the containing segment is synced.
func (w *WAL) SaveCommitIndex(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("save commit index: WAL is closed")
	}
	if index < w.commitIndex {
		return fmt.Errorf("save commit index: index decreased from %d to %d", w.commitIndex, index)
	}
	if index > w.logLength {
		return fmt.Errorf("save commit index: index %d exceeds log ending at %d", index, w.logLength)
	}
	if index == w.commitIndex {
		return nil
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, index)
	if err := w.appendRecord(recordCommitIndex, payload); err != nil {
		return fmt.Errorf("append commit index %d: %w", index, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync commit index %d: %w", index, err)
	}
	w.commitIndex = index
	return nil
}

// WAL is an ordered, synced sequence of consensus records. A WAL must not be
// used after Close.
type WAL struct {
	mu          sync.Mutex
	directory   string
	segmentSize int64
	segment     uint64
	file        *os.File
	offset      int64
	logLength   uint64
	commitIndex uint64
}

// Open creates or recovers the WAL in directory. Existing durable identity
// must match the configured identity exactly.
func Open(directory string, identity Identity) (*WAL, RecoveredState, error) {
	return open(directory, identity, defaultSegmentSize)
}

func open(directory string, identity Identity, segmentSize int64) (*WAL, RecoveredState, error) {
	if strings.TrimSpace(identity.ClusterID) == "" || strings.TrimSpace(identity.NodeID) == "" {
		return nil, RecoveredState{}, errors.New("open WAL: Cluster Identity and Node Identity are required")
	}
	if segmentSize <= segmentHeaderSize+frameHeaderSize {
		return nil, RecoveredState{}, fmt.Errorf("open WAL: segment size %d is too small", segmentSize)
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, RecoveredState{}, fmt.Errorf("create WAL directory %q: %w", directory, err)
	}

	segments, err := findSegments(directory)
	if err != nil {
		return nil, RecoveredState{}, err
	}
	wal := &WAL{directory: directory, segmentSize: segmentSize}
	if len(segments) == 0 {
		if err := wal.createSegment(1); err != nil {
			return nil, RecoveredState{}, err
		}
		if err := wal.appendRecord(recordClusterIdentity, []byte(identity.ClusterID)); err != nil {
			wal.Close()
			return nil, RecoveredState{}, fmt.Errorf("append Cluster Identity: %w", err)
		}
		if err := wal.appendRecord(recordNodeIdentity, []byte(identity.NodeID)); err != nil {
			wal.Close()
			return nil, RecoveredState{}, fmt.Errorf("append Node Identity: %w", err)
		}
		if err := wal.file.Sync(); err != nil {
			wal.Close()
			return nil, RecoveredState{}, fmt.Errorf("sync durable identity: %w", err)
		}
		return wal, RecoveredState{Identity: identity}, nil
	}

	recovered, err := recoverSegments(segments)
	if err != nil {
		return nil, RecoveredState{}, err
	}
	if recovered.Identity != identity {
		return nil, RecoveredState{}, fmt.Errorf(
			"durable identity mismatch: configured Cluster %q Node %q, WAL contains Cluster %q Node %q",
			identity.ClusterID, identity.NodeID, recovered.Identity.ClusterID, recovered.Identity.NodeID,
		)
	}

	last := segments[len(segments)-1]
	file, err := os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, RecoveredState{}, fmt.Errorf("open active WAL segment %q: %w", last.path, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, RecoveredState{}, fmt.Errorf("stat active WAL segment %q: %w", last.path, err)
	}
	wal.segment = last.number
	wal.file = file
	wal.offset = info.Size()
	wal.logLength = uint64(len(recovered.Log))
	wal.commitIndex = recovered.CommitIndex
	return wal, recovered, nil
}

// SaveHardState appends Term and vote together and returns only after the
// containing segment is synced.
func (w *WAL) SaveHardState(state HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("save hard state: WAL is closed")
	}
	payload := make([]byte, 8+len(state.VotedFor))
	binary.BigEndian.PutUint64(payload, state.Term)
	copy(payload[8:], state.VotedFor)
	if err := w.appendRecord(recordHardState, payload); err != nil {
		return fmt.Errorf("append hard state for Term %d: %w", state.Term, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync hard state for Term %d: %w", state.Term, err)
	}
	return nil
}

// Close releases the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	if err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	return nil
}

type segmentFile struct {
	number uint64
	path   string
}

func findSegments(directory string) ([]segmentFile, error) {
	matches, err := filepath.Glob(filepath.Join(directory, "wal-*.qwal"))
	if err != nil {
		return nil, fmt.Errorf("find WAL segments: %w", err)
	}
	segments := make([]segmentFile, 0, len(matches))
	for _, path := range matches {
		var number uint64
		name := filepath.Base(path)
		if _, err := fmt.Sscanf(name, "wal-%016d.qwal", &number); err != nil {
			return nil, fmt.Errorf("parse WAL segment name %q: %w", name, err)
		}
		segments = append(segments, segmentFile{number: number, path: path})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].number < segments[j].number })
	for index, segment := range segments {
		want := uint64(index + 1)
		if segment.number != want {
			return nil, fmt.Errorf("WAL segment sequence contains %d, want %d", segment.number, want)
		}
	}
	return segments, nil
}

func (w *WAL) createSegment(number uint64) error {
	path := filepath.Join(w.directory, fmt.Sprintf("wal-%016d.qwal", number))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create WAL segment %q: %w", path, err)
	}
	header := make([]byte, segmentHeaderSize)
	copy(header, segmentMagic)
	binary.BigEndian.PutUint32(header[len(segmentMagic):], formatVersion)
	if _, err := file.Write(header); err != nil {
		file.Close()
		return fmt.Errorf("write WAL segment header %q: %w", path, err)
	}
	w.segment = number
	w.file = file
	w.offset = segmentHeaderSize
	return nil
}

func (w *WAL) appendRecord(kind recordType, payload []byte) error {
	body := append([]byte{byte(kind)}, payload...)
	if uint64(len(body)) > uint64(maxRecordSize) {
		return fmt.Errorf("record size %d exceeds limit %d", len(body), maxRecordSize)
	}
	frameSize := frameHeaderSize + int64(len(body))
	if w.offset > segmentHeaderSize && w.offset+frameSize > w.segmentSize {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync full WAL segment: %w", err)
		}
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close full WAL segment: %w", err)
		}
		w.file = nil
		if err := w.createSegment(w.segment + 1); err != nil {
			return err
		}
	}
	header := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(body))
	if _, err := w.file.Write(header); err != nil {
		return fmt.Errorf("write record frame: %w", err)
	}
	if _, err := w.file.Write(body); err != nil {
		return fmt.Errorf("write record body: %w", err)
	}
	w.offset += frameSize
	return nil
}

func recoverSegments(segments []segmentFile) (RecoveredState, error) {
	var state RecoveredState
	for index, segment := range segments {
		finalSegment := index == len(segments)-1
		flags := os.O_RDONLY
		if finalSegment {
			flags = os.O_RDWR
		}
		file, err := os.OpenFile(segment.path, flags, 0)
		if err != nil {
			return RecoveredState{}, fmt.Errorf("open WAL segment %q: %w", segment.path, err)
		}
		err = readSegment(file, segment.path, finalSegment, &state)
		closeErr := file.Close()
		if err != nil {
			return RecoveredState{}, err
		}
		if closeErr != nil {
			return RecoveredState{}, fmt.Errorf("close WAL segment %q: %w", segment.path, closeErr)
		}
	}
	if state.Identity.ClusterID == "" || state.Identity.NodeID == "" {
		return RecoveredState{}, errors.New("WAL does not contain complete durable identity")
	}
	return state, nil
}

func readSegment(file *os.File, path string, recoverFinalFrame bool, state *RecoveredState) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL segment %q: %w", path, err)
	}
	size := info.Size()
	header := make([]byte, segmentHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("read WAL segment header %q: %w", path, err)
	}
	if string(header[:len(segmentMagic)]) != segmentMagic {
		return fmt.Errorf("WAL segment %q has invalid magic", path)
	}
	version := binary.BigEndian.Uint32(header[len(segmentMagic):])
	if version != formatVersion {
		return fmt.Errorf("WAL segment %q has unsupported format version %d", path, version)
	}

	for offset := segmentHeaderSize; ; {
		if offset == size {
			return nil
		}
		if size-offset < frameHeaderSize {
			if recoverFinalFrame {
				return truncateFinalFrame(file, path, offset)
			}
			return fmt.Errorf("read WAL segment %q byte offset %d frame header: %w", path, offset, io.ErrUnexpectedEOF)
		}
		frameHeader := make([]byte, frameHeaderSize)
		if _, err := file.ReadAt(frameHeader, offset); err != nil {
			return fmt.Errorf("read WAL segment %q byte offset %d frame header: %w", path, offset, err)
		}
		length := binary.BigEndian.Uint32(frameHeader[0:4])
		if length == 0 {
			return fmt.Errorf("WAL segment %q byte offset %d frame has zero length", path, offset)
		}
		if length > maxRecordSize {
			return fmt.Errorf("WAL segment %q byte offset %d frame length %d exceeds limit %d", path, offset, length, maxRecordSize)
		}
		frameEnd := offset + frameHeaderSize + int64(length)
		if frameEnd > size {
			if recoverFinalFrame {
				return truncateFinalFrame(file, path, offset)
			}
			return fmt.Errorf("read WAL segment %q byte offset %d frame body: %w", path, offset, io.ErrUnexpectedEOF)
		}
		body := make([]byte, length)
		if _, err := file.ReadAt(body, offset+frameHeaderSize); err != nil {
			return fmt.Errorf("read WAL segment %q byte offset %d frame body: %w", path, offset, err)
		}
		wantChecksum := binary.BigEndian.Uint32(frameHeader[4:8])
		if got := crc32.ChecksumIEEE(body); got != wantChecksum {
			if recoverFinalFrame && frameEnd == size {
				return truncateFinalFrame(file, path, offset)
			}
			return fmt.Errorf("WAL segment %q byte offset %d frame checksum mismatch: got %08x, want %08x", path, offset, got, wantChecksum)
		}
		if err := applyRecord(recordType(body[0]), body[1:], state); err != nil {
			return fmt.Errorf("decode WAL segment %q byte offset %d frame: %w", path, offset, err)
		}
		offset = frameEnd
	}
}

func truncateFinalFrame(file *os.File, path string, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate interrupted final WAL frame in segment %q at byte offset %d: %w", path, offset, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync truncated WAL segment %q at byte offset %d: %w", path, offset, err)
	}
	return nil
}

func applyRecord(kind recordType, payload []byte, state *RecoveredState) error {
	switch kind {
	case recordClusterIdentity:
		if state.Identity.ClusterID != "" {
			return errors.New("duplicate Cluster Identity record")
		}
		state.Identity.ClusterID = string(payload)
	case recordNodeIdentity:
		if state.Identity.ClusterID == "" {
			return errors.New("node identity appears before Cluster Identity")
		}
		if state.Identity.NodeID != "" {
			return errors.New("duplicate Node Identity record")
		}
		state.Identity.NodeID = string(payload)
	case recordHardState:
		if state.Identity.NodeID == "" {
			return errors.New("hard state appears before durable identity")
		}
		if len(payload) < 8 {
			return errors.New("hard-state record is shorter than its Term")
		}
		term := binary.BigEndian.Uint64(payload[:8])
		if term < state.HardState.Term {
			return fmt.Errorf("hard-state Term decreased from %d to %d", state.HardState.Term, term)
		}
		state.HardState = HardState{Term: term, VotedFor: string(payload[8:])}
	case recordLogEntries:
		if len(payload) == 0 || len(payload)%17 != 0 {
			return fmt.Errorf("log-entry record length %d is invalid", len(payload))
		}
		for offset := 0; offset < len(payload); offset += 17 {
			entry := LogEntry{
				Index: binary.BigEndian.Uint64(payload[offset : offset+8]),
				Term:  binary.BigEndian.Uint64(payload[offset+8 : offset+16]),
				Type:  EntryType(payload[offset+16]),
			}
			lastIndex := uint64(len(state.Log))
			if entry.Index != lastIndex+1 {
				return fmt.Errorf("log entry index %d follows index %d", entry.Index, lastIndex)
			}
			state.Log = append(state.Log, entry)
		}
	case recordLogEntriesV2:
		if len(payload) == 0 || len(payload)%33 != 0 {
			return fmt.Errorf("session log-entry record length %d is invalid", len(payload))
		}
		for offset := 0; offset < len(payload); offset += 33 {
			entry := LogEntry{
				Index: binary.BigEndian.Uint64(payload[offset : offset+8]),
				Term:  binary.BigEndian.Uint64(payload[offset+8 : offset+16]),
				Type:  EntryType(payload[offset+16]),
			}
			copy(entry.SessionID[:], payload[offset+17:offset+33])
			lastIndex := uint64(len(state.Log))
			if entry.Index != lastIndex+1 {
				return fmt.Errorf("log entry index %d follows index %d", entry.Index, lastIndex)
			}
			state.Log = append(state.Log, entry)
		}
	case recordLogEntryV3:
		if len(payload) < 49 {
			return fmt.Errorf("log-entry record length %d is shorter than its header", len(payload))
		}
		keyLength := binary.BigEndian.Uint32(payload[41:45])
		valueLength := binary.BigEndian.Uint32(payload[45:49])
		wantLength := uint64(49) + uint64(keyLength) + uint64(valueLength)
		if wantLength != uint64(len(payload)) {
			return fmt.Errorf("log-entry record length %d does not match encoded length %d", len(payload), wantLength)
		}
		entry := LogEntry{
			Index:    binary.BigEndian.Uint64(payload[0:8]),
			Term:     binary.BigEndian.Uint64(payload[8:16]),
			Type:     EntryType(payload[16]),
			Sequence: binary.BigEndian.Uint64(payload[33:41]),
			Key:      string(payload[49 : 49+keyLength]),
			Value:    append([]byte(nil), payload[49+keyLength:]...),
		}
		copy(entry.SessionID[:], payload[17:33])
		lastIndex := uint64(len(state.Log))
		if entry.Index != lastIndex+1 {
			return fmt.Errorf("log entry index %d follows index %d", entry.Index, lastIndex)
		}
		state.Log = append(state.Log, entry)
	case recordLogTruncation:
		if len(payload) != 8 {
			return fmt.Errorf("log-truncation record length %d, want 8", len(payload))
		}
		firstIndex := binary.BigEndian.Uint64(payload)
		if firstIndex == 0 || firstIndex > uint64(len(state.Log))+1 {
			return fmt.Errorf("log truncation from index %d is outside log ending at %d", firstIndex, len(state.Log))
		}
		if firstIndex <= state.CommitIndex {
			return fmt.Errorf("log truncation from index %d would remove committed index %d", firstIndex, state.CommitIndex)
		}
		state.Log = state.Log[:firstIndex-1]
	case recordCommitIndex:
		if len(payload) != 8 {
			return fmt.Errorf("commit-index record length %d, want 8", len(payload))
		}
		index := binary.BigEndian.Uint64(payload)
		if index < state.CommitIndex {
			return fmt.Errorf("commit index decreased from %d to %d", state.CommitIndex, index)
		}
		if index > uint64(len(state.Log)) {
			return fmt.Errorf("commit index %d exceeds log ending at %d", index, len(state.Log))
		}
		state.CommitIndex = index
	default:
		return fmt.Errorf("unknown record type %d", kind)
	}
	return nil
}
