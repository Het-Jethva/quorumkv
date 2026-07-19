// Package wal persists the consensus state owned by one QuorumKV Node.
package wal

import (
	"bufio"
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

// RecoveredState is the latest valid state reconstructed from all WAL segments.
type RecoveredState struct {
	Identity  Identity
	HardState HardState
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
	for _, segment := range segments {
		file, err := os.Open(segment.path)
		if err != nil {
			return RecoveredState{}, fmt.Errorf("open WAL segment %q: %w", segment.path, err)
		}
		err = readSegment(file, segment.path, &state)
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

func readSegment(file *os.File, path string, state *RecoveredState) error {
	header := make([]byte, segmentHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("read WAL segment header %q: %w", path, err)
	}
	if string(header[:len(segmentMagic)]) != segmentMagic {
		return fmt.Errorf("WAL segment %q has invalid magic", path)
	}
	version := binary.BigEndian.Uint32(header[len(segmentMagic):])
	if version != formatVersion {
		return fmt.Errorf("WAL segment %q has unsupported format version %d", path, version)
	}

	reader := bufio.NewReader(file)
	for frame := 1; ; frame++ {
		frameHeader := make([]byte, frameHeaderSize)
		_, err := io.ReadFull(reader, frameHeader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read WAL segment %q frame %d header: %w", path, frame, err)
		}
		length := binary.BigEndian.Uint32(frameHeader[0:4])
		if length == 0 {
			return fmt.Errorf("WAL segment %q frame %d has zero length", path, frame)
		}
		if length > maxRecordSize {
			return fmt.Errorf("WAL segment %q frame %d length %d exceeds limit %d", path, frame, length, maxRecordSize)
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			return fmt.Errorf("read WAL segment %q frame %d body: %w", path, frame, err)
		}
		wantChecksum := binary.BigEndian.Uint32(frameHeader[4:8])
		if got := crc32.ChecksumIEEE(body); got != wantChecksum {
			return fmt.Errorf("WAL segment %q frame %d checksum mismatch: got %08x, want %08x", path, frame, got, wantChecksum)
		}
		if err := applyRecord(recordType(body[0]), body[1:], state); err != nil {
			return fmt.Errorf("decode WAL segment %q frame %d: %w", path, frame, err)
		}
	}
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
	default:
		return fmt.Errorf("unknown record type %d", kind)
	}
	return nil
}
