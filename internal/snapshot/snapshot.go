// Package snapshot stores immutable point-in-time images of replicated state.
package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	magic          = "QKVSNP01"
	formatVersion  = uint32(1)
	headerSize     = len(magic) + 4 + 8 + 4
	maxPayloadSize = uint64(4 << 30)
)

// Identity binds a Snapshot to one Node and the fixed membership of its Cluster.
type Identity struct {
	ClusterID string   `json:"cluster_id"`
	NodeID    string   `json:"node_id"`
	MemberIDs []string `json:"member_ids"`
}

// Session is the replicated state retained for one Client Session.
type Session struct {
	ID                [16]byte `json:"id"`
	Closed            bool     `json:"closed"`
	LastSequence      uint64   `json:"last_sequence"`
	LastResult        uint8    `json:"last_result"`
	LastDeleteExisted bool     `json:"last_delete_existed"`
}

// State is the complete replicated state through IncludedIndex.
type State struct {
	Identity      Identity          `json:"identity"`
	IncludedIndex uint64            `json:"included_index"`
	IncludedTerm  uint64            `json:"included_term"`
	Values        map[string][]byte `json:"values"`
	Sessions      []Session         `json:"sessions"`
}

// Compatibility describes the durable WAL history a Snapshot must match.
type Compatibility struct {
	Identity      Identity
	CommitIndex   uint64
	LogStartIndex uint64
	LogTerms      []uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Handle provides read-only access to one validated immutable Snapshot file.
// The caller owns the Handle and must close it.
type Handle struct {
	file     *os.File
	length   uint64
	checksum uint32
}

// Length returns the complete encoded Snapshot file length.
func (h *Handle) Length() uint64 {
	return h.length
}

// Checksum returns the checksum of the complete encoded Snapshot file.
func (h *Handle) Checksum() uint32 {
	return h.checksum
}

// ReadAt reads an encoded Snapshot range without permitting reads beyond the
// validated immutable file.
func (h *Handle) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) > h.length || uint64(len(data)) > h.length-uint64(offset) {
		return 0, fmt.Errorf("read Snapshot range at offset %d with length %d exceeds file length %d", offset, len(data), h.length)
	}
	return h.file.ReadAt(data, offset)
}

// Close releases the open immutable Snapshot file.
func (h *Handle) Close() error {
	return h.file.Close()
}

// Save writes and syncs a temporary file before atomically installing a
// uniquely named immutable Snapshot.
func Save(directory string, state State) (string, error) {
	if err := validateState(state); err != nil {
		return "", fmt.Errorf("save Snapshot: %w", err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("encode Snapshot: %w", err)
	}
	if uint64(len(payload)) > maxPayloadSize {
		return "", fmt.Errorf("save Snapshot: payload size %d exceeds limit %d", len(payload), maxPayloadSize)
	}

	header := make([]byte, headerSize)
	copy(header, magic)
	binary.BigEndian.PutUint32(header[len(magic):], formatVersion)
	binary.BigEndian.PutUint64(header[len(magic)+4:], uint64(len(payload)))
	binary.BigEndian.PutUint32(header[len(magic)+12:], crc32.ChecksumIEEE(payload))

	if err := os.MkdirAll(directory, 0o750); err != nil {
		return "", fmt.Errorf("create Snapshot directory %q: %w", directory, err)
	}
	temporary, err := os.CreateTemp(directory, fmt.Sprintf("snapshot-%020d-%020d-*.tmp", state.IncludedIndex, state.IncludedTerm))
	if err != nil {
		return "", fmt.Errorf("create temporary Snapshot: %w", err)
	}
	temporaryName := temporary.Name()
	installed := false
	defer func() {
		if !installed {
			_ = temporary.Close()
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", fmt.Errorf("set temporary Snapshot permissions: %w", err)
	}
	if _, err := temporary.Write(header); err != nil {
		return "", fmt.Errorf("write Snapshot header: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		return "", fmt.Errorf("write Snapshot payload: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return "", fmt.Errorf("sync temporary Snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close temporary Snapshot: %w", err)
	}

	finalName := strings.TrimSuffix(temporaryName, ".tmp") + ".qsnap"
	if _, err := os.Lstat(finalName); err == nil {
		return "", fmt.Errorf("install Snapshot %q: immutable destination already exists", finalName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect Snapshot destination %q: %w", finalName, err)
	}
	if err := os.Rename(temporaryName, finalName); err != nil {
		return "", fmt.Errorf("install Snapshot %q: %w", finalName, err)
	}
	installed = true
	if err := syncDirectory(directory); err != nil {
		return "", fmt.Errorf("sync Snapshot directory: %w", err)
	}
	return finalName, nil
}

// LoadNewest validates final Snapshot candidates and returns the newest one
// whose included position is present in the durable committed WAL.
func LoadNewest(directory string, compatibility Compatibility) (*State, error) {
	matches, err := filepath.Glob(filepath.Join(directory, "snapshot-*.qsnap"))
	if err != nil {
		return nil, fmt.Errorf("find Snapshots: %w", err)
	}
	if compatibility.SnapshotIndex > 0 {
		if err := requireCheckpointSnapshot(directory, compatibility); err != nil {
			return nil, err
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	var newest *State
	var newestName string
	for _, name := range matches {
		state, err := read(name)
		if err != nil {
			return nil, err
		}
		if !equalIdentity(state.Identity, compatibility.Identity) {
			return nil, fmt.Errorf("stored Snapshot %q identity does not match configured Cluster membership", name)
		}
		if !matchesWAL(*state, compatibility) {
			continue
		}
		if newest == nil || state.IncludedIndex > newest.IncludedIndex ||
			(state.IncludedIndex == newest.IncludedIndex && (state.IncludedTerm > newest.IncludedTerm ||
				(state.IncludedTerm == newest.IncludedTerm && name > newestName))) {
			newest, newestName = state, name
		}
	}
	if compatibility.SnapshotIndex > 0 && newest == nil {
		return nil, errors.New("no stored Snapshot is compatible with the durable WAL")
	}
	return newest, nil
}

func requireCheckpointSnapshot(directory string, compatibility Compatibility) error {
	index, term := compatibility.SnapshotIndex, compatibility.SnapshotTerm
	pattern := filepath.Join(directory, fmt.Sprintf("snapshot-%020d-%020d-*.qsnap", index, term))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("find required Snapshot at %d/%d: %w", index, term, err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("required Snapshot at %d/%d is not installed", index, term)
	}
	for _, name := range matches {
		state, err := read(name)
		if err != nil {
			return fmt.Errorf("required Snapshot at %d/%d is invalid: %w", index, term, err)
		}
		if state.IncludedIndex != index || state.IncludedTerm != term {
			return fmt.Errorf(
				"required Snapshot at %d/%d has encoded position %d/%d",
				index, term, state.IncludedIndex, state.IncludedTerm,
			)
		}
		if !equalIdentity(state.Identity, compatibility.Identity) {
			return fmt.Errorf("required Snapshot at %d/%d identity does not match configured Cluster membership", index, term)
		}
	}
	return nil
}

// Open locates and validates the newest immutable Snapshot file at the
// requested position. Validation streams through the file and does not retain
// its complete encoding in memory.
func Open(directory string, includedIndex, includedTerm uint64) (*Handle, error) {
	pattern := filepath.Join(directory, fmt.Sprintf("snapshot-%020d-%020d-*.qsnap", includedIndex, includedTerm))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("find snapshot at %d/%d: %w", includedIndex, includedTerm, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("snapshot at %d/%d is not installed", includedIndex, includedTerm)
	}
	sort.Strings(matches)
	name := matches[len(matches)-1]
	file, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open snapshot %q: %w", name, err)
	}
	handle, state, err := validateFile(name, file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if state.IncludedIndex != includedIndex || state.IncludedTerm != includedTerm {
		_ = file.Close()
		return nil, fmt.Errorf("snapshot at %d/%d has encoded position %d/%d", includedIndex, includedTerm, state.IncludedIndex, state.IncludedTerm)
	}
	return handle, nil
}

// Encoded returns a complete validated encoding. Transfer paths should use
// Open so large Snapshot files are not retained in memory.
func Encoded(directory string, includedIndex, includedTerm uint64) ([]byte, error) {
	handle, err := Open(directory, includedIndex, includedTerm)
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	contents := make([]byte, handle.Length())
	if _, err := handle.ReadAt(contents, 0); err != nil {
		return nil, fmt.Errorf("read encoded Snapshot: %w", err)
	}
	return contents, nil
}

// Decode validates an encoded Snapshot file before returning its state.
func Decode(contents []byte) (*State, error) {
	return decode("received Snapshot", contents)
}

func read(name string) (*State, error) {
	info, err := os.Stat(name)
	if err != nil {
		return nil, fmt.Errorf("stat Snapshot %q: %w", name, err)
	}
	if info.Size() > int64(maxPayloadSize)+int64(headerSize) {
		return nil, fmt.Errorf("stored Snapshot %q size %d exceeds limit %d", name, info.Size(), maxPayloadSize+uint64(headerSize))
	}
	contents, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read Snapshot %q: %w", name, err)
	}
	return decode(name, contents)
}

func validateFile(name string, file *os.File) (*Handle, *State, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("stat Snapshot %q: %w", name, err)
	}
	maxSize := int64(maxPayloadSize) + int64(headerSize)
	if info.Size() > maxSize {
		return nil, nil, fmt.Errorf("stored Snapshot %q size %d exceeds limit %d", name, info.Size(), maxSize)
	}
	if info.Size() < int64(headerSize) {
		return nil, nil, fmt.Errorf("stored Snapshot %q is shorter than its header", name)
	}

	header := make([]byte, headerSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, nil, fmt.Errorf("read Snapshot %q header: %w", name, err)
	}
	if string(header[:len(magic)]) != magic {
		return nil, nil, fmt.Errorf("stored Snapshot %q has invalid magic", name)
	}
	version := binary.BigEndian.Uint32(header[len(magic):])
	if version != formatVersion {
		return nil, nil, fmt.Errorf("stored Snapshot %q has unsupported format version %d", name, version)
	}
	payloadLength := binary.BigEndian.Uint64(header[len(magic)+4:])
	if payloadLength > maxPayloadSize {
		return nil, nil, fmt.Errorf("stored Snapshot %q payload length %d exceeds limit %d", name, payloadLength, maxPayloadSize)
	}
	if payloadLength != uint64(info.Size()-int64(headerSize)) {
		return nil, nil, fmt.Errorf("stored Snapshot %q length is %d, header records %d", name, info.Size()-int64(headerSize), payloadLength)
	}

	payloadChecksum := crc32.NewIEEE()
	fileChecksum := crc32.NewIEEE()
	_, _ = fileChecksum.Write(header)
	payload := io.NewSectionReader(file, int64(headerSize), int64(payloadLength))
	stream := io.TeeReader(payload, io.MultiWriter(payloadChecksum, fileChecksum))
	decoder := json.NewDecoder(stream)
	decoder.DisallowUnknownFields()
	var state State
	decodeErr := decoder.Decode(&state)
	if decodeErr == nil {
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			decodeErr = errors.New("trailing payload data")
		}
	}
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return nil, nil, fmt.Errorf("read Snapshot %q payload: %w", name, err)
	}
	wantChecksum := binary.BigEndian.Uint32(header[len(magic)+12:])
	if got := payloadChecksum.Sum32(); got != wantChecksum {
		return nil, nil, fmt.Errorf("stored Snapshot %q checksum mismatch: got %08x, want %08x", name, got, wantChecksum)
	}
	if decodeErr != nil {
		return nil, nil, fmt.Errorf("decode Snapshot %q: %w", name, decodeErr)
	}
	if err := validateState(state); err != nil {
		return nil, nil, fmt.Errorf("validate Snapshot %q: %w", name, err)
	}
	return &Handle{file: file, length: uint64(info.Size()), checksum: fileChecksum.Sum32()}, &state, nil
}

func decode(name string, contents []byte) (*State, error) {
	if uint64(len(contents)) > maxPayloadSize+uint64(headerSize) {
		return nil, fmt.Errorf("stored Snapshot %q size %d exceeds limit %d", name, len(contents), maxPayloadSize+uint64(headerSize))
	}
	if len(contents) < headerSize {
		return nil, fmt.Errorf("stored Snapshot %q is shorter than its header", name)
	}
	if string(contents[:len(magic)]) != magic {
		return nil, fmt.Errorf("stored Snapshot %q has invalid magic", name)
	}
	version := binary.BigEndian.Uint32(contents[len(magic):])
	if version != formatVersion {
		return nil, fmt.Errorf("stored Snapshot %q has unsupported format version %d", name, version)
	}
	length := binary.BigEndian.Uint64(contents[len(magic)+4:])
	if length > maxPayloadSize {
		return nil, fmt.Errorf("stored Snapshot %q payload length %d exceeds limit %d", name, length, maxPayloadSize)
	}
	if length != uint64(len(contents)-headerSize) {
		return nil, fmt.Errorf("stored Snapshot %q length is %d, header records %d", name, len(contents)-headerSize, length)
	}
	payload := contents[headerSize:]
	wantChecksum := binary.BigEndian.Uint32(contents[len(magic)+12:])
	if got := crc32.ChecksumIEEE(payload); got != wantChecksum {
		return nil, fmt.Errorf("stored Snapshot %q checksum mismatch: got %08x, want %08x", name, got, wantChecksum)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var state State
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode Snapshot %q: %w", name, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode Snapshot %q: trailing payload data", name)
	}
	if err := validateState(state); err != nil {
		return nil, fmt.Errorf("validate Snapshot %q: %w", name, err)
	}
	return &state, nil
}

func validateState(state State) error {
	if strings.TrimSpace(state.Identity.ClusterID) == "" || strings.TrimSpace(state.Identity.NodeID) == "" {
		return errors.New("missing Snapshot Cluster Identity or Node Identity")
	}
	if len(state.Identity.MemberIDs) != 3 {
		return fmt.Errorf("member identities contain %d Nodes, want 3", len(state.Identity.MemberIDs))
	}
	nodeIsMember := false
	for index, id := range state.Identity.MemberIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("member identity must not be empty")
		}
		if index > 0 && state.Identity.MemberIDs[index-1] >= id {
			return errors.New("member identities must be unique and sorted")
		}
		if id == state.Identity.NodeID {
			nodeIsMember = true
		}
	}
	if !nodeIsMember {
		return errors.New("local Node Identity is absent from member identities")
	}
	if (state.IncludedIndex == 0) != (state.IncludedTerm == 0) {
		return errors.New("included index and Term must both be zero or both be non-zero")
	}
	seen := make(map[[16]byte]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		if _, exists := seen[session.ID]; exists {
			return fmt.Errorf("duplicate Client Session identity %x", session.ID)
		}
		seen[session.ID] = struct{}{}
	}
	return nil
}

func matchesWAL(state State, compatibility Compatibility) bool {
	if state.IncludedIndex > compatibility.CommitIndex {
		return false
	}
	if state.IncludedIndex == 0 {
		return state.IncludedTerm == 0
	}
	if state.IncludedIndex == compatibility.SnapshotIndex {
		return state.IncludedTerm == compatibility.SnapshotTerm
	}
	start := compatibility.LogStartIndex
	if start == 0 {
		start = 1
	}
	if state.IncludedIndex < start {
		return false
	}
	offset := state.IncludedIndex - start
	if offset >= uint64(len(compatibility.LogTerms)) {
		return false
	}
	return compatibility.LogTerms[offset] == state.IncludedTerm
}

func equalIdentity(left, right Identity) bool {
	if left.ClusterID != right.ClusterID || left.NodeID != right.NodeID || len(left.MemberIDs) != len(right.MemberIDs) {
		return false
	}
	for index := range left.MemberIDs {
		if left.MemberIDs[index] != right.MemberIDs[index] {
			return false
		}
	}
	return true
}

func syncDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
