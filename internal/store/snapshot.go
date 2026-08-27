package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const snapshotMagic = "WSNP"

var errInvalidSnapshot = errors.New("invalid snapshot")

var errInvalidSnapshotName = errors.New("invalid snapshot name")

type snapshotBody struct {
	Applied uint64            `json:"applied"`
	Values  map[string][]byte `json:"values"`
}

// SnapshotInfo describes one primary or archived snapshot without exposing
// the stored key/value contents.
type SnapshotInfo struct {
	Name      string    `json:"name"`
	Applied   uint64    `json:"appliedSeq"`
	Keys      int       `json:"keys"`
	Bytes     int64     `json:"bytes"`
	Checksum  string    `json:"checksum"`
	CreatedAt time.Time `json:"createdAt"`
}

// Snapshot writes a checksummed temporary file, fsyncs it, then atomically
// renames it over the previous snapshot.
func (s *Store) Snapshot() (uint64, error) {
	info, err := s.writeSnapshot("latest", filepath.Join(s.dir, "snapshot.dat"))
	return info.Applied, err
}

// SaveSnapshot stores a named, immutable-at-write-time archive alongside the
// primary recovery snapshot.
func (s *Store) SaveSnapshot(name string) (SnapshotInfo, error) {
	if !validSnapshotName(name) {
		return SnapshotInfo{}, errInvalidSnapshotName
	}
	return s.writeSnapshot(name, filepath.Join(s.dir, "snapshots", name+".snapshot"))
}

func (s *Store) writeSnapshot(name, path string) (SnapshotInfo, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.log.Sync(); err != nil {
		return SnapshotInfo{}, err
	}
	s.mu.RLock()
	body := snapshotBody{Applied: s.applied, Values: make(map[string][]byte, len(s.kv))}
	for key, value := range s.kv {
		body.Values[key] = cloneBytes(value)
	}
	s.mu.RUnlock()
	payload, err := json.Marshal(body)
	if err != nil {
		return SnapshotInfo{}, err
	}
	data := make([]byte, 12+len(payload))
	copy(data[:4], snapshotMagic)
	binary.BigEndian.PutUint32(data[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(data[8:12], crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	copy(data[12:], payload)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return SnapshotInfo{}, err
	}
	if err := writeAtomic(path, data); err != nil {
		return SnapshotInfo{}, err
	}
	info, err := snapshotInfo(path, name)
	if err != nil {
		return SnapshotInfo{}, err
	}
	return info, nil
}

func (s *Store) loadSnapshot() error {
	path := filepath.Join(s.dir, "snapshot.dat")
	body, _, err := readSnapshot(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.kv = make(map[string][]byte, len(body.Values))
	for key, value := range body.Values {
		s.kv[key] = cloneBytes(value)
	}
	s.applied = body.Applied
	s.mu.Unlock()
	return nil
}

// ListSnapshots returns named archives ordered by their names.
func (s *Store) ListSnapshots() ([]SnapshotInfo, error) {
	dir := filepath.Join(s.dir, "snapshots")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []SnapshotInfo{}, nil
	}
	if err != nil {
		return nil, err
	}
	infos := make([]SnapshotInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snapshot") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".snapshot")
		if !validSnapshotName(name) {
			continue
		}
		info, err := snapshotInfo(filepath.Join(dir, entry.Name()), name)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

// InspectSnapshot verifies and returns metadata for a named archive.
func (s *Store) InspectSnapshot(name string) (SnapshotInfo, error) {
	if !validSnapshotName(name) {
		return SnapshotInfo{}, errInvalidSnapshotName
	}
	return snapshotInfo(filepath.Join(s.dir, "snapshots", name+".snapshot"), name)
}

// DeleteSnapshot removes one named archive. It never removes the primary
// snapshot used for restart recovery.
func (s *Store) DeleteSnapshot(name string) error {
	if !validSnapshotName(name) {
		return errInvalidSnapshotName
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	path := filepath.Join(s.dir, "snapshots", name+".snapshot")
	if err := os.Remove(path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func readSnapshot(path string) (snapshotBody, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return snapshotBody{}, nil, err
	}
	if len(data) < 12 || string(data[:4]) != snapshotMagic {
		return snapshotBody{}, nil, fmt.Errorf("%w: header", errInvalidSnapshot)
	}
	length := binary.BigEndian.Uint32(data[4:8])
	if uint64(length) != uint64(len(data)-12) {
		return snapshotBody{}, nil, fmt.Errorf("%w: length", errInvalidSnapshot)
	}
	payload := data[12:]
	want := binary.BigEndian.Uint32(data[8:12])
	got := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	if want != got {
		return snapshotBody{}, nil, fmt.Errorf("%w: checksum mismatch", errInvalidSnapshot)
	}
	var body snapshotBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return snapshotBody{}, nil, fmt.Errorf("%w: decode: %v", errInvalidSnapshot, err)
	}
	return body, data, nil
}

func snapshotInfo(path, name string) (SnapshotInfo, error) {
	body, data, err := readSnapshot(path)
	if err != nil {
		return SnapshotInfo{}, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return SnapshotInfo{}, err
	}
	return SnapshotInfo{
		Name:      name,
		Applied:   body.Applied,
		Keys:      len(body.Values),
		Bytes:     int64(len(data)),
		Checksum:  fmt.Sprintf("%08x", binary.BigEndian.Uint32(data[8:12])),
		CreatedAt: stat.ModTime().UTC(),
	}, nil
}

func validSnapshotName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
