package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const segmentSuffix = ".wal"

type segment struct {
	startSeq uint64
	path     string
	file     *os.File
	size     int64
}

// SegmentInfo is a read-only description of one physical WAL file.
type SegmentInfo struct {
	StartSeq uint64 `json:"startSeq"`
	EndSeq   uint64 `json:"endSeq"`
	Bytes    int64  `json:"bytes"`
	Current  bool   `json:"current"`
}

func segmentName(start uint64) string {
	return fmt.Sprintf("%020d%s", start, segmentSuffix)
}

func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	base := strings.TrimSuffix(name, segmentSuffix)
	if len(base) != 20 {
		return 0, false
	}
	seq, err := strconv.ParseUint(base, 10, 64)
	return seq, err == nil && seq > 0
}

func listSegmentStarts(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	starts := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if seq, ok := parseSegmentName(entry.Name()); ok {
			starts = append(starts, seq)
		}
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	return starts, nil
}

func openSegment(dir string, start uint64, create bool) (*segment, error) {
	path := filepath.Join(dir, segmentName(start))
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &segment{startSeq: start, path: path, file: f, size: info.Size()}, nil
}

func (s *segment) write(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	n, err := s.file.WriteAt(data, s.size)
	s.size += int64(n)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short WAL write: wrote %d of %d", n, len(data))
	}
	return nil
}

func (s *segment) truncate(size int64) error {
	if size < 0 || size > s.size {
		return fmt.Errorf("invalid truncate size %d for segment size %d", size, s.size)
	}
	if err := s.file.Truncate(size); err != nil {
		return err
	}
	s.size = size
	return nil
}

func (s *segment) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// SegmentInfo returns segment boundaries without exposing file handles. The
// ending sequence is inferred from the next segment's start and the current
// WAL high watermark; this is exact because records never cross segments.
func (w *WAL) SegmentInfo() []SegmentInfo {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]SegmentInfo, 0, len(w.segments))
	for i, seg := range w.segments {
		end := w.nextSeq - 1
		if i+1 < len(w.segments) {
			end = w.segments[i+1].startSeq - 1
		}
		bytes := seg.size
		if seg == w.cur {
			bytes += int64(w.pending.len())
		}
		result = append(result, SegmentInfo{
			StartSeq: seg.startSeq,
			EndSeq:   end,
			Bytes:    bytes,
			Current:  seg == w.cur,
		})
	}
	return result
}
