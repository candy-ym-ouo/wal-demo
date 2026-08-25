package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// RecoveryResult summarizes startup validation and repair.
type RecoveryResult struct {
	Records   uint64 `json:"records"`
	LastSeq   uint64 `json:"lastSeq"`
	Truncated bool   `json:"truncated"`
}

type scanState struct {
	assembling bool
	seq        uint64
	start      int64
	data       []byte
	lastSeq    uint64
	records    uint64
}

func (w *WAL) recoverLocked(fn ReplayFunc) (RecoveryResult, error) {
	w.index.reset()
	state := scanState{}
	if len(w.segments) > 0 {
		// A snapshot may have allowed older segments to be removed. In that
		// case the first remaining segment legitimately starts above sequence 1.
		state.lastSeq = w.segments[0].startSeq - 1
	}
	result := RecoveryResult{}
	for si, seg := range w.segments {
		if si > 0 && seg.startSeq != state.lastSeq+1 {
			return result, &CorruptionError{
				Segment: seg.startSeq,
				Offset:  0,
				Reason:  fmt.Sprintf("segment starts at %d, expected %d", seg.startSeq, state.lastSeq+1),
			}
		}
		isLast := si == len(w.segments)-1
		truncated, err := w.scanSegment(seg, isLast, &state, fn)
		if err != nil {
			return result, err
		}
		if truncated {
			result.Truncated = true
			if err := w.discardSegmentsAfter(seg.startSeq); err != nil {
				return result, err
			}
			break
		}
	}
	if state.assembling {
		seg := w.segmentForSeq(state.seq)
		if seg == nil {
			return result, fmt.Errorf("incomplete record segment missing")
		}
		if err := w.trimTail(seg, state.start); err != nil {
			return result, err
		}
		if err := w.discardSegmentsAfter(seg.startSeq); err != nil {
			return result, err
		}
		result.Truncated = true
		state.lastSeq = state.seq - 1
	}
	result.Records = state.records
	result.LastSeq = state.lastSeq
	w.nextSeq = state.lastSeq + 1
	w.durable.Store(state.lastSeq)
	w.metrics.replayed.Add(state.records)
	if len(w.segments) == 0 {
		seg, err := openSegment(w.dir, w.nextSeq, true)
		if err != nil {
			return result, err
		}
		w.segments = append(w.segments, seg)
	}
	w.cur = w.segments[len(w.segments)-1]
	w.metrics.segments.Store(uint64(len(w.segments)))
	return result, nil
}

func (w *WAL) scanSegment(seg *segment, isLast bool, state *scanState, fn ReplayFunc) (bool, error) {
	offset := int64(0)
	for offset < seg.size {
		recStart := offset
		rec, next, err := decodeRecordAt(seg.file, offset, w.opts.MaxRecordBytes)
		if err != nil {
			if isLast && tailError(err) {
				if state.assembling {
					recStart = state.start
					state.assembling = false
					state.data = nil
				}
				return true, w.trimTail(seg, recStart)
			}
			w.metrics.checksumFailure.Add(1)
			return false, &CorruptionError{Segment: seg.startSeq, Offset: offset, Reason: err.Error()}
		}
		if rec.flags&flagPadding != 0 {
			offset = next
			continue
		}
		if rec.flags&flagFirst != 0 {
			if state.assembling || rec.seq != state.lastSeq+1 {
				return false, &CorruptionError{Segment: seg.startSeq, Offset: offset, Reason: "unexpected record sequence"}
			}
			state.assembling = true
			state.seq = rec.seq
			state.start = recStart
			state.data = state.data[:0]
		} else if !state.assembling || rec.seq != state.seq {
			return false, &CorruptionError{Segment: seg.startSeq, Offset: offset, Reason: "orphan continuation fragment"}
		}
		state.data = append(state.data, rec.data...)
		if rec.flags&flagLast != 0 {
			payload := append([]byte(nil), state.data...)
			w.index.put(rec.seq, location{segment: seg.startSeq, offset: state.start, bytes: next - state.start})
			if fn != nil {
				if err := fn(rec.seq, payload); err != nil {
					return false, err
				}
			}
			state.records++
			state.lastSeq = rec.seq
			state.assembling = false
			state.data = nil
		}
		offset = next
	}
	if state.assembling && !isLast {
		return false, &CorruptionError{Segment: seg.startSeq, Offset: state.start, Reason: "record crosses segment boundary"}
	}
	return false, nil
}

func loadSegments(dir string) ([]*segment, error) {
	starts, err := listSegmentStarts(dir)
	if err != nil {
		return nil, err
	}
	segments := make([]*segment, 0, len(starts))
	for _, start := range starts {
		seg, err := openSegment(dir, start, false)
		if err != nil {
			for _, opened := range segments {
				_ = opened.close()
			}
			return nil, err
		}
		segments = append(segments, seg)
	}
	return segments, nil
}

func removeFileAndSyncDir(path, dir string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}

var _ = io.EOF
