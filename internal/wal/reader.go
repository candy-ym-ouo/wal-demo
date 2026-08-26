package wal

import (
	"context"
	"errors"
	"io"
)

// Entry is a logical record returned by Reader.
type Entry struct {
	Seq     uint64 `json:"seq"`
	Offset  int64  `json:"offset"`
	Segment uint64 `json:"segment"`
	Data    []byte `json:"data"`
}

// Read returns a defensive copy of one complete logical payload.
func (w *WAL) Read(seq uint64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	if seq > w.durable.Load() {
		if _, err := w.flushLocked(); err != nil {
			return nil, err
		}
	}
	loc, ok := w.index.get(seq)
	if !ok {
		return nil, ErrNotFound
	}
	return w.readLocation(seq, loc)
}

func (w *WAL) readLocation(seq uint64, loc location) ([]byte, error) {
	seg := w.segmentByStart(loc.segment)
	if seg == nil {
		return nil, ErrNotFound
	}
	offset := loc.offset
	end := loc.offset + loc.bytes
	data := make([]byte, 0, loc.bytes)
	first := true
	for offset < end {
		rec, next, err := decodeRecordAt(seg.file, offset, w.opts.MaxRecordBytes)
		if err != nil {
			return nil, &CorruptionError{Segment: seg.startSeq, Offset: offset, Reason: err.Error()}
		}
		if rec.seq != seq || (first && rec.flags&flagFirst == 0) {
			return nil, &CorruptionError{Segment: seg.startSeq, Offset: offset, Reason: "index points to unrelated fragment"}
		}
		data = append(data, rec.data...)
		first = false
		offset = next
		if rec.flags&flagLast != 0 {
			return data, nil
		}
	}
	return nil, io.ErrUnexpectedEOF
}

func (w *WAL) segmentByStart(start uint64) *segment {
	for _, seg := range w.segments {
		if seg.startSeq == start {
			return seg
		}
	}
	return nil
}

func (w *WAL) segmentForSeq(seq uint64) *segment {
	var found *segment
	for _, seg := range w.segments {
		if seg.startSeq > seq {
			break
		}
		found = seg
	}
	return found
}

// Reader walks logical records in sequence order.
type Reader struct {
	w    *WAL
	next uint64
}

func (w *WAL) NewReader() *Reader { return &Reader{w: w, next: 1} }

func (w *WAL) NewReaderFrom(seq uint64) *Reader {
	if seq == 0 {
		seq = 1
	}
	return &Reader{w: w, next: seq}
}

// ReadRange returns up to limit logical records beginning at from. A canceled
// context stops the scan before the next record is read.
func (w *WAL) ReadRange(ctx context.Context, from uint64, limit int) ([]Entry, error) {
	if limit < 1 {
		return []Entry{}, nil
	}
	reader := w.NewReaderFrom(from)
	entries := make([]Entry, 0, limit)
	for len(entries) < limit {
		if err := ctx.Err(); err != nil {
			return entries, err
		}
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (r *Reader) Next() (Entry, error) {
	for {
		if r.next > r.w.LastSeq() {
			return Entry{}, io.EOF
		}
		data, err := r.w.Read(r.next)
		if errors.Is(err, ErrNotFound) {
			r.next++
			continue
		}
		if err != nil {
			return Entry{}, err
		}
		loc, _ := r.w.index.get(r.next)
		entry := Entry{Seq: r.next, Offset: loc.offset, Segment: loc.segment, Data: data}
		r.next++
		return entry, nil
	}
}
