package wal

import (
	"errors"
	"io"
)

func tailError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// trimTail keeps the valid prefix of a segment and synchronizes the new file
// length. Truncating only happens while the WAL append lock is held.
func (w *WAL) trimTail(seg *segment, valid int64) error {
	if valid == seg.size {
		return nil
	}
	if err := seg.truncate(valid); err != nil {
		return err
	}
	if err := seg.file.Sync(); err != nil {
		return err
	}
	w.metrics.truncations.Add(1)
	return nil
}

func (w *WAL) discardSegmentsAfter(start uint64) error {
	kept := w.segments[:0]
	for _, seg := range w.segments {
		if seg.startSeq <= start {
			kept = append(kept, seg)
			continue
		}
		_ = seg.close()
		if err := removeFileAndSyncDir(seg.path, w.dir); err != nil {
			return err
		}
	}
	w.segments = kept
	w.metrics.segments.Store(uint64(len(kept)))
	return nil
}
