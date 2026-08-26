package wal

// ReplayStats describes a completed replay pass.
type ReplayStats struct {
	Records uint64 `json:"records"`
	Bytes   uint64 `json:"bytes"`
	LastSeq uint64 `json:"lastSeq"`
}

// VerificationResult reports a complete checksum walk over indexed records.
// Verify stops at the first error, so Checked and Bytes describe the known-good
// prefix and make an operator-visible corruption report useful.
type VerificationResult struct {
	Checked uint64 `json:"checked"`
	Bytes   uint64 `json:"bytes"`
	LastSeq uint64 `json:"lastSeq"`
	Valid   bool   `json:"valid"`
}

// Replay visits every indexed durable logical record in ascending order.
func (w *WAL) Replay(fn ReplayFunc) (ReplayStats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ReplayStats{}, ErrClosed
	}
	if _, err := w.flushLocked(); err != nil {
		return ReplayStats{}, err
	}
	stats := ReplayStats{}
	for seq := uint64(1); seq <= w.durable.Load(); seq++ {
		loc, ok := w.index.get(seq)
		if !ok {
			continue
		}
		payload, err := w.readLocation(seq, loc)
		if err != nil {
			return stats, err
		}
		if fn != nil {
			if err := fn(seq, append([]byte(nil), payload...)); err != nil {
				return stats, err
			}
		}
		stats.Records++
		stats.Bytes += uint64(len(payload))
		stats.LastSeq = seq
	}
	return stats, nil
}

// Verify flushes pending data and decodes every logical record. Decoding
// repeats magic, length, fragment-order, and CRC checks rather than trusting
// only the startup index.
func (w *WAL) Verify() (VerificationResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := VerificationResult{Valid: true}
	if w.closed {
		return result, ErrClosed
	}
	if _, err := w.flushLocked(); err != nil {
		result.Valid = false
		return result, err
	}
	for seq := uint64(1); seq <= w.durable.Load(); seq++ {
		loc, ok := w.index.get(seq)
		if !ok {
			continue
		}
		payload, err := w.readLocation(seq, loc)
		if err != nil {
			result.Valid = false
			w.metrics.checksumFailure.Add(1)
			return result, err
		}
		result.Checked++
		result.Bytes += uint64(len(payload))
		result.LastSeq = seq
	}
	return result, nil
}

// CleanOldSegments removes segments wholly preceding beforeSeq. The current
// segment is always retained, even if the requested watermark is newer.
func (w *WAL) CleanOldSegments(beforeSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if _, err := w.flushLocked(); err != nil {
		return err
	}
	kept := make([]*segment, 0, len(w.segments))
	for i, seg := range w.segments {
		last := w.nextSeq - 1
		if i+1 < len(w.segments) {
			last = w.segments[i+1].startSeq - 1
		}
		if seg != w.cur && last < beforeSeq {
			if err := seg.close(); err != nil {
				return err
			}
			if err := removeFileAndSyncDir(seg.path, w.dir); err != nil {
				return err
			}
			continue
		}
		kept = append(kept, seg)
	}
	w.segments = kept
	w.index.removeBefore(beforeSeq)
	w.metrics.segments.Store(uint64(len(kept)))
	return nil
}

// Corrupt flips a payload byte for demonstrations and checksum tests.
func (w *WAL) Corrupt(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if _, err := w.flushLocked(); err != nil {
		return err
	}
	loc, ok := w.index.get(seq)
	if !ok {
		return ErrNotFound
	}
	seg := w.segmentByStart(loc.segment)
	if seg == nil {
		return ErrNotFound
	}
	// Flip the stored checksum rather than a payload byte. This always targets
	// the selected record, including zero-length payloads, and cannot spill into
	// the following record.
	position := loc.offset + 3
	var one [1]byte
	if _, err := seg.file.ReadAt(one[:], position); err != nil {
		return err
	}
	one[0] ^= 0xff
	if _, err := seg.file.WriteAt(one[:], position); err != nil {
		return err
	}
	return seg.file.Sync()
}
