package wal

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var newTicker = time.NewTicker

// WAL is a concurrency-safe segmented write-ahead log.
type WAL struct {
	dir           string
	opts          Options
	mu            sync.Mutex
	cond          *sync.Cond
	pending       pendingBuffer
	segments      []*segment
	cur           *segment
	index         *sequenceIndex
	metrics       Metrics
	durable       atomic.Uint64
	unsyncedHigh  uint64
	unsyncedBytes uint64
	nextSeq       uint64
	closed        bool
	stop          chan struct{}
	done          chan struct{}
}

// Open creates or validates a WAL directory and repairs a torn final record.
func Open(dir string, opts Options) (*WAL, error) {
	normalized, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	segments, err := loadSegments(dir)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		dir:      dir,
		opts:     normalized,
		segments: segments,
		index:    newIndex(),
		nextSeq:  1,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	if _, err := w.recoverLocked(normalized.ReplayFn); err != nil {
		for _, seg := range w.segments {
			_ = seg.close()
		}
		return nil, err
	}
	go w.periodicFlush()
	return w, nil
}

// Append buffers a copy of payload and follows the configured sync policy.
func (w *WAL) Append(payload []byte) (uint64, error) {
	return w.append(payload, false)
}

// AppendBatch buffers payload. It synchronizes when the configured batch
// threshold is reached, allowing many callers to share one fsync.
func (w *WAL) AppendBatch(payload []byte) (uint64, error) {
	return w.append(payload, false)
}

// AppendSync returns only after this sequence is durable.
func (w *WAL) AppendSync(payload []byte) (uint64, error) {
	return w.append(payload, true)
}

func (w *WAL) append(payload []byte, force bool) (uint64, error) {
	if len(payload) > w.opts.MaxPayloadBytes {
		return 0, ErrRecordTooBig
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	seq := w.nextSeq
	parts := fragments(seq, payload, w.opts.MaxRecordBytes)
	total := encodedSize(parts)
	oversized := int64(total) > w.opts.MaxSegmentBytes
	if oversized && (w.cur.size > 0 || !w.pending.empty()) {
		if err := w.rotateLocked(seq); err != nil {
			return 0, err
		}
	} else if !oversized && w.cur.size+int64(w.pending.len()+total) > w.opts.MaxSegmentBytes {
		if err := w.rotateLocked(seq); err != nil {
			return 0, err
		}
	}
	base := w.cur.size + int64(w.pending.len())
	if _, err := w.pending.append(seq, parts); err != nil {
		return 0, err
	}
	w.index.put(seq, location{segment: w.cur.startSeq, offset: base, bytes: int64(total)})
	w.nextSeq++
	w.metrics.recordsWritten.Add(1)
	shouldSync := force || w.opts.SyncPolicy == SyncAlways || w.pending.len() >= w.opts.BatchSize
	if shouldSync {
		if _, err := w.flushLocked(); err != nil {
			return 0, err
		}
	}
	return seq, nil
}

// Sync persists all records currently in the group-commit buffer.
func (w *WAL) Sync() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.durable.Load(), ErrClosed
	}
	return w.flushLocked()
}

// DurableSeq returns the highest sequence confirmed by fsync.
func (w *WAL) DurableSeq() uint64 { return w.durable.Load() }

// LastSeq returns the highest assigned logical sequence.
func (w *WAL) LastSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq - 1
}

// Metrics returns a consistent-enough monitoring snapshot.
func (w *WAL) Metrics() MetricsSnapshot {
	w.mu.Lock()
	last := w.nextSeq - 1
	pending := w.pending.len() + int(w.unsyncedBytes)
	w.mu.Unlock()
	return w.metrics.snapshot(w.durable.Load(), last, pending)
}

// Recover rescans durable files. Pending asynchronous records are synced first.
func (w *WAL) Recover(fn ReplayFunc) (RecoveryResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return RecoveryResult{}, ErrClosed
	}
	if _, err := w.flushLocked(); err != nil {
		return RecoveryResult{}, err
	}
	oldIndex := w.index
	w.index = newIndex()
	result, err := w.recoverLocked(fn)
	if err != nil {
		w.index = oldIndex
		return result, err
	}
	return result, nil
}

// Close flushes the final batch and releases all file descriptors.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	_, flushErr := w.flushLocked()
	close(w.stop)
	w.mu.Unlock()
	<-w.done
	var closeErr error
	for _, seg := range w.segments {
		if err := seg.close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
