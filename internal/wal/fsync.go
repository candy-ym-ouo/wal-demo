package wal

import "fmt"

var syncFile = func(file interface{ Sync() error }) error { return file.Sync() }

func (w *WAL) flushLocked() (uint64, error) {
	if w.pending.empty() && w.unsyncedHigh == 0 {
		return w.durable.Load(), nil
	}
	if !w.pending.empty() {
		data, low, high := w.pending.take()
		before := w.cur.size
		if err := w.cur.write(data); err != nil {
			written := w.cur.size - before
			if written > 0 {
				if truncateErr := w.cur.truncate(before); truncateErr != nil {
					return w.durable.Load(), fmt.Errorf("write failed: %v; rollback failed: %w", err, truncateErr)
				}
			}
			w.pending.resetWith(data, low, high)
			return w.durable.Load(), err
		}
		w.unsyncedHigh = high
		w.unsyncedBytes += uint64(len(data))
	}
	if err := syncFile(w.cur.file); err != nil {
		return w.durable.Load(), err
	}
	w.durable.Store(w.unsyncedHigh)
	w.metrics.bytesWritten.Add(w.unsyncedBytes)
	w.metrics.fsyncs.Add(1)
	w.unsyncedHigh = 0
	w.unsyncedBytes = 0
	w.cond.Broadcast()
	return w.durable.Load(), nil
}

func (w *WAL) rotateLocked(next uint64) error {
	if _, err := w.flushLocked(); err != nil {
		return err
	}
	seg, err := openSegment(w.dir, next, true)
	if err != nil {
		return err
	}
	w.segments = append(w.segments, seg)
	w.cur = seg
	w.metrics.segments.Store(uint64(len(w.segments)))
	return nil
}

func (w *WAL) periodicFlush() {
	defer close(w.done)
	ticker := w.opts.FlushInterval
	if ticker <= 0 {
		<-w.stop
		return
	}
	t := newTicker(ticker)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.mu.Lock()
			if !w.closed {
				_, _ = w.flushLocked()
			}
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}
}
