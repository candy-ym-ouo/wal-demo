package wal

import "sync/atomic"

// Metrics contains lock-free counters updated by the write and recovery paths.
type Metrics struct {
	recordsWritten  atomic.Uint64
	bytesWritten    atomic.Uint64
	fsyncs          atomic.Uint64
	checksumFailure atomic.Uint64
	segments        atomic.Uint64
	replayed        atomic.Uint64
	truncations     atomic.Uint64
}

// MetricsSnapshot is stable JSON data suitable for monitoring endpoints.
type MetricsSnapshot struct {
	RecordsWritten   uint64 `json:"recordsWritten"`
	BytesWritten     uint64 `json:"bytesWritten"`
	Fsyncs           uint64 `json:"fsyncs"`
	ChecksumFailures uint64 `json:"checksumFailures"`
	Segments         uint64 `json:"segments"`
	Replayed         uint64 `json:"replayed"`
	Truncations      uint64 `json:"truncations"`
	DurableSeq       uint64 `json:"durableSeq"`
	LastSeq          uint64 `json:"lastSeq"`
	PendingBytes     int    `json:"pendingBytes"`
}

func (m *Metrics) snapshot(durable, last uint64, pending int) MetricsSnapshot {
	return MetricsSnapshot{
		RecordsWritten:   m.recordsWritten.Load(),
		BytesWritten:     m.bytesWritten.Load(),
		Fsyncs:           m.fsyncs.Load(),
		ChecksumFailures: m.checksumFailure.Load(),
		Segments:         m.segments.Load(),
		Replayed:         m.replayed.Load(),
		Truncations:      m.truncations.Load(),
		DurableSeq:       durable,
		LastSeq:          last,
		PendingBytes:     pending,
	}
}
