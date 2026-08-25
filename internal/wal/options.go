package wal

import (
	"fmt"
	"time"
)

// SyncPolicy controls when buffered bytes are forced to stable storage.
type SyncPolicy uint8

const (
	SyncAlways SyncPolicy = iota
	SyncBatch
	SyncAsync
)

// ReplayFunc receives one fully reassembled logical record.
type ReplayFunc func(seq uint64, payload []byte) error

// Options configures storage bounds and durability behavior.
type Options struct {
	SyncPolicy      SyncPolicy
	BatchSize       int
	FlushInterval   time.Duration
	MaxSegmentBytes int64
	MaxRecordBytes  int
	MaxPayloadBytes int
	ReplayFn        ReplayFunc
}

// PolicyName returns a stable label for logs, diagnostics, and configuration
// displays. Unknown values are retained as "unknown" instead of being silently
// interpreted as a valid durability mode.
func (o Options) PolicyName() string {
	switch o.SyncPolicy {
	case SyncAlways:
		return "always"
	case SyncBatch:
		return "batch"
	case SyncAsync:
		return "async"
	default:
		return "unknown"
	}
}

// Validate applies defaults and checks all interdependent size constraints.
// The returned value is safe to pass to Open and is useful to programs that
// want to print their effective settings before creating any files.
func (o Options) Validate() (Options, error) {
	return o.normalized()
}

// DefaultOptions returns conservative production-friendly defaults.
func DefaultOptions() Options {
	return Options{
		SyncPolicy:      SyncBatch,
		BatchSize:       64 << 10,
		FlushInterval:   10 * time.Millisecond,
		MaxSegmentBytes: 64 << 20,
		MaxRecordBytes:  32 << 10,
		MaxPayloadBytes: 1 << 30,
	}
}

func (o Options) normalized() (Options, error) {
	d := DefaultOptions()
	if o.SyncPolicy == SyncAlways && o.BatchSize == 0 && o.FlushInterval == 0 &&
		o.MaxSegmentBytes == 0 && o.MaxRecordBytes == 0 && o.MaxPayloadBytes == 0 && o.ReplayFn == nil {
		o.SyncPolicy = d.SyncPolicy
	}
	if o.BatchSize == 0 {
		o.BatchSize = d.BatchSize
	}
	if o.FlushInterval == 0 {
		o.FlushInterval = d.FlushInterval
	}
	if o.MaxSegmentBytes == 0 {
		o.MaxSegmentBytes = d.MaxSegmentBytes
	}
	if o.MaxRecordBytes == 0 {
		o.MaxRecordBytes = d.MaxRecordBytes
	}
	if o.MaxPayloadBytes == 0 {
		o.MaxPayloadBytes = d.MaxPayloadBytes
	}
	if o.SyncPolicy > SyncAsync {
		return o, fmt.Errorf("%w: unknown sync policy %d", ErrInvalidOption, o.SyncPolicy)
	}
	if o.BatchSize < headerSize || o.MaxRecordBytes < 1 {
		return o, fmt.Errorf("%w: batch and record sizes must be positive", ErrInvalidOption)
	}
	if o.MaxPayloadBytes < o.MaxRecordBytes {
		return o, fmt.Errorf("%w: max payload must be >= max record", ErrInvalidOption)
	}
	if o.MaxSegmentBytes < int64(headerSize+o.MaxRecordBytes) {
		return o, fmt.Errorf("%w: segment cannot hold one maximum fragment", ErrInvalidOption)
	}
	if o.FlushInterval < 0 {
		return o, fmt.Errorf("%w: flush interval cannot be negative", ErrInvalidOption)
	}
	return o, nil
}
