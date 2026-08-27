package wal

import (
	"errors"
	"fmt"
)

var (
	ErrClosed        = errors.New("wal is closed")
	ErrNotFound      = errors.New("wal record not found")
	ErrInvalidOption = errors.New("invalid wal option")
	ErrRecordTooBig  = errors.New("logical record exceeds maximum size")
	ErrCorrupt       = errors.New("wal corruption detected")
)

// CorruptionError identifies the exact segment and byte offset that failed
// validation. Callers may use errors.Is(err, ErrCorrupt) to classify it.
type CorruptionError struct {
	Segment uint64 `json:"segment"`
	Offset  int64  `json:"offset"`
	Reason  string `json:"reason"`
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("%v: segment %020d offset %d: %s", ErrCorrupt, e.Segment, e.Offset, e.Reason)
}

func (e *CorruptionError) Unwrap() error { return ErrCorrupt }
