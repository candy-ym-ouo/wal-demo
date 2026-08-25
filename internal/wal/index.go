package wal

import "sync"

type location struct {
	segment uint64
	offset  int64
	bytes   int64
}

type sequenceIndex struct {
	mu      sync.RWMutex
	entries map[uint64]location
}

func newIndex() *sequenceIndex {
	return &sequenceIndex{entries: make(map[uint64]location)}
}

func (i *sequenceIndex) put(seq uint64, loc location) {
	i.mu.Lock()
	i.entries[seq] = loc
	i.mu.Unlock()
}

func (i *sequenceIndex) get(seq uint64) (location, bool) {
	i.mu.RLock()
	loc, ok := i.entries[seq]
	i.mu.RUnlock()
	return loc, ok
}

func (i *sequenceIndex) removeBefore(seq uint64) {
	i.mu.Lock()
	for current := range i.entries {
		if current < seq {
			delete(i.entries, current)
		}
	}
	i.mu.Unlock()
}

func (i *sequenceIndex) reset() {
	i.mu.Lock()
	i.entries = make(map[uint64]location)
	i.mu.Unlock()
}

func (i *sequenceIndex) sequences(from uint64, limit int) []uint64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if limit <= 0 {
		return nil
	}
	result := make([]uint64, 0, limit)
	for seq := from; len(result) < limit; seq++ {
		if _, ok := i.entries[seq]; ok {
			result = append(result, seq)
		}
		if seq == ^uint64(0) {
			break
		}
	}
	return result
}
