package store

import (
	"errors"
	"sort"
	"sync"

	"wal-demo/internal/wal"
)

var ErrKeyNotFound = errors.New("store key not found")

// Item is a serializable key/value pair used by API listings.
type Item struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ItemPage is a bounded, deterministic view of live keys.
type ItemPage struct {
	Items  []Item `json:"items"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
	Total  int    `json:"total"`
}

// Pair is one mutation supplied to SetBatch. Values are copied before the
// method returns so callers may immediately reuse their input buffers.
type Pair struct {
	Key   string
	Value []byte
}

// Store is a small in-memory state machine whose mutations are WAL-first.
type Store struct {
	writeMu sync.Mutex
	mu      sync.RWMutex
	log     *wal.WAL
	dir     string
	kv      map[string][]byte
	applied uint64
}

// New loads a valid snapshot, then replays newer WAL commands.
func New(log *wal.WAL, dir string) (*Store, error) {
	s := &Store{log: log, dir: dir, kv: make(map[string][]byte)}
	if err := s.loadSnapshot(); err != nil && !errors.Is(err, errInvalidSnapshot) {
		return nil, err
	}
	if _, err := log.Replay(s.Apply); err != nil {
		return nil, err
	}
	return s, nil
}

// Apply idempotently applies one WAL command to memory.
func (s *Store) Apply(seq uint64, payload []byte) error {
	cmd, err := decodeCommand(payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.applied {
		return nil
	}
	s.applyLocked(cmd)
	s.applied = seq
	return nil
}

func (s *Store) applyLocked(cmd command) {
	switch cmd.Op {
	case opSet:
		s.kv[cmd.Key] = cloneBytes(cmd.Value)
	case opDelete:
		delete(s.kv, cmd.Key)
	}
}

// Set writes the WAL synchronously before exposing the new memory value.
func (s *Store) Set(key string, value []byte) (uint64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	payload, err := encodeCommand(opSet, key, cloneBytes(value))
	if err != nil {
		return 0, err
	}
	seq, err := s.log.AppendSync(payload)
	if err != nil {
		return 0, err
	}
	if err := s.Apply(seq, payload); err != nil {
		return 0, err
	}
	return seq, nil
}

// SetBatch appends every command to the WAL buffer, performs one explicit
// sync, and only then publishes the mutations to readers. This demonstrates
// group commit while preserving the WAL-before-memory ordering rule.
func (s *Store) SetBatch(pairs []Pair) ([]uint64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if len(pairs) == 0 {
		return []uint64{}, nil
	}
	payloads := make([][]byte, len(pairs))
	for i, pair := range pairs {
		payload, err := encodeCommand(opSet, pair.Key, cloneBytes(pair.Value))
		if err != nil {
			return nil, err
		}
		payloads[i] = payload
	}
	seqs := make([]uint64, 0, len(payloads))
	for _, payload := range payloads {
		seq, err := s.log.AppendBatch(payload)
		if err != nil {
			return nil, err
		}
		seqs = append(seqs, seq)
	}
	if _, err := s.log.Sync(); err != nil {
		return nil, err
	}
	for i, seq := range seqs {
		if err := s.Apply(seq, payloads[i]); err != nil {
			return nil, err
		}
	}
	return seqs, nil
}

// Delete durably records a deletion before modifying memory.
func (s *Store) Delete(key string) (uint64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	payload, err := encodeCommand(opDelete, key, nil)
	if err != nil {
		return 0, err
	}
	seq, err := s.log.AppendSync(payload)
	if err != nil {
		return 0, err
	}
	if err := s.Apply(seq, payload); err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	value, ok := s.kv[key]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrKeyNotFound
	}
	return cloneBytes(value), nil
}

func (s *Store) Items() []Item {
	s.mu.RLock()
	items := make([]Item, 0, len(s.kv))
	for key, value := range s.kv {
		items = append(items, Item{Key: key, Value: string(value)})
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items
}

// ItemsPage returns a stable slice of the sorted live-key view.
func (s *Store) ItemsPage(offset, limit int) ItemPage {
	if offset < 0 {
		offset = 0
	}
	if limit < 1 {
		limit = 50
	}
	items := s.Items()
	total := len(items)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := append([]Item(nil), items[offset:end]...)
	return ItemPage{Items: page, Offset: offset, Limit: limit, Total: total}
}

func (s *Store) AppliedSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Count returns the number of live keys without allocating an Items slice.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.kv)
}

// Reload rebuilds memory from the latest valid snapshot and durable WAL.
func (s *Store) Reload() (wal.ReplayStats, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	s.kv = make(map[string][]byte)
	s.applied = 0
	s.mu.Unlock()
	if err := s.loadSnapshot(); err != nil {
		if !errors.Is(err, errInvalidSnapshot) {
			return wal.ReplayStats{}, err
		}
	}
	return s.log.Replay(s.Apply)
}

func (s *Store) WAL() *wal.WAL { return s.log }
