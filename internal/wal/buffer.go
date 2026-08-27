package wal

type pendingBuffer struct {
	data []byte
	low  uint64
	high uint64
}

func (b *pendingBuffer) append(seq uint64, records []record) (start int64, err error) {
	start = int64(len(b.data))
	if len(b.data) == 0 {
		b.low = seq
	}
	for _, rec := range records {
		b.data, err = encodeRecord(b.data, rec)
		if err != nil {
			return start, err
		}
	}
	b.high = seq
	return start, nil
}

func (b *pendingBuffer) len() int { return len(b.data) }

func (b *pendingBuffer) empty() bool { return len(b.data) == 0 }

func (b *pendingBuffer) take() (data []byte, low, high uint64) {
	data = b.data
	low, high = b.low, b.high
	b.data = nil
	b.low = 0
	b.high = 0
	return data, low, high
}

func (b *pendingBuffer) resetWith(data []byte, low, high uint64) {
	if len(data) == 0 {
		return
	}
	b.data = append(data, b.data...)
	b.low = low
	if b.high == 0 {
		b.high = high
	}
}
