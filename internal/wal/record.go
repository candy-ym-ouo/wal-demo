package wal

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	recordMagic uint16 = 0x5741
	headerSize         = 19
	flagFirst   byte   = 1 << 0
	flagLast    byte   = 1 << 1
	flagPadding byte   = 1 << 2
)

type record struct {
	flags byte
	crc   uint32
	seq   uint64
	data  []byte
}

func fragments(seq uint64, payload []byte, max int) []record {
	if len(payload) == 0 {
		return []record{{flags: flagFirst | flagLast, seq: seq, data: nil}}
	}
	count := (len(payload) + max - 1) / max
	result := make([]record, 0, count)
	for start := 0; start < len(payload); start += max {
		end := start + max
		if end > len(payload) {
			end = len(payload)
		}
		flags := byte(0)
		if start == 0 {
			flags |= flagFirst
		}
		if end == len(payload) {
			flags |= flagLast
		}
		part := append([]byte(nil), payload[start:end]...)
		result = append(result, record{flags: flags, seq: seq, data: part})
	}
	return result
}

func encodedSize(records []record) int {
	n := 0
	for _, r := range records {
		n += headerSize + len(r.data)
	}
	return n
}

func encodeRecord(dst []byte, r record) ([]byte, error) {
	if uint64(len(r.data)) > uint64(^uint32(0)) {
		return dst, ErrRecordTooBig
	}
	start := len(dst)
	dst = append(dst, make([]byte, headerSize+len(r.data))...)
	binary.BigEndian.PutUint16(dst[start:start+2], recordMagic)
	dst[start+2] = r.flags
	binary.BigEndian.PutUint32(dst[start+7:start+11], uint32(len(r.data)))
	binary.BigEndian.PutUint64(dst[start+11:start+19], r.seq)
	copy(dst[start+19:], r.data)
	crc := recordChecksum(r.flags, uint32(len(r.data)), r.seq, r.data)
	binary.BigEndian.PutUint32(dst[start+3:start+7], crc)
	return dst, nil
}

func decodeHeader(header []byte) (record, uint32, error) {
	if len(header) != headerSize {
		return record{}, 0, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint16(header[:2]) != recordMagic {
		return record{}, 0, fmt.Errorf("invalid magic 0x%x", binary.BigEndian.Uint16(header[:2]))
	}
	r := record{
		flags: header[2],
		crc:   binary.BigEndian.Uint32(header[3:7]),
		seq:   binary.BigEndian.Uint64(header[11:19]),
	}
	length := binary.BigEndian.Uint32(header[7:11])
	return r, length, nil
}

func decodeRecordAt(r io.ReaderAt, offset int64, max int) (record, int64, error) {
	header := make([]byte, headerSize)
	if _, err := r.ReadAt(header, offset); err != nil {
		return record{}, offset, err
	}
	rec, length, err := decodeHeader(header)
	if err != nil {
		return record{}, offset, err
	}
	if int64(length) > int64(max) {
		return record{}, offset, fmt.Errorf("fragment length %d exceeds limit %d", length, max)
	}
	rec.data = make([]byte, int(length))
	if length > 0 {
		if _, err := r.ReadAt(rec.data, offset+headerSize); err != nil {
			return record{}, offset, err
		}
	}
	if !validChecksum(rec) {
		return record{}, offset, fmt.Errorf("checksum mismatch")
	}
	return rec, offset + headerSize + int64(length), nil
}
