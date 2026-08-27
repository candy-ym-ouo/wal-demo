package wal

import (
	"encoding/binary"
	"hash/crc32"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// recordChecksum covers all semantically important fields except magic and
// the checksum field itself. A valid magic plus CRC rejects accidental
// boundary matches and detects flag, sequence, length, and payload changes.
func recordChecksum(flags byte, length uint32, seq uint64, payload []byte) uint32 {
	h := crc32.New(crcTable)
	var fixed [13]byte
	fixed[0] = flags
	binary.BigEndian.PutUint32(fixed[1:5], length)
	binary.BigEndian.PutUint64(fixed[5:13], seq)
	_, _ = h.Write(fixed[:])
	_, _ = h.Write(payload)
	return h.Sum32()
}

func validChecksum(r record) bool {
	return r.crc == recordChecksum(r.flags, uint32(len(r.data)), r.seq, r.data)
}
