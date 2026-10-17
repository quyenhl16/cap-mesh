package benchmark

import (
	"bytes"
	"encoding/binary"
	"time"
)

const HeaderSize = 20

var packetMagic = []byte{'C', 'M', 'B', '1'}

func encodeHeader(packet []byte, runID, sequence uint32, sentAt time.Time) {
	copy(packet[:4], packetMagic)
	binary.BigEndian.PutUint32(packet[4:8], runID)
	binary.BigEndian.PutUint32(packet[8:12], sequence)
	binary.BigEndian.PutUint64(packet[12:20], uint64(sentAt.UnixNano()))
}

func decodeHeader(packet []byte, runID uint32) (uint32, time.Time, bool) {
	for offset := 0; offset+HeaderSize <= len(packet); {
		index := bytes.Index(packet[offset:], packetMagic)
		if index < 0 {
			return 0, time.Time{}, false
		}
		index += offset
		if index+HeaderSize <= len(packet) && binary.BigEndian.Uint32(packet[index+4:index+8]) == runID {
			sequence := binary.BigEndian.Uint32(packet[index+8 : index+12])
			sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(packet[index+12:index+20])))
			return sequence, sentAt, true
		}
		offset = index + len(packetMagic)
	}
	return 0, time.Time{}, false
}
