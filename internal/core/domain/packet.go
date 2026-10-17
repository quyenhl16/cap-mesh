package domain

import "time"

type Packet struct {
	Timestamp      time.Time
	CapturedLength uint32
	OriginalLength uint32
	LinkType       uint32
	SequenceNumber uint64
	Data           []byte
}

type PacketBatch struct {
	SessionID     string
	NodeName      string
	InterfaceName string
	Source        CaptureSource
	Packets       []Packet
}

// CapturedBatch is owned by the capture pipeline. Packets and their payloads
// must not be mutated after the batch is published to a consumer.
type CapturedBatch struct {
	Packets       []Packet
	CapturedBytes int
	StartedAt     time.Time
	BuildDuration time.Duration
	FlushReason   string
}

func (b PacketBatch) Clone() PacketBatch {
	out := b
	out.Packets = make([]Packet, len(b.Packets))
	for i, packet := range b.Packets {
		out.Packets[i] = packet
		out.Packets[i].Data = append([]byte(nil), packet.Data...)
	}
	return out
}
