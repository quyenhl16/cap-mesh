//go:build linux

package capture

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/afpacket"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type reusedRingReader struct {
	buffer []byte
	reads  int
}

func (r *reusedRingReader) ZeroCopyReadPacketData() ([]byte, gopacket.CaptureInfo, error) {
	r.reads++
	switch r.reads {
	case 1:
		copy(r.buffer, []byte{1, 2, 3, 4})
		return r.buffer, gopacket.CaptureInfo{Timestamp: time.Unix(1, 0), CaptureLength: 4, Length: 4}, nil
	case 2:
		copy(r.buffer, []byte{5, 6, 7, 8})
		return r.buffer, gopacket.CaptureInfo{Timestamp: time.Unix(2, 0), CaptureLength: 4, Length: 4}, nil
	default:
		return nil, gopacket.CaptureInfo{}, errors.New("stop")
	}
}

func (*reusedRingReader) SocketStats() (afpacket.SocketStats, afpacket.SocketStatsV3, error) {
	return afpacket.SocketStats{}, afpacket.SocketStatsV3{}, nil
}

func (*reusedRingReader) Stats() (afpacket.Stats, error) { return afpacket.Stats{}, nil }

func TestAFPacketBatchOwnsPayloadAfterRingReuse(t *testing.T) {
	engine := &AFPacket{Config: DefaultAFPacketConfig()}
	reader := &reusedRingReader{buffer: make([]byte, 4)}
	batches := make(chan domain.CapturedBatch, 1)
	request := ports.CaptureRequest{Snaplen: 64, MaxPackets: 2, MaxBytes: 128, MaxDelay: time.Second}
	err := engine.captureLoop(context.Background(), reader, request, batches)
	if err == nil || err.Error() != "read AF_PACKET ring: stop" {
		t.Fatalf("captureLoop error = %v", err)
	}
	batch := <-batches
	if got := batch.Packets[0].Data; len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("first payload changed after ring reuse: %v", got)
	}
	if got := batch.Packets[1].Data; len(got) != 4 || got[0] != 5 || got[3] != 8 {
		t.Fatalf("second payload = %v", got)
	}
}

func TestAppendCapturedPacketRestoresVLANWithoutIntermediateBuffer(t *testing.T) {
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0x08, 0x00, 45}
	info := gopacket.CaptureInfo{AncillaryData: []interface{}{afpacket.AncillaryVLAN{VLAN: 100}}}
	arena, packet := appendCapturedPacket(make([]byte, 0, 64), data, info, 64)
	if len(arena) != len(data)+4 || len(packet) != len(data)+4 {
		t.Fatalf("VLAN packet length = %d/%d, want %d", len(arena), len(packet), len(data)+4)
	}
	wantTag := []byte{0x81, 0x00, 0x00, 0x64}
	for i := range wantTag {
		if packet[12+i] != wantTag[i] {
			t.Fatalf("VLAN tag = %v, want %v", packet[12:16], wantTag)
		}
	}
	if packet[16] != 0x08 || packet[17] != 0x00 {
		t.Fatalf("ether type moved incorrectly: %v", packet[16:18])
	}
}
