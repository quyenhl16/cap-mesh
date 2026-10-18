//go:build linux

package capture

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/afpacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

func (a *AFPacket) Capture(ctx context.Context, request ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	if err := validateCaptureRequest(request); err != nil {
		return nil, nil, err
	}
	if a.Config.BlockTimeout > request.MaxDelay {
		return nil, nil, fmt.Errorf("%w: AF_PACKET block timeout %s exceeds batch delay %s", ErrInvalidCapture, a.Config.BlockTimeout, request.MaxDelay)
	}
	if a.Config.PollTimeout > request.MaxDelay {
		return nil, nil, fmt.Errorf("%w: AF_PACKET poll timeout %s exceeds batch delay %s", ErrInvalidCapture, a.Config.PollTimeout, request.MaxDelay)
	}
	instructions, err := a.Compiler.Compile(ctx, request.InterfaceName, request.Filter, request.Snaplen)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrFilterCompile, err)
	}
	releaseBudget, err := a.Budget.acquire(int64(a.Config.RingSize))
	if err != nil {
		return nil, nil, err
	}
	handle, err := afpacket.NewTPacket(
		afpacket.OptInterface(request.InterfaceName),
		afpacket.OptFrameSize(a.Config.FrameSize),
		afpacket.OptBlockSize(a.Config.BlockSize),
		afpacket.OptNumBlocks(a.Config.RingSize/a.Config.BlockSize),
		afpacket.OptBlockTimeout(a.Config.BlockTimeout),
		afpacket.OptPollTimeout(a.Config.PollTimeout),
		afpacket.OptTPacketVersion(afpacket.TPacketVersion3),
		afpacket.OptSocketType(afpacket.SocketRaw),
		afpacket.OptAddVLANHeader(false),
	)
	if err != nil {
		releaseBudget()
		return nil, nil, fmt.Errorf("%w: open ring on %s: %v", ErrAFPacketInitialize, request.InterfaceName, err)
	}
	if err := handle.SetBPF(instructions); err != nil {
		handle.Close()
		releaseBudget()
		return nil, nil, fmt.Errorf("%w: attach filter: %v", ErrAFPacketInitialize, err)
	}
	if err := handle.SetPromiscuous(true); err != nil {
		handle.Close()
		releaseBudget()
		return nil, nil, fmt.Errorf("%w: enable promiscuous mode on %s: %v", ErrAFPacketInitialize, request.InterfaceName, err)
	}

	batches := make(chan domain.CapturedBatch, 16)
	captureErrors := make(chan error, 1)
	if a.Metrics != nil {
		a.Metrics.AFPacketRingOpened(a.Config.RingSize)
	}
	go func() {
		defer close(batches)
		defer close(captureErrors)
		defer func() {
			handle.Close()
			releaseBudget()
			if a.Metrics != nil {
				a.Metrics.AFPacketRingClosed(a.Config.RingSize)
			}
		}()
		if err := a.captureLoop(ctx, handle, request, batches); err != nil && !errors.Is(err, context.Canceled) {
			captureErrors <- err
		}
	}()
	return batches, captureErrors, nil
}

type afpacketStats struct {
	packets uint64
	drops   uint64
	freezes uint64
	polls   uint64
}

type afpacketReader interface {
	ZeroCopyReadPacketData() ([]byte, gopacket.CaptureInfo, error)
	SocketStats() (afpacket.SocketStats, afpacket.SocketStatsV3, error)
	Stats() (afpacket.Stats, error)
}

func (a *AFPacket) observeStats(handle afpacketReader, previous *afpacketStats) {
	if a.Metrics == nil {
		return
	}
	_, socket, err := handle.SocketStats()
	if err != nil {
		return
	}
	stats, err := handle.Stats()
	if err != nil {
		return
	}
	current := afpacketStats{
		packets: uint64(socket.Packets()),
		drops:   uint64(socket.Drops()),
		freezes: uint64(socket.QueueFreezes()),
		polls:   uint64(stats.Polls),
	}
	a.Metrics.ObserveAFPacketStats(
		counterDelta(current.packets, previous.packets),
		counterDelta(current.drops, previous.drops),
		counterDelta(current.freezes, previous.freezes),
		counterDelta(current.polls, previous.polls),
	)
	*previous = current
}

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return current
	}
	return current - previous
}

func (a *AFPacket) captureLoop(ctx context.Context, handle afpacketReader, request ports.CaptureRequest, batches chan<- domain.CapturedBatch) error {
	var sequence uint64
	var stats afpacketStats
	defer a.observeStats(handle, &stats)
	nextStats := time.Now().Add(a.Config.StatsInterval)
	batch := newAFPacketBatch(request)
	flush := func(reason string) bool {
		if len(batch.packets) == 0 {
			return true
		}
		captured := domain.CapturedBatch{
			Packets:       batch.packets,
			CapturedBytes: batch.bytes,
			StartedAt:     batch.startedAt,
			BuildDuration: time.Since(batch.startedAt),
			FlushReason:   reason,
		}
		select {
		case batches <- captured:
			batch = newAFPacketBatch(request)
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := time.Now()
		if !now.Before(nextStats) {
			a.observeStats(handle, &stats)
			nextStats = now.Add(a.Config.StatsInterval)
		}
		data, captureInfo, err := handle.ZeroCopyReadPacketData()
		if errors.Is(err, afpacket.ErrTimeout) {
			if len(batch.packets) > 0 && time.Since(batch.startedAt) >= request.MaxDelay && !flush("timeout") {
				return ctx.Err()
			}
			continue
		}
		if err != nil {
			_ = flush("capture_end")
			return fmt.Errorf("read AF_PACKET ring: %w", err)
		}
		packetLength := capturedPacketLength(data, captureInfo, request.Snaplen)
		if len(batch.packets) > 0 && time.Since(batch.startedAt) >= request.MaxDelay {
			if !flush("timeout") {
				return ctx.Err()
			}
		}
		if len(batch.packets) > 0 && batch.bytes+packetLength > request.MaxBytes {
			if !flush("bytes") {
				return ctx.Err()
			}
		}
		if len(batch.packets) == 0 {
			batch.startedAt = time.Now()
		}
		var packetData []byte
		batch.arena, packetData = appendCapturedPacket(batch.arena, data, captureInfo, request.Snaplen)
		originalLength := captureInfo.Length
		if originalLength < len(packetData) {
			originalLength = len(packetData)
		}
		sequence++
		batch.packets = append(batch.packets, domain.Packet{
			Timestamp:      captureInfo.Timestamp,
			CapturedLength: uint32(len(packetData)),
			OriginalLength: uint32(originalLength),
			LinkType:       uint32(layers.LinkTypeEthernet),
			SequenceNumber: sequence,
			Data:           packetData,
		})
		batch.bytes += len(packetData)
		if len(batch.packets) >= request.MaxPackets || batch.bytes >= request.MaxBytes {
			reason := "packets"
			if batch.bytes >= request.MaxBytes {
				reason = "bytes"
			}
			if !flush(reason) {
				return ctx.Err()
			}
		}
	}
}

func capturedPacketLength(data []byte, captureInfo gopacket.CaptureInfo, snaplen uint32) int {
	length := len(data)
	if packetVLAN(captureInfo) >= 0 && len(data) >= 12 {
		length += 4
	}
	if length > int(snaplen) {
		return int(snaplen)
	}
	return length
}

func appendCapturedPacket(arena, data []byte, captureInfo gopacket.CaptureInfo, snaplen uint32) ([]byte, []byte) {
	start := len(arena)
	vlan := packetVLAN(captureInfo)
	if vlan >= 0 && len(data) >= 12 {
		arena = append(arena, data[:12]...)
		arena = append(arena, 0x81, 0x00, byte(vlan>>8), byte(vlan))
		arena = append(arena, data[12:]...)
	} else {
		arena = append(arena, data...)
	}
	end := len(arena)
	if maximum := start + int(snaplen); end > maximum {
		end = maximum
		arena = arena[:end]
	}
	return arena, arena[start:end]
}

func packetVLAN(captureInfo gopacket.CaptureInfo) int {
	for _, value := range captureInfo.AncillaryData {
		if vlan, ok := value.(afpacket.AncillaryVLAN); ok {
			return vlan.VLAN
		}
	}
	return -1
}

type afpacketBatch struct {
	packets   []domain.Packet
	arena     []byte
	bytes     int
	startedAt time.Time
}

func newAFPacketBatch(request ports.CaptureRequest) afpacketBatch {
	packetCapacity := request.MaxPackets
	if packetCapacity > 1024 {
		packetCapacity = 1024
	}
	arenaCapacity := request.MaxBytes
	if request.MaxPackets <= request.MaxBytes/int(request.Snaplen) {
		if maximumUseful := request.MaxPackets * int(request.Snaplen); maximumUseful < arenaCapacity {
			arenaCapacity = maximumUseful
		}
	}
	return afpacketBatch{
		packets: make([]domain.Packet, 0, packetCapacity),
		arena:   make([]byte, 0, arenaCapacity),
	}
}
