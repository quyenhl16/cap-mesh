package benchmark

import (
	"context"
	"fmt"
	"net"
	"time"
)

const ethernetIPv4UDPOverhead = 14 + 20 + 8

type GenerateResult struct {
	Packets  uint64
	Bytes    uint64
	Started  time.Time
	Finished time.Time
}

func Generate(ctx context.Context, receiver string, runID uint32, packetSize, packetsPerSecond int, duration time.Duration) (GenerateResult, error) {
	if packetSize < ethernetIPv4UDPOverhead+HeaderSize {
		return GenerateResult{}, fmt.Errorf("packet size must be at least %d bytes", ethernetIPv4UDPOverhead+HeaderSize)
	}
	if packetsPerSecond < 1 {
		return GenerateResult{}, fmt.Errorf("packet rate must be positive")
	}
	if duration <= 0 {
		return GenerateResult{}, fmt.Errorf("duration must be positive")
	}
	address, err := net.ResolveUDPAddr("udp4", receiver)
	if err != nil {
		return GenerateResult{}, fmt.Errorf("resolve receiver: %w", err)
	}
	connection, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		return GenerateResult{}, fmt.Errorf("connect receiver: %w", err)
	}
	defer connection.Close()

	payload := make([]byte, packetSize-ethernetIPv4UDPOverhead)
	for index := HeaderSize; index < len(payload); index++ {
		payload[index] = byte(index)
	}
	total := uint64(float64(packetsPerSecond) * duration.Seconds())
	if total > uint64(^uint32(0)) {
		return GenerateResult{}, fmt.Errorf("scenario generates %d packets, maximum is %d", total, uint64(^uint32(0)))
	}
	result := GenerateResult{Started: time.Now()}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for result.Packets < total {
		select {
		case <-ctx.Done():
			result.Finished = time.Now()
			return result, ctx.Err()
		case now := <-ticker.C:
			elapsed := now.Sub(result.Started)
			target := uint64(float64(packetsPerSecond) * elapsed.Seconds())
			if target > total {
				target = total
			}
			for result.Packets < target {
				encodeHeader(payload, runID, uint32(result.Packets), time.Now())
				if _, err := connection.Write(payload); err != nil {
					result.Finished = time.Now()
					return result, fmt.Errorf("send packet %d: %w", result.Packets, err)
				}
				result.Packets++
				result.Bytes += uint64(packetSize)
			}
		}
	}
	result.Finished = time.Now()
	return result, nil
}
