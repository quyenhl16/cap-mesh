package capture

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strconv"
	"sync/atomic"

	"github.com/gopacket/gopacket/pcapgo"
	"github.com/quyenhl16/capmesh/internal/core/domain"
)

type Dumpcap struct {
	Binary string
	Logger *slog.Logger
}

func (d Dumpcap) Capture(ctx context.Context, iface, filter string, snaplen uint32) (<-chan domain.Packet, <-chan error, error) {
	args := []string{"-i", iface}
	if filter != "" {
		args = append(args, "-f", filter)
	}
	args = append(args, "-s", strconv.FormatUint(uint64(snaplen), 10), "-F", "pcap", "-w", "-")
	cmd := exec.CommandContext(ctx, d.Binary, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("dumpcap stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("dumpcap stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("start dumpcap: %w", err)
	}
	packets := make(chan domain.Packet, 256)
	errors := make(chan error, 1)
	go func() {
		_, _ = io.Copy(&logWriter{logger: d.Logger}, stderr)
	}()
	go func() {
		defer close(packets)
		defer close(errors)
		reader, err := pcapgo.NewReader(stdout)
		if err != nil {
			errors <- fmt.Errorf("read pcap header: %w", err)
			_ = cmd.Wait()
			return
		}
		var sequence atomic.Uint64
		linkType := uint32(reader.LinkType())
		for {
			data, ci, err := reader.ReadPacketData()
			if err != nil {
				if err != io.EOF && ctx.Err() == nil {
					errors <- fmt.Errorf("read pcap packet: %w", err)
				}
				break
			}
			packet := domain.Packet{Timestamp: ci.Timestamp, CapturedLength: uint32(ci.CaptureLength), OriginalLength: uint32(ci.Length), LinkType: linkType, SequenceNumber: sequence.Add(1), Data: append([]byte(nil), data...)}
			select {
			case packets <- packet:
			case <-ctx.Done():
				_ = cmd.Wait()
				return
			}
		}
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			select {
			case errors <- fmt.Errorf("dumpcap exited: %w", err):
			default:
			}
		}
	}()
	return packets, errors, nil
}

type logWriter struct{ logger *slog.Logger }

func (w *logWriter) Write(value []byte) (int, error) {
	if w.logger != nil {
		w.logger.Debug("dumpcap", "output", string(value))
	}
	return len(value), nil
}
