package capture

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/gopacket/gopacket/pcapgo"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type Dumpcap struct {
	Binary string
	Logger *slog.Logger
}

func (d Dumpcap) Capture(ctx context.Context, request ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	if request.MaxPackets < 1 || request.MaxBytes < 1 || request.MaxDelay <= 0 {
		return nil, nil, errors.New("capture batch limits must be positive")
	}
	args := []string{"-i", request.InterfaceName}
	if request.Filter != "" {
		args = append(args, "-f", request.Filter)
	}
	// Do not force the output format here. Older dumpcap releases use -P for
	// pcap while newer releases prefer -F pcap. Reading either native format
	// keeps the agent compatible with both generations.
	args = append(args, "-s", strconv.FormatUint(uint64(request.Snaplen), 10), "-w", "-")
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
	batches := make(chan domain.CapturedBatch, 16)
	errors := make(chan error, 1)
	readDone := make(chan error, 1)
	dumpcapLog := &logWriter{logger: d.Logger}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(dumpcapLog, stderr)
	}()
	go func() {
		defer close(packets)
		reader, err := newPacketReader(stdout)
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			<-stderrDone
			readDone <- withDumpcapOutput(fmt.Errorf("read capture header: %w", err), dumpcapLog.String())
			return
		}
		var sequence atomic.Uint64
		linkType := uint32(reader.LinkType())
		for {
			data, ci, err := reader.ReadPacketData()
			if err != nil {
				if err != io.EOF && ctx.Err() == nil {
					readDone <- fmt.Errorf("read pcap packet: %w", err)
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					<-stderrDone
					return
				}
				break
			}
			packet := domain.Packet{Timestamp: ci.Timestamp, CapturedLength: uint32(ci.CaptureLength), OriginalLength: uint32(ci.Length), LinkType: linkType, SequenceNumber: sequence.Add(1), Data: append([]byte(nil), data...)}
			select {
			case packets <- packet:
			case <-ctx.Done():
				_ = cmd.Wait()
				<-stderrDone
				readDone <- ctx.Err()
				return
			}
		}
		waitErr := cmd.Wait()
		<-stderrDone
		if waitErr != nil && ctx.Err() == nil {
			readDone <- withDumpcapOutput(fmt.Errorf("dumpcap exited: %w", waitErr), dumpcapLog.String())
			return
		}
		readDone <- nil
	}()
	go batchCapturedPackets(ctx, request, packets, readDone, batches, errors)
	return batches, errors, nil
}

func batchCapturedPackets(ctx context.Context, request ports.CaptureRequest, packets <-chan domain.Packet, readDone <-chan error, batches chan<- domain.CapturedBatch, captureErrors chan<- error) {
	defer close(batches)
	defer close(captureErrors)
	timer := time.NewTimer(request.MaxDelay)
	if !timer.Stop() {
		<-timer.C
	}
	var batch domain.CapturedBatch
	flush := func(reason string) bool {
		if len(batch.Packets) == 0 {
			return true
		}
		batch.FlushReason = reason
		batch.BuildDuration = time.Since(batch.StartedAt)
		select {
		case batches <- batch:
			batch = domain.CapturedBatch{}
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		var timerC <-chan time.Time
		if len(batch.Packets) > 0 {
			timerC = timer.C
		}
		select {
		case packet, ok := <-packets:
			if !ok {
				_ = flush("capture_end")
				if err := <-readDone; err != nil && !errors.Is(err, context.Canceled) {
					captureErrors <- err
				}
				return
			}
			packetBytes := int(packet.CapturedLength)
			if len(batch.Packets) > 0 && batch.CapturedBytes+packetBytes > request.MaxBytes {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if !flush("bytes") {
					return
				}
			}
			if len(batch.Packets) == 0 {
				batch.StartedAt = time.Now()
				timer.Reset(request.MaxDelay)
			}
			batch.Packets = append(batch.Packets, packet)
			batch.CapturedBytes += packetBytes
			if len(batch.Packets) >= request.MaxPackets || batch.CapturedBytes >= request.MaxBytes {
				reason := "packets"
				if batch.CapturedBytes >= request.MaxBytes {
					reason = "bytes"
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if !flush(reason) {
					return
				}
			}
		case <-timerC:
			if !flush("timeout") {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

type packetReader interface {
	ReadPacketData() ([]byte, gopacket.CaptureInfo, error)
	LinkType() layers.LinkType
}

var pcapngMagic = []byte{0x0a, 0x0d, 0x0d, 0x0a}

func newPacketReader(input io.Reader) (packetReader, error) {
	buffered := bufio.NewReader(input)
	magic, err := buffered.Peek(len(pcapngMagic))
	if err != nil {
		return nil, err
	}
	if bytes.Equal(magic, pcapngMagic) {
		return pcapgo.NewNgReader(buffered, pcapgo.DefaultNgReaderOptions)
	}
	return pcapgo.NewReader(buffered)
}

func withDumpcapOutput(err error, output string) error {
	if output = strings.TrimSpace(output); output != "" {
		return fmt.Errorf("%w: dumpcap output: %s", err, output)
	}
	return err
}

const maxDumpcapLogBytes = 16 * 1024

type logWriter struct {
	logger *slog.Logger
	mu     sync.Mutex
	output strings.Builder
}

func (w *logWriter) Write(value []byte) (int, error) {
	written := len(value)
	if w.logger != nil {
		w.logger.Debug("dumpcap", "output", string(value))
	}
	w.mu.Lock()
	if remaining := maxDumpcapLogBytes - w.output.Len(); remaining > 0 {
		if len(value) > remaining {
			value = value[:remaining]
		}
		_, _ = w.output.Write(value)
	}
	w.mu.Unlock()
	return written, nil
}

func (w *logWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}
