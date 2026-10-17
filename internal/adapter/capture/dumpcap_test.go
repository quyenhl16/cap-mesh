package capture

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/gopacket/gopacket/pcapgo"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

func TestNewPacketReaderSupportsPCAPAndPCAPNG(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 123_000_000)
	data := []byte{0, 1, 2, 3}
	info := gopacket.CaptureInfo{
		Timestamp:     timestamp,
		CaptureLength: len(data),
		Length:        len(data),
	}

	tests := []struct {
		name  string
		build func(*testing.T) []byte
	}{
		{
			name: "pcap",
			build: func(t *testing.T) []byte {
				t.Helper()
				var output bytes.Buffer
				writer := pcapgo.NewWriter(&output)
				if err := writer.WriteFileHeader(256, layers.LinkTypeEthernet); err != nil {
					t.Fatal(err)
				}
				if err := writer.WritePacket(info, data); err != nil {
					t.Fatal(err)
				}
				return output.Bytes()
			},
		},
		{
			name: "pcapng",
			build: func(t *testing.T) []byte {
				t.Helper()
				var output bytes.Buffer
				writer, err := pcapgo.NewNgWriter(&output, layers.LinkTypeEthernet)
				if err != nil {
					t.Fatal(err)
				}
				if err := writer.WritePacket(info, data); err != nil {
					t.Fatal(err)
				}
				if err := writer.Flush(); err != nil {
					t.Fatal(err)
				}
				return output.Bytes()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, err := newPacketReader(bytes.NewReader(test.build(t)))
			if err != nil {
				t.Fatal(err)
			}
			if reader.LinkType() != layers.LinkTypeEthernet {
				t.Fatalf("unexpected link type: %v", reader.LinkType())
			}
			gotData, gotInfo, err := reader.ReadPacketData()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotData, data) {
				t.Fatalf("unexpected packet data: %v", gotData)
			}
			if gotInfo.CaptureLength != len(data) || gotInfo.Length != len(data) {
				t.Fatalf("unexpected capture info: %+v", gotInfo)
			}
		})
	}
}

func TestBatchCapturedPacketsHonorsPacketLimit(t *testing.T) {
	packets := make(chan domain.Packet, 3)
	packets <- domain.Packet{CapturedLength: 10}
	packets <- domain.Packet{CapturedLength: 20}
	packets <- domain.Packet{CapturedLength: 30}
	close(packets)
	readDone := make(chan error, 1)
	readDone <- nil
	batches := make(chan domain.CapturedBatch, 2)
	errorsChannel := make(chan error, 1)
	go batchCapturedPackets(context.Background(), ports.CaptureRequest{MaxPackets: 2, MaxBytes: 1024, MaxDelay: time.Second}, packets, readDone, batches, errorsChannel)

	first := <-batches
	second := <-batches
	if len(first.Packets) != 2 || first.CapturedBytes != 30 || first.FlushReason != "packets" {
		t.Fatalf("unexpected first batch: %#v", first)
	}
	if len(second.Packets) != 1 || second.CapturedBytes != 30 || second.FlushReason != "capture_end" {
		t.Fatalf("unexpected second batch: %#v", second)
	}
	if err := <-errorsChannel; err != nil {
		t.Fatal(err)
	}
}

func TestBatchCapturedPacketsHonorsByteAndDelayLimits(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		packets := make(chan domain.Packet, 2)
		packets <- domain.Packet{CapturedLength: 8}
		packets <- domain.Packet{CapturedLength: 8}
		close(packets)
		readDone := make(chan error, 1)
		readDone <- nil
		batches := make(chan domain.CapturedBatch, 2)
		errorsChannel := make(chan error, 1)
		go batchCapturedPackets(context.Background(), ports.CaptureRequest{MaxPackets: 10, MaxBytes: 10, MaxDelay: time.Second}, packets, readDone, batches, errorsChannel)
		if batch := <-batches; len(batch.Packets) != 1 || batch.FlushReason != "bytes" {
			t.Fatalf("unexpected byte-limited batch: %#v", batch)
		}
	})

	t.Run("delay", func(t *testing.T) {
		packets := make(chan domain.Packet, 1)
		readDone := make(chan error, 1)
		batches := make(chan domain.CapturedBatch, 1)
		errorsChannel := make(chan error, 1)
		go batchCapturedPackets(context.Background(), ports.CaptureRequest{MaxPackets: 10, MaxBytes: 1024, MaxDelay: 5 * time.Millisecond}, packets, readDone, batches, errorsChannel)
		packets <- domain.Packet{CapturedLength: 8}
		select {
		case batch := <-batches:
			if len(batch.Packets) != 1 || batch.FlushReason != "timeout" {
				t.Fatalf("unexpected timeout batch: %#v", batch)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for delayed batch")
		}
		close(packets)
		readDone <- nil
	})
}

func TestWithDumpcapOutput(t *testing.T) {
	err := withDumpcapOutput(errors.New("capture failed"), " Capturing failed: bad filter\n")
	if got, want := err.Error(), "capture failed: dumpcap output: Capturing failed: bad filter"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
