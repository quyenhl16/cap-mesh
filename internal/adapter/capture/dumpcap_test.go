package capture

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/gopacket/gopacket/pcapgo"
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

func TestWithDumpcapOutput(t *testing.T) {
	err := withDumpcapOutput(errors.New("capture failed"), " Capturing failed: bad filter\n")
	if got, want := err.Error(), "capture failed: dumpcap output: Capturing failed: bad filter"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
