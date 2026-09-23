package pcapng

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/gopacket/gopacket/pcapgo"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

func TestWriterProducesMultiInterfacePCAPNG(t *testing.T) {
	var output bytes.Buffer
	writer := NewWriter(&output, 64, time.Hour)
	now := time.Now()
	for index, node := range []string{"worker-1", "worker-2"} {
		packet := domain.Packet{Timestamp: now.Add(time.Duration(index) * time.Millisecond), CapturedLength: 4, OriginalLength: 4, LinkType: 1, Data: []byte{0, 1, 2, 3}}
		if err := writer.WriteBatch(domain.PacketBatch{NodeName: node, InterfaceName: "eth0", Packets: []domain.Packet{packet}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := pcapgo.NewNgReader(bytes.NewReader(output.Bytes()), pcapgo.DefaultNgReaderOptions)
	if err != nil {
		t.Fatal(err)
	}
	var interfaces []int
	for {
		_, info, err := reader.ReadPacketData()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		interfaces = append(interfaces, info.InterfaceIndex)
	}
	if len(interfaces) != 2 || interfaces[0] != 0 || interfaces[1] != 1 {
		t.Fatalf("unexpected interface IDs: %v", interfaces)
	}
}
