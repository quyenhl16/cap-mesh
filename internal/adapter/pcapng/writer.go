package pcapng

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/gopacket/gopacket/pcapgo"
	"github.com/quyenhl16/capmesh/internal/core/domain"
)

type Writer struct {
	output        *bufio.Writer
	writer        *pcapgo.NgWriter
	interfaces    map[string]int
	linkTypes     map[string]uint32
	packetCount   int
	flushPackets  int
	flushInterval time.Duration
	lastFlush     time.Time
}

func NewWriter(output io.Writer, flushPackets int, flushInterval time.Duration) *Writer {
	return &Writer{output: bufio.NewWriterSize(output, 256*1024), interfaces: make(map[string]int), linkTypes: make(map[string]uint32), flushPackets: flushPackets, flushInterval: flushInterval, lastFlush: time.Now()}
}

func (w *Writer) WriteBatch(batch domain.PacketBatch) error {
	key := batch.NodeName + "/" + batch.InterfaceName
	for _, packet := range batch.Packets {
		interfaceID, err := w.ensureInterface(key, packet.LinkType)
		if err != nil {
			return err
		}
		if packet.LinkType != w.linkTypes[key] {
			return fmt.Errorf("link type changed for interface %s", key)
		}
		ci := gopacket.CaptureInfo{Timestamp: packet.Timestamp, CaptureLength: int(packet.CapturedLength), Length: int(packet.OriginalLength), InterfaceIndex: interfaceID}
		if err := w.writer.WritePacket(ci, packet.Data); err != nil {
			return fmt.Errorf("write PCAPNG packet: %w", err)
		}
		w.packetCount++
		if w.packetCount >= w.flushPackets || time.Since(w.lastFlush) >= w.flushInterval {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Writer) ensureInterface(name string, rawLinkType uint32) (int, error) {
	if id, exists := w.interfaces[name]; exists {
		return id, nil
	}
	iface := pcapgo.NgInterface{Name: name, LinkType: layers.LinkType(rawLinkType), SnapLength: 65535, TimestampResolution: 9}
	if w.writer == nil {
		writer, err := pcapgo.NewNgWriterInterface(w.output, iface, pcapgo.DefaultNgWriterOptions)
		if err != nil {
			return 0, fmt.Errorf("write PCAPNG header: %w", err)
		}
		w.writer = writer
		w.interfaces[name] = 0
		w.linkTypes[name] = rawLinkType
		return 0, nil
	}
	id, err := w.writer.AddInterface(iface)
	if err != nil {
		return 0, fmt.Errorf("write PCAPNG interface: %w", err)
	}
	w.interfaces[name] = id
	w.linkTypes[name] = rawLinkType
	return id, nil
}

func (w *Writer) Flush() error {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush PCAPNG writer: %w", err)
		}
	}
	if err := w.output.Flush(); err != nil {
		return fmt.Errorf("flush output: %w", err)
	}
	w.packetCount = 0
	w.lastFlush = time.Now()
	return nil
}
