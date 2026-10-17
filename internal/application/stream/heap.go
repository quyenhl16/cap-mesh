package stream

import (
	"container/heap"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

type packetItem struct {
	template   domain.PacketBatch
	packet     domain.Packet
	receivedAt time.Time
	index      int
}

type packetHeap []*packetItem

func (h packetHeap) Len() int { return len(h) }
func (h packetHeap) Less(i, j int) bool {
	a, b := h[i].packet, h[j].packet
	if a.Timestamp.Equal(b.Timestamp) {
		return a.SequenceNumber < b.SequenceNumber
	}
	return a.Timestamp.Before(b.Timestamp)
}
func (h packetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index, h[j].index = i, j }
func (h *packetHeap) Push(value any) {
	item := value.(*packetItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *packetHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return item
}

func pushPacket(h *packetHeap, batch domain.PacketBatch, packet domain.Packet, receivedAt time.Time) {
	batch.Packets = nil
	heap.Push(h, &packetItem{template: batch, packet: packet, receivedAt: receivedAt})
}
