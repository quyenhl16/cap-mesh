package stream

import (
	"container/heap"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

type packetItem struct {
	batch domain.PacketBatch
	index int
}

type packetHeap []*packetItem

func (h packetHeap) Len() int { return len(h) }
func (h packetHeap) Less(i, j int) bool {
	a, b := h[i].batch.Packets[0], h[j].batch.Packets[0]
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

func pushPacket(h *packetHeap, batch domain.PacketBatch, packet domain.Packet) {
	batch.Packets = []domain.Packet{packet}
	heap.Push(h, &packetItem{batch: batch})
}
