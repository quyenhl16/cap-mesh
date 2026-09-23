package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quyenhl16/capmesh/internal/core/domain"
)

type testMetrics struct {
	mu    sync.Mutex
	drops int
	late  int
}

func (*testMetrics) PacketsReceived(int)            {}
func (*testMetrics) PacketsEmitted(int)             {}
func (m *testMetrics) LatePacket()                  { m.mu.Lock(); m.late++; m.mu.Unlock() }
func (m *testMetrics) SubscriberDrop()              { m.mu.Lock(); m.drops++; m.mu.Unlock() }
func (*testMetrics) SetReorderBuffer(int)           {}
func (*testMetrics) ObserveSubscriberQueue(float64) {}

func TestReordersPacketsByTimestamp(t *testing.T) {
	metrics := &testMetrics{}
	service := NewService(metrics)
	if err := service.OpenSession("s1", 20*time.Millisecond, 10); err != nil {
		t.Fatal(err)
	}
	defer service.CloseSession("s1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output, err := service.Subscribe(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	service.Publish(domain.PacketBatch{SessionID: "s1", Packets: []domain.Packet{{Timestamp: now.Add(5 * time.Millisecond), SequenceNumber: 2}, {Timestamp: now, SequenceNumber: 1}}})
	var sequences []uint64
	deadline := time.After(time.Second)
	for len(sequences) < 2 {
		select {
		case batch := <-output:
			sequences = append(sequences, batch.Packets[0].SequenceNumber)
		case <-deadline:
			t.Fatal("timed out waiting for reordered packets")
		}
	}
	if sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("unexpected order: %v", sequences)
	}
}

func TestSlowSubscriberDoesNotBlockFastSubscriber(t *testing.T) {
	metrics := &testMetrics{}
	service := NewService(metrics)
	if err := service.OpenSession("s1", 0, 1); err != nil {
		t.Fatal(err)
	}
	defer service.CloseSession("s1")
	slow, _ := service.Subscribe(context.Background(), "s1")
	_ = slow
	fast, _ := service.Subscribe(context.Background(), "s1")
	for i := 0; i < 5; i++ {
		service.Publish(domain.PacketBatch{SessionID: "s1", Packets: []domain.Packet{{Timestamp: time.Now().Add(-time.Second), SequenceNumber: uint64(i + 1)}}})
		select {
		case <-fast:
		case <-time.After(time.Second):
			t.Fatal("fast subscriber was blocked")
		}
	}
	metrics.mu.Lock()
	drops := metrics.drops
	metrics.mu.Unlock()
	if drops == 0 {
		t.Fatal("expected slow subscriber drops")
	}
}
