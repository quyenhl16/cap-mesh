package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

type testMetrics struct {
	mu    sync.Mutex
	drops int
	late  int
}

func (*testMetrics) PacketsReceived(int)                            {}
func (*testMetrics) PacketsEmitted(int)                             {}
func (*testMetrics) BatchReceived(int, int)                         {}
func (*testMetrics) SequenceGap(uint64)                             {}
func (m *testMetrics) LatePacket()                                  { m.mu.Lock(); m.late++; m.mu.Unlock() }
func (m *testMetrics) SubscriberDrop(packets int)                   { m.mu.Lock(); m.drops += packets; m.mu.Unlock() }
func (*testMetrics) AddReorderBuffer(int)                           {}
func (*testMetrics) ObserveReorderWait(uint64, time.Duration)       {}
func (*testMetrics) SubscriberAdded()                               {}
func (*testMetrics) SubscriberRemoved()                             {}
func (*testMetrics) ObserveFanout(time.Duration, int)               {}
func (*testMetrics) ObserveSubscriberQueue(float64)                 {}
func (*testMetrics) ObserveRebatch(int, int, time.Duration, string) {}

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
			for _, packet := range batch.Packets {
				sequences = append(sequences, packet.SequenceNumber)
			}
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

func TestLossAwareSubscriberIsDisconnectedOnOverflow(t *testing.T) {
	service := NewService(&testMetrics{})
	if err := service.OpenSession("s1", 0, 10); err != nil {
		t.Fatal(err)
	}
	defer service.CloseSession("s1")
	recorder, err := service.SubscribeLossAware(context.Background(), "s1", 1)
	if err != nil {
		t.Fatal(err)
	}
	live, err := service.Subscribe(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Second)
	for i := 0; i < 2; i++ {
		service.Publish(domain.PacketBatch{SessionID: "s1", Packets: []domain.Packet{{Timestamp: now.Add(time.Duration(i) * time.Nanosecond), SequenceNumber: uint64(i + 1)}}})
		select {
		case <-live:
		case <-time.After(time.Second):
			t.Fatal("live subscriber did not receive all packets")
		}
	}
	select {
	case <-recorder.Dropped:
	case <-time.After(time.Second):
		t.Fatal("loss-aware subscriber was not notified of overflow")
	}
}

func TestRebatchesAfterReorderAndSharesImmutablePayload(t *testing.T) {
	service := NewServiceWithRebatch(&testMetrics{}, RebatchConfig{MaxPackets: 2, MaxBytes: 1024, MaxDelay: 0})
	if err := service.OpenSession("s1", 0, 10); err != nil {
		t.Fatal(err)
	}
	defer service.CloseSession("s1")
	first, err := service.Subscribe(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Subscribe(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(-time.Second)
	service.Publish(domain.PacketBatch{SessionID: "s1", Packets: []domain.Packet{
		{Timestamp: now, SequenceNumber: 1, CapturedLength: 1, Data: []byte{1}},
		{Timestamp: now.Add(time.Nanosecond), SequenceNumber: 2, CapturedLength: 1, Data: []byte{2}},
		{Timestamp: now.Add(2 * time.Nanosecond), SequenceNumber: 3, CapturedLength: 1, Data: []byte{3}},
	}})

	firstBatch := receiveBatch(t, first)
	secondBatch := receiveBatch(t, second)
	if len(firstBatch.Packets) != 2 || len(secondBatch.Packets) != 2 {
		t.Fatalf("first rebatch sizes = %d and %d, want 2", len(firstBatch.Packets), len(secondBatch.Packets))
	}
	if &firstBatch.Packets[0].Data[0] != &secondBatch.Packets[0].Data[0] {
		t.Fatal("subscriber payload was deep-cloned")
	}
	if got := len(receiveBatch(t, first).Packets); got != 1 {
		t.Fatalf("second rebatch size = %d, want 1", got)
	}
}

func receiveBatch(t *testing.T, batches <-chan domain.PacketBatch) domain.PacketBatch {
	t.Helper()
	select {
	case batch := <-batches:
		return batch
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch")
		return domain.PacketBatch{}
	}
}
