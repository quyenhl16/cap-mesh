package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

func TestServerMetricsExposePipelineSignals(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewServerWithSampling(registry, 2)

	metrics.BatchReceived(3, 300)
	metrics.PacketsReceived(3)
	metrics.SequenceGap(2)
	metrics.AddReorderBuffer(3)
	metrics.ObserveReorderWait(1, time.Millisecond)
	metrics.ObserveReorderWait(2, 2*time.Millisecond)
	metrics.SubscriberAdded()
	metrics.ObserveFanout(time.Millisecond, 2)
	metrics.ObserveRebatch(3, 300, time.Millisecond, "packets")

	assertMetricValue(t, registry, "capmesh_server_batches_received_total", 1)
	assertMetricValue(t, registry, "capmesh_server_sequence_gap_packets_total", 2)
	assertMetricValue(t, registry, "capmesh_server_reorder_buffer_packets", 3)
	assertHistogramCount(t, registry, "capmesh_server_reorder_wait_seconds", 1)
	assertHistogramCount(t, registry, "capmesh_server_rebatch_packets", 1)
}

func TestAgentMetricsSeparateAcceptedAndTransportErrors(t *testing.T) {
	registry := prometheus.NewRegistry()
	reporter := &stubReporter{}
	metrics := NewAgent(registry, reporter)

	batch := domain.PacketBatch{Packets: []domain.Packet{{CapturedLength: 64}, {CapturedLength: 128}}}
	if err := metrics.SendBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	metrics.ObserveBatch(2, 192, time.Millisecond, "size")
	metrics.ObserveTransportSend("batch", time.Millisecond, errors.New("send failed"))

	assertMetricValue(t, registry, "capmesh_agent_batches_sent_total", 1)
	assertMetricValue(t, registry, "capmesh_agent_packets_sent_total", 2)
	assertMetricValue(t, registry, "capmesh_agent_stream_errors_total", 1)
	assertHistogramCount(t, registry, "capmesh_agent_batch_packets", 1)
}

func TestAgentMetricsCountDroppedBatchWhenOutboundQueueIsFull(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewAgent(registry, &fullReporter{})
	batch := domain.PacketBatch{Packets: []domain.Packet{{}, {}, {}}}
	if err := metrics.SendBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	assertMetricValue(t, registry, "capmesh_agent_outbound_queue_overflows_total", 1)
	assertMetricValue(t, registry, "capmesh_agent_outbound_dropped_packets_total", 3)
	assertMetricValue(t, registry, "capmesh_agent_batches_sent_total", 0)
}

func TestAgentMetricsExposeAFPacketSignals(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewAgent(registry, &stubReporter{})
	metrics.CaptureEngineSelected("afpacket")
	metrics.CaptureEngineFallback("ring_budget")
	metrics.AFPacketRingOpened(64 << 20)
	metrics.ObserveAFPacketStats(100, 2, 1, 10)

	assertMetricValue(t, registry, "capmesh_agent_capture_engine_selections_total", 1)
	assertMetricValue(t, registry, "capmesh_agent_capture_engine_fallbacks_total", 1)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_socket_packets_total", 100)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_socket_drops_total", 2)
	assertMetricValue(t, registry, "capmesh_agent_packets_dropped_total", 2)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_queue_freezes_total", 1)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_polls_total", 10)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_active_rings", 1)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_ring_bytes", 64<<20)

	metrics.AFPacketRingClosed(64 << 20)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_active_rings", 0)
	assertMetricValue(t, registry, "capmesh_agent_afpacket_ring_bytes", 0)
}

type stubReporter struct{}

func (*stubReporter) SendBatch(context.Context, domain.PacketBatch) error { return nil }
func (*stubReporter) SendStatus(context.Context, string, string, string, string) error {
	return nil
}

type fullReporter struct{}

func (*fullReporter) SendBatch(context.Context, domain.PacketBatch) error { return ports.ErrQueueFull }
func (*fullReporter) SendStatus(context.Context, string, string, string, string) error {
	return nil
}

func assertMetricValue(t *testing.T, registry *prometheus.Registry, name string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name || len(family.Metric) == 0 {
			continue
		}
		metric := family.Metric[0]
		var got float64
		switch family.GetType().String() {
		case "COUNTER":
			got = metric.GetCounter().GetValue()
		case "GAUGE":
			got = metric.GetGauge().GetValue()
		default:
			t.Fatalf("metric %s has unsupported type %s", name, family.GetType())
		}
		if got != want {
			t.Fatalf("metric %s = %v, want %v", name, got, want)
		}
		return
	}
	t.Fatalf("metric %s not found", name)
}

func assertHistogramCount(t *testing.T, registry *prometheus.Registry, name string, want uint64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) > 0 {
			if got := family.Metric[0].GetHistogram().GetSampleCount(); got != want {
				t.Fatalf("metric %s count = %d, want %d", name, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s not found", name)
}
