package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
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

	assertMetricValue(t, registry, "capmesh_server_batches_received_total", 1)
	assertMetricValue(t, registry, "capmesh_server_sequence_gap_packets_total", 2)
	assertMetricValue(t, registry, "capmesh_server_reorder_buffer_packets", 3)
	assertHistogramCount(t, registry, "capmesh_server_reorder_wait_seconds", 1)
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

type stubReporter struct{}

func (*stubReporter) SendBatch(context.Context, domain.PacketBatch) error { return nil }
func (*stubReporter) SendStatus(context.Context, string, string, string, string) error {
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
