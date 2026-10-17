package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Server struct {
	received           prometheus.Counter
	emitted            prometheus.Counter
	batches            prometheus.Counter
	batchPackets       prometheus.Histogram
	batchBytes         prometheus.Histogram
	sequenceGapEvents  prometheus.Counter
	sequenceGapPackets prometheus.Counter
	late               prometheus.Counter
	subscriberDrops    prometheus.Counter
	subscribers        prometheus.Gauge
	reorderSize        prometheus.Gauge
	reorderWait        prometheus.Histogram
	fanoutDuration     prometheus.Histogram
	fanoutDeliveries   prometheus.Counter
	queueUsage         prometheus.Histogram
	rebatchPackets     prometheus.Histogram
	rebatchBytes       prometheus.Histogram
	rebatchAge         prometheus.Histogram
	rebatchFlushes     *prometheus.CounterVec
	sampleEvery        uint64
}

func NewServer(registerer prometheus.Registerer) *Server {
	return NewServerWithSampling(registerer, 1024)
}

func NewServerWithSampling(registerer prometheus.Registerer, sampleEvery uint64) *Server {
	if sampleEvery < 1 {
		sampleEvery = 1
	}
	m := &Server{
		received:           prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_packets_received_total", Help: "Packets received from agents."}),
		emitted:            prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_packets_emitted_total", Help: "Packets emitted to live subscribers."}),
		batches:            prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_batches_received_total", Help: "Packet batches received from agents."}),
		batchPackets:       prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_batch_packets", Help: "Packets per batch received from agents.", Buckets: prometheus.ExponentialBuckets(1, 2, 11)}),
		batchBytes:         prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_batch_bytes", Help: "Captured bytes per batch received from agents.", Buckets: prometheus.ExponentialBuckets(1024, 2, 13)}),
		sequenceGapEvents:  prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_sequence_gap_events_total", Help: "Discontinuities detected in agent packet sequences."}),
		sequenceGapPackets: prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_sequence_gap_packets_total", Help: "Packets estimated missing from agent packet sequences."}),
		late:               prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_late_packets_total", Help: "Packets received after the emission watermark."}),
		subscriberDrops:    prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_subscriber_drops_total", Help: "Packets dropped from full subscriber queues."}),
		subscribers:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_server_subscribers", Help: "Current packet stream subscribers."}),
		reorderSize:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_server_reorder_buffer_packets", Help: "Packets currently buffered for reordering across all sessions."}),
		reorderWait:        prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_reorder_wait_seconds", Help: "Sampled time packets spend in the reorder buffer.", Buckets: prometheus.ExponentialBuckets(10e-6, 2, 18)}),
		fanoutDuration:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_fanout_duration_seconds", Help: "Time to enqueue an immutable batch for every subscriber.", Buckets: prometheus.ExponentialBuckets(10e-6, 2, 18)}),
		fanoutDeliveries:   prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_fanout_deliveries_total", Help: "Successful batch deliveries to subscriber queues."}),
		queueUsage:         prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_subscriber_queue_usage", Help: "Subscriber queue utilization ratio.", Buckets: prometheus.LinearBuckets(0, .1, 11)}),
		rebatchPackets:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_rebatch_packets", Help: "Packets per immutable batch emitted after reorder.", Buckets: prometheus.ExponentialBuckets(1, 2, 11)}),
		rebatchBytes:       prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_rebatch_bytes", Help: "Captured bytes per immutable batch emitted after reorder.", Buckets: prometheus.ExponentialBuckets(1024, 2, 13)}),
		rebatchAge:         prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_rebatch_age_seconds", Help: "Time from receiving the oldest packet until its immutable batch is emitted.", Buckets: prometheus.ExponentialBuckets(10e-6, 2, 18)}),
		rebatchFlushes:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_server_rebatch_flush_total", Help: "Immutable reorder batches emitted by flush reason."}, []string{"reason"}),
		sampleEvery:        sampleEvery,
	}
	registerer.MustRegister(m.received, m.emitted, m.batches, m.batchPackets, m.batchBytes, m.sequenceGapEvents, m.sequenceGapPackets, m.late, m.subscriberDrops, m.subscribers, m.reorderSize, m.reorderWait, m.fanoutDuration, m.fanoutDeliveries, m.queueUsage, m.rebatchPackets, m.rebatchBytes, m.rebatchAge, m.rebatchFlushes)
	return m
}

func (m *Server) PacketsReceived(n int) { m.received.Add(float64(n)) }
func (m *Server) PacketsEmitted(n int)  { m.emitted.Add(float64(n)) }
func (m *Server) BatchReceived(packets, bytes int) {
	m.batches.Inc()
	m.batchPackets.Observe(float64(packets))
	m.batchBytes.Observe(float64(bytes))
}
func (m *Server) SequenceGap(packets uint64) {
	m.sequenceGapEvents.Inc()
	m.sequenceGapPackets.Add(float64(packets))
}
func (m *Server) LatePacket()                          { m.late.Inc() }
func (m *Server) SubscriberDrop(packets int)           { m.subscriberDrops.Add(float64(packets)) }
func (m *Server) AddReorderBuffer(delta int)           { m.reorderSize.Add(float64(delta)) }
func (m *Server) SubscriberAdded()                     { m.subscribers.Inc() }
func (m *Server) SubscriberRemoved()                   { m.subscribers.Dec() }
func (m *Server) ObserveSubscriberQueue(value float64) { m.queueUsage.Observe(value) }
func (m *Server) ObserveFanout(duration time.Duration, deliveries int) {
	m.fanoutDuration.Observe(duration.Seconds())
	m.fanoutDeliveries.Add(float64(deliveries))
}
func (m *Server) ObserveReorderWait(sequence uint64, duration time.Duration) {
	if sequence%m.sampleEvery == 0 {
		m.reorderWait.Observe(duration.Seconds())
	}
}
func (m *Server) ObserveRebatch(packets, bytes int, age time.Duration, reason string) {
	m.rebatchPackets.Observe(float64(packets))
	m.rebatchBytes.Observe(float64(bytes))
	m.rebatchAge.Observe(age.Seconds())
	m.rebatchFlushes.WithLabelValues(reason).Inc()
}

type Noop struct{}

func (Noop) PacketsReceived(int)                            {}
func (Noop) PacketsEmitted(int)                             {}
func (Noop) BatchReceived(int, int)                         {}
func (Noop) SequenceGap(uint64)                             {}
func (Noop) LatePacket()                                    {}
func (Noop) SubscriberDrop(int)                             {}
func (Noop) AddReorderBuffer(int)                           {}
func (Noop) ObserveReorderWait(uint64, time.Duration)       {}
func (Noop) SubscriberAdded()                               {}
func (Noop) SubscriberRemoved()                             {}
func (Noop) ObserveFanout(time.Duration, int)               {}
func (Noop) ObserveSubscriberQueue(float64)                 {}
func (Noop) ObserveRebatch(int, int, time.Duration, string) {}
