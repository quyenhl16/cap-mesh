package metrics

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type Agent struct {
	inner               ports.AgentReporter
	captured            prometheus.Counter
	dropped             prometheus.Counter
	captureErrors       prometheus.Counter
	captureQueueDepth   prometheus.Gauge
	captureQueueCap     prometheus.Gauge
	sent                prometheus.Counter
	batches             prometheus.Counter
	batchPackets        prometheus.Histogram
	batchBytes          prometheus.Histogram
	batchAge            prometheus.Histogram
	batchFlushes        *prometheus.CounterVec
	streamErrors        prometheus.Counter
	streamReconnects    prometheus.Counter
	transportDuration   *prometheus.HistogramVec
	transportMessages   *prometheus.CounterVec
	capturedBytes       prometheus.Counter
	outboundQueueDepth  prometheus.Gauge
	outboundQueueCap    prometheus.Gauge
	outboundOverflows   prometheus.Counter
	outboundDropped     prometheus.Counter
	outboundDisconnects prometheus.Counter
	captureEngines      *prometheus.CounterVec
	captureFallbacks    *prometheus.CounterVec
	afpacketPackets     prometheus.Counter
	afpacketDrops       prometheus.Counter
	afpacketFreezes     prometheus.Counter
	afpacketPolls       prometheus.Counter
	afpacketRingBytes   prometheus.Gauge
	afpacketActiveRings prometheus.Gauge
}

func NewAgent(registerer prometheus.Registerer, inner ports.AgentReporter) *Agent {
	a := &Agent{
		inner:               inner,
		captured:            prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_packets_captured_total", Help: "Packets read from the capture engine."}),
		dropped:             prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_packets_dropped_total", Help: "Packets reported dropped by the capture engine when statistics are available."}),
		captureErrors:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_capture_errors_total", Help: "Errors reported by the capture engine."}),
		captureQueueDepth:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_agent_capture_queue_depth", Help: "Immutable batches waiting between the capture engine and agent pipeline."}),
		captureQueueCap:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_agent_capture_queue_capacity", Help: "Capacity of the bounded queue between the capture engine and agent pipeline."}),
		sent:                prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_packets_sent_total", Help: "Packets accepted by the outbound agent reporter."}),
		batches:             prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_batches_sent_total", Help: "Packet batches accepted by the outbound agent reporter."}),
		batchPackets:        prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_agent_batch_packets", Help: "Packets per agent batch.", Buckets: prometheus.ExponentialBuckets(1, 2, 11)}),
		batchBytes:          prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_agent_batch_bytes", Help: "Captured bytes per agent batch.", Buckets: prometheus.ExponentialBuckets(1024, 2, 13)}),
		batchAge:            prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_agent_batch_age_seconds", Help: "Time from the first packet entering a batch until the batch is flushed.", Buckets: prometheus.ExponentialBuckets(10e-6, 2, 18)}),
		batchFlushes:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_agent_batch_flush_total", Help: "Agent batches flushed by reason."}, []string{"reason"}),
		streamErrors:        prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_stream_errors_total", Help: "Errors queueing or sending agent stream messages."}),
		streamReconnects:    prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_stream_reconnects_total", Help: "Agent stream connections that ended and required reconnecting."}),
		transportDuration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "capmesh_agent_grpc_send_duration_seconds", Help: "Time spent sending messages on the agent gRPC stream.", Buckets: prometheus.ExponentialBuckets(10e-6, 2, 18)}, []string{"type"}),
		transportMessages:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_agent_grpc_messages_total", Help: "Messages sent on the agent gRPC stream."}, []string{"type", "result"}),
		capturedBytes:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_capture_bytes_total", Help: "Captured packet bytes."}),
		outboundQueueDepth:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_agent_outbound_queue_depth", Help: "Batches and status messages waiting for the gRPC stream."}),
		outboundQueueCap:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_agent_outbound_queue_capacity", Help: "Capacity of the bounded gRPC outbound queue."}),
		outboundOverflows:   prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_outbound_queue_overflows_total", Help: "Capture batches dropped because the bounded gRPC outbound queue was full."}),
		outboundDropped:     prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_outbound_dropped_packets_total", Help: "Packets dropped because the bounded gRPC outbound queue was full."}),
		outboundDisconnects: prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_outbound_disconnect_dropped_batches_total", Help: "Queued capture batches discarded when the gRPC stream disconnected."}),
		captureEngines:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_agent_capture_engine_selections_total", Help: "Capture sessions started by backend."}, []string{"engine"}),
		captureFallbacks:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_agent_capture_engine_fallbacks_total", Help: "Automatic fallbacks from AF_PACKET to dumpcap."}, []string{"reason"}),
		afpacketPackets:     prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_afpacket_socket_packets_total", Help: "Packets reported by AF_PACKET socket statistics."}),
		afpacketDrops:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_afpacket_socket_drops_total", Help: "Packets dropped by AF_PACKET sockets."}),
		afpacketFreezes:     prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_afpacket_queue_freezes_total", Help: "TPACKET_V3 queue freeze events."}),
		afpacketPolls:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_afpacket_polls_total", Help: "Blocking poll calls made by AF_PACKET readers."}),
		afpacketRingBytes:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_agent_afpacket_ring_bytes", Help: "Total mmap ring bytes held by active AF_PACKET captures."}),
		afpacketActiveRings: prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_agent_afpacket_active_rings", Help: "Number of active AF_PACKET mmap rings."}),
	}
	registerer.MustRegister(a.captured, a.dropped, a.captureErrors, a.captureQueueDepth, a.captureQueueCap, a.sent, a.batches, a.batchPackets, a.batchBytes, a.batchAge, a.batchFlushes, a.streamErrors, a.streamReconnects, a.transportDuration, a.transportMessages, a.capturedBytes, a.outboundQueueDepth, a.outboundQueueCap, a.outboundOverflows, a.outboundDropped, a.outboundDisconnects, a.captureEngines, a.captureFallbacks, a.afpacketPackets, a.afpacketDrops, a.afpacketFreezes, a.afpacketPolls, a.afpacketRingBytes, a.afpacketActiveRings)
	return a
}

func (a *Agent) ObserveCaptured(packet domain.Packet) {
	a.captured.Inc()
	a.capturedBytes.Add(float64(packet.CapturedLength))
}

func (a *Agent) ObserveBatch(packets, bytes int, age time.Duration, reason string) {
	a.batchPackets.Observe(float64(packets))
	a.batchBytes.Observe(float64(bytes))
	a.batchAge.Observe(age.Seconds())
	a.batchFlushes.WithLabelValues(reason).Inc()
}

func (a *Agent) SendBatch(ctx context.Context, batch domain.PacketBatch) error {
	err := a.inner.SendBatch(ctx, batch)
	if errors.Is(err, ports.ErrQueueFull) {
		a.outboundOverflows.Inc()
		a.outboundDropped.Add(float64(len(batch.Packets)))
		return nil
	}
	if err != nil {
		a.streamErrors.Inc()
		return err
	}
	a.batches.Inc()
	a.sent.Add(float64(len(batch.Packets)))
	return nil
}

func (a *Agent) SendStatus(ctx context.Context, sessionID, sourceID, state, message string) error {
	err := a.inner.SendStatus(ctx, sessionID, sourceID, state, message)
	if err != nil {
		a.streamErrors.Inc()
	}
	return err
}

func (a *Agent) ObserveTransportSend(messageType string, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
		a.streamErrors.Inc()
	}
	a.transportDuration.WithLabelValues(messageType).Observe(duration.Seconds())
	a.transportMessages.WithLabelValues(messageType, result).Inc()
}

func (a *Agent) StreamReconnect() { a.streamReconnects.Inc() }

func (a *Agent) ObserveOutboundQueue(depth, capacity int) {
	a.outboundQueueDepth.Set(float64(depth))
	a.outboundQueueCap.Set(float64(capacity))
}

func (a *Agent) ObserveOutboundDiscard(packets int) {
	a.outboundDisconnects.Inc()
	a.outboundDropped.Add(float64(packets))
}

func (a *Agent) CaptureEngineSelected(engine string) {
	a.captureEngines.WithLabelValues(engine).Inc()
}

func (a *Agent) CaptureEngineFallback(reason string) {
	a.captureFallbacks.WithLabelValues(reason).Inc()
}

func (a *Agent) AFPacketRingOpened(bytes int) {
	a.afpacketActiveRings.Inc()
	a.afpacketRingBytes.Add(float64(bytes))
}

func (a *Agent) AFPacketRingClosed(bytes int) {
	a.afpacketActiveRings.Dec()
	a.afpacketRingBytes.Sub(float64(bytes))
}

func (a *Agent) ObserveAFPacketStats(packets, drops, freezes, polls uint64) {
	a.afpacketPackets.Add(float64(packets))
	a.afpacketDrops.Add(float64(drops))
	a.dropped.Add(float64(drops))
	a.afpacketFreezes.Add(float64(freezes))
	a.afpacketPolls.Add(float64(polls))
}

type CaptureEngine struct {
	inner   ports.CaptureEngine
	metrics *Agent
}

func NewCaptureEngine(inner ports.CaptureEngine, metrics *Agent) *CaptureEngine {
	return &CaptureEngine{inner: inner, metrics: metrics}
}

func (e *CaptureEngine) Capture(ctx context.Context, request ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	in, captureErrors, err := e.inner.Capture(ctx, request)
	if err != nil {
		e.metrics.captureErrors.Inc()
		return nil, nil, err
	}
	out := make(chan domain.CapturedBatch, 16)
	errorsOut := make(chan error, 1)
	e.metrics.captureQueueCap.Set(float64(cap(out)))
	go func() {
		defer close(out)
		defer close(errorsOut)
		defer e.metrics.captureQueueDepth.Set(0)
		batches := in
		errorsChannel := captureErrors
		for batches != nil || errorsChannel != nil {
			select {
			case batch, ok := <-batches:
				if !ok {
					batches = nil
					continue
				}
				for _, packet := range batch.Packets {
					e.metrics.ObserveCaptured(packet)
				}
				select {
				case out <- batch:
					e.metrics.captureQueueDepth.Set(float64(len(out)))
				case <-ctx.Done():
					return
				}
			case captureError, ok := <-errorsChannel:
				if !ok {
					errorsChannel = nil
					continue
				}
				if captureError != nil {
					e.metrics.captureErrors.Inc()
				}
				select {
				case errorsOut <- captureError:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errorsOut, nil
}
