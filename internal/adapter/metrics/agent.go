package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type Agent struct {
	inner         ports.AgentReporter
	captured      prometheus.Counter
	dropped       prometheus.Counter
	sent          prometheus.Counter
	streamErrors  prometheus.Counter
	capturedBytes prometheus.Counter
}

func NewAgent(registerer prometheus.Registerer, inner ports.AgentReporter) *Agent {
	a := &Agent{
		inner:         inner,
		captured:      prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_packets_captured_total", Help: "Packets read from the capture engine."}),
		dropped:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_packets_dropped_total", Help: "Packets reported dropped by the capture engine."}),
		sent:          prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_packets_sent_total", Help: "Packets sent to the server."}),
		streamErrors:  prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_stream_errors_total", Help: "Errors sending agent stream messages."}),
		capturedBytes: prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_agent_capture_bytes_total", Help: "Captured packet bytes."}),
	}
	registerer.MustRegister(a.captured, a.dropped, a.sent, a.streamErrors, a.capturedBytes)
	return a
}

func (a *Agent) ObserveCaptured(packet domain.Packet) {
	a.captured.Inc()
	a.capturedBytes.Add(float64(packet.CapturedLength))
}

func (a *Agent) SendBatch(ctx context.Context, batch domain.PacketBatch) error {
	err := a.inner.SendBatch(ctx, batch)
	if err != nil {
		a.streamErrors.Inc()
		return err
	}
	a.sent.Add(float64(len(batch.Packets)))
	return nil
}

func (a *Agent) SendStatus(ctx context.Context, sessionID, state, message string) error {
	err := a.inner.SendStatus(ctx, sessionID, state, message)
	if err != nil {
		a.streamErrors.Inc()
	}
	return err
}

type CaptureEngine struct {
	inner   ports.CaptureEngine
	metrics *Agent
}

func NewCaptureEngine(inner ports.CaptureEngine, metrics *Agent) *CaptureEngine {
	return &CaptureEngine{inner: inner, metrics: metrics}
}

func (e *CaptureEngine) Capture(ctx context.Context, iface, filter string, snaplen uint32) (<-chan domain.Packet, <-chan error, error) {
	in, captureErrors, err := e.inner.Capture(ctx, iface, filter, snaplen)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan domain.Packet, 256)
	go func() {
		defer close(out)
		for packet := range in {
			e.metrics.ObserveCaptured(packet)
			select {
			case out <- packet:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, captureErrors, nil
}
