package logrecording

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	active          prometheus.Gauge
	streams         prometheus.Gauge
	lines           prometheus.Counter
	bytes           prometheus.Counter
	reconnects      prometheus.Counter
	segments        prometheus.Counter
	deletedSegments prometheus.Counter
	deletedBytes    prometheus.Counter
	errors          prometheus.Counter
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		active:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_log_capture_active", Help: "Whether the singleton workload log capture is active."}),
		streams:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_log_capture_streams", Help: "Current Kubernetes pod log streams."}),
		lines:           prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_lines_total", Help: "Log records written."}),
		bytes:           prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_bytes_total", Help: "Raw log bytes written."}),
		reconnects:      prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_reconnects_total", Help: "Kubernetes pod log stream reconnects."}),
		segments:        prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_segments_total", Help: "Finalized log segments."}),
		deletedSegments: prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_deleted_segments_total", Help: "Log segments deleted by retention."}),
		deletedBytes:    prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_deleted_bytes_total", Help: "Log bytes deleted by retention."}),
		errors:          prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_log_capture_errors_total", Help: "Workload log capture errors."}),
	}
	registerer.MustRegister(m.active, m.streams, m.lines, m.bytes, m.reconnects, m.segments, m.deletedSegments, m.deletedBytes, m.errors)
	return m
}

func (m *Metrics) SetActive(active bool) {
	if active {
		m.active.Set(1)
	} else {
		m.active.Set(0)
	}
}
func (m *Metrics) SetStreams(count int) { m.streams.Set(float64(count)) }
func (m *Metrics) Reconnect()           { m.reconnects.Inc() }
func (m *Metrics) Error()               { m.errors.Inc() }
