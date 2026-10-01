package recording

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	active          prometheus.Gauge
	packets         prometheus.Counter
	capturedBytes   prometheus.Counter
	fileBytes       prometheus.Counter
	segments        prometheus.Counter
	queueDrops      prometheus.Counter
	completed       *prometheus.CounterVec
	deletedSegments prometheus.Counter
	deletedBytes    prometheus.Counter
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		active:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_recording_active", Help: "Capture recordings currently active."}),
		packets:         prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_packets_total", Help: "Packets written to capture recordings."}),
		capturedBytes:   prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_captured_bytes_total", Help: "Captured packet bytes written to recordings before PCAPNG encoding."}),
		fileBytes:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_file_bytes_total", Help: "Encoded PCAPNG bytes finalized on disk."}),
		segments:        prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_segments_total", Help: "PCAPNG recording segments finalized."}),
		queueDrops:      prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_queue_overflows_total", Help: "Recordings made partial because their packet queue overflowed."}),
		completed:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_recording_completed_total", Help: "Recordings ended, partitioned by final status."}, []string{"status"}),
		deletedSegments: prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_continuous_recording_deleted_segments_total", Help: "Old continuous recording segments deleted by retention."}),
		deletedBytes:    prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_continuous_recording_deleted_bytes_total", Help: "Old continuous recording bytes deleted by retention."}),
	}
	registerer.MustRegister(metrics.active, metrics.packets, metrics.capturedBytes, metrics.fileBytes, metrics.segments, metrics.queueDrops, metrics.completed, metrics.deletedSegments, metrics.deletedBytes)
	return metrics
}
