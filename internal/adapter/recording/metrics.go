package recording

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	active            prometheus.Gauge
	packets           prometheus.Counter
	capturedBytes     prometheus.Counter
	fileBytes         prometheus.Counter
	segments          prometheus.Counter
	queueDrops        prometheus.Counter
	queueDepth        prometheus.Gauge
	queueCapacity     prometheus.Gauge
	queueUsage        prometheus.Histogram
	batchPackets      prometheus.Histogram
	writeDuration     prometheus.Histogram
	flushDuration     prometheus.Histogram
	syncDuration      prometheus.Histogram
	finalizeDuration  prometheus.Histogram
	writeBytes        prometheus.Counter
	writeErrors       prometheus.Counter
	completed         *prometheus.CounterVec
	deletedSegments   prometheus.Counter
	deletedBytes      prometheus.Counter
	normalStorage     prometheus.Gauge
	normalDirectories prometheus.Gauge
	cleanupDeleted    prometheus.Counter
	cleanupBytes      prometheus.Counter
	cleanupFailures   prometheus.Counter
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		active:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_recording_active", Help: "Capture recordings currently active."}),
		packets:           prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_packets_total", Help: "Packets written to capture recordings."}),
		capturedBytes:     prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_captured_bytes_total", Help: "Captured packet bytes written to recordings before PCAPNG encoding."}),
		fileBytes:         prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_file_bytes_total", Help: "Encoded PCAPNG bytes finalized on disk."}),
		segments:          prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_segments_total", Help: "PCAPNG recording segments finalized."}),
		queueDrops:        prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_queue_overflows_total", Help: "Recordings made partial because their packet queue overflowed."}),
		queueDepth:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_recording_queue_depth", Help: "Approximate batches waiting across all recording queues."}),
		queueCapacity:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_recording_queue_capacity", Help: "Configured capacity of each recording queue."}),
		queueUsage:        prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_recording_queue_usage", Help: "Recording queue utilization ratio sampled when batches are consumed.", Buckets: prometheus.LinearBuckets(0, .1, 11)}),
		batchPackets:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_recording_batch_packets", Help: "Packets per batch received by the recorder.", Buckets: prometheus.ExponentialBuckets(1, 2, 11)}),
		writeDuration:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_recording_write_duration_seconds", Help: "Time to encode and write one packet batch.", Buckets: prometheus.ExponentialBuckets(100e-6, 2, 18)}),
		flushDuration:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_recording_flush_duration_seconds", Help: "Time to flush PCAPNG buffers.", Buckets: prometheus.ExponentialBuckets(100e-6, 2, 18)}),
		syncDuration:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_recording_sync_duration_seconds", Help: "Time to flush and fsync a recording segment.", Buckets: prometheus.ExponentialBuckets(100e-6, 2, 18)}),
		finalizeDuration:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_recording_finalize_duration_seconds", Help: "Time to flush, sync, close, and rename a recording segment.", Buckets: prometheus.ExponentialBuckets(100e-6, 2, 18)}),
		writeBytes:        prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_write_bytes_total", Help: "PCAPNG bytes written on the recorder hot path."}),
		writeErrors:       prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_recording_write_errors_total", Help: "Recorder write, flush, sync, or finalize errors."}),
		completed:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "capmesh_recording_completed_total", Help: "Recordings ended, partitioned by final status."}, []string{"status"}),
		deletedSegments:   prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_continuous_recording_deleted_segments_total", Help: "Old continuous recording segments deleted by retention."}),
		deletedBytes:      prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_continuous_recording_deleted_bytes_total", Help: "Old continuous recording bytes deleted by retention."}),
		normalStorage:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_normal_recording_storage_bytes", Help: "Bytes used by normal session recording directories."}),
		normalDirectories: prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_normal_recording_directories", Help: "Normal session recording directories currently stored."}),
		cleanupDeleted:    prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_normal_recording_cleanup_deleted_total", Help: "Normal session recording directories deleted by cleanup."}),
		cleanupBytes:      prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_normal_recording_cleanup_deleted_bytes_total", Help: "Normal session recording bytes deleted by cleanup."}),
		cleanupFailures:   prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_normal_recording_cleanup_failures_total", Help: "Normal session recording cleanup failures."}),
	}
	registerer.MustRegister(metrics.active, metrics.packets, metrics.capturedBytes, metrics.fileBytes, metrics.segments, metrics.queueDrops, metrics.queueDepth, metrics.queueCapacity, metrics.queueUsage, metrics.batchPackets, metrics.writeDuration, metrics.flushDuration, metrics.syncDuration, metrics.finalizeDuration, metrics.writeBytes, metrics.writeErrors, metrics.completed, metrics.deletedSegments, metrics.deletedBytes, metrics.normalStorage, metrics.normalDirectories, metrics.cleanupDeleted, metrics.cleanupBytes, metrics.cleanupFailures)
	return metrics
}
