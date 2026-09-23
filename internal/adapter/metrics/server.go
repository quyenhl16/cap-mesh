package metrics

import "github.com/prometheus/client_golang/prometheus"

type Server struct {
	received        prometheus.Counter
	emitted         prometheus.Counter
	late            prometheus.Counter
	subscriberDrops prometheus.Counter
	reorderSize     prometheus.Gauge
	queueUsage      prometheus.Histogram
}

func NewServer(registerer prometheus.Registerer) *Server {
	m := &Server{
		received:        prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_packets_received_total", Help: "Packets received from agents."}),
		emitted:         prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_packets_emitted_total", Help: "Packets emitted to the live stream."}),
		late:            prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_late_packets_total", Help: "Packets received after the emission watermark."}),
		subscriberDrops: prometheus.NewCounter(prometheus.CounterOpts{Name: "capmesh_server_subscriber_drops_total", Help: "Packets dropped from full subscriber queues."}),
		reorderSize:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "capmesh_server_reorder_buffer_size", Help: "Packets currently buffered for reordering."}),
		queueUsage:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "capmesh_server_subscriber_queue_usage", Help: "Subscriber queue utilization ratio.", Buckets: prometheus.LinearBuckets(0, .1, 11)}),
	}
	registerer.MustRegister(m.received, m.emitted, m.late, m.subscriberDrops, m.reorderSize, m.queueUsage)
	return m
}

func (m *Server) PacketsReceived(n int)                { m.received.Add(float64(n)) }
func (m *Server) PacketsEmitted(n int)                 { m.emitted.Add(float64(n)) }
func (m *Server) LatePacket()                          { m.late.Inc() }
func (m *Server) SubscriberDrop()                      { m.subscriberDrops.Inc() }
func (m *Server) SetReorderBuffer(n int)               { m.reorderSize.Set(float64(n)) }
func (m *Server) ObserveSubscriberQueue(value float64) { m.queueUsage.Observe(value) }

type Noop struct{}

func (Noop) PacketsReceived(int)            {}
func (Noop) PacketsEmitted(int)             {}
func (Noop) LatePacket()                    {}
func (Noop) SubscriberDrop()                {}
func (Noop) SetReorderBuffer(int)           {}
func (Noop) ObserveSubscriberQueue(float64) {}
