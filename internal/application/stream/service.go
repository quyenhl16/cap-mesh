package stream

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type Service struct {
	mu        sync.RWMutex
	pipelines map[string]*pipeline
	metrics   ports.Metrics
	rebatch   RebatchConfig
}

type RebatchConfig struct {
	MaxPackets int
	MaxBytes   int
	MaxDelay   time.Duration
}

type pipeline struct {
	mu          sync.Mutex
	window      time.Duration
	queueSize   int
	packets     packetHeap
	subscribers map[uint64]*subscriber
	nextID      uint64
	lastEmitted time.Time
	closed      bool
	wake        chan struct{}
	done        chan struct{}
	metrics     ports.Metrics
	rebatch     RebatchConfig
}

type subscriber struct {
	batches          chan domain.PacketBatch
	dropped          chan struct{}
	disconnectOnDrop bool
}

func NewService(metrics ports.Metrics) *Service {
	return NewServiceWithRebatch(metrics, RebatchConfig{MaxPackets: 256, MaxBytes: 1024 * 1024, MaxDelay: time.Millisecond})
}

func NewServiceWithRebatch(metrics ports.Metrics, config RebatchConfig) *Service {
	if config.MaxPackets < 1 {
		config.MaxPackets = 1
	}
	if config.MaxBytes < 1 {
		config.MaxBytes = 1
	}
	if config.MaxDelay < 0 {
		config.MaxDelay = 0
	}
	return &Service{pipelines: make(map[string]*pipeline), metrics: metrics, rebatch: config}
}

func (s *Service) OpenSession(id string, window time.Duration, queueSize int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pipelines[id]; exists {
		return ports.ErrAlreadyExists
	}
	if queueSize < 1 {
		queueSize = 1
	}
	p := &pipeline{window: window, queueSize: queueSize, subscribers: make(map[uint64]*subscriber), wake: make(chan struct{}, 1), done: make(chan struct{}), metrics: s.metrics, rebatch: s.rebatch}
	heap.Init(&p.packets)
	s.pipelines[id] = p
	go p.run()
	return nil
}

func (s *Service) Publish(batch domain.PacketBatch) {
	s.mu.RLock()
	p := s.pipelines[batch.SessionID]
	s.mu.RUnlock()
	if p == nil || len(batch.Packets) == 0 {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	receivedAt := time.Now()
	for _, packet := range batch.Packets {
		if !p.lastEmitted.IsZero() && packet.Timestamp.Before(p.lastEmitted) {
			p.metrics.LatePacket()
		}
		pushPacket(&p.packets, batch, packet, receivedAt)
	}
	p.metrics.PacketsReceived(len(batch.Packets))
	p.metrics.AddReorderBuffer(len(batch.Packets))
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (s *Service) Subscribe(ctx context.Context, sessionID string) (<-chan domain.PacketBatch, error) {
	subscription, err := s.subscribe(ctx, sessionID, 0, false)
	if err != nil {
		return nil, err
	}
	return subscription.Batches, nil
}

func (s *Service) SubscribeLossAware(ctx context.Context, sessionID string, queueSize int) (ports.PacketSubscription, error) {
	return s.subscribe(ctx, sessionID, queueSize, true)
}

func (s *Service) subscribe(ctx context.Context, sessionID string, queueSize int, disconnectOnDrop bool) (ports.PacketSubscription, error) {
	s.mu.RLock()
	p := s.pipelines[sessionID]
	s.mu.RUnlock()
	if p == nil {
		return ports.PacketSubscription{}, ports.ErrNotFound
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ports.PacketSubscription{}, ports.ErrNotFound
	}
	if queueSize < 1 {
		queueSize = p.queueSize
	}
	id := p.nextID
	p.nextID++
	sub := &subscriber{batches: make(chan domain.PacketBatch, queueSize), dropped: make(chan struct{}), disconnectOnDrop: disconnectOnDrop}
	p.subscribers[id] = sub
	p.metrics.SubscriberAdded()
	p.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			p.removeSubscriber(id)
		case <-p.done:
		}
	}()
	return ports.PacketSubscription{Batches: sub.batches, Dropped: sub.dropped}, nil
}

func (s *Service) CloseSession(id string) {
	s.mu.Lock()
	p := s.pipelines[id]
	delete(s.pipelines, id)
	s.mu.Unlock()
	if p != nil {
		p.close()
	}
}

func (p *pipeline) run() {
	interval := p.window / 4
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	if interval > 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			p.emitUntil(now.Add(-p.window), now.Add(-p.rebatch.MaxDelay), false)
		case <-p.wake:
			now := time.Now()
			p.emitUntil(now.Add(-p.window), now.Add(-p.rebatch.MaxDelay), false)
		case <-p.done:
			return
		}
	}
}

func (p *pipeline) emitUntil(timestampCutoff, receivedCutoff time.Time, all bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.packets.Len() > 0 {
		first := p.packets[0]
		if !all && (first.packet.Timestamp.After(timestampCutoff) || first.receivedAt.After(receivedCutoff)) {
			break
		}
		batch := first.template
		batch.Packets = make([]domain.Packet, 0, min(p.packets.Len(), p.rebatch.MaxPackets))
		batchBytes := 0
		batchStarted := first.receivedAt
		reason := "watermark"
		for p.packets.Len() > 0 {
			item := p.packets[0]
			if !all && (item.packet.Timestamp.After(timestampCutoff) || item.receivedAt.After(receivedCutoff)) {
				break
			}
			if !sameBatchStream(batch, item.template) {
				break
			}
			packetBytes := int(item.packet.CapturedLength)
			if len(batch.Packets) > 0 && (len(batch.Packets) >= p.rebatch.MaxPackets || batchBytes+packetBytes > p.rebatch.MaxBytes) {
				if len(batch.Packets) >= p.rebatch.MaxPackets {
					reason = "packets"
				} else {
					reason = "bytes"
				}
				break
			}
			heap.Pop(&p.packets)
			p.metrics.AddReorderBuffer(-1)
			p.metrics.ObserveReorderWait(item.packet.SequenceNumber, time.Since(item.receivedAt))
			batch.Packets = append(batch.Packets, item.packet)
			batchBytes += packetBytes
			p.lastEmitted = item.packet.Timestamp
			if len(batch.Packets) >= p.rebatch.MaxPackets || batchBytes >= p.rebatch.MaxBytes {
				if len(batch.Packets) >= p.rebatch.MaxPackets {
					reason = "packets"
				} else {
					reason = "bytes"
				}
				break
			}
		}
		if all && reason == "watermark" {
			reason = "shutdown"
		}
		p.metrics.ObserveRebatch(len(batch.Packets), batchBytes, time.Since(batchStarted), reason)
		fanoutStarted := time.Now()
		deliveries := 0
		for id, subscriber := range p.subscribers {
			select {
			case subscriber.batches <- batch:
				deliveries++
			default:
				p.metrics.SubscriberDrop(len(batch.Packets))
				if subscriber.disconnectOnDrop {
					close(subscriber.dropped)
					close(subscriber.batches)
					delete(p.subscribers, id)
					p.metrics.SubscriberRemoved()
					continue
				}
			}
			p.metrics.ObserveSubscriberQueue(float64(len(subscriber.batches)) / float64(cap(subscriber.batches)))
		}
		p.metrics.ObserveFanout(time.Since(fanoutStarted), deliveries)
		p.metrics.PacketsEmitted(len(batch.Packets))
	}
}

func sameBatchStream(left, right domain.PacketBatch) bool {
	return left.SessionID == right.SessionID && left.NodeName == right.NodeName && left.InterfaceName == right.InterfaceName && left.Source == right.Source
}

func (p *pipeline) removeSubscriber(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if subscriber, ok := p.subscribers[id]; ok {
		delete(p.subscribers, id)
		close(subscriber.batches)
		p.metrics.SubscriberRemoved()
	}
}

func (p *pipeline) close() {
	p.emitUntil(time.Time{}, time.Time{}, true)
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		for id, subscriber := range p.subscribers {
			delete(p.subscribers, id)
			close(subscriber.batches)
			p.metrics.SubscriberRemoved()
		}
		close(p.done)
	}
	p.mu.Unlock()
}
