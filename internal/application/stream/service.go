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
}

type subscriber struct {
	batches          chan domain.PacketBatch
	dropped          chan struct{}
	disconnectOnDrop bool
}

func NewService(metrics ports.Metrics) *Service {
	return &Service{pipelines: make(map[string]*pipeline), metrics: metrics}
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
	p := &pipeline{window: window, queueSize: queueSize, subscribers: make(map[uint64]*subscriber), wake: make(chan struct{}, 1), done: make(chan struct{}), metrics: s.metrics}
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
	for _, packet := range batch.Packets {
		if !p.lastEmitted.IsZero() && packet.Timestamp.Before(p.lastEmitted) {
			p.metrics.LatePacket()
		}
		pushPacket(&p.packets, batch, packet)
	}
	p.metrics.PacketsReceived(len(batch.Packets))
	p.metrics.SetReorderBuffer(p.packets.Len())
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
			p.emitUntil(time.Now().Add(-p.window), false)
		case <-p.wake:
			p.emitUntil(time.Now().Add(-p.window), false)
		case <-p.done:
			return
		}
	}
}

func (p *pipeline) emitUntil(cutoff time.Time, all bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.packets.Len() > 0 {
		item := p.packets[0]
		packet := item.batch.Packets[0]
		if !all && packet.Timestamp.After(cutoff) {
			break
		}
		heap.Pop(&p.packets)
		p.lastEmitted = packet.Timestamp
		for id, subscriber := range p.subscribers {
			select {
			case subscriber.batches <- item.batch.Clone():
			default:
				p.metrics.SubscriberDrop()
				if subscriber.disconnectOnDrop {
					close(subscriber.dropped)
					close(subscriber.batches)
					delete(p.subscribers, id)
					continue
				}
			}
			p.metrics.ObserveSubscriberQueue(float64(len(subscriber.batches)) / float64(cap(subscriber.batches)))
		}
		p.metrics.PacketsEmitted(1)
	}
	p.metrics.SetReorderBuffer(p.packets.Len())
}

func (p *pipeline) removeSubscriber(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if subscriber, ok := p.subscribers[id]; ok {
		delete(p.subscribers, id)
		close(subscriber.batches)
	}
}

func (p *pipeline) close() {
	p.emitUntil(time.Time{}, true)
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		for id, subscriber := range p.subscribers {
			delete(p.subscribers, id)
			close(subscriber.batches)
		}
		close(p.done)
	}
	p.mu.Unlock()
}
