package stream

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/quyenhl16/capmesh/internal/core/domain"
	"github.com/quyenhl16/capmesh/internal/core/ports"
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
	subscribers map[uint64]chan domain.PacketBatch
	nextID      uint64
	lastEmitted time.Time
	closed      bool
	wake        chan struct{}
	done        chan struct{}
	metrics     ports.Metrics
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
	p := &pipeline{window: window, queueSize: queueSize, subscribers: make(map[uint64]chan domain.PacketBatch), wake: make(chan struct{}, 1), done: make(chan struct{}), metrics: s.metrics}
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
	s.mu.RLock()
	p := s.pipelines[sessionID]
	s.mu.RUnlock()
	if p == nil {
		return nil, ports.ErrNotFound
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ports.ErrNotFound
	}
	id := p.nextID
	p.nextID++
	ch := make(chan domain.PacketBatch, p.queueSize)
	p.subscribers[id] = ch
	p.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			p.removeSubscriber(id)
		case <-p.done:
		}
	}()
	return ch, nil
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
		for _, subscriber := range p.subscribers {
			select {
			case subscriber <- item.batch.Clone():
			default:
				p.metrics.SubscriberDrop()
			}
			p.metrics.ObserveSubscriberQueue(float64(len(subscriber)) / float64(cap(subscriber)))
		}
		p.metrics.PacketsEmitted(1)
	}
	p.metrics.SetReorderBuffer(p.packets.Len())
}

func (p *pipeline) removeSubscriber(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ch, ok := p.subscribers[id]; ok {
		delete(p.subscribers, id)
		close(ch)
	}
}

func (p *pipeline) close() {
	p.emitUntil(time.Time{}, true)
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		for id, ch := range p.subscribers {
			delete(p.subscribers, id)
			close(ch)
		}
		close(p.done)
	}
	p.mu.Unlock()
}
