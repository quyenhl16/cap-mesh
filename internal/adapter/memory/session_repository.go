package memory

import (
	"context"
	"sync"

	"github.com/quyenhl16/capmesh/internal/core/domain"
	"github.com/quyenhl16/capmesh/internal/core/ports"
)

type SessionRepository struct {
	mu       sync.RWMutex
	sessions map[string]domain.Session
}

func NewSessionRepository() *SessionRepository {
	return &SessionRepository{sessions: make(map[string]domain.Session)}
}

func (r *SessionRepository) Create(_ context.Context, session domain.Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sessions[session.ID]; exists {
		return ports.ErrAlreadyExists
	}
	r.sessions[session.ID] = session.Clone()
	return nil
}

func (r *SessionRepository) Get(_ context.Context, id string) (domain.Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, exists := r.sessions[id]
	if !exists {
		return domain.Session{}, ports.ErrNotFound
	}
	return session.Clone(), nil
}

func (r *SessionRepository) Update(_ context.Context, session domain.Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sessions[session.ID]; !exists {
		return ports.ErrNotFound
	}
	r.sessions[session.ID] = session.Clone()
	return nil
}

func (r *SessionRepository) List(_ context.Context) ([]domain.Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sessions := make([]domain.Session, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session.Clone())
	}
	return sessions, nil
}
