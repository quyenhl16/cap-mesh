package domain

import (
	"errors"
	"time"
)

type SessionStatus string

const (
	SessionStarting SessionStatus = "STARTING"
	SessionRunning  SessionStatus = "RUNNING"
	SessionStopping SessionStatus = "STOPPING"
	SessionStopped  SessionStatus = "STOPPED"
	SessionFailed   SessionStatus = "FAILED"
)

var ErrInvalidTransition = errors.New("invalid session state transition")

type Session struct {
	ID               string
	Nodes            []string
	LogicalInterface string
	Filter           string
	Snaplen          uint32
	ReorderWindow    time.Duration
	CreatedAt        time.Time
	ExpiresAt        time.Time
	Status           SessionStatus
	Message          string
}

func (s *Session) Transition(next SessionStatus) error {
	valid := map[SessionStatus]map[SessionStatus]bool{
		SessionStarting: {SessionRunning: true, SessionStopping: true, SessionFailed: true},
		SessionRunning:  {SessionStopping: true, SessionFailed: true},
		SessionStopping: {SessionStopped: true, SessionFailed: true},
	}
	if s.Status == next {
		return nil
	}
	if !valid[s.Status][next] {
		return ErrInvalidTransition
	}
	s.Status = next
	return nil
}

func (s Session) Clone() Session {
	s.Nodes = append([]string(nil), s.Nodes...)
	return s
}
