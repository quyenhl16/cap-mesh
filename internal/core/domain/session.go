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
	Targets          []CaptureTarget
}

type CaptureTarget struct {
	ID        string           `json:"id"`
	Interface *InterfaceTarget `json:"interface,omitempty"`
	Workload  *WorkloadTarget  `json:"workload,omitempty"`
}

type InterfaceTarget struct {
	Nodes            []string `json:"nodes,omitempty"`
	LogicalInterface string   `json:"logical_interface"`
}

type WorkloadTarget struct {
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Direction string `json:"direction"`
	Follow    bool   `json:"follow"`
	MaxPods   uint32 `json:"max_pods"`
}

type CaptureSource struct {
	ID               string
	TargetID         string
	TargetType       string
	NodeName         string
	LogicalInterface string
	InterfaceName    string
	Namespace        string
	PodName          string
	PodUID           string
	PodIP            string
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
	s.Targets = cloneTargets(s.Targets)
	return s
}

func cloneTargets(input []CaptureTarget) []CaptureTarget {
	out := make([]CaptureTarget, len(input))
	for i, target := range input {
		out[i] = target
		if target.Interface != nil {
			value := *target.Interface
			value.Nodes = append([]string(nil), value.Nodes...)
			out[i].Interface = &value
		}
		if target.Workload != nil {
			value := *target.Workload
			out[i].Workload = &value
		}
	}
	return out
}
