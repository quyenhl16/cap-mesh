package domain

import "testing"

func TestSessionTransitions(t *testing.T) {
	session := Session{Status: SessionStarting}
	for _, state := range []SessionStatus{SessionRunning, SessionStopping, SessionStopped} {
		if err := session.Transition(state); err != nil {
			t.Fatalf("transition to %s: %v", state, err)
		}
	}
	if err := session.Transition(SessionRunning); err == nil {
		t.Fatal("expected stopped to running transition to fail")
	}
}
