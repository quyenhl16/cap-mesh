package benchmark

import (
	"testing"
	"time"
)

func TestHeaderRoundTripInsideCapturedPacket(t *testing.T) {
	runID := uint32(0x12345678)
	sentAt := time.Unix(1700000000, 123456789)
	payload := make([]byte, HeaderSize+4)
	encodeHeader(payload, runID, 42, sentAt)
	captured := append([]byte{0, 1, 2, 3, 4}, payload...)
	sequence, decodedTime, ok := decodeHeader(captured, runID)
	if !ok {
		t.Fatal("header was not found")
	}
	if sequence != 42 || !decodedTime.Equal(sentAt) {
		t.Fatalf("decoded sequence/time = %d/%s, want 42/%s", sequence, decodedTime, sentAt)
	}
}

func TestHeaderRejectsAnotherRun(t *testing.T) {
	payload := make([]byte, HeaderSize)
	encodeHeader(payload, 1, 2, time.Now())
	if _, _, ok := decodeHeader(payload, 3); ok {
		t.Fatal("header from another run was accepted")
	}
}
