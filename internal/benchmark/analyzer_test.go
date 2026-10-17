package benchmark

import (
	"testing"
	"time"
)

func TestAnalyzerCountsUniqueDuplicateAndOutOfOrderPackets(t *testing.T) {
	const runID = 10
	analyzer := NewAnalyzer(runID, 4)
	now := time.Unix(1700000000, 0)
	observe := func(sequence uint32, latency time.Duration) {
		packet := make([]byte, HeaderSize)
		encodeHeader(packet, runID, sequence, now.Add(-latency))
		if !analyzer.Observe(packet, now) {
			t.Fatalf("packet %d was ignored", sequence)
		}
	}
	observe(0, time.Millisecond)
	observe(2, 3*time.Millisecond)
	observe(1, 2*time.Millisecond)
	observe(1, 2*time.Millisecond)
	result := analyzer.Result()
	if result.Received != 3 || result.Duplicates != 1 || result.OutOfOrder != 1 {
		t.Fatalf("unexpected analysis: %+v", result)
	}
	if result.LatencyP50 != 2*time.Millisecond || result.LatencyP99 != 3*time.Millisecond {
		t.Fatalf("unexpected latency percentiles: %+v", result)
	}
}
