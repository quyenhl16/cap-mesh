package benchmark

import (
	"sort"
	"time"
)

const maxLatencySamples = 1_000_000

type Analyzer struct {
	runID       uint32
	expected    uint64
	seen        []uint64
	received    uint64
	duplicates  uint64
	outOfOrder  uint64
	highest     uint32
	haveHighest bool
	sampleEvery uint64
	latencies   []int64
	latency     bool
}

func NewAnalyzer(runID uint32, expected uint64) *Analyzer {
	return newAnalyzer(runID, expected, true)
}

func NewSequenceAnalyzer(runID uint32, expected uint64) *Analyzer {
	return newAnalyzer(runID, expected, false)
}

func newAnalyzer(runID uint32, expected uint64, latency bool) *Analyzer {
	sampleEvery := uint64(1)
	if expected > maxLatencySamples {
		sampleEvery = (expected + maxLatencySamples - 1) / maxLatencySamples
	}
	latencyCapacity := uint64(0)
	if latency {
		latencyCapacity = min(expected/sampleEvery+1, maxLatencySamples)
	}
	return &Analyzer{
		runID:       runID,
		expected:    expected,
		seen:        make([]uint64, (expected+63)/64),
		sampleEvery: sampleEvery,
		latencies:   make([]int64, 0, latencyCapacity),
		latency:     latency,
	}
}

func (a *Analyzer) Observe(packet []byte, receivedAt time.Time) bool {
	sequence, sentAt, ok := decodeHeader(packet, a.runID)
	if !ok || uint64(sequence) >= a.expected {
		return false
	}
	word := sequence / 64
	mask := uint64(1) << (sequence % 64)
	if a.seen[word]&mask != 0 {
		a.duplicates++
		return true
	}
	if a.haveHighest && sequence < a.highest {
		a.outOfOrder++
	}
	if !a.haveHighest || sequence > a.highest {
		a.highest = sequence
		a.haveHighest = true
	}
	a.seen[word] |= mask
	a.received++
	if a.latency && uint64(sequence)%a.sampleEvery == 0 {
		latency := receivedAt.Sub(sentAt)
		if latency >= 0 {
			a.latencies = append(a.latencies, int64(latency))
		}
	}
	return true
}

type Analysis struct {
	Received       uint64
	Duplicates     uint64
	OutOfOrder     uint64
	LatencySamples uint64
	LatencyP50     time.Duration
	LatencyP95     time.Duration
	LatencyP99     time.Duration
	LatencyMax     time.Duration
}

func (a *Analyzer) Result() Analysis {
	sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
	return Analysis{
		Received:       a.received,
		Duplicates:     a.duplicates,
		OutOfOrder:     a.outOfOrder,
		LatencySamples: uint64(len(a.latencies)),
		LatencyP50:     percentile(a.latencies, 50),
		LatencyP95:     percentile(a.latencies, 95),
		LatencyP99:     percentile(a.latencies, 99),
		LatencyMax:     percentile(a.latencies, 100),
	}
}

func percentile(values []int64, percent int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(values) {
		index = len(values)
	}
	return time.Duration(values[index-1])
}
