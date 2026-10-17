package benchmark

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcclient"
)

type RunConfig struct {
	Server          string
	Insecure        bool
	CAFile          string
	TLSServerName   string
	Token           string
	Version         string
	GitCommit       string
	RunnerNode      string
	Scenario        string
	Receiver        string
	TargetNamespace string
	TargetKind      string
	TargetName      string
	PacketSize      int
	PacketRate      int
	Duration        time.Duration
	Warmup          time.Duration
	Settle          time.Duration
	Drain           time.Duration
	Subscribers     int
	Snaplen         uint32
	ReorderWindow   time.Duration
	MaxLossPercent  float64
	MaxP99Latency   time.Duration
}

func (c RunConfig) validate() error {
	switch {
	case strings.TrimSpace(c.Server) == "":
		return errors.New("server address is required")
	case strings.TrimSpace(c.Receiver) == "":
		return errors.New("receiver address is required")
	case c.TargetNamespace == "" || c.TargetName == "":
		return errors.New("target namespace and name are required")
	case c.TargetKind != "deployment" && c.TargetKind != "statefulset":
		return errors.New("target kind must be deployment or statefulset")
	case c.PacketSize < ethernetIPv4UDPOverhead+HeaderSize || c.PacketSize > 65535:
		return fmt.Errorf("packet size must be between %d and 65535 bytes", ethernetIPv4UDPOverhead+HeaderSize)
	case c.PacketRate < 1:
		return errors.New("packet rate must be positive")
	case c.Duration <= 0:
		return errors.New("duration must be positive")
	case c.Warmup < 0 || c.Settle < 0 || c.Drain < 0:
		return errors.New("warmup, settle, and drain must not be negative")
	case c.Subscribers < 1:
		return errors.New("subscriber count must be positive")
	case c.Snaplen < uint32(c.PacketSize) || c.Snaplen > 65535:
		return errors.New("snaplen must be at least packet size and no greater than 65535")
	case c.ReorderWindow < 0 || c.ReorderWindow > 5*time.Second:
		return errors.New("reorder window must be between 0 and 5 seconds")
	case c.MaxLossPercent < 0 || c.MaxLossPercent > 100:
		return errors.New("maximum loss percent must be between 0 and 100")
	}
	total := float64(c.PacketRate) * c.Duration.Seconds()
	if total < 1 {
		return errors.New("scenario must generate at least one packet")
	}
	if total > float64(math.MaxUint32) {
		return fmt.Errorf("scenario generates %.0f packets, maximum is %d", total, uint64(math.MaxUint32))
	}
	return nil
}

func Run(ctx context.Context, config RunConfig) (Result, error) {
	if err := config.validate(); err != nil {
		return Result{}, err
	}
	connection, err := grpcclient.Dial(grpcclient.DialConfig{
		Address: config.Server, Insecure: config.Insecure, CAFile: config.CAFile,
		ServerName: config.TLSServerName, Token: config.Token,
	})
	if err != nil {
		return Result{}, fmt.Errorf("connect to CapMesh: %w", err)
	}
	defer connection.Close()
	client := capmeshv1.NewCaptureServiceClient(connection)
	authContext := grpcclient.AuthContext(ctx, config.Token)
	_, portText, err := net.SplitHostPort(config.Receiver)
	if err != nil {
		return Result{}, fmt.Errorf("receiver must use host:port format: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return Result{}, fmt.Errorf("invalid receiver port %q", portText)
	}
	ttl := config.Settle + config.Warmup + config.Duration + config.Drain + 30*time.Second
	request := &capmeshv1.CreateSessionRequest{
		Filter:          fmt.Sprintf("udp dst port %d", port),
		Snaplen:         config.Snaplen,
		TtlSeconds:      uint32(math.Ceil(ttl.Seconds())),
		ReorderWindowMs: uint32(config.ReorderWindow.Milliseconds()),
		Targets: []*capmeshv1.CaptureTarget{{
			Id: "benchmark-target",
			WorkloadTarget: &capmeshv1.WorkloadTarget{
				Namespace: config.TargetNamespace,
				Kind:      config.TargetKind,
				Name:      config.TargetName,
				Direction: "ingress",
				Follow:    false,
				MaxPods:   1,
			},
		}},
	}
	session, err := client.CreateSession(authContext, request)
	if err != nil {
		return Result{}, fmt.Errorf("create capture session: %w", err)
	}
	stopped := false
	defer func() {
		if stopped {
			return
		}
		stopContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.StopSession(grpcclient.AuthContext(stopContext, config.Token), &capmeshv1.StopSessionRequest{SessionId: session.GetId()})
	}()

	runID, err := randomRunID()
	if err != nil {
		return Result{}, err
	}
	expected := uint64(float64(config.PacketRate) * config.Duration.Seconds())
	analyzers := make([]*Analyzer, config.Subscribers)
	for index := range analyzers {
		if index == 0 {
			analyzers[index] = NewAnalyzer(runID, expected)
		} else {
			analyzers[index] = NewSequenceAnalyzer(runID, expected)
		}
	}
	streamContext, cancelStreams := context.WithCancel(authContext)
	defer cancelStreams()
	streams := make([]capmeshv1.CaptureService_StreamPacketsClient, 0, config.Subscribers)
	for index := 0; index < config.Subscribers; index++ {
		stream, err := client.StreamPackets(streamContext, &capmeshv1.StreamPacketsRequest{SessionId: session.GetId()})
		if err != nil {
			return Result{}, fmt.Errorf("open subscriber %d: %w", index+1, err)
		}
		if _, err := stream.Header(); err != nil {
			return Result{}, fmt.Errorf("start subscriber %d: %w", index+1, err)
		}
		streams = append(streams, stream)
	}

	var wait sync.WaitGroup
	streamErrors := make(chan error, len(streams))
	for index, stream := range streams {
		wait.Add(1)
		go func(analyzer *Analyzer, packetStream capmeshv1.CaptureService_StreamPacketsClient) {
			defer wait.Done()
			for {
				batch, err := packetStream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) && streamContext.Err() == nil {
						streamErrors <- err
					}
					return
				}
				receivedAt := time.Now()
				for _, packet := range batch.GetPackets() {
					analyzer.Observe(packet.GetData(), receivedAt)
				}
			}
		}(analyzers[index], stream)
	}

	if !waitOrDone(ctx, config.Settle) {
		return Result{}, ctx.Err()
	}
	if config.Warmup > 0 {
		if _, err := Generate(ctx, config.Receiver, runID^0xffffffff, config.PacketSize, config.PacketRate, config.Warmup); err != nil {
			return Result{}, fmt.Errorf("warmup: %w", err)
		}
	}
	generated, err := Generate(ctx, config.Receiver, runID, config.PacketSize, config.PacketRate, config.Duration)
	if err != nil {
		return Result{}, fmt.Errorf("generate traffic: %w", err)
	}
	if !waitOrDone(ctx, config.Drain) {
		return Result{}, ctx.Err()
	}
	stopContext, stopCancel := context.WithTimeout(authContext, 10*time.Second)
	_, stopErr := client.StopSession(stopContext, &capmeshv1.StopSessionRequest{SessionId: session.GetId()})
	stopCancel()
	if stopErr != nil {
		return Result{}, fmt.Errorf("stop capture session: %w", stopErr)
	}
	stopped = true
	waitDone := make(chan struct{})
	go func() {
		wait.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(10 * time.Second):
		cancelStreams()
		<-waitDone
	case <-ctx.Done():
		cancelStreams()
		<-waitDone
		return Result{}, ctx.Err()
	}
	select {
	case err := <-streamErrors:
		return Result{}, fmt.Errorf("packet stream: %w", err)
	default:
	}

	analyses := make([]Analysis, len(analyzers))
	for index, analyzer := range analyzers {
		analyses[index] = analyzer.Result()
	}
	measurementDuration := generated.Finished.Sub(generated.Started)
	metrics := calculateMetrics(config, generated, analyses, measurementDuration)
	environment := CurrentEnvironment()
	environment.RunnerNode = config.RunnerNode
	environment.CaptureNodes = append([]string(nil), session.GetNodes()...)
	result := Result{
		SchemaVersion: ResultSchemaVersion,
		RunID:         fmt.Sprintf("%08x", runID),
		Version:       config.Version,
		GitCommit:     config.GitCommit,
		StartedAt:     generated.Started,
		FinishedAt:    generated.Finished,
		Environment:   environment,
		Config: ResultConfig{
			Scenario: config.Scenario, Server: config.Server, Receiver: config.Receiver,
			TargetNamespace: config.TargetNamespace, TargetKind: config.TargetKind, TargetName: config.TargetName,
			PacketSize: config.PacketSize, PacketRate: config.PacketRate, Duration: config.Duration,
			Warmup: config.Warmup, Settle: config.Settle, Drain: config.Drain,
			Subscribers: config.Subscribers, Snaplen: config.Snaplen,
			ReorderWindow: config.ReorderWindow, MaxLossPercent: config.MaxLossPercent, MaxP99Latency: config.MaxP99Latency,
		},
		Metrics: metrics,
		Verdict: "PASS",
	}
	if metrics.WorstSubscriberLoss > config.MaxLossPercent {
		result.Verdict = "FAIL"
		result.FailureReason = fmt.Sprintf("worst subscriber loss %.4f%% exceeds %.4f%%", metrics.WorstSubscriberLoss, config.MaxLossPercent)
	}
	if config.MaxP99Latency > 0 && metrics.LatencyP99 > config.MaxP99Latency {
		result.Verdict = "FAIL"
		reason := fmt.Sprintf("p99 latency %s exceeds %s", metrics.LatencyP99, config.MaxP99Latency)
		if result.FailureReason != "" {
			result.FailureReason += "; " + reason
		} else {
			result.FailureReason = reason
		}
	}
	return result, nil
}

func calculateMetrics(config RunConfig, generated GenerateResult, analyses []Analysis, duration time.Duration) ResultMetrics {
	analysis := analyses[0]
	lost := uint64(0)
	if generated.Packets > analysis.Received {
		lost = generated.Packets - analysis.Received
	}
	lossPercent := 0.0
	if generated.Packets > 0 {
		lossPercent = float64(lost) / float64(generated.Packets) * 100
	}
	subscriberPackets := make([]uint64, len(analyses))
	worstSubscriberLoss := 0.0
	for index, subscriber := range analyses {
		subscriberPackets[index] = subscriber.Received
		if generated.Packets > 0 {
			loss := float64(generated.Packets-subscriber.Received) / float64(generated.Packets) * 100
			if loss > worstSubscriberLoss {
				worstSubscriberLoss = loss
			}
		}
	}
	seconds := duration.Seconds()
	actualRate := float64(generated.Packets) / seconds
	capturedRate := float64(analysis.Received) / seconds
	return ResultMetrics{
		PacketsSent: generated.Packets, PacketsCaptured: analysis.Received, PacketsLost: lost,
		LossPercent: lossPercent, SubscriberPackets: subscriberPackets, WorstSubscriberLoss: worstSubscriberLoss,
		Duplicates: analysis.Duplicates, OutOfOrder: analysis.OutOfOrder,
		ActualPacketsPerSecond: actualRate, CapturedPacketsPerSec: capturedRate,
		DataRateBitsPerSecond: capturedRate * float64(config.PacketSize) * 8,
		LatencySamples:        analysis.LatencySamples, LatencyP50: analysis.LatencyP50,
		LatencyP95: analysis.LatencyP95, LatencyP99: analysis.LatencyP99, LatencyMax: analysis.LatencyMax,
		GeneratorSaturated: actualRate < float64(config.PacketRate)*0.95,
	}
}

func randomRunID() (uint32, error) {
	var value [4]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0, fmt.Errorf("generate run ID: %w", err)
	}
	return binary.BigEndian.Uint32(value[:]), nil
}

func waitOrDone(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
