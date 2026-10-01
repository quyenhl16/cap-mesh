package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcapi"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcclient"
	"github.com/quyenhl16/cap-mesh/internal/adapter/pcapng"
	"github.com/quyenhl16/cap-mesh/internal/core/interfacealias"
)

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }
func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	server := flag.String("server", "127.0.0.1:18443", "capmesh-server address")
	sessionID := flag.String("session", "", "existing capture session ID")
	create := flag.Bool("create", false, "create a session before subscribing")
	continuousStart := flag.Bool("continuous-start", false, "start the singleton server-owned continuous capture and exit")
	continuousStop := flag.Bool("continuous-stop", false, "stop the singleton continuous capture and exit")
	continuousStatus := flag.Bool("continuous-status", false, "show singleton continuous capture status and exit")
	nodes := flag.String("nodes", "", "comma-separated node names for a new session")
	var logicalInterfaces stringList
	flag.Var(&logicalInterfaces, "interface", "logical interface alias configured on the agents; repeat to capture multiple interfaces")
	workloadNamespace := flag.String("namespace", "", "namespace of the workload capture target")
	workloadKind := flag.String("workload-kind", "", "workload kind: statefulset or deployment")
	workloadName := flag.String("workload-name", "", "name of the StatefulSet or Deployment")
	direction := flag.String("direction", "egress", "workload traffic direction: egress, ingress, or both")
	follow := flag.Bool("follow", true, "follow workload scale, restart, and reschedule changes")
	maxPods := flag.Uint("max-pods", 100, "maximum pods allowed for a workload target")
	filter := flag.String("filter", "", "BPF capture filter")
	snaplen := flag.Uint("snaplen", 256, "packet snapshot length")
	ttl := flag.Duration("ttl", 5*time.Minute, "capture session lifetime")
	reorderWindow := flag.Duration("reorder-window", 300*time.Millisecond, "packet reorder window")
	token := flag.String("token", os.Getenv("CAPMESH_TOKEN"), "shared bearer token")
	insecureTransport := flag.Bool("insecure", false, "disable TLS (local development only)")
	caFile := flag.String("tls-ca", "", "server CA certificate")
	serverName := flag.String("tls-server-name", "", "TLS server name override")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	controlModes := boolCount(*continuousStart, *continuousStop, *continuousStatus)
	if controlModes > 1 || (controlModes > 0 && (*create || *sessionID != "")) {
		logger.Error("--continuous-start, --continuous-stop, --continuous-status, --create, and --session are mutually exclusive")
		os.Exit(2)
	}
	if !*create && *sessionID == "" && controlModes == 0 {
		logger.Error("--session, --create, or a continuous capture control flag is required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	connection, err := grpcclient.Dial(grpcclient.DialConfig{Address: *server, Insecure: *insecureTransport, CAFile: *caFile, ServerName: *serverName, Token: *token})
	if err != nil {
		logger.Error("connect failed", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	client := capmeshv1.NewCaptureServiceClient(connection)
	authContext := grpcclient.AuthContext(ctx, *token)
	if *continuousStop {
		capture, err := client.StopContinuousCapture(authContext, &capmeshv1.StopContinuousCaptureRequest{})
		if err != nil {
			logger.Error("stop continuous capture failed", "error", err)
			os.Exit(1)
		}
		printContinuousCapture(capture)
		return
	}
	if *continuousStatus {
		capture, err := client.GetContinuousCapture(authContext, &capmeshv1.GetContinuousCaptureRequest{})
		if err != nil {
			logger.Error("get continuous capture failed", "error", err)
			os.Exit(1)
		}
		printContinuousCapture(capture)
		return
	}
	if *continuousStart {
		targets, _, err := buildTargets(logicalInterfaces, *nodes, *workloadNamespace, *workloadKind, *workloadName, *direction, *follow, *maxPods)
		if err != nil {
			logger.Error("invalid capture target", "error", err)
			os.Exit(2)
		}
		capture, err := client.StartContinuousCapture(authContext, &capmeshv1.StartContinuousCaptureRequest{Targets: targets, Filter: *filter, Snaplen: uint32(*snaplen), ReorderWindowMs: uint32(reorderWindow.Milliseconds())})
		if err != nil {
			logger.Error("start continuous capture failed", "error", err)
			os.Exit(1)
		}
		printContinuousCapture(capture)
		return
	}
	created := false
	if *create {
		targets, workloadRequested, err := buildTargets(logicalInterfaces, *nodes, *workloadNamespace, *workloadKind, *workloadName, *direction, *follow, *maxPods)
		if err != nil {
			logger.Error("invalid capture target", "error", err)
			os.Exit(2)
		}
		request := &capmeshv1.CreateSessionRequest{Filter: *filter, Snaplen: uint32(*snaplen), TtlSeconds: uint32(ttl.Seconds()), ReorderWindowMs: uint32(reorderWindow.Milliseconds())}
		if !workloadRequested && len(targets) == 1 && targets[0].GetInterfaceTarget() != nil {
			request.Nodes = splitNodes(*nodes)
			request.LogicalInterface = targets[0].GetInterfaceTarget().GetLogicalInterface()
		} else {
			request.Targets = targets
		}
		session, err := client.CreateSession(authContext, request)
		if err != nil {
			logger.Error("create session failed", "error", err)
			os.Exit(1)
		}
		*sessionID = session.GetId()
		created = true
		logger.Info("capture session created", "session_id", *sessionID)
	}
	if created {
		defer func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			_, _ = client.StopSession(grpcclient.AuthContext(stopCtx, *token), &capmeshv1.StopSessionRequest{SessionId: *sessionID})
		}()
	}
	stream, err := client.StreamPackets(authContext, &capmeshv1.StreamPacketsRequest{SessionId: *sessionID})
	if err != nil {
		logger.Error("subscribe failed", "error", err)
		os.Exit(1)
	}
	writer := pcapng.NewWriter(os.Stdout, 64, 50*time.Millisecond)
	packetsReceived := 0
	type streamResult struct {
		batch *capmeshv1.PacketBatch
		err   error
	}
	incoming := make(chan streamResult)
	go func() {
		for {
			batch, err := stream.Recv()
			select {
			case incoming <- streamResult{batch: batch, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	flushTicker := time.NewTicker(50 * time.Millisecond)
	defer flushTicker.Stop()
	streaming := true
	for streaming {
		select {
		case result := <-incoming:
			if result.err != nil {
				if !errors.Is(result.err, io.EOF) && ctx.Err() == nil {
					logger.Error("packet stream failed", "error", result.err)
					os.Exit(1)
				}
				streaming = false
				continue
			}
			domainBatch := grpcapi.PacketBatchFromProto(result.batch)
			if err := writer.WriteBatch(domainBatch); err != nil {
				logger.Error("PCAPNG output failed", "error", err)
				os.Exit(1)
			}
			packetsReceived += len(domainBatch.Packets)
		case <-flushTicker.C:
			if err := writer.Flush(); err != nil {
				logger.Error("PCAPNG output failed", "error", err)
				os.Exit(1)
			}
		case <-ctx.Done():
			streaming = false
		}
	}
	if err := writer.Flush(); err != nil {
		logger.Error("flush PCAPNG failed", "error", err)
		os.Exit(1)
	}
	logger.Info("capture stream completed", "session_id", *sessionID, "packets_received", packetsReceived, "pcapng_blocks_written", packetsReceived)
}

func buildTargets(logicalInterfaces []string, nodes, namespace, kind, name, direction string, follow bool, maxPods uint) ([]*capmeshv1.CaptureTarget, bool, error) {
	workloadRequested := namespace != "" || kind != "" || name != ""
	if workloadRequested && (namespace == "" || kind == "" || name == "") {
		return nil, false, errors.New("--namespace, --workload-kind, and --workload-name must be provided together")
	}
	if !workloadRequested && len(logicalInterfaces) == 0 {
		logicalInterfaces = []string{"A"}
	}
	var targets []*capmeshv1.CaptureTarget
	for index, logicalInterface := range logicalInterfaces {
		interfaceAlias, err := interfacealias.Normalize(logicalInterface)
		if err != nil {
			return nil, false, err
		}
		targets = append(targets, &capmeshv1.CaptureTarget{Id: fmt.Sprintf("interface-%d", index+1), InterfaceTarget: &capmeshv1.InterfaceTarget{Nodes: splitNodes(nodes), LogicalInterface: interfaceAlias}})
	}
	if workloadRequested {
		targets = append(targets, &capmeshv1.CaptureTarget{Id: "workload-1", WorkloadTarget: &capmeshv1.WorkloadTarget{Namespace: namespace, Kind: kind, Name: name, Direction: direction, Follow: follow, MaxPods: uint32(maxPods)}})
	}
	return targets, workloadRequested, nil
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func printContinuousCapture(capture *capmeshv1.ContinuousCapture) {
	fmt.Printf("session_id=%s status=%s retained_size=%d segments=%d message=%q\n", capture.GetSessionId(), capture.GetStatus(), capture.GetRetainedSize(), capture.GetSegmentCount(), capture.GetMessage())
}

func splitNodes(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	var nodes []string
	for _, node := range strings.Split(value, ",") {
		if node = strings.TrimSpace(node); node != "" {
			nodes = append(nodes, node)
		}
	}
	return nodes
}
