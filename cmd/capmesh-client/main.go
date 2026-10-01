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
	"sort"
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
	logStart := flag.Bool("log-start", false, "start the singleton server-owned workload log capture and exit")
	logStop := flag.Bool("log-stop", false, "stop the singleton workload log capture and exit")
	logStatus := flag.Bool("log-status", false, "show singleton workload log capture status and exit")
	listAgents := flag.Bool("list-agents", false, "list connected agents and their interface mappings")
	listSessions := flag.Bool("list-sessions", false, "list running normal capture sessions")
	nodes := flag.String("nodes", "", "comma-separated node names for a new session")
	var logicalInterfaces stringList
	flag.Var(&logicalInterfaces, "interface", "logical interface alias configured on the agents; repeat to capture multiple interfaces")
	workloadNamespace := flag.String("namespace", "", "namespace of the workload capture target")
	workloadKind := flag.String("workload-kind", "", "workload kind: statefulset or deployment")
	workloadName := flag.String("workload-name", "", "name of the StatefulSet or Deployment")
	direction := flag.String("direction", "egress", "workload traffic direction: egress, ingress, or both")
	follow := flag.Bool("follow", true, "follow workload scale, restart, and reschedule changes")
	maxPods := flag.Uint("max-pods", 100, "maximum pods allowed for a workload target")
	var logWorkloads stringList
	flag.Var(&logWorkloads, "log-workload", "workload to capture logs from as namespace/statefulset|deployment/name; repeat for multiple workloads")
	var logContainers stringList
	flag.Var(&logContainers, "log-container", "container name to capture for every log workload; repeat for multiple containers; empty captures all regular containers")
	logSince := flag.Duration("log-since", 0, "include workload logs this far before start; 0 captures only new logs")
	filter := flag.String("filter", "", "BPF capture filter")
	snaplen := flag.Uint("snaplen", 4096, "packet snapshot length")
	ttl := flag.Duration("ttl", 5*time.Minute, "capture session lifetime")
	reorderWindow := flag.Duration("reorder-window", 300*time.Millisecond, "packet reorder window")
	token := flag.String("token", os.Getenv("CAPMESH_TOKEN"), "shared bearer token")
	insecureTransport := flag.Bool("insecure", false, "disable TLS (local development only)")
	caFile := flag.String("tls-ca", "", "server CA certificate")
	serverName := flag.String("tls-server-name", "", "TLS server name override")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	controlModes := boolCount(*continuousStart, *continuousStop, *continuousStatus, *logStart, *logStop, *logStatus, *listAgents, *listSessions)
	if controlModes > 1 || (controlModes > 0 && (*create || *sessionID != "")) {
		logger.Error("capture control, log control, list, --create, and --session modes are mutually exclusive")
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
	if *logStop {
		capture, err := client.StopWorkloadLogCapture(authContext, &capmeshv1.StopWorkloadLogCaptureRequest{})
		if err != nil {
			logger.Error("stop workload log capture failed", "error", err)
			os.Exit(1)
		}
		printWorkloadLogCapture(capture)
		return
	}
	if *logStatus {
		capture, err := client.GetWorkloadLogCapture(authContext, &capmeshv1.GetWorkloadLogCaptureRequest{})
		if err != nil {
			logger.Error("get workload log capture failed", "error", err)
			os.Exit(1)
		}
		printWorkloadLogCapture(capture)
		return
	}
	if *logStart {
		if *logSince < 0 {
			logger.Error("--log-since must not be negative")
			os.Exit(2)
		}
		targets, err := buildLogTargets(logWorkloads, logContainers, *maxPods)
		if err != nil {
			logger.Error("invalid workload log target", "error", err)
			os.Exit(2)
		}
		capture, err := client.StartWorkloadLogCapture(authContext, &capmeshv1.StartWorkloadLogCaptureRequest{Targets: targets, SinceSeconds: uint32(logSince.Seconds())})
		if err != nil {
			logger.Error("start workload log capture failed", "error", err)
			os.Exit(1)
		}
		printWorkloadLogCapture(capture)
		return
	}
	if *listAgents {
		response, err := client.ListAgents(authContext, &capmeshv1.ListAgentsRequest{})
		if err != nil {
			logger.Error("list agents failed", "error", err)
			os.Exit(1)
		}
		printAgents(response)
		return
	}
	if *listSessions {
		response, err := client.ListSessions(authContext, &capmeshv1.ListSessionsRequest{Status: "RUNNING", Mode: "NORMAL"})
		if err != nil {
			logger.Error("list sessions failed", "error", err)
			os.Exit(1)
		}
		printSessions(response)
		return
	}
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

func printWorkloadLogCapture(capture *capmeshv1.WorkloadLogCapture) {
	targets := make([]string, 0, len(capture.GetTargets()))
	for _, target := range capture.GetTargets() {
		value := target.GetNamespace() + "/" + target.GetKind() + "/" + target.GetName()
		if len(target.GetContainers()) > 0 {
			value += "[" + strings.Join(target.GetContainers(), ",") + "]"
		}
		targets = append(targets, value)
	}
	fmt.Printf("run_id=%s status=%s retained_size=%d segments=%d active_streams=%d started_at=%s targets=%q message=%q\n", capture.GetRunId(), capture.GetStatus(), capture.GetRetainedSize(), capture.GetSegmentCount(), capture.GetActiveStreams(), formatTimestamp(capture.GetStartedAtNs()), strings.Join(targets, ";"), capture.GetMessage())
}

func buildLogTargets(values, containers []string, maxPods uint) ([]*capmeshv1.WorkloadLogTarget, error) {
	if len(values) == 0 {
		return nil, errors.New("at least one --log-workload is required")
	}
	if maxPods == 0 {
		return nil, errors.New("--max-pods must be positive")
	}
	result := make([]*capmeshv1.WorkloadLogTarget, 0, len(values))
	seen := make(map[string]struct{})
	for _, value := range values {
		parts := strings.Split(value, "/")
		if len(parts) != 3 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[2]) == "" {
			return nil, fmt.Errorf("%q must use namespace/statefulset|deployment/name", value)
		}
		kind := strings.ToLower(strings.TrimSpace(parts[1]))
		if kind != "statefulset" && kind != "deployment" {
			return nil, fmt.Errorf("unsupported workload kind %q", parts[1])
		}
		key := strings.TrimSpace(parts[0]) + "/" + kind + "/" + strings.TrimSpace(parts[2])
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate workload %q", key)
		}
		seen[key] = struct{}{}
		result = append(result, &capmeshv1.WorkloadLogTarget{Namespace: strings.TrimSpace(parts[0]), Kind: kind, Name: strings.TrimSpace(parts[2]), Containers: append([]string(nil), containers...), MaxPods: uint32(maxPods)})
	}
	return result, nil
}

func printAgents(response *capmeshv1.ListAgentsResponse) {
	fmt.Printf("agents=%d\n", len(response.GetAgents()))
	for _, agent := range response.GetAgents() {
		aliases := make([]string, 0, len(agent.GetInterfaces()))
		for alias := range agent.GetInterfaces() {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)
		mappings := make([]string, 0, len(aliases))
		for _, alias := range aliases {
			mappings = append(mappings, alias+"="+agent.GetInterfaces()[alias])
		}
		fmt.Printf("node=%s status=%s connected_at=%s last_seen_at=%s interfaces=%q\n",
			agent.GetNodeName(), agent.GetStatus(), formatTimestamp(agent.GetConnectedAtNs()), formatTimestamp(agent.GetLastSeenAtNs()), strings.Join(mappings, ","))
	}
}

func formatTimestamp(timestamp int64) string {
	if timestamp == 0 {
		return "-"
	}
	return time.Unix(0, timestamp).UTC().Format(time.RFC3339)
}

func printSessions(response *capmeshv1.ListSessionsResponse) {
	fmt.Printf("sessions=%d\n", len(response.GetSessions()))
	for _, captureSession := range response.GetSessions() {
		fmt.Printf("session_id=%s mode=%s status=%s nodes=%q created_at=%s expires_at=%s targets=%q filter=%q message=%q\n",
			captureSession.GetId(), captureSession.GetMode(), captureSession.GetStatus(), strings.Join(captureSession.GetNodes(), ","),
			formatTimestamp(captureSession.GetCreatedAtNs()), formatTimestamp(captureSession.GetExpiresAtNs()),
			formatTargets(captureSession.GetTargets()), captureSession.GetFilter(), captureSession.GetMessage())
	}
}

func formatTargets(targets []*capmeshv1.CaptureTarget) string {
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		if interfaceTarget := target.GetInterfaceTarget(); interfaceTarget != nil {
			nodes := strings.Join(interfaceTarget.GetNodes(), ",")
			if nodes == "" {
				nodes = "*"
			}
			values = append(values, "interface:"+interfaceTarget.GetLogicalInterface()+"@"+nodes)
			continue
		}
		if workload := target.GetWorkloadTarget(); workload != nil {
			values = append(values, "workload:"+workload.GetNamespace()+"/"+workload.GetKind()+"/"+workload.GetName()+"("+workload.GetDirection()+")")
		}
	}
	return strings.Join(values, ";")
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
