package kubernetes

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

func TestWorkloadResolverResolvesOwnedDeploymentPodAndCalicoInterface(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/apis/apps/v1/namespaces/payment/deployments/api":
			_, _ = writer.Write([]byte(`{"metadata":{"uid":"deployment-uid"},"spec":{"selector":{"matchLabels":{"app":"api"}}}}`))
		case "/apis/apps/v1/namespaces/payment/replicasets":
			_, _ = writer.Write([]byte(`{"items":[{"metadata":{"uid":"replicaset-uid","ownerReferences":[{"kind":"Deployment","uid":"deployment-uid"}]}}]}`))
		case "/api/v1/namespaces/payment/pods":
			_, _ = writer.Write([]byte(`{"items":[{"metadata":{"name":"api-0","uid":"pod-uid","ownerReferences":[{"kind":"ReplicaSet","uid":"replicaset-uid"}]},"spec":{"nodeName":"worker-1"},"status":{"podIP":"10.0.0.10"}},{"metadata":{"name":"other","uid":"other-uid","ownerReferences":[{"kind":"ReplicaSet","uid":"other-rs"}]},"spec":{"nodeName":"worker-1"},"status":{"podIP":"10.0.0.11"}}]}`))
		case "/apis/crd.projectcalico.org/v1/namespaces/payment/workloadendpoints":
			_, _ = writer.Write([]byte(`{"items":[{"spec":{"node":"worker-1","pod":"api-0","interfaceName":"cali123","ipNetworks":["10.0.0.10/32"]}}]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	resolver := &WorkloadResolver{client: server.Client(), baseURL: server.URL}
	resolver.once.Do(func() {})
	sources, err := resolver.Resolve(context.Background(), domain.WorkloadTarget{Namespace: "payment", Kind: "deployment", Name: "api"}, "target-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].InterfaceName != "cali123" || sources[0].PodName != "api-0" || sources[0].NodeName != "worker-1" {
		t.Fatalf("unexpected sources: %#v", sources)
	}
}

func TestStreamLogsUsesRawKubernetesPodLogEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/namespaces/payment/pods/app-0/log" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if request.URL.Query().Get("container") != "main" || request.URL.Query().Get("follow") != "true" || request.URL.Query().Get("timestamps") != "false" {
			t.Fatalf("query = %v", request.URL.Query())
		}
		if request.URL.Query().Get("sinceTime") == "" {
			t.Fatal("sinceTime is missing")
		}
		_, _ = writer.Write([]byte("raw log line\n"))
	}))
	defer server.Close()
	resolver := testResolver(server)
	resolver.streamClient = server.Client()
	body, err := resolver.StreamLogs(context.Background(), domain.PodLogRequest{Namespace: "payment", PodName: "app-0", Container: "main", SinceTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "raw log line\n" {
		t.Fatalf("body = %q", data)
	}
}

func TestWorkloadResolverFallsBackToNativeV3WorkloadEndpoints(t *testing.T) {
	server := newStatefulSetResolverServer(t, func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/apis/crd.projectcalico.org/v1/namespaces/payment/workloadendpoints":
			http.NotFound(writer, request)
		case "/apis/projectcalico.org/v3/namespaces/payment/workloadendpoints":
			_, _ = writer.Write([]byte(`{"items":[{"spec":{"node":"worker-1","pod":"worker-0","interfaceName":"cali-v3","ipNetworks":["10.0.0.20/32"]}}]}`))
		default:
			http.NotFound(writer, request)
		}
	})
	defer server.Close()

	sources, err := testResolver(server).Resolve(context.Background(), domain.WorkloadTarget{Namespace: "payment", Kind: "statefulset", Name: "worker"}, "target-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].InterfaceName != "cali-v3" {
		t.Fatalf("unexpected sources: %#v", sources)
	}
}

func TestWorkloadResolverUsesAgentRouteFallbackWhenCalicoAPIsAreMissing(t *testing.T) {
	server := newStatefulSetResolverServer(t, func(writer http.ResponseWriter, request *http.Request) {
		http.NotFound(writer, request)
	})
	defer server.Close()

	sources, err := testResolver(server).Resolve(context.Background(), domain.WorkloadTarget{Namespace: "payment", Kind: "statefulset", Name: "worker"}, "target-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].InterfaceName != "" || sources[0].PodIP != "10.0.0.20" || sources[0].NodeName != "worker-1" {
		t.Fatalf("unexpected sources for agent route fallback: %#v", sources)
	}
}

func TestWorkloadResolverReturnsContainersAndRestartCountsForLogs(t *testing.T) {
	server := newStatefulSetResolverServer(t, func(writer http.ResponseWriter, _ *http.Request) { http.NotFound(writer, nil) })
	defer server.Close()
	pods, err := testResolver(server).ResolvePods(context.Background(), domain.WorkloadLogTarget{Namespace: "payment", Kind: "statefulset", Name: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 1 || len(pods[0].Containers) != 1 || pods[0].Containers[0] != "main" || pods[0].RestartCount["main"] != 2 {
		t.Fatalf("unexpected pods: %#v", pods)
	}
}

func TestWorkloadResolverDoesNotHideCalicoAuthorizationErrors(t *testing.T) {
	server := newStatefulSetResolverServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "forbidden", http.StatusForbidden)
	})
	defer server.Close()

	_, err := testResolver(server).Resolve(context.Background(), domain.WorkloadTarget{Namespace: "payment", Kind: "statefulset", Name: "worker"}, "target-1")
	if err == nil || !strings.Contains(err.Error(), "403 Forbidden") {
		t.Fatalf("error = %v, want Calico API authorization error", err)
	}
}

func newStatefulSetResolverServer(t *testing.T, calicoHandler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/apis/apps/v1/namespaces/payment/statefulsets/worker":
			_, _ = writer.Write([]byte(`{"metadata":{"uid":"statefulset-uid"},"spec":{"selector":{"matchLabels":{"app":"worker"}}}}`))
		case "/api/v1/namespaces/payment/pods":
			_, _ = writer.Write([]byte(`{"items":[{"metadata":{"name":"worker-0","uid":"pod-uid","ownerReferences":[{"kind":"StatefulSet","uid":"statefulset-uid"}]},"spec":{"nodeName":"worker-1","containers":[{"name":"main"}]},"status":{"podIP":"10.0.0.20","containerStatuses":[{"name":"main","restartCount":2}]}}]}`))
		default:
			calicoHandler(writer, request)
		}
	}))
}

func testResolver(server *httptest.Server) *WorkloadResolver {
	resolver := &WorkloadResolver{client: server.Client(), baseURL: server.URL}
	resolver.once.Do(func() {})
	return resolver
}

func TestSelectorStringSupportsMatchExpressions(t *testing.T) {
	selector := labelSelector{MatchLabels: map[string]string{"app": "api"}}
	selector.MatchExpressions = append(selector.MatchExpressions, struct {
		Key      string   `json:"key"`
		Operator string   `json:"operator"`
		Values   []string `json:"values"`
	}{Key: "tier", Operator: "In", Values: []string{"backend", "worker"}})
	value, err := selectorString(selector)
	if err != nil {
		t.Fatal(err)
	}
	if value != "app=api,tier in (backend,worker)" {
		t.Fatalf("selector = %q", value)
	}
}
