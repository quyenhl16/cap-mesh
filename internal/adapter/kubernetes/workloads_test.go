package kubernetes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

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
