package kubernetes

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

type WorkloadResolver struct {
	once    sync.Once
	client  *http.Client
	baseURL string
	token   string
	err     error
}

type apiResponseError struct {
	Path       string
	StatusCode int
	Status     string
}

func (e *apiResponseError) Error() string {
	return fmt.Sprintf("Kubernetes API %s returned %s", e.Path, e.Status)
}

func NewWorkloadResolver() *WorkloadResolver {
	return &WorkloadResolver{}
}

type objectMeta struct {
	Name              string `json:"name"`
	UID               string `json:"uid"`
	DeletionTimestamp string `json:"deletionTimestamp"`
	OwnerReferences   []struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"ownerReferences"`
}

type labelSelector struct {
	MatchLabels      map[string]string `json:"matchLabels"`
	MatchExpressions []struct {
		Key      string   `json:"key"`
		Operator string   `json:"operator"`
		Values   []string `json:"values"`
	} `json:"matchExpressions"`
}

type workloadObject struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		Selector labelSelector `json:"selector"`
	} `json:"spec"`
}

type podList struct {
	Items []struct {
		Metadata objectMeta `json:"metadata"`
		Spec     struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
		Status struct {
			PodIP string `json:"podIP"`
		} `json:"status"`
	} `json:"items"`
}

type replicaSetList struct {
	Items []struct {
		Metadata objectMeta `json:"metadata"`
	} `json:"items"`
}

type workloadEndpointList struct {
	Items []struct {
		Spec struct {
			Node          string   `json:"node"`
			Pod           string   `json:"pod"`
			InterfaceName string   `json:"interfaceName"`
			IPNetworks    []string `json:"ipNetworks"`
		} `json:"spec"`
	} `json:"items"`
}

func (r *WorkloadResolver) Resolve(ctx context.Context, target domain.WorkloadTarget, targetID string) ([]domain.CaptureSource, error) {
	if err := r.initialize(); err != nil {
		return nil, err
	}
	namespace := url.PathEscape(target.Namespace)
	resource := "deployments"
	if target.Kind == "statefulset" {
		resource = "statefulsets"
	}
	var workload workloadObject
	if err := r.get(ctx, "/apis/apps/v1/namespaces/"+namespace+"/"+resource+"/"+url.PathEscape(target.Name), nil, &workload); err != nil {
		return nil, err
	}
	selector, err := selectorString(workload.Spec.Selector)
	if err != nil {
		return nil, err
	}
	query := url.Values{"labelSelector": []string{selector}}
	var pods podList
	if err := r.get(ctx, "/api/v1/namespaces/"+namespace+"/pods", query, &pods); err != nil {
		return nil, err
	}

	owners := map[string]struct{}{workload.Metadata.UID: {}}
	ownerKind := "StatefulSet"
	if target.Kind == "deployment" {
		ownerKind = "ReplicaSet"
		owners = make(map[string]struct{})
		var replicas replicaSetList
		if err := r.get(ctx, "/apis/apps/v1/namespaces/"+namespace+"/replicasets", query, &replicas); err != nil {
			return nil, err
		}
		for _, replica := range replicas.Items {
			if ownedBy(replica.Metadata, "Deployment", workload.Metadata.UID) {
				owners[replica.Metadata.UID] = struct{}{}
			}
		}
	}

	endpoints, err := r.listWorkloadEndpoints(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("list Calico workload endpoints: %w", err)
	}
	var sources []domain.CaptureSource
	for _, pod := range pods.Items {
		if pod.Metadata.DeletionTimestamp != "" || pod.Spec.NodeName == "" || pod.Status.PodIP == "" || !ownedByAny(pod.Metadata, ownerKind, owners) {
			continue
		}
		iface := endpointInterface(endpoints, pod.Metadata.Name, pod.Spec.NodeName, pod.Status.PodIP)
		sources = append(sources, domain.CaptureSource{
			ID:            "pod:" + targetID + ":" + pod.Metadata.UID,
			TargetID:      targetID,
			TargetType:    "workload",
			NodeName:      pod.Spec.NodeName,
			InterfaceName: iface,
			Namespace:     target.Namespace,
			PodName:       pod.Metadata.Name,
			PodUID:        pod.Metadata.UID,
			PodIP:         pod.Status.PodIP,
		})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })
	return sources, nil
}

func (r *WorkloadResolver) listWorkloadEndpoints(ctx context.Context, namespace string) (workloadEndpointList, error) {
	paths := []string{
		"/apis/crd.projectcalico.org/v1/namespaces/" + namespace + "/workloadendpoints",
		"/apis/projectcalico.org/v3/namespaces/" + namespace + "/workloadendpoints",
	}
	for _, path := range paths {
		var endpoints workloadEndpointList
		err := r.get(ctx, path, nil, &endpoints)
		if err == nil {
			return endpoints, nil
		}
		if !isAPIStatus(err, http.StatusNotFound) {
			return workloadEndpointList{}, err
		}
	}
	// WorkloadEndpoint is optional. The agent resolves an empty interface name
	// on the Pod's node with `ip route get <pod-ip>`.
	return workloadEndpointList{}, nil
}

func isAPIStatus(err error, statusCode int) bool {
	var responseError *apiResponseError
	return errors.As(err, &responseError) && responseError.StatusCode == statusCode
}

func (r *WorkloadResolver) initialize() error {
	r.once.Do(func() {
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
		if port == "" {
			port = "443"
		}
		if host == "" {
			r.err = fmt.Errorf("KUBERNETES_SERVICE_HOST is not set")
			return
		}
		token, err := os.ReadFile(filepath.Join(serviceAccountPath, "token"))
		if err != nil {
			r.err = fmt.Errorf("read service account token: %w", err)
			return
		}
		caPEM, err := os.ReadFile(filepath.Join(serviceAccountPath, "ca.crt"))
		if err != nil {
			r.err = fmt.Errorf("read Kubernetes CA: %w", err)
			return
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(caPEM) {
			r.err = fmt.Errorf("Kubernetes CA contains no certificates")
			return
		}
		r.client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, Timeout: 15 * time.Second}
		r.baseURL = "https://" + host + ":" + port
		r.token = strings.TrimSpace(string(token))
	})
	return r.err
}

func (r *WorkloadResolver) get(ctx context.Context, path string, query url.Values, output any) error {
	endpoint := r.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+r.token)
	response, err := r.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &apiResponseError{Path: path, StatusCode: response.StatusCode, Status: response.Status}
	}
	if err := json.NewDecoder(response.Body).Decode(output); err != nil {
		return fmt.Errorf("decode Kubernetes API response: %w", err)
	}
	return nil
}

func selectorString(selector labelSelector) (string, error) {
	var parts []string
	for key, value := range selector.MatchLabels {
		parts = append(parts, key+"="+value)
	}
	for _, expression := range selector.MatchExpressions {
		values := strings.Join(expression.Values, ",")
		switch expression.Operator {
		case "In":
			parts = append(parts, expression.Key+" in ("+values+")")
		case "NotIn":
			parts = append(parts, expression.Key+" notin ("+values+")")
		case "Exists":
			parts = append(parts, expression.Key)
		case "DoesNotExist":
			parts = append(parts, "!"+expression.Key)
		default:
			return "", fmt.Errorf("unsupported label selector operator %q", expression.Operator)
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("workload selector is empty")
	}
	sort.Strings(parts)
	return strings.Join(parts, ","), nil
}

func ownedBy(metadata objectMeta, kind, uid string) bool {
	for _, owner := range metadata.OwnerReferences {
		if owner.Kind == kind && owner.UID == uid {
			return true
		}
	}
	return false
}

func ownedByAny(metadata objectMeta, kind string, uids map[string]struct{}) bool {
	for _, owner := range metadata.OwnerReferences {
		if owner.Kind == kind {
			if _, exists := uids[owner.UID]; exists {
				return true
			}
		}
	}
	return false
}

func endpointInterface(endpoints workloadEndpointList, pod, node, podIP string) string {
	for _, endpoint := range endpoints.Items {
		if endpoint.Spec.Pod != pod || endpoint.Spec.Node != node {
			continue
		}
		for _, network := range endpoint.Spec.IPNetworks {
			if strings.TrimSpace(strings.SplitN(network, "/", 2)[0]) == podIP {
				return endpoint.Spec.InterfaceName
			}
		}
	}
	return ""
}
