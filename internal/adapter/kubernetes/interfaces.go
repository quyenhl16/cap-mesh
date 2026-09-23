package kubernetes

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount"

func InterfaceAnnotations(ctx context.Context, nodeName string) (map[string]string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if host == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST is not set")
	}
	token, err := os.ReadFile(filepath.Join(serviceAccountPath, "token"))
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(serviceAccountPath, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("read Kubernetes CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("Kubernetes CA contains no certificates")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, Timeout: 10 * time.Second}
	endpoint := "https://" + host + ":" + port + "/api/v1/nodes/" + url.PathEscape(nodeName)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("get node metadata: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get node metadata: Kubernetes API returned %s", response.Status)
	}
	var node struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(response.Body).Decode(&node); err != nil {
		return nil, fmt.Errorf("decode node metadata: %w", err)
	}
	result := make(map[string]string)
	for logical, suffix := range map[string]string{"A": "a", "B": "b", "C": "c"} {
		if value := strings.TrimSpace(node.Metadata.Annotations["capture.capmesh.io/interface-"+suffix]); value != "" {
			result[logical] = value
		}
	}
	return result, nil
}
