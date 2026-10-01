package kubernetes

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

func (r *WorkloadResolver) StreamLogs(ctx context.Context, input domain.PodLogRequest) (io.ReadCloser, error) {
	if err := r.initialize(); err != nil {
		return nil, err
	}
	query := url.Values{
		"container":  []string{input.Container},
		"follow":     []string{"true"},
		"timestamps": []string{"false"},
	}
	if !input.SinceTime.IsZero() {
		query.Set("sinceTime", input.SinceTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
	}
	path := "/api/v1/namespaces/" + url.PathEscape(input.Namespace) + "/pods/" + url.PathEscape(input.PodName) + "/log"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+path+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	token, err := r.authorizationToken()
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := r.streamClient
	if client == nil {
		client = r.client
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		detail := strings.TrimSpace(string(body))
		if detail != "" {
			return nil, fmt.Errorf("Kubernetes API %s returned %s: %s", path, response.Status, detail)
		}
		return nil, &apiResponseError{Path: path, StatusCode: response.StatusCode, Status: response.Status}
	}
	return response.Body, nil
}
