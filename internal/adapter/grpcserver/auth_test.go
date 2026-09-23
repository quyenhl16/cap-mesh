package grpcserver

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRoleTokens(t *testing.T) {
	config := AuthConfig{AdminToken: "admin", ViewerToken: "viewer", AgentToken: "agent"}
	tests := []struct {
		method string
		token  string
		code   codes.Code
	}{
		{"/capmesh.v1.CaptureService/CreateSession", "admin", codes.OK},
		{"/capmesh.v1.CaptureService/CreateSession", "viewer", codes.PermissionDenied},
		{"/capmesh.v1.CaptureService/StreamPackets", "viewer", codes.OK},
		{"/capmesh.v1.AgentService/Connect", "agent", codes.OK},
		{"/capmesh.v1.AgentService/Connect", "admin", codes.PermissionDenied},
	}
	for _, test := range tests {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+test.token))
		err := authorize(ctx, config.tokensForMethod(test.method), config.enabled())
		if status.Code(err) != test.code {
			t.Errorf("method %s token %s: got %s, want %s", test.method, test.token, status.Code(err), test.code)
		}
	}
}

func TestSharedTokenCanAccessEveryRole(t *testing.T) {
	config := AuthConfig{SharedToken: "shared", AdminToken: "admin", ViewerToken: "viewer", AgentToken: "agent"}
	for _, method := range []string{"/capmesh.v1.CaptureService/CreateSession", "/capmesh.v1.CaptureService/StreamPackets", "/capmesh.v1.AgentService/Connect"} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer shared"))
		if err := authorize(ctx, config.tokensForMethod(method), config.enabled()); err != nil {
			t.Errorf("shared token rejected for %s: %v", method, err)
		}
	}
}
