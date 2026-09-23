package grpcserver

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthConfig struct {
	SharedToken string
	AdminToken  string
	ViewerToken string
	AgentToken  string
}

func UnaryAuth(config AuthConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorize(ctx, config.tokensForMethod(info.FullMethod), config.enabled()); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
}

func StreamAuth(config AuthConfig) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(stream.Context(), config.tokensForMethod(info.FullMethod), config.enabled()); err != nil {
			return err
		}
		return handler(server, stream)
	}
}

func (c AuthConfig) tokensForMethod(method string) []string {
	switch method {
	case "/capmesh.v1.AgentService/Connect":
		return []string{c.SharedToken, c.AgentToken}
	case "/capmesh.v1.CaptureService/CreateSession", "/capmesh.v1.CaptureService/StopSession":
		return []string{c.SharedToken, c.AdminToken}
	case "/capmesh.v1.CaptureService/GetSession", "/capmesh.v1.CaptureService/StreamPackets":
		return []string{c.SharedToken, c.AdminToken, c.ViewerToken}
	default:
		return []string{c.SharedToken, c.AdminToken}
	}
}

func (c AuthConfig) enabled() bool {
	return c.SharedToken != "" || c.AdminToken != "" || c.ViewerToken != "" || c.AgentToken != ""
}

func authorize(ctx context.Context, allowed []string, enabled bool) error {
	if !enabled {
		return nil
	}
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	actual := strings.TrimPrefix(values[0], "Bearer ")
	for _, expected := range allowed {
		if expected != "" && subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1 {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "token is not authorized for this operation")
}
