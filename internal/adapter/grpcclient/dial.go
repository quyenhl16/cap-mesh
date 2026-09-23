package grpcclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type DialConfig struct {
	Address    string
	Insecure   bool
	CAFile     string
	ServerName string
	Token      string
}

func Dial(config DialConfig) (*grpc.ClientConn, error) {
	var transport credentials.TransportCredentials
	if config.Insecure {
		transport = insecure.NewCredentials()
	} else {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.ServerName}
		if config.CAFile != "" {
			pem, err := os.ReadFile(config.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read CA file: %w", err)
			}
			pool, err := x509.SystemCertPool()
			if err != nil || pool == nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("CA file contains no certificates")
			}
			tlsConfig.RootCAs = pool
		}
		transport = credentials.NewTLS(tlsConfig)
	}
	return grpc.NewClient(config.Address, grpc.WithTransportCredentials(transport))
}

func AuthContext(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
