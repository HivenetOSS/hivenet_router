// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package grpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "hivenet_router/proto"
)

// Client wraps a gRPC connection to the router's AuthService
type Client struct {
	conn *grpc.ClientConn
	auth pb.AuthServiceClient
}

// NewClient creates a new gRPC client connection to the router.
// expectedServerKey is the ED25519 public key the router's TLS certificate must
// present — derived from the shared JWT secret via auth.DeriveGRPCCredentials.
// The connection is encrypted (TLS 1.3) and the server identity is pinned:
// a mismatched key (wrong JWT secret on either side, via env or --jwt-secret-file) causes an immediate error.
func NewClient(addr string, expectedServerKey ed25519.PublicKey) (*Client, error) {
	if len(expectedServerKey) == 0 {
		return nil, fmt.Errorf("expectedServerKey must not be empty — derive it from the shared JWT secret via auth.DeriveGRPCCredentials")
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		// InsecureSkipVerify disables hostname/chain validation only.
		// The actual identity check is performed by VerifyPeerCertificate below,
		// which pins the expected ED25519 public key derived from the shared secret.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("gRPC server presented no certificate")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse server certificate: %w", err)
			}
			serverKey, ok := cert.PublicKey.(ed25519.PublicKey)
			if !ok {
				return fmt.Errorf("gRPC server certificate has unexpected key type (expected ED25519)")
			}
			if !bytes.Equal(serverKey, expectedServerKey) {
				return fmt.Errorf("gRPC server key mismatch — check that router and agent use the same JWT secret (env or --jwt-secret-file)")
			}
			return nil
		},
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	return &Client{
		conn: conn,
		auth: pb.NewAuthServiceClient(conn),
	}, nil
}

// Authenticate sends an authentication request to the router.
func (c *Client) Authenticate(ctx context.Context, credentials string, metadata *pb.AgentMetadata) (*pb.AuthResponse, error) {
	return c.auth.Authenticate(ctx, &pb.AuthRequest{
		Credentials: credentials,
		Metadata:    metadata,
	})
}

// AuthenticateWithTimeout is a convenience method that creates a context with timeout
func (c *Client) AuthenticateWithTimeout(credentials string, metadata *pb.AgentMetadata, timeout time.Duration) (*pb.AuthResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Authenticate(ctx, credentials, metadata)
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	return c.conn.Close()
}
