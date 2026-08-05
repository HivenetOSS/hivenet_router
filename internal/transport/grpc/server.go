// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/transport/p2p"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "hivenet_router/proto"
)

var log = logging.Logger("grpc")

// AuthServer implements the gRPC AuthService defined in the protobuf.
// It is responsible for authenticating agents and bootstrapping
// their connection to the router (session + libp2p info).
type AuthServer struct {
	pb.UnimplementedAuthServiceServer // Forward compatibility with proto changes

	cfg            *config.Config       // Global router configuration
	jwtValidator   *auth.JWTValidator   // Validates agent JWT credentials
	sessionManager *auth.SessionManager // Manages short-lived agent sessions
	p2pHost        host.Host            // Router's libp2p host
}

// NewAuthServer creates a new authentication server
func NewAuthServer(
	cfg *config.Config,
	jwtValidator *auth.JWTValidator,
	sessionManager *auth.SessionManager,
	p2pHost host.Host,
) *AuthServer {
	return &AuthServer{
		cfg:            cfg,
		jwtValidator:   jwtValidator,
		sessionManager: sessionManager,
		p2pHost:        p2pHost,
	}
}

// Authenticate handles agent authentication requests over gRPC
// High-level flow:
// 1. Validate JWT credentials
// 2. Validate agent metadata
// 3. Create a session token
// 4. Return libp2p connection info and router configuration
func (s *AuthServer) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
	log.Debugf("gRPC Auth: Request from agent (model: %s)", req.Metadata.Model)

	// Validate JWT Token (On success, this returns the agent's unique identity)
	agentID, err := s.jwtValidator.ValidateToken(req.Credentials)
	if err != nil {
		log.Errorf("JWT validation failed: %v", err)
		return &pb.AuthResponse{
			Success: false,
			Message: fmt.Sprintf("Authentication failed: %v", err),
		}, nil
	}
	log.Debugf("JWT validated for agent: %s", agentID)

	// Validate Metadata
	if err := s.validateMetadata(req.Metadata); err != nil {
		return &pb.AuthResponse{
			Success: false,
			Message: fmt.Sprintf("Invalid metadata: %v", err),
		}, nil
	}

	// Generate Session Token (This token will later be verified over the libp2p connection.)
	sessionToken := s.sessionManager.CreateSession(agentID, req.Metadata)
	log.Debugf("Session created: %s... (expires in %v)", sessionToken[:min(len(sessionToken), 6)], s.cfg.SessionTTL)

	// Return authentication success along with:
	// - Session token
	// - Router libp2p connection details
	// - Router configuration parameters
	return &pb.AuthResponse{
		Success:      true,
		Message:      "Authentication successful",
		SessionToken: sessionToken,

		// libp2p bootstrap info used by the agent to connect back to the router
		Libp2PInfo: &pb.LibP2PInfo{
			PeerId:    s.p2pHost.ID().String(),
			Addresses: p2p.GetAddressesWithPeerID(s.p2pHost),
			Protocol:  s.cfg.ProtocolID,
		},

		// Router-side configuration hints for the agent
		Config: &pb.RouterConfig{
			HeartbeatInterval: int32(s.cfg.HeartbeatInterval.Seconds()),
			MaxRequestSize:    10 * 1024 * 1024,
			SessionTtl:        int32(s.cfg.SessionTTL.Seconds()),
		},
	}, nil
}

// validateMetadata ensures required fields are present and valid
func (s *AuthServer) validateMetadata(meta *pb.AgentMetadata) error {
	if meta.Model == "" {
		return fmt.Errorf("model is required")
	}
	if meta.Capacity <= 0 {
		return fmt.Errorf("capacity must be positive")
	}
	if meta.Version == "" {
		return fmt.Errorf("version is required")
	}
	if meta.Engine == "" {
		return fmt.Errorf("engine is required")
	}
	if meta.Region == "" {
		return fmt.Errorf("region is required")
	}
	if meta.Organization == "" {
		return fmt.Errorf("organization is required")
	}
	return nil
}

// StartServer starts the gRPC server with TLS enabled.
// The TLS certificate is derived from the JWT secret via DeriveGRPCCredentials,
// so no external cert files are required.
func StartServer(port string, authServer *AuthServer, tlsCert tls.Certificate) error {
	log.Infof("gRPC Auth server starting on %s (TLS)", port)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS13,
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	pb.RegisterAuthServiceServer(grpcServer, authServer)

	log.Info("gRPC Auth server ready")
	return grpcServer.Serve(lis)
}
