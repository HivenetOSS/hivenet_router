// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "hivenet_router/proto"
)

// An auth request without metadata must be refused, not crash the router.
func TestAuthenticateWithoutMetadataIsRefused(t *testing.T) {
	s := NewAuthServer(nil, nil, nil, nil)
	for _, req := range []*pb.AuthRequest{{}, {Credentials: "anything"}} {
		resp, err := s.Authenticate(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatal("a request without metadata must not succeed")
		}
	}
}

// A panic in a handler must become an Internal error instead of killing the process.
func TestRecoverUnaryTurnsPanicIntoError(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Panic"}
	_, err := recoverUnary(context.Background(), nil, info, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want codes.Internal", err)
	}
}

// An agent's country must be an ISO 3166-1 alpha-2 code; empty is allowed
// (served only to scope "all").
func TestValidateMetadataCountry(t *testing.T) {
	s := NewAuthServer(nil, nil, nil, nil)
	meta := func(cc string) *pb.AgentMetadata {
		return &pb.AgentMetadata{Model: "m", Version: "v", Engine: "vllm", Region: "r", Organization: "o", Country: cc}
	}
	for _, ok := range []string{"", "FR", "US"} {
		if err := s.validateMetadata(meta(ok)); err != nil {
			t.Errorf("country %q: %v", ok, err)
		}
	}
	for _, bad := range []string{"fr", "FRA", "F1"} {
		if err := s.validateMetadata(meta(bad)); err == nil {
			t.Errorf("country %q: accepted", bad)
		}
	}
}
