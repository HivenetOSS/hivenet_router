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
