// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"net/http"
	"testing"

	"hivenet_router/internal/agent"
)

// TestBackendHeader covers the credential swap the agent applies to every
// backend request (chat, streaming chat, and the generic proxy path).
func TestBackendHeader(t *testing.T) {
	tests := []struct {
		name      string
		src       http.Header
		apiKey    string
		wantAuth  string
		wantXKey  string
		wantTrace string
	}{
		{
			// No key configured: client headers pass through untouched.
			name:      "no key forwards client credentials",
			src:       http.Header{"Authorization": {"Bearer client"}, "X-Api-Key": {"client-x"}, "Traceparent": {"00-abc"}},
			wantAuth:  "Bearer client",
			wantXKey:  "client-x",
			wantTrace: "00-abc",
		},
		{
			// Key configured: Authorization replaced, x-api-key dropped, other headers kept.
			name:      "key replaces client credentials",
			src:       http.Header{"Authorization": {"Bearer client"}, "X-Api-Key": {"client-x"}, "Traceparent": {"00-abc"}},
			apiKey:    "sk-backend",
			wantAuth:  "Bearer sk-backend",
			wantTrace: "00-abc",
		},
		{
			// Key configured and client sent no credentials at all.
			name:     "key added when client sent none",
			src:      http.Header{},
			apiKey:   "sk-backend",
			wantAuth: "Bearer sk-backend",
		},
		{
			// Nil source must not panic.
			name:     "nil source",
			src:      nil,
			apiKey:   "sk-backend",
			wantAuth: "Bearer sk-backend",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var before string
			if tc.src != nil {
				before = tc.src.Get("Authorization")
			}
			got := agent.BackendHeader(tc.src, tc.apiKey)
			if g := got.Get("Authorization"); g != tc.wantAuth {
				t.Errorf("Authorization = %q, want %q", g, tc.wantAuth)
			}
			if g := got.Get("X-Api-Key"); g != tc.wantXKey {
				t.Errorf("X-Api-Key = %q, want %q", g, tc.wantXKey)
			}
			if g := got.Get("Traceparent"); g != tc.wantTrace {
				t.Errorf("Traceparent = %q, want %q", g, tc.wantTrace)
			}
			// The source header must never be mutated (the router reuses it).
			if tc.src != nil && tc.src.Get("Authorization") != before {
				t.Errorf("source header mutated")
			}
		})
	}
}
