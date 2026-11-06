// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
)

// Only a chat model is warmed: an embedding or rerank model has no chat
// endpoint, so a chat warm-up would keep it from ever registering.
func TestWaitForBackend_WarmsOnlyChatModels(t *testing.T) {
	for _, tc := range []struct {
		capability string
		wantWarm   bool
	}{
		{"", true},
		{domain.CapabilityLLM, true},
		{domain.CapabilityEmbedding, false},
		{domain.CapabilityReranker, false},
	} {
		t.Run("capability="+tc.capability, func(t *testing.T) {
			var chats atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/chat/completions" {
					chats.Add(1)
				}
				rw.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			a := &Agent{
				cfg:        &config.AgentConfig{BackendURL: srv.URL, Model: "m", Capability: tc.capability},
				engine:     &OllamaEngine{},
				httpClient: srv.Client(),
				stopCh:     make(chan struct{}),
			}
			model, err := a.waitForBackend()
			if err != nil || model != "m" {
				t.Fatalf("waitForBackend = %q, %v", model, err)
			}
			if got := chats.Load() > 0; got != tc.wantWarm {
				t.Errorf("warm-up sent: %v, want %v", got, tc.wantWarm)
			}
		})
	}
}
