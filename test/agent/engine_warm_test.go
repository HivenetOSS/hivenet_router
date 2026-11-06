// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hivenet_router/internal/agent"
)

// TestWarm_LoadsTheModel checks that Ollama and SGLang warm their model with
// a one-token chat completion, and report a backend that cannot answer yet.
func TestWarm_LoadsTheModel(t *testing.T) {
	for _, engine := range []agent.Engine{&agent.OllamaEngine{}, &agent.SGLangEngine{}} {
		t.Run(engine.Name(), func(t *testing.T) {
			w, ok := engine.(agent.Warmer)
			if !ok {
				t.Fatal("engine does not warm its model")
			}
			var got struct {
				Model     string `json:"model"`
				MaxTokens int    `json:"max_tokens"`
			}
			status := http.StatusInternalServerError // still loading
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
					t.Errorf("warm-up sent %s %s", r.Method, r.URL.Path)
				}
				_ = json.NewDecoder(r.Body).Decode(&got)
				rw.WriteHeader(status)
			}))
			defer srv.Close()

			if err := w.Warm(context.Background(), srv.URL, srv.Client(), "m"); err == nil {
				t.Error("a backend answering 500 was reported warm")
			}
			status = http.StatusOK
			if err := w.Warm(context.Background(), srv.URL, srv.Client(), "m"); err != nil {
				t.Errorf("Warm: %v", err)
			}
			if got.Model != "m" || got.MaxTokens != 1 {
				t.Errorf("warm-up request for %q with max_tokens %d, want m and 1", got.Model, got.MaxTokens)
			}
		})
	}
}
