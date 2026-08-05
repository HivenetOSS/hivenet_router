// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"hivenet_router/internal/agent"
)

func TestOllama_NameAndReady(t *testing.T) {
	e := &agent.OllamaEngine{}
	if e.Name() != "ollama" {
		t.Errorf("Name = %q, want ollama", e.Name())
	}
	// Ollama exposes /api/tags (not /health); a 200 there — even with no models
	// loaded — is the readiness signal, so an empty list still counts as ready.
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tags" {
			io.WriteString(w, `{"models":[]}`)
		}
	})
	if err := e.WaitForReady(context.Background(), url, cl); err != nil {
		t.Errorf("WaitForReady on 200 /api/tags: %v", err)
	}
}

// TestOllama_DiscoverModels also locks in the implicit ":latest" suffix stripping
// (so Ollama names align with vLLM), while explicit tags are preserved.
func TestOllama_DiscoverModels(t *testing.T) {
	// Two models: one tagged ":latest" (the implicit default tag) and one with an
	// explicit ":v2" tag — the discovery logic normalizes the former, keeps the latter.
	url, cl := vllmBackend(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"models":[{"name":"llama3:latest"},{"name":"qwen:v2"}]}`)
	})
	models, err := (&agent.OllamaEngine{}).DiscoverModels(context.Background(), url, cl)
	if err != nil {
		t.Fatalf("DiscoverModels: %v", err)
	}
	if len(models) != 2 || models[0] != "llama3" || models[1] != "qwen:v2" {
		t.Errorf("models = %v, want [llama3 qwen:v2] (:latest stripped, :v2 kept)", models)
	}
}
