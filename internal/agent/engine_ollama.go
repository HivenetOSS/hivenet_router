// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"hivenet_router/internal/domain"
)

// OllamaEngine implements Engine for Ollama inference servers.
//
// Health check: GET /api/tags (Ollama has no /health endpoint)
// Model discovery: GET /api/tags → parse model names
// Request forwarding: POST /v1/chat/completions (OpenAI-compatible endpoint)
//
// The OpenAI-compatible endpoint requires "Authorization: Bearer ollama" header.
type OllamaEngine struct{}

func (e *OllamaEngine) Name() string { return "ollama" }

func (e *OllamaEngine) WaitForReady(_ context.Context, baseURL string, httpClient *http.Client) error {
	resp, err := httpClient.Get(baseURL + "/api/tags")
	if err != nil {
		return fmt.Errorf("cannot reach Ollama server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama health check failed: status %d", resp.StatusCode)
	}
	return nil
}

// ollamaTagsResponse represents the JSON response from Ollama's /api/tags endpoint.
type ollamaTagsResponse struct {
	Models []struct {
		Name string `json:"name"`
	} `json:"models"`
}

func (e *OllamaEngine) DiscoverModels(_ context.Context, baseURL string, httpClient *http.Client) ([]string, error) {
	resp, err := httpClient.Get(baseURL + "/api/tags")
	if err != nil {
		return nil, fmt.Errorf("failed to query Ollama models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama tags endpoint returned status %d", resp.StatusCode)
	}

	var tagsResp ollamaTagsResponse
	if err := json.NewDecoder(resp.Body).Decode(&tagsResp); err != nil {
		return nil, fmt.Errorf("failed to decode Ollama tags response: %w", err)
	}

	if len(tagsResp.Models) == 0 {
		return nil, fmt.Errorf("no models found on Ollama instance")
	}

	models := make([]string, len(tagsResp.Models))
	for i, m := range tagsResp.Models {
		// Strip the implicit :latest suffix so Ollama model names align with
		// vLLM names. Ollama always appends :latest when no tag is specified,
		// so "openai/gpt-oss-20b:latest" and "openai/gpt-oss-20b" are the
		// same model. Explicit non-latest tags (e.g. :v2) are preserved.
		models[i] = strings.TrimSuffix(m.Name, ":latest")
	}
	return models, nil
}

func (e *OllamaEngine) ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error) {
	// Ollama requires an Authorization header. Inject the default value only if
	// the client has not already provided one (e.g. a real token for a secured instance).
	if req.HttpHeaders != nil && req.HttpHeaders.Get("Authorization") == "" {
		req.HttpHeaders.Set("Authorization", "Bearer ollama")
	}
	return forwardOpenAIChatCompletion(ctx, baseURL, httpClient, model, req.RawBytes, e.Name(), options...)
}
