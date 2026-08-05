// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"hivenet_router/internal/domain"
)

// InfinityEngine implements Engine for Infinity inference servers.
// Infinity serves embedding models (POST /v1/embeddings) and reranking
// models (POST /v1/rerank) with an OpenAI-compatible API.
//
// Health check:     GET /health
// Model discovery:  GET /v1/models
// Chat completions: not supported — use --capability embedding or reranker
type InfinityEngine struct{}

func (e *InfinityEngine) Name() string { return "infinity" }

func (e *InfinityEngine) WaitForReady(_ context.Context, baseURL string, httpClient *http.Client) error {
	resp, err := httpClient.Get(baseURL + "/health")
	if err != nil {
		return fmt.Errorf("cannot reach Infinity server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("infinity health check failed: status %d", resp.StatusCode)
	}
	return nil
}

func (e *InfinityEngine) DiscoverModels(_ context.Context, baseURL string, httpClient *http.Client) ([]string, error) {
	resp, err := httpClient.Get(baseURL + "/v1/models")
	if err != nil {
		return nil, fmt.Errorf("failed to query Infinity models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("infinity models endpoint returned status %d", resp.StatusCode)
	}

	var modelsResp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return nil, fmt.Errorf("failed to decode Infinity models response: %w", err)
	}
	if len(modelsResp.Data) == 0 {
		return nil, fmt.Errorf("no models found on Infinity instance")
	}

	models := make([]string, len(modelsResp.Data))
	for i, m := range modelsResp.Data {
		models[i] = m.ID
	}
	return models, nil
}

// ForwardChat is not supported by Infinity — it serves embeddings and reranking,
// not chat completions. The /v1/chat/completions P2P handler is still registered
// (all agents share the same mux), but calls are rejected here with a structured
// error so the router receives a clear message instead of a generic backend failure.
func (e *InfinityEngine) ForwardChat(_ context.Context, _ string, _ *http.Client, _ string, _ domain.ChatRequest, _ ...EngineOption) ([]byte, http.Header, error) {
	return nil, nil, domain.NewRouterError(
		domain.ErrCodeRequestInvalid,
		"the Infinity engine does not support chat completions — restart the agent with --capability embedding or --capability reranker",
		domain.SourceRouter,
	)
}
