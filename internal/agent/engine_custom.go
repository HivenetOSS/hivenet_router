// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"fmt"
	"net/http"

	"hivenet_router/internal/domain"
)

// CustomEngine implements Engine for any OpenAI-compatible inference server.
//
// Unlike VLLMEngine and OllamaEngine, CustomEngine does not auto-detect the
// model name — the model must be provided explicitly via --model.
//
// Health check: GET <HealthURL> (configured via --health-url)
// Model discovery: not supported — always uses the model set via --model
// Request forwarding: POST /v1/chat/completions (standard OpenAI-compatible)
//
// Typical targets: LM Studio, llama.cpp server, LocalAI, TGI, TEI, or any
// proxy that exposes an OpenAI-compatible API.
type CustomEngine struct {
	HealthURL string // explicit health endpoint, e.g. http://localhost:1234/health
}

func (e *CustomEngine) Name() string { return "custom" }

func (e *CustomEngine) WaitForReady(_ context.Context, _ string, httpClient *http.Client) error {
	resp, err := httpClient.Get(e.HealthURL)
	if err != nil {
		return fmt.Errorf("cannot reach custom backend at %s: %v", e.HealthURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("custom backend health check failed: status %d", resp.StatusCode)
	}
	return nil
}

// DiscoverModels is not supported for the custom engine — the model must be
// specified via --model. This method should never be called because waitForBackend
// skips discovery when cfg.Model is already set.
func (e *CustomEngine) DiscoverModels(_ context.Context, _ string, _ *http.Client) ([]string, error) {
	return nil, fmt.Errorf("custom engine does not support model auto-discovery: use --model to specify the model name")
}

func (e *CustomEngine) ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error) {
	return forwardOpenAIChatCompletion(ctx, baseURL, httpClient, model, req.RawBytes, e.Name(), options...)
}
