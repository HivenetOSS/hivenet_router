// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
)

const openAIBaseURL = "https://api.openai.com"

type openAIProvider struct {
	apiKey  string
	baseURL string
	client  *http.Client
	metrics *metrics.RouterMetrics
}

func newOpenAI(cfg Config) *openAIProvider {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 120 * time.Second
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = openAIBaseURL
	}
	return &openAIProvider{
		apiKey:  cfg.APIKey,
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
		metrics: cfg.Metrics,
	}
}

func (p *openAIProvider) Name() string { return "openai" }

func (p *openAIProvider) Complete(ctx context.Context, req *domain.ChatRequest, model string) (*domain.ChatResponse, error) {
	// Copy the request and override the model with the fallback model from policy.
	reqCopy := *req
	if req.Model != model {
		reqCopy.Model = model
	}

	body, err := json.Marshal(reqCopy)
	if err != nil {
		return nil, fmt.Errorf("openai: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	start := time.Now()
	resp, err := p.client.Do(httpReq)
	durationSec := time.Since(start).Seconds()
	if err != nil {
		if p.metrics != nil {
			p.metrics.ObserveHTTPClientRequest("openai", 0, durationSec)
		}
		return nil, fmt.Errorf("openai: HTTP call: %w", err)
	}
	defer resp.Body.Close()
	if p.metrics != nil {
		p.metrics.ObserveHTTPClientRequest("openai", resp.StatusCode, durationSec)
	}

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("openai: HTTP %d: %s", resp.StatusCode, string(errBody))
	}

	var chatResp domain.ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, fmt.Errorf("openai: decode response: %w", err)
	}

	if len(chatResp.Choices) == 0 {
		return nil, fmt.Errorf("openai: response contains no choices")
	}

	chatResp.ProcessedBy = "provider:openai"
	return &chatResp, nil
}
