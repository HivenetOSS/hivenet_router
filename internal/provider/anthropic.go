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

const (
	anthropicBaseURL = "https://api.anthropic.com"
	anthropicVersion = "2023-06-01"
	// defaultMaxTokens is used when the original request did not specify max_tokens.
	defaultMaxTokens = 4096
)

type anthropicProvider struct {
	apiKey  string
	baseURL string
	client  *http.Client
	metrics *metrics.RouterMetrics
}

func newAnthropic(cfg Config) *anthropicProvider {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 120 * time.Second
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = anthropicBaseURL
	}
	return &anthropicProvider{
		apiKey:  cfg.APIKey,
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
		metrics: cfg.Metrics,
	}
}

func (p *anthropicProvider) Name() string { return "anthropic" }

// anthropicRequest is the Anthropic Messages API wire format.
type anthropicRequest struct {
	Model       string             `json:"model"`
	MaxTokens   int                `json:"max_tokens"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
	Temperature float64            `json:"temperature,omitempty"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// anthropicResponse is the Anthropic Messages API response wire format.
type anthropicResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string `json:"stop_reason"`
	Usage      struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

func (p *anthropicProvider) Complete(ctx context.Context, req *domain.ChatRequest, model string) (*domain.ChatResponse, error) {
	anthropicReq, err := p.translateRequest(req, model)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(anthropicReq)
	if err != nil {
		return nil, fmt.Errorf("anthropic: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	start := time.Now()
	resp, err := p.client.Do(httpReq)
	durationSec := time.Since(start).Seconds()
	if err != nil {
		if p.metrics != nil {
			p.metrics.ObserveHTTPClientRequest("anthropic", 0, durationSec)
		}
		return nil, fmt.Errorf("anthropic: HTTP call: %w", err)
	}
	defer resp.Body.Close()
	if p.metrics != nil {
		p.metrics.ObserveHTTPClientRequest("anthropic", resp.StatusCode, durationSec)
	}

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("anthropic: HTTP %d: %s", resp.StatusCode, string(errBody))
	}

	var anthropicResp anthropicResponse
	if err := json.NewDecoder(resp.Body).Decode(&anthropicResp); err != nil {
		return nil, fmt.Errorf("anthropic: decode response: %w", err)
	}

	return p.translateResponse(&anthropicResp)
}

// translateRequest converts a domain.ChatRequest (OpenAI format) to Anthropic wire format.
// System role messages are extracted and joined as the top-level system field;
// user and assistant messages are forwarded as-is.
// Returns an error if no non-system messages remain (Anthropic requires at least one).
func (p *anthropicProvider) translateRequest(req *domain.ChatRequest, model string) (*anthropicRequest, error) {
	var system string
	var messages []anthropicMessage

	for _, msg := range req.Messages {
		if msg.Role == "system" {
			if system != "" {
				system += "\n"
			}
			system += domain.GetMessageTextContent(msg)
		} else {
			messages = append(messages, anthropicMessage{
				Role:    msg.Role,
				Content: domain.GetMessageTextContent(msg),
			})
		}
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("anthropic: request must contain at least one non-system message")
	}

	maxTokens := req.MaxTokens
	if maxTokens == 0 {
		maxTokens = defaultMaxTokens
	}

	return &anthropicRequest{
		Model:       model,
		MaxTokens:   maxTokens,
		System:      system,
		Messages:    messages,
		Temperature: req.Temperature,
	}, nil
}

// translateResponse converts an Anthropic response to domain.ChatResponse (OpenAI format).
// Returns an error if the response contains no text content block.
func (p *anthropicProvider) translateResponse(resp *anthropicResponse) (*domain.ChatResponse, error) {
	content := ""
	found := false
	for _, block := range resp.Content {
		if block.Type == "text" {
			content = block.Text
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("anthropic: response contains no text content block (stop_reason: %s)", resp.StopReason)
	}

	// Map Anthropic stop reasons to OpenAI finish reasons.
	finishReason := "stop"
	switch resp.StopReason {
	case "max_tokens":
		finishReason = "length"
	case "tool_use":
		finishReason = "tool_calls"
	}

	return &domain.ChatResponse{
		ID:      resp.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   resp.Model,
		Choices: []domain.Choice{{
			Index: 0,
			Message: &domain.Message{
				Role:    "assistant",
				Content: content,
			},
			FinishReason: finishReason,
		}},
		Usage: domain.Usage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
		ProcessedBy: "provider:anthropic",
	}, nil
}
