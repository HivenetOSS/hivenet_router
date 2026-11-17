// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"hivenet_router/internal/domain"
)

// BackendError carries the raw HTTP status and body returned by a backend
// (vLLM, Ollama, SGLang, custom) on a non-200 response. It implements the
// error interface so it can flow through ForwardChat unchanged. Error leaves
// the body out: engines often echo the prompt there, and errors end up in
// logs and span statuses. The body reaches the client only, through
// classifyBackendHTTPStatus.
type BackendError struct {
	HTTPStatus int
	Body       []byte
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("backend error %d", e.HTTPStatus)
}

// classifyBackendHTTPStatus maps a backend HTTP status and response body to
// a RouterError from the backend (domain.ClassifyBackendError, which the
// router also uses for an error body no agent classified).
func classifyBackendHTTPStatus(status int, body []byte) *domain.RouterError {
	return domain.ClassifyBackendError(status, body)
}

// Engine abstracts the backend inference server (vLLM, Ollama, etc.).
//
// Each implementation encapsulates the backend-specific logic for:
//   - Health checking (is the server up?)
//   - Model discovery (what models are loaded?)
//   - Request forwarding (send a chat completion request)
//
// The generic Agent delegates these operations to whichever Engine
// was selected at startup via the --engine flag.
type Engine interface {
	// Name returns a short identifier for this engine (e.g. "vllm", "ollama").
	// Used in logs and JWT subject ("agent-vllm", "agent-ollama").
	Name() string

	// WaitForReady checks whether the backend server is healthy and accepting requests.
	// Returns nil if ready, or an error describing why it's not.
	WaitForReady(ctx context.Context, baseURL string, httpClient *http.Client) error

	// DiscoverModels queries the backend to find which models are currently loaded.
	// Returns a list of model identifiers (at least one expected).
	DiscoverModels(ctx context.Context, baseURL string, httpClient *http.Client) ([]string, error)

	// ForwardChat sends a chat completion request to the backend after applying original headers if any and returns
	// the raw JSON response bytes, and the response headers.
	ForwardChat(ctx context.Context, baseURL string, httpClient *http.Client, model string, req domain.ChatRequest, options ...EngineOption) ([]byte, http.Header, error)
}

// Warmer is an optional Engine capability, for a backend that reports healthy
// before it can answer quickly: it loads the model, or compiles kernels, on
// its first request. The agent calls Warm once the model is known and
// registers only after it succeeds, so a model woken from zero is released to
// held requests only when it can serve them, and the first of them does not
// pay for the load.
type Warmer interface {
	Warm(ctx context.Context, baseURL string, httpClient *http.Client, model string) error
}

// warmChat sends a one-token chat completion for model, the cheapest request
// that makes an OpenAI-compatible backend load and run the model.
func warmChat(ctx context.Context, baseURL string, httpClient *http.Client, model string) error {
	body, err := json.Marshal(map[string]any{
		"model":      model,
		"messages":   []map[string]string{{"role": "user", "content": "hi"}},
		"max_tokens": 1,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("warm-up request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("warm-up request: status %d", resp.StatusCode)
	}
	return nil
}
