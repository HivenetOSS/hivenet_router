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

// BackendError carries the raw HTTP status and body returned by a backend
// (vLLM, Ollama, SGLang, custom) on a non-200 response. It implements the
// error interface so it can flow through ForwardChat unchanged.
type BackendError struct {
	HTTPStatus int
	Body       []byte
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("backend error %d: %s", e.HTTPStatus, string(e.Body))
}

// classifyBackendHTTPStatus maps a backend HTTP status + response body to a
// structured RouterError. The original body is preserved verbatim as the
// message (truncated at 512 bytes) so the client sees the real backend reason.
func classifyBackendHTTPStatus(status int, body []byte) *domain.RouterError {
	// Truncate body for the message.
	msg := string(body)
	if len(msg) > 512 {
		msg = msg[:512]
	}

	// Try to extract a human-readable message and error type from the JSON body.
	// Handles both flat format {"message":"...","type":"..."} (vLLM / SGLang)
	// and nested format {"error":{"message":"...","type":"..."}} (OpenAI-style).
	var parsed struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Error   struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		if parsed.Message != "" {
			msg = parsed.Message
		} else if parsed.Error.Message != "" {
			msg = parsed.Error.Message
		}
		errType := parsed.Type
		if errType == "" {
			errType = parsed.Error.Type
		}
		// Map well-known backend error type strings directly.
		switch errType {
		case "context_length_exceeded":
			return domain.NewRouterError(domain.ErrCodeContextLengthExceeded, msg, domain.SourceBackend)
		case "invalid_request_error", "invalid_parameter":
			return domain.NewRouterError(domain.ErrCodeInvalidParameter, msg, domain.SourceBackend)
		}
	}

	// Fall back to HTTP status + keyword matching on the raw body.
	bodyLower := strings.ToLower(string(body))
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		if strings.Contains(bodyLower, "context") &&
			(strings.Contains(bodyLower, "length") ||
				strings.Contains(bodyLower, "window") ||
				strings.Contains(bodyLower, "token")) {
			return domain.NewRouterError(domain.ErrCodeContextLengthExceeded, msg, domain.SourceBackend)
		}
		return domain.NewRouterError(domain.ErrCodeInvalidParameter, msg, domain.SourceBackend)
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return domain.NewRouterError(domain.ErrCodeBackendUnavailable, msg, domain.SourceBackend)
	default:
		return domain.NewRouterError(domain.ErrCodeBackendError, msg, domain.SourceBackend)
	}
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
