// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package provider

import (
	"context"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
)

// Provider is the abstraction for a closed-source AI API used as last-resort fallback.
// When all local routing steps are exhausted, the processor calls Complete on the
// configured provider, forwarding the original request with the model overridden
// by the value declared in the policy's fallback_provider block.
type Provider interface {
	// Name returns the canonical provider name ("openai", "anthropic").
	Name() string

	// Complete forwards the request to the external API using model as the target
	// model (overriding req.Model). Returns a domain.ChatResponse on success.
	Complete(ctx context.Context, req *domain.ChatRequest, model string) (*domain.ChatResponse, error)
}

// Config holds parameters needed to construct a Provider.
type Config struct {
	// Name identifies the provider: "openai" or "anthropic".
	Name string

	// APIKey is passed in the provider-specific auth header. Required.
	APIKey string

	// BaseURL overrides the provider's default API endpoint (e.g. to target an
	// OpenAI-compatible proxy or a local mock). Empty means the provider's
	// canonical public URL. No trailing slash.
	BaseURL string

	// Timeout for individual HTTP calls. 0 means 120 seconds.
	Timeout time.Duration

	// Metrics records outbound HTTP client durations. May be nil.
	Metrics *metrics.RouterMetrics
}
