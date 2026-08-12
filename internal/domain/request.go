// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// PendingRequest represents a queued request waiting for processing
type PendingRequest struct {
	ID       string
	Request  *ChatRequest
	Response chan *ChatResponse
	Error    chan error
	Deadline time.Time

	// Ctx carries the trace context from the inbound HTTP request so the
	// processor can propagate it when forwarding to agents. The deadline
	// is still managed via Deadline (not ctx) to keep cancellation explicit.
	Ctx          context.Context
	TenantID     string // billing unit; "default" when auth mode is none (NoAuthProvider)
	KeyID        string // API key identifier; "anonymous" when auth mode is none
	DeploymentID string // model_deployment UUID; "unset" until the processor stamps it from the selected agent's metadata

	// Capability is the inference type required by this request: "llm", "embedding",
	// or "reranker". The policy executor uses it to filter agents — only agents whose
	// Metadata.Capability matches this value are considered. Defaults to "llm".
	Capability string

	// Path is the agent-facing HTTP path the processor forwards this request to
	// (e.g. "/v1/messages" for the Anthropic passthrough). Empty means the
	// default "/v1/chat/completions", so existing chat requests are unaffected.
	Path string

	// TokensPerDay is the tenant's configured daily token budget (0 = unlimited).
	// Copied from QuotaLimits at enqueue time so the processor can enforce it
	// without re-reading the Gin context.
	TokensPerDay int

	// QuotaModel selects the limiter bucket for AllowOutputTokens. Empty for
	// legacy flat keys (one bucket per tenant); set to the requested model
	// name for per-model keys (one bucket per (tenant, model)). Stamped by the
	// handler at enqueue time so the processor charges the SAME bucket the
	// front-door admission charged AllowInputTokens against.
	QuotaModel string

	// RemainingTokens is set by the processor after AllowOutputTokens succeeds.
	// -1 means "not set" (limiter disabled or unlimited tenant); the handler skips
	// the header update in that case. 0 is a valid value meaning the budget is
	// exactly exhausted after this response.
	// The handler reads this to populate X-RateLimit-Remaining-Tokens on the response.
	RemainingTokens int

	// DispatchedAgentID is stored atomically by the processor the moment an agent
	// is selected and the HTTP call is about to start. The handler reads it on the
	// timeout path to record which agent was in-flight when the deadline was exceeded.
	// Written by the processor goroutine, read by the handler goroutine — must be atomic.
	DispatchedAgentID atomic.Value // stores string; zero value → no agent dispatched yet

	// StreamedPromptTokens / StreamedCompletionTokens carry the streaming token
	// totals from the processor's meter goroutine back to the handler so the audit
	// log records the real counts (the handler initially has only resp.Usage, which
	// is zero for streaming because the processor returns before the body finishes).
	// The processor writes them inside the streaming goroutine BEFORE pw.Close(); the
	// handler reads them AFTER io.Copy returns — so by the LIFO defer order the values
	// are guaranteed visible. Zero for non-streaming responses (audit uses resp.Usage).
	StreamedPromptTokens     atomic.Int64
	StreamedCompletionTokens atomic.Int64

	// Reservation is the occupancy-budget reservation this request holds, or nil
	// when the budget gate is inert for its model. The processor grows it as
	// undeclared output streams; the handler releases it exactly once when the
	// request finishes. Set by the handler before enqueue.
	Reservation Reservation

	// EstimatedInputTokens is the prompt-token estimate charged at admission,
	// stashed so the handler can true up the reservation by the difference once
	// the backend reports the exact prompt_tokens.
	EstimatedInputTokens int
}

// Reservation is the occupancy-budget slot a request holds for its lifetime.
// The concrete implementation lives in internal/admission; this interface keeps
// domain free of that dependency. All methods are safe on a nil receiver.
type Reservation interface {
	// Grow adds streamed output tokens for an undeclared request (a no-op for a
	// declared one, so it may be called unconditionally per output chunk).
	Grow(tokens int)
	// Adjust applies a signed true-up correction (exact input − estimated input).
	Adjust(delta int)
	// Release returns the reservation to the budget; idempotent.
	Release()
}

// NewPendingRequest creates a new pending request
func NewPendingRequest(id string, request *ChatRequest, timeout time.Duration) *PendingRequest {
	return &PendingRequest{
		ID:              id,
		Request:         request,
		Response:        make(chan *ChatResponse, 1),
		Error:           make(chan error, 1),
		Deadline:        time.Now().Add(timeout),
		RemainingTokens: -1, // sentinel: processor has not set a value yet
	}
}

// IsExpired checks if the request has exceeded its deadline
func (p *PendingRequest) IsExpired() bool {
	return time.Now().After(p.Deadline)
}

func CopyHttpHeaders(dst, src http.Header) {
	if dst == nil {
		return
	}
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
	dst.Del("Content-Length") // The handler will set the correct Content-Length.
}

func IsStreamingResponse(headers http.Header) bool {
	// OpenAI and vLLM both use "text/event-stream" for streaming responses.
	return strings.Contains(headers.Get("Content-Type"), "text/event-stream")
}
