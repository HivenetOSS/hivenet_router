// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package domain_test — behavioural tests for the core domain types: Agent
// slot/health accounting, Session lifecycle, RouterError → HTTP status mapping,
// PendingRequest deadlines, and OpenAI message content extraction.
package domain_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestAgent_SlotAccounting(t *testing.T) {
	// Capacity 2 caps concurrent in-flight requests; this test exercises the
	// acquire/decrement bookkeeping the selector uses to avoid overloading agents.
	a := domain.NewAgent(peer.ID("a"), domain.AgentMetadata{Capacity: 2}, "")
	if !a.IsHealthy() || !a.IsBackendHealthy() {
		t.Error("new agent must start healthy")
	}
	if a.GetLoad() != 0 {
		t.Errorf("initial load = %d, want 0", a.GetLoad())
	}

	// Fill both slots: each acquire increments load atomically.
	if !a.TryAcquireSlot() || !a.TryAcquireSlot() { //nolint:staticcheck // SA4000: intentional double acquire to fill both slots (capacity 2)
		t.Fatal("first two acquisitions must succeed (capacity 2)")
	}
	if a.GetLoad() != 2 {
		t.Errorf("load = %d, want 2", a.GetLoad())
	}
	// At capacity → next acquire fails.
	if a.TryAcquireSlot() {
		t.Error("acquiring beyond capacity must fail")
	}
	// Completing a request frees a slot, which must become available again.
	a.DecrementLoad()
	if a.GetLoad() != 1 {
		t.Errorf("load after decrement = %d, want 1", a.GetLoad())
	}
	if !a.TryAcquireSlot() {
		t.Error("a freed slot must be re-acquirable")
	}
}

func TestAgent_HealthToggles(t *testing.T) {
	// Two independent health signals: liveness (heartbeat) and backend health
	// (the model server behind the agent). Both gate routing separately.
	a := domain.NewAgent(peer.ID("a"), domain.AgentMetadata{}, "")
	a.SetHealthy(false)
	if a.IsHealthy() {
		t.Error("SetHealthy(false) not reflected")
	}
	// SetBackendHealthy returns the previous value so the router can act only on
	// the healthy→unhealthy edge (e.g. increment a failure counter once).
	if prev := a.SetBackendHealthy(false); prev != true {
		t.Errorf("SetBackendHealthy prev = %v, want true", prev)
	}
	if a.IsBackendHealthy() {
		t.Error("backend should be unhealthy")
	}
	if prev := a.SetBackendHealthy(true); prev != false {
		t.Errorf("SetBackendHealthy prev = %v, want false", prev)
	}
}

func TestSession_Lifecycle(t *testing.T) {
	// Sessions expire on a TTL; Refresh (driven by heartbeats) is what keeps a
	// live agent's session from lapsing. Negative TTL forces immediate expiry.
	s := domain.NewSession("agent-1", nil, time.Hour)
	if s.IsExpired() {
		t.Error("fresh session must not be expired")
	}
	expired := domain.NewSession("agent-2", nil, -1*time.Second)
	if !expired.IsExpired() {
		t.Error("negative-TTL session must be expired")
	}
	// Refresh extends the deadline into the future.
	expired.Refresh(time.Hour)
	if expired.IsExpired() {
		t.Error("Refresh must extend the session past now")
	}
}

func TestHTTPStatusFor(t *testing.T) {
	// Each internal error code maps to a specific HTTP status so clients get
	// correct, actionable semantics (retryable 503 vs terminal 400 vs 429 etc.).
	// The unknown-code case guards the default: never leak an internal code, fall
	// back to 500.
	cases := map[domain.ErrorCode]int{
		domain.ErrCodeUnauthorized:          http.StatusUnauthorized,
		domain.ErrCodeModelForbidden:        http.StatusForbidden,
		domain.ErrCodeRequestInvalid:        http.StatusBadRequest,
		domain.ErrCodeContextLengthExceeded: http.StatusBadRequest,
		domain.ErrCodeModelNotFound:         http.StatusNotFound,
		domain.ErrCodeNoCapacity:            http.StatusServiceUnavailable,
		domain.ErrCodeQueueFull:             http.StatusServiceUnavailable,
		domain.ErrCodeBackendError:          http.StatusBadGateway,
		domain.ErrCodeRequestTimeout:        http.StatusGatewayTimeout,
		domain.ErrCodeRateLimitExceeded:     http.StatusTooManyRequests,
		domain.ErrCodeTokenLimitExceeded:    http.StatusTooManyRequests,
		domain.ErrorCode("unknown-code"):    http.StatusInternalServerError,
	}
	for code, want := range cases {
		if got := domain.HTTPStatusFor(code); got != want {
			t.Errorf("HTTPStatusFor(%q) = %d, want %d", code, got, want)
		}
	}
}

func TestRouterError(t *testing.T) {
	// RouterError carries the human message, machine code, and origin (router vs
	// backend) together so failures stay classifiable end-to-end.
	e := domain.NewRouterError(domain.ErrCodeNoCapacity, "at capacity", domain.SourceRouter)
	if e.Error() != "at capacity" {
		t.Errorf("Error() = %q", e.Error())
	}
	if e.Code != domain.ErrCodeNoCapacity || e.Source != domain.SourceRouter {
		t.Errorf("unexpected fields: %+v", e)
	}
}

func TestPendingRequest_Deadline(t *testing.T) {
	// A pending request carries its own deadline so the router can abandon stale
	// work; the -1 RemainingTokens sentinel means "no token limit set" (distinct
	// from a real 0 budget).
	live := domain.NewPendingRequest("id", &domain.ChatRequest{}, time.Hour)
	if live.IsExpired() {
		t.Error("request with 1h timeout must not be expired")
	}
	if live.RemainingTokens != -1 {
		t.Errorf("RemainingTokens = %d, want -1 sentinel", live.RemainingTokens)
	}
	dead := domain.NewPendingRequest("id", &domain.ChatRequest{}, -1*time.Second)
	if !dead.IsExpired() {
		t.Error("request with negative timeout must be expired")
	}
}

func TestIsStreamingResponse(t *testing.T) {
	// The router branches on this: SSE responses must be streamed through
	// chunk-by-chunk, whereas JSON is buffered — so the Content-Type sniff drives
	// two different proxy code paths.
	h := http.Header{}
	h.Set("Content-Type", "text/event-stream")
	if !domain.IsStreamingResponse(h) {
		t.Error("text/event-stream must be detected as streaming")
	}
	h.Set("Content-Type", "application/json")
	if domain.IsStreamingResponse(h) {
		t.Error("application/json must not be streaming")
	}
}

func TestCopyHttpHeaders(t *testing.T) {
	// When proxying a backend response, most headers pass through but
	// Content-Length must be dropped: the router re-frames the body, so a stale
	// length would corrupt the response.
	src := http.Header{}
	src.Set("X-Custom", "v")
	src.Set("Content-Length", "123")
	dst := http.Header{}
	domain.CopyHttpHeaders(dst, src)
	if dst.Get("X-Custom") != "v" {
		t.Error("custom header not copied")
	}
	if dst.Get("Content-Length") != "" {
		t.Error("Content-Length must be stripped (handler sets its own)")
	}
	// nil dst must be a safe no-op.
	domain.CopyHttpHeaders(nil, src)
}

func TestGetMessageTextContent(t *testing.T) {
	// OpenAI message content is polymorphic: either a plain string or an array of
	// typed parts. This extractor normalizes both to plain text (for token
	// counting / logging), so both encodings must be handled.
	// String content.
	var m domain.ChatCompletionMessage
	if err := json.Unmarshal([]byte(`{"role":"user","content":"hello"}`), &m); err != nil {
		t.Fatalf("unmarshal string content: %v", err)
	}
	if got := domain.GetMessageTextContent(m); got != "hello" {
		t.Errorf("string content = %q, want hello", got)
	}

	// Array (multimodal) content: only text parts are concatenated.
	var mm domain.ChatCompletionMessage
	body := `{"role":"user","content":[{"type":"text","text":"a"},{"type":"image_url","image_url":{"url":"x"}},{"type":"text","text":"b"}]}`
	if err := json.Unmarshal([]byte(body), &mm); err != nil {
		t.Fatalf("unmarshal array content: %v", err)
	}
	if got := domain.GetMessageTextContent(mm); got != "ab" {
		t.Errorf("multimodal text = %q, want ab", got)
	}
}
