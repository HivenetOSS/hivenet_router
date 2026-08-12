// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package api_test contains black-box tests for the occupancy admit budget wired
// into the passthrough handler: an over-budget request is a 429
// concurrency_limit_exceeded with Retry-After before queueing, and an admitted
// request's reservation is released on every exit path (success, error, timeout),
// so the weighted in-flight sum always returns to zero.
package api_test

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

// errReader yields one SSE chunk, then fails — simulating a stream that breaks
// mid-flight after some output was already delivered.
type errReader struct{ sent bool }

func (e *errReader) Read(p []byte) (int, error) {
	if !e.sent {
		e.sent = true
		return copy(p, []byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")), nil
	}
	return 0, io.ErrUnexpectedEOF
}
func (e *errReader) Close() error { return nil }

const b2Model = "gemma"

// newB2Handlers builds a Handlers whose executor serves global for every model,
// with a real admission controller so the occupancy budget is live.
func newB2Handlers(q chan *domain.PendingRequest, ctrl *admission.Controller, global *policy.Policy, timeout time.Duration) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, global, 0, 0)
	return api.NewHandlers(
		nil, nil, q, timeout,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		ctrl, nil, nil, nil,
		nil, nil,
	)
}

func b2Body(maxTokens int) []byte {
	if maxTokens > 0 {
		return []byte(`{"model":"gemma","messages":[{"role":"user","content":"hi"}],"max_tokens":` + strconv.Itoa(maxTokens) + `}`)
	}
	return []byte(`{"model":"gemma","messages":[{"role":"user","content":"hi"}]}`)
}

// TestB2_OverBudget_429WithRetryAfter verifies a request whose footprint exceeds
// the budget is rejected up front with 429 concurrency_limit_exceeded and a
// Retry-After header, and is not queued.
func TestB2_OverBudget_429WithRetryAfter(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0) // no parking → immediate reject
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 50}, time.Second)
	// footprint = input(4) + declared max_tokens(100) = 104 > 50.
	c, w := newCtx("/v1/chat/completions", b2Body(100))

	h.Passthrough(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 over budget, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), string(domain.ErrCodeConcurrencyLimit)) {
		t.Errorf("expected %q in body, got %s", domain.ErrCodeConcurrencyLimit, w.Body.String())
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("a 429 must carry a Retry-After header")
	}
	if len(q) != 0 {
		t.Errorf("a rejected request must not be queued, depth=%d", len(q))
	}
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Errorf("a rejected request must hold no reservation; got sumW=%d count=%d", sumW, count)
	}
}

// TestB2_ReleasedOnSuccess verifies the reservation is held while the request is
// in flight and released when it completes successfully.
func TestB2_ReleasedOnSuccess(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, time.Second)
	c, w := newCtx("/v1/chat/completions", b2Body(100))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if sumW, count := ctrl.Occupancy(b2Model); sumW == 0 || count != 1 {
			t.Errorf("reservation must be held in flight; got sumW=%d count=%d", sumW, count)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Fatalf("reservation must be released after success; got sumW=%d count=%d", sumW, count)
	}
}

// TestB2_ReleasedOnError verifies the reservation is released when the request
// ends in a backend error (the same exit path a failed provider failover takes).
func TestB2_ReleasedOnError(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, time.Second)
	c, _ := newCtx("/v1/chat/completions", b2Body(100))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Error <- domain.NewRouterError(domain.ErrCodeBackendError, "boom", domain.SourceBackend)
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Fatalf("reservation must be released after an error; got sumW=%d count=%d", sumW, count)
	}
}

// TestB2_ReleasedOnStreamingMidStreamError verifies the reservation is released
// even when a streaming response breaks part-way through — the single deferred
// release covers the streaming exit path just like the clean ones.
func TestB2_ReleasedOnStreamingMidStreamError(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, time.Second)
	c, _ := newCtx("/v1/chat/completions", b2Body(0)) // undeclared → reservation grows as it streams

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{Body: &errReader{}}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Fatalf("reservation must be released even when the stream errors mid-flight; got sumW=%d count=%d", sumW, count)
	}
}

// TestB2_ReleasedOnTimeout verifies the reservation is released when the request
// times out waiting for a response.
func TestB2_ReleasedOnTimeout(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, 80*time.Millisecond)
	c, w := newCtx("/v1/chat/completions", b2Body(100))

	h.Passthrough(c) // no responder drains q → awaitResponse times out

	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("expected 504 on timeout, got %d", w.Code)
	}
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Fatalf("reservation must be released after a timeout; got sumW=%d count=%d", sumW, count)
	}
}

// TestB2_DeclaredFootprintReservedUpFront verifies a declared max_tokens is
// reserved in full at admission (input + max_tokens), not metered live.
func TestB2_DeclaredFootprintReservedUpFront(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, time.Second)
	c, _ := newCtx("/v1/chat/completions", b2Body(100)) // input 4 + declared 100

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if sumW, _ := ctrl.Occupancy(b2Model); sumW != 104 {
			t.Errorf("declared footprint must be input+max_tokens=104; got %d", sumW)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
}

// TestB2_UndeclaredFootprintInputOnly verifies an undeclared request reserves only
// its input at admission (output is grown live as it streams).
func TestB2_UndeclaredFootprintInputOnly(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, time.Second)
	c, _ := newCtx("/v1/chat/completions", b2Body(0)) // no max_tokens → input 4 only

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if sumW, _ := ctrl.Occupancy(b2Model); sumW != 4 {
			t.Errorf("undeclared footprint must be input only (4); got %d", sumW)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
}

// TestB2_InertWhenNoBudget verifies a policy that declares neither a budget nor a
// backstop attaches no reservation and holds no occupancy.
func TestB2_InertWhenNoBudget(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{}, time.Second) // no admit_budget_tokens, no max_inflight
	c, _ := newCtx("/v1/chat/completions", b2Body(100))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if pending.Reservation != nil {
			t.Error("an inert budget must attach no reservation")
		}
		if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
			t.Errorf("an inert budget must hold no occupancy; got sumW=%d count=%d", sumW, count)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
}

// TestB2_MaxInflightBackstop verifies the in-flight count cap rejects a second
// concurrent request with 429 even when the token budget has room.
func TestB2_MaxInflightBackstop(t *testing.T) {
	q := make(chan *domain.PendingRequest, 2)
	ctrl := admission.NewController(1.0, 0)
	// No token budget; max_inflight=1.
	h := newB2Handlers(q, ctrl, &policy.Policy{MaxInflight: 1}, time.Second)

	// First request occupies the single in-flight slot; hold it in the queue.
	c1, _ := newCtx("/v1/chat/completions", b2Body(0))
	done1 := make(chan struct{})
	go func() { h.Passthrough(c1); close(done1) }()
	var pending1 *domain.PendingRequest
	select {
	case pending1 = <-q:
	case <-time.After(time.Second):
		t.Fatal("first request was not enqueued")
	}

	// Second request must be rejected by the backstop while the first is in flight.
	c2, w2 := newCtx("/v1/chat/completions", b2Body(0))
	h.Passthrough(c2)
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("second concurrent request must hit the max_inflight backstop (429), got %d", w2.Code)
	}

	pending1.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	<-done1
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Fatalf("occupancy must be zero once the first request completes; got sumW=%d count=%d", sumW, count)
	}
}

// TestB2_NegativeMaxTokens_TreatedAsUndeclared verifies a negative max_tokens
// cannot shrink the charged footprint: it is treated as undeclared (input-only
// reservation that grows with output), not subtracted from the input estimate.
func TestB2_NegativeMaxTokens_TreatedAsUndeclared(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	ctrl := admission.NewController(1.0, 0)
	h := newB2Handlers(q, ctrl, &policy.Policy{AdmitBudgetTokens: 1_000_000}, time.Second)
	body := []byte(`{"model":"gemma","messages":[{"role":"user","content":"hi"}],"max_tokens":-1000}`)
	c, _ := newCtx("/v1/chat/completions", body)

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if sumW, count := ctrl.Occupancy(b2Model); sumW <= 0 || count != 1 {
			t.Errorf("footprint must be the positive input estimate, not input-1000; got sumW=%d count=%d", sumW, count)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if sumW, count := ctrl.Occupancy(b2Model); sumW != 0 || count != 0 {
		t.Fatalf("reservation must be released; got sumW=%d count=%d", sumW, count)
	}
}
