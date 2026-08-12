// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/tokenizer"
)

// TestLearnsInputRatioFromResponse verifies the true-up wiring: after a request
// completes, the handler folds the backend's exact prompt_tokens into the
// per-model estimator, so its ratio moves off cold-start toward the observed
// value — the mechanism that lets B2 run at 0.90.
func TestLearnsInputRatioFromResponse(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	est := tokenizer.NewEstimator()
	before := est.RatioFor(b2Model)

	exec := policy.NewExecutor(nil, nil, &policy.Policy{}, 0, 0)
	h := api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, est,
	)
	c, w := newCtx("/v1/chat/completions", b2Body(0)) // model "gemma", "hi" prompt

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		// The backend reports a much higher prompt_tokens than the tiny prompt's
		// cold estimate, so the learned ratio must rise.
		pending.Response <- &domain.ChatResponse{
			RawBytes: []byte(`{"ok":true}`),
			Usage:    domain.Usage{PromptTokens: 50, CompletionTokens: 5, TotalTokens: 55},
		}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if after := est.RatioFor(b2Model); after <= before {
		t.Errorf("estimator must learn from backend usage; ratio %.4f did not rise (%.4f)", before, after)
	}
}

// learnHandler builds a Handlers wired only with the estimator, for the learning
// guard tests.
func learnHandler(q chan *domain.PendingRequest, est *tokenizer.Estimator) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, &policy.Policy{}, 0, 0)
	return api.NewHandlers(
		nil, nil, q, time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, est,
	)
}

// TestDoesNotLearnFromImageRequest verifies an image request is skipped for
// learning: its prompt_tokens include image tokens that would inflate the text
// tokens-per-byte ratio.
func TestDoesNotLearnFromImageRequest(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	est := tokenizer.NewEstimator()
	h := learnHandler(q, est)
	// imageBody uses model "m"; capture that model's ratio.
	before := est.RatioFor("m")
	c, w := newCtx("/v1/chat/completions", imageBody(1))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{
			RawBytes: []byte(`{"ok":true}`),
			Usage:    domain.Usage{PromptTokens: 900, CompletionTokens: 5, TotalTokens: 905}, // image tokens
		}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	if after := est.RatioFor("m"); after != before {
		t.Errorf("an image request must not move the text ratio: %.4f → %.4f", before, after)
	}
}

// TestDoesNotLearnFromEstimatedUsage verifies the estimator ignores a prompt
// count the backend did not report (locally estimated), so it never relearns its
// own heuristic.
func TestDoesNotLearnFromEstimatedUsage(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	est := tokenizer.NewEstimator()
	h := learnHandler(q, est)
	before := est.RatioFor(b2Model)
	c, w := newCtx("/v1/chat/completions", b2Body(0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		// No Usage object, but Choices present → the handler fills usage from a
		// local estimate. That estimate must NOT be fed back to the learner.
		msg := &domain.Message{Role: "assistant", Content: "hello"}
		pending.Response <- &domain.ChatResponse{Choices: []domain.Choice{{Message: msg}}}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	if after := est.RatioFor(b2Model); after != before {
		t.Errorf("estimated (non-exact) usage must not move the ratio: %.4f → %.4f", before, after)
	}
}
