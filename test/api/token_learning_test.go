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
