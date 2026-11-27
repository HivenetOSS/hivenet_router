// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"errors"
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
)

// TestPoolingTokens: the backend's usage counts when it reports one in
// tokens; otherwise, and always for Infinity (whose usage counts characters),
// the admission estimate counts, flagged as an estimate.
func TestPoolingTokens(t *testing.T) {
	pending := &domain.PendingRequest{EstimatedInputTokens: 40}
	withUsage := []byte(`{"data":[],"usage":{"prompt_tokens":26,"total_tokens":26}}`)
	for _, tc := range []struct {
		name      string
		engine    string
		body      []byte
		tokens    int
		estimated bool
	}{
		{"vLLM with usage", "vllm", withUsage, 26, false},
		{"llama.cpp with usage", "llamacpp", withUsage, 26, false},
		{"no usage", "vllm", []byte(`{"data":[]}`), 40, true},
		{"Infinity counts characters", "infinity", []byte(`{"data":[],"usage":{"prompt_tokens":109,"total_tokens":109}}`), 40, true},
	} {
		tokens, estimated := poolingTokens(tc.engine, tc.body, pending)
		if tokens != tc.tokens || estimated != tc.estimated {
			t.Errorf("%s: (%d, %v), want (%d, %v)", tc.name, tokens, estimated, tc.tokens, tc.estimated)
		}
	}
}

// outputLimiter records the output-token charges; the rest is unused.
type outputLimiter struct {
	auth.RateLimiter
	allowed   bool
	remaining int
	err       error
	charged   []int
}

func (l *outputLimiter) AllowOutputTokens(_ auth.DailyBudget, n int) (bool, int, error) {
	l.charged = append(l.charged, n)
	return l.allowed, l.remaining, l.err
}

// TestChargePoolingTokens: the daily budgets are charged every input token
// the response counted; a charge that runs a budget out leaves RemainingTokens
// at 0 (the answer is still returned), and a limiter error fails open.
func TestChargePoolingTokens(t *testing.T) {
	for _, tc := range []struct {
		name          string
		tokens        int
		lim           *outputLimiter
		wantCharges   []int
		wantRemaining int
	}{
		{"charged in full", 26, &outputLimiter{allowed: true, remaining: 974}, []int{26}, 974},
		{"no tokens", 0, &outputLimiter{allowed: true}, nil, -1},
		{"budget runs out", 26, &outputLimiter{allowed: false}, []int{26}, 0},
		{"limiter error", 26, &outputLimiter{err: errors.New("down")}, []int{26}, -1},
	} {
		p := &RequestProcessor{limiter: tc.lim}
		pending := &domain.PendingRequest{ID: "r", RemainingTokens: -1}
		p.chargePoolingTokens(pending, tc.tokens)
		if len(tc.lim.charged) != len(tc.wantCharges) || (len(tc.wantCharges) > 0 && tc.lim.charged[0] != tc.wantCharges[0]) {
			t.Errorf("%s: charged %v, want %v", tc.name, tc.lim.charged, tc.wantCharges)
		}
		if pending.RemainingTokens != tc.wantRemaining {
			t.Errorf("%s: RemainingTokens %d, want %d", tc.name, pending.RemainingTokens, tc.wantRemaining)
		}
	}
}

// TestChargePoolingTokens_SettlesHold: with a daily hold the request is
// settled with the input tokens the backend read, not the held estimate plus
// them; a response with no tokens releases the hold.
func TestChargePoolingTokens_SettlesHold(t *testing.T) {
	p := &RequestProcessor{limiter: &outputLimiter{allowed: true}}
	h := &recHold{}
	pending := &domain.PendingRequest{ID: "r", RemainingTokens: -1, DailyHold: h, HeldInputTokens: 40}
	p.chargePoolingTokens(pending, 26)
	if len(h.settled) != 1 || h.settled[0] != 26 || pending.RemainingTokens != 7 {
		t.Errorf("settled %v, remaining %d; want [26] and 7", h.settled, pending.RemainingTokens)
	}
	if lim := p.limiter.(*outputLimiter); len(lim.charged) != 0 {
		t.Errorf("charged %v besides the settle", lim.charged)
	}
}
