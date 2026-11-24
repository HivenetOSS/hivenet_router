// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
)

// recHold records what a hold was settled with.
type recHold struct{ settled []int }

func (h *recHold) Settle(n int) (bool, int, error) {
	h.settled = append(h.settled, n)
	return true, 7, nil
}
func (h *recHold) Release() {}

// TestSettleDaily: the hold is settled with the backend's prompt and
// completion, or the held estimate for a prompt it did not report; without a
// hold, the completion is charged as before.
func TestSettleDaily(t *testing.T) {
	p := &RequestProcessor{}
	h := &recHold{}
	pending := &domain.PendingRequest{DailyHold: h, HeldInputTokens: 40}
	if settled, ok, rem, _ := p.settleDaily(pending, 120, 30); !settled || !ok || rem != 7 || h.settled[0] != 150 {
		t.Errorf("exact usage: (%v, %v, %d) settled %v, want 150", settled, ok, rem, h.settled)
	}
	if p.settleDaily(pending, 0, 30); h.settled[1] != 70 {
		t.Errorf("no prompt reported: settled %v, want the held 40 + 30", h.settled)
	}

	lim := &settleLimiter{allowed: true, remaining: 9}
	p = &RequestProcessor{limiter: lim}
	if settled, _, _, _ := p.settleDaily(&domain.PendingRequest{}, 100, 25); !settled || len(lim.charged) != 1 || lim.charged[0] != 25 {
		t.Errorf("no hold: settled %v, charged %v, want the completion 25", settled, lim.charged)
	}
	if settled, _, _, _ := p.settleDaily(&domain.PendingRequest{}, 100, 0); settled {
		t.Error("no hold and no completion: charged something")
	}
}

// settleLimiter records the output-token charges; the rest is unused.
type settleLimiter struct {
	auth.RateLimiter
	allowed   bool
	remaining int
	charged   []int
}

func (l *settleLimiter) AllowOutputTokens(_ auth.DailyBudget, n int) (bool, int, error) {
	l.charged = append(l.charged, n)
	return l.allowed, l.remaining, nil
}
