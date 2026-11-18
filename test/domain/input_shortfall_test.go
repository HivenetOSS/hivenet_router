// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"testing"

	"hivenet_router/internal/domain"
)

// TestInputShortfall: what the backend's prompt_tokens exceed the daily
// budgets' admission charge by; nothing when nothing was charged, when the
// backend reported nothing, or when the estimate was higher (no refund).
func TestInputShortfall(t *testing.T) {
	for _, tc := range []struct{ charged, reported, want int }{
		{100, 250, 150},
		{100, 100, 0},
		{100, 40, 0},
		{0, 500, 0},
		{100, 0, 0},
	} {
		p := &domain.PendingRequest{ChargedInputTokens: tc.charged}
		if got := p.InputShortfall(tc.reported); got != tc.want {
			t.Errorf("charged %d, reported %d: shortfall %d, want %d", tc.charged, tc.reported, got, tc.want)
		}
	}
}
