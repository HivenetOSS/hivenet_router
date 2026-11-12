// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
)

// refusingLimiter refuses every input charge and explains the refusal as
// told, like an auth.InMemoryLimiter would after a charge it refused.
type refusingLimiter struct {
	fakeLimiter
	why auth.Refusal
}

func (r *refusingLimiter) DailyRefusal(auth.DailyBudget, int) auth.Refusal { return r.why }

// TestPassthrough_BudgetRefusalSaysWhy: a request the daily budgets refuse
// gets 429 token_limit_exceeded saying why. A spent key budget keeps the key
// message; a spent tenant budget names the organization's; a refusal the
// budgets do not explain (this router's share of a budget with room left is
// used up for the moment, or today's usage cannot be read yet) says so and
// carries Retry-After, in the header and the body, because a retry shortly
// can succeed.
func TestPassthrough_BudgetRefusalSaysWhy(t *testing.T) {
	for _, tc := range []struct {
		why        auth.Refusal
		message    string
		retryAfter int
	}{
		{auth.RefusalKeyBudget, "daily token budget exceeded", 0},
		{auth.RefusalNone, "daily token budget exceeded", 0},
		{auth.RefusalTenantBudget, "your organization's daily token budget is exhausted", 0},
		{auth.RefusalShare, auth.RefusalShare.Message(""), 1},
		{auth.RefusalUnknown, auth.RefusalUnknown.Message(""), 1},
	} {
		q := make(chan *domain.PendingRequest, 1)
		lim := &refusingLimiter{fakeLimiter: fakeLimiter{remaining: 1_000_000, inAllowed: false}, why: tc.why}
		h := newTestHandlers(q, lim)
		c, w := newCtx("/v1/chat/completions", []byte(`{"model":"m","messages":[{"role":"user","content":"hello there"}]}`))
		c.Set("tenant_id", "t")
		c.Set("quota_limits", auth.QuotaLimits{TenantTokensPerDay: 1_000})
		h.Passthrough(c)
		var env domain.RouterErrorResponse
		_ = json.Unmarshal(w.Body.Bytes(), &env)
		if w.Code != http.StatusTooManyRequests || env.Error == nil || env.Error.Code != domain.ErrCodeTokenLimitExceeded {
			t.Fatalf("%v: status %d body %s, want 429 token_limit_exceeded", tc.why, w.Code, w.Body)
		}
		if env.Error.Message != tc.message {
			t.Errorf("%v: message %q, want %q", tc.why, env.Error.Message, tc.message)
		}
		header := w.Header().Get("Retry-After")
		if (tc.retryAfter > 0) != (header == "1") || env.Error.RetryAfter != tc.retryAfter {
			t.Errorf("%v: Retry-After header %q, body retry_after %d; want %d", tc.why, header, env.Error.RetryAfter, tc.retryAfter)
		}
		if len(q) != 0 {
			t.Errorf("%v: a refused request was queued", tc.why)
		}
	}
}
