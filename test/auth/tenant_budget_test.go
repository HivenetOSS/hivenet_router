// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"slices"
	"testing"

	"hivenet_router/internal/auth"
)

// TestTenantBudget_LowestAmongKeys: a tenant's daily budget is the lowest
// tenant_tokens_per_day its keys declare (unset = no budget), and every key of
// the tenant authenticates with it, static or dynamic. Lowering the value on
// one key applies to the whole tenant at once; raising it applies once every
// key carries the new value. Another tenant's keys never count.
func TestTenantBudget_LowestAmongKeys(t *testing.T) {
	t.Run("static", func(t *testing.T) {
		entry := func(token, owner string, budget int) auth.APIKeyEntry {
			e := staticEntry(token, owner)
			e.Quota.TenantTokensPerDay = budget
			return e
		}
		p, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
			entry("sk-a1", "tenant-a", 5000),
			entry("sk-a2", "tenant-a", 2000),
			entry("sk-a3", "tenant-a", 0),
			entry("sk-b1", "tenant-b", 0),
		})
		if err != nil {
			t.Fatal(err)
		}
		for token, want := range map[string]int{"sk-a1": 2000, "sk-a2": 2000, "sk-a3": 2000, "sk-b1": 0} {
			if got := authenticate(t, p, token).QuotaLimits.TenantTokensPerDay; got != want {
				t.Errorf("%s: tenant budget %d, want %d", token, got, want)
			}
		}
		if got := p.Tenants()["tenant-a"].TenantTokensPerDay; got != 2000 {
			t.Errorf("Tenants: tenant-a budget %d, want 2000", got)
		}
		if ids := p.TenantIDs(); !slices.Equal(ids, []string{"tenant-a", "tenant-b"}) {
			t.Errorf("TenantIDs = %v, want [tenant-a tenant-b]", ids)
		}
	})

	t.Run("dynamic", func(t *testing.T) {
		p := auth.NewDynamicKeyProvider()
		push := func(id, owner string, budget int) {
			t.Helper()
			e := auth.DynamicKeyEntry{ID: id, KeyHash: hashOf("tok-" + id), Owner: owner, Name: "n", Enabled: true,
				Quota: auth.QuotaLimits{TenantTokensPerDay: budget}}
			if err := p.Upsert("v1", e); err != nil {
				t.Fatal(err)
			}
		}
		budgetOf := func(id string) int {
			t.Helper()
			return authenticate(t, p, "tok-"+id).QuotaLimits.TenantTokensPerDay
		}
		push("a1", "tenant-a", 5000)
		push("a2", "tenant-a", 5000)
		push("b1", "tenant-b", 100)
		if budgetOf("a1") != 5000 || budgetOf("b1") != 100 {
			t.Fatalf("budgets a1=%d b1=%d, want 5000 and 100", budgetOf("a1"), budgetOf("b1"))
		}
		// Lowered on one key: applies to the whole tenant at once.
		push("a1", "tenant-a", 1000)
		if budgetOf("a1") != 1000 || budgetOf("a2") != 1000 {
			t.Errorf("after lowering a1: a1=%d a2=%d, want 1000 for both", budgetOf("a1"), budgetOf("a2"))
		}
		// Raised on one key only: the lowest still applies until every key
		// carries the new value.
		push("a1", "tenant-a", 8000)
		if budgetOf("a2") != 5000 {
			t.Errorf("after raising a1 only: a2=%d, want 5000 (a2 still says 5000)", budgetOf("a2"))
		}
		push("a2", "tenant-a", 8000)
		if budgetOf("a1") != 8000 {
			t.Errorf("after raising both: a1=%d, want 8000", budgetOf("a1"))
		}
		// Deleting the key with the lowest value lifts it.
		if err := p.Delete("v2", "b1"); err != nil {
			t.Fatal(err)
		}
		ids := p.TenantIDs()
		slices.Sort(ids)
		if !slices.Equal(ids, []string{"tenant-a"}) {
			t.Errorf("TenantIDs after deleting tenant-b's only key = %v, want [tenant-a]", ids)
		}
	})

	t.Run("negative value refused", func(t *testing.T) {
		e := staticEntry("sk-x", "tenant-a")
		e.Quota.TenantTokensPerDay = -1
		if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{e}); err == nil {
			t.Error("tenant_tokens_per_day -1 accepted")
		}
	})
}

// TestTenantBudget_LimitGaugeCeiling: the tenant's daily limit gauge reads the
// tighter of the sum of its flat keys' budgets and its own budget.
func TestTenantBudget_LimitGaugeCeiling(t *testing.T) {
	for _, tc := range []struct {
		keyTPD, tenantTPD, want int
	}{
		{0, 0, 0},
		{3000, 0, 3000},
		{0, 1000, 1000},
		{3000, 1000, 1000},
		{500, 1000, 500},
	} {
		q := auth.QuotaLimits{TokensPerDay: tc.keyTPD, TenantTokensPerDay: tc.tenantTPD}
		if got := q.DailyTokenCeiling(); got != tc.want {
			t.Errorf("keys %d, tenant %d: ceiling %d, want %d", tc.keyTPD, tc.tenantTPD, got, tc.want)
		}
	}
}

// TestMinute_KeyReassignedToOtherTenantStartsFresh: the tokens-per-minute
// buckets belong to a (tenant, key) pair, so a key ID that moves to another
// tenant does not inherit the previous tenant's spent minute buckets.
func TestMinute_KeyReassignedToOtherTenantStartsFresh(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	if !l.AllowInputTokens("tenant-a", "key-1", "m", 100, 100) {
		t.Fatal("first input charge refused")
	}
	if l.AllowInputTokens("tenant-a", "key-1", "m", 100, 50) {
		t.Fatal("tenant-a's spent bucket admitted more")
	}
	if !l.AllowInputTokens("tenant-b", "key-1", "m", 100, 100) {
		t.Error("key-1 under tenant-b inherited tenant-a's input bucket")
	}
	l.ChargeOutputTokens("tenant-a", "key-1", "m", 100, 100)
	if !l.OutputExhausted("tenant-a", "key-1", "m", 100) {
		t.Fatal("tenant-a's output bucket not exhausted after a full charge")
	}
	if l.OutputExhausted("tenant-b", "key-1", "m", 100) {
		t.Error("key-1 under tenant-b inherited tenant-a's output bucket")
	}
}

// TestSpentRefusal_NamesTheBudgetThatRanOut: after an output charge the
// budgets refused (it is applied, filling the budget it overflowed), the
// explanation names the budget that ran out, not one that still has room:
// with the tenant at 9500 of 10000 and the key at 900 of 1000, a 300-token
// completion overflows the key, and the tenant's other keys can still spend
// what is left.
func TestSpentRefusal_NamesTheBudgetThatRanOut(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	key := auth.DailyBudget{TenantID: "t", KeyID: "k1", KeyTokensPerDay: 1000, TenantTokensPerDay: 10000}
	other := auth.DailyBudget{TenantID: "t", KeyID: "k2", TenantTokensPerDay: 10000}
	if ok, _, _ := l.AllowInputTokens(key, 900); !ok {
		t.Fatal("900 on k1 refused")
	}
	if ok, _, _ := l.AllowInputTokens(other, 8600); !ok {
		t.Fatal("8600 on k2 refused")
	}
	if ok, _, _ := l.AllowOutputTokens(key, 300); ok {
		t.Fatal("300 output tokens fit a key with 100 left")
	}
	if r := auth.ExplainSpent(l, key); r != auth.RefusalKeyBudget {
		t.Errorf("key overflowed, tenant has room: %v, want RefusalKeyBudget", r)
	}
	// The tenant's own budget overflowing is named as the organization's.
	if ok, _, _ := l.AllowOutputTokens(other, 1000); ok {
		t.Fatal("1000 output tokens fit a tenant with little left")
	}
	if r := auth.ExplainSpent(l, other); r != auth.RefusalTenantBudget {
		t.Errorf("tenant overflowed: %v, want RefusalTenantBudget", r)
	}
}

// TestMinute_ChargeInputTokens: the prompt tokens the backend reported
// beyond the admission estimate drain the key's ITPM bucket after the
// response, so the next request sees them; capped at one minute's capacity.
func TestMinute_ChargeInputTokens(t *testing.T) {
	l := auth.NewMinuteRateLimiter()
	if !l.AllowInputTokens("t", "k", "m", 1000, 100) {
		t.Fatal("first 100 refused")
	}
	l.ChargeInputTokens("t", "k", "m", 1000, 950)
	if l.AllowInputTokens("t", "k", "m", 1000, 100) {
		t.Error("100 more admitted after the backend reported 950 tokens beyond the estimate")
	}
	if !l.AllowInputTokens("t", "other", "m", 1000, 100) {
		t.Error("another key was charged")
	}
}
