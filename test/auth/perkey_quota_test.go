// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/auth"
)

// TestInMemoryLimiter_PerKeyBucketsWithinTenant: two keys of one tenant each
// have their own RPM and daily-token buckets, flat and per-model. One key
// exhausting its budget leaves the other key's budget whole.
func TestInMemoryLimiter_PerKeyBucketsWithinTenant(t *testing.T) {
	for _, model := range []string{"", "m"} {
		t.Run(fmt.Sprintf("model=%q", model), func(t *testing.T) {
			l := auth.NewInMemoryLimiter()

			// RPM: k1 drains its burst of 3; k2 still has its own 3.
			for i := 0; i < 3; i++ {
				if ok, _, _ := l.AllowRequest("t", "k1", model, 3); !ok {
					t.Fatalf("k1 request %d denied within its RPM", i)
				}
			}
			if ok, _, _ := l.AllowRequest("t", "k1", model, 3); ok {
				t.Fatal("k1 allowed past its RPM")
			}
			if ok, remaining, _ := l.AllowRequest("t", "k2", model, 3); !ok || remaining != 2 {
				t.Errorf("k2 after k1 drained: allowed=%v remaining=%d, want true / 2", ok, remaining)
			}

			// TPD: k1 spends its whole 100; k2 still has 100.
			if ok, _, _ := l.AllowInputTokens("t", "k1", model, 100, 100); !ok {
				t.Fatal("k1 charge within its TPD denied")
			}
			if ok, _, _ := l.AllowOutputTokens("t", "k1", model, 100, 1); ok {
				t.Fatal("k1 allowed past its TPD")
			}
			if rem, _ := l.RemainingTokens("t", "k2", model, 100); rem != 100 {
				t.Errorf("k2 remaining %d after k1 spent its budget, want 100", rem)
			}
		})
	}
}

// TestInMemoryLimiter_KeyCeilingNotOverwrittenByOtherKey: keys of one tenant
// with different RPM limits interleave requests. Each keeps its own ceiling;
// before buckets were per key, the shared bucket's rate flipped to whichever
// key called last, so the generous key was throttled to the strict key's rate.
func TestInMemoryLimiter_KeyCeilingNotOverwrittenByOtherKey(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	big, small := 0, 0
	for i := 0; i < 100; i++ {
		if ok, _, _ := l.AllowRequest("t", "generous", "", 60); ok {
			big++
		}
		if ok, _, _ := l.AllowRequest("t", "strict", "", 6); ok {
			small++
		}
	}
	// Within one instant each bucket admits its burst (= its RPM).
	if big != 60 || small != 6 {
		t.Errorf("admitted generous=%d strict=%d, want 60 and 6", big, small)
	}
}

// TestInMemoryLimiter_SameKeyIDDifferentTenantIsolated: a key ID reused by
// another tenant (e.g. reassigned by the key-sync service) does not inherit
// the first tenant's usage.
func TestInMemoryLimiter_SameKeyIDDifferentTenantIsolated(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	if ok, _, _ := l.AllowInputTokens("tenant-a", "key-1", "", 100, 100); !ok {
		t.Fatal("charge denied")
	}
	if rem, _ := l.RemainingTokens("tenant-b", "key-1", "", 100); rem != 100 {
		t.Errorf("tenant-b key-1 remaining %d, want 100 (a fresh bucket)", rem)
	}
	if ok, _, _ := l.AllowRequest("tenant-a", "key-1", "", 1); !ok {
		t.Fatal("first request denied")
	}
	if ok, _, _ := l.AllowRequest("tenant-b", "key-1", "", 1); !ok {
		t.Error("tenant-b key-1 shares tenant-a's RPM bucket")
	}
}

// TestInMemoryLimiter_TenantUsageSumsKeysAndModels: the usage reported for
// hivenet_tenant_tokens_used_today is the tenant's total across its keys and
// models, counting only tokens actually charged, and another tenant's usage
// never adds to it.
func TestInMemoryLimiter_TenantUsageSumsKeysAndModels(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	var mu sync.Mutex
	last := map[string]int{}
	l.SetOnTokensUsed(func(tenant string, used int) {
		mu.Lock()
		last[tenant] = used
		mu.Unlock()
	})

	l.AllowInputTokens("t", "k1", "", 1000, 100)   //nolint:errcheck
	l.AllowInputTokens("t", "k2", "m", 1000, 50)   //nolint:errcheck
	l.AllowOutputTokens("t", "k1", "", 1000, 25)   //nolint:errcheck
	l.AllowInputTokens("other", "k9", "", 1000, 7) //nolint:errcheck
	// A refused charge (k2 asks for more than it has left) serves nothing, so
	// the tenant total does not move, even though k2's own bucket is marked
	// full.
	l.AllowInputTokens("t", "k2", "m", 1000, 2000) //nolint:errcheck

	mu.Lock()
	defer mu.Unlock()
	if last["t"] != 100+50+25 || last["other"] != 7 {
		t.Errorf("reported usage %v, want t=175 other=7", last)
	}
	if got := l.TenantUsageToday("t"); got != 175 {
		t.Errorf("TenantUsageToday = %d, want 175", got)
	}
}

// memQuotaStore is an in-memory DailyQuotaStore.
type memQuotaStore struct {
	mu sync.Mutex
	m  map[string]int64
}

func (s *memQuotaStore) GetTokensUsed(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

func (s *memQuotaStore) SetTokensUsed(key string, n int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = n
	return nil
}

// TestBadgerLimiter_PerKeyPersistence: daily counters persist per key under
// versioned per-key bucket names and survive a restart per key. Counters
// written before buckets were per key (no version marker) are not read.
func TestBadgerLimiter_PerKeyPersistence(t *testing.T) {
	day := time.Now().UTC().Unix() / 86400
	store := &memQuotaStore{m: map[string]int64{
		fmt.Sprintf("quotaTPD:t:%d", day): 999, // legacy per-tenant counter
	}}
	l := auth.NewBadgerLimiter(store)
	if rem, _ := l.RemainingTokens("t", "k1", "", 1000); rem != 1000 {
		t.Errorf("remaining %d with only a legacy counter stored, want 1000 (legacy not read)", rem)
	}
	l.AllowInputTokens("t", "k1", "", 1000, 300) //nolint:errcheck
	l.AllowInputTokens("t", "k2", "", 1000, 40)  //nolint:errcheck
	l.Flush()

	perKey := 0
	for k := range store.m {
		if strings.HasPrefix(k, "quotaTPD:k1\x00t\x00") {
			perKey++
		}
	}
	if perKey != 2 {
		t.Errorf("stored keys %v, want two versioned per-key entries (one per key)", store.m)
	}

	restarted := auth.NewBadgerLimiter(store)
	if rem, _ := restarted.RemainingTokens("t", "k1", "", 1000); rem != 700 {
		t.Errorf("k1 remaining after restart %d, want 700", rem)
	}
	if rem, _ := restarted.RemainingTokens("t", "k2", "", 1000); rem != 960 {
		t.Errorf("k2 remaining after restart %d, want 960", rem)
	}
}

// TestTenants_AggregatesKeyLimits: Tenants() (which seeds the per-tenant
// limit gauges) reports each tenant's total ceiling: its keys' limits summed,
// unlimited if any key is unlimited, per-model entries summed per model, and
// a single key's limits unchanged.
func TestTenants_AggregatesKeyLimits(t *testing.T) {
	key := func(token, owner string, q auth.QuotaConfig) auth.APIKeyEntry {
		e := staticEntry(token, owner)
		e.Quota = q
		return e
	}
	perModel := func(rpm, tpd int) auth.QuotaConfig {
		return auth.QuotaConfig{PerModel: map[string]*auth.PerModelQuotaConfig{
			"m": {RequestsPerMinutePerReplica: &rpm, TokensPerDay: &tpd},
		}}
	}
	p, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		key("a1", "summed", auth.QuotaConfig{RequestsPerMinute: 10, TokensPerDay: 1000}),
		key("a2", "summed", auth.QuotaConfig{RequestsPerMinute: 20, TokensPerDay: 500}),
		key("b1", "one-unlimited", auth.QuotaConfig{RequestsPerMinute: 10}),
		key("b2", "one-unlimited", auth.QuotaConfig{RequestsPerMinute: 0}),
		key("c1", "per-model", perModel(5, 100)),
		key("c2", "per-model", perModel(7, 300)),
		key("d1", "mixed", auth.QuotaConfig{RequestsPerMinute: 4, TokensPerDay: 40}),
		key("d2", "mixed", perModel(5, 100)),
		key("e1", "single", auth.QuotaConfig{RequestsPerMinute: 9, TokensPerDay: 90}),
	})
	if err != nil {
		t.Fatal(err)
	}
	got := p.Tenants()
	check := func(tenant string, rpm, tpd int, m *auth.PerModelQuotaLimits) {
		t.Helper()
		g := got[tenant]
		if g.RequestsPerMinute != rpm || g.TokensPerDay != tpd {
			t.Errorf("%s: rpm %d tpd %d, want %d / %d", tenant, g.RequestsPerMinute, g.TokensPerDay, rpm, tpd)
		}
		if m != nil && g.PerModel["m"] != *m {
			t.Errorf("%s: per-model %+v, want %+v", tenant, g.PerModel["m"], *m)
		}
	}
	check("summed", 30, 1500, nil)
	check("one-unlimited", 0, 0, nil)
	check("per-model", 0, 0, &auth.PerModelQuotaLimits{RequestsPerMinutePerReplica: 12, TokensPerDay: 400})
	check("mixed", 4, 40, &auth.PerModelQuotaLimits{RequestsPerMinutePerReplica: 5, TokensPerDay: 100})
	check("single", 9, 90, nil)

	dyn := auth.NewDynamicKeyProvider()
	for i, q := range []auth.QuotaLimits{{RequestsPerMinute: 3}, {RequestsPerMinute: 4}} {
		id := fmt.Sprintf("key-%d", i)
		if err := dyn.Upsert("v1", auth.DynamicKeyEntry{ID: id, KeyHash: hashOf(id), Owner: "org", Name: "n", Enabled: true, Quota: q}); err != nil {
			t.Fatal(err)
		}
	}
	if rpm := dyn.Tenants()["org"].RequestsPerMinute; rpm != 7 {
		t.Errorf("dynamic tenant rpm %d, want 7 (3 + 4)", rpm)
	}
}
