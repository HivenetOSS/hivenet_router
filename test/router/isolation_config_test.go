// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

// salt enables per-tenant cache salting with the given secret.
func salt(secret string) func(*config.Config) {
	return func(c *config.Config) { c.CacheSalt, c.CacheSaltSecret = true, secret }
}

// TestValidateIsolationConfig: each tenant-isolation setting is checked
// against the API auth provider; a combination that cannot work refuses to
// start (and a SIGHUP that would create it keeps the previous providers).
func TestValidateIsolationConfig(t *testing.T) {
	static, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Metadata: auth.KeyMetadata{Name: "k", Owner: "t"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	admin, err := auth.NewStaticAdminKeyProvider([]string{"admin-key"})
	if err != nil {
		t.Fatal(err)
	}
	// multi turns on multi-tenant mode with what it needs to start: a salt
	// secret and a metrics token.
	multi := func(more ...func(*config.Config)) func(*config.Config) {
		return func(c *config.Config) {
			c.MultiTenant = true
			c.CacheSaltSecret = strings.Repeat("s", 32)
			c.MetricsBearerToken = "metrics-token"
			router.ApplyMultiTenant(c)
			for _, f := range more {
				f(c)
			}
		}
	}
	for _, tc := range []struct {
		name    string
		mutate  func(*config.Config)
		api     auth.Provider
		admin   auth.Provider
		wantErr bool
	}{
		// Defaults accept every auth mode.
		{"defaults, no auth", nil, auth.NewNoAuthProvider(), nil, false},
		{"defaults, static keys", nil, static, nil, false},
		// Requiring an allowlist without API keys would refuse every model.
		{"require allowlist, no auth", func(c *config.Config) { c.RequireModelAllowlist = true }, auth.NewNoAuthProvider(), nil, true},
		{"require allowlist, static keys", func(c *config.Config) { c.RequireModelAllowlist = true }, static, nil, false},
		{"require allowlist, dynamic keys", func(c *config.Config) { c.RequireModelAllowlist = true }, auth.NewDynamicKeyProvider(), nil, false},
		// Salting needs its own secret: at least 32 bytes, not the JWT secret
		// (agents hold it) and not the key-store secret.
		{"salt without secret", salt(""), static, nil, true},
		{"salt with short secret", salt(strings.Repeat("s", 31)), static, nil, true},
		{"salt with the JWT secret", func(c *config.Config) { c.CacheSalt, c.CacheSaltSecret = true, c.JWTSecret }, static, nil, true},
		{"salt with the key-store secret", func(c *config.Config) { c.CacheSalt, c.CacheSaltSecret = true, c.KeyStoreSecret }, static, nil, true},
		{"salt with its own secret", salt(strings.Repeat("s", 32)), static, nil, false},
		{"secret without salting", func(c *config.Config) { c.CacheSaltSecret = "short" }, static, nil, false},
		// Multi-tenant mode: keys on both surfaces, a protected /metrics, and
		// the salt rules (salting is forced on).
		{"multi-tenant, complete", multi(), static, admin, false},
		{"multi-tenant, dynamic keys", multi(), auth.NewDynamicKeyProvider(), admin, false},
		{"multi-tenant, no API auth", multi(), auth.NewNoAuthProvider(), admin, true},
		{"multi-tenant, insecure admin", multi(), static, auth.NewNoAuthProvider(), true},
		{"multi-tenant, open metrics", multi(func(c *config.Config) { c.MetricsBearerToken = "" }), static, admin, true},
		{"multi-tenant, metrics on 0.0.0.0", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "0.0.0.0:2112" }), static, admin, true},
		{"multi-tenant, metrics on a private address", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "10.0.0.5:2112" }), static, admin, false},
		{"multi-tenant, no salt secret", multi(func(c *config.Config) { c.CacheSaltSecret = "" }), static, admin, true},
		// Several routers in the site need shared counters, with a password.
		{"multi-tenant, 3 routers, no sync store", multi(func(c *config.Config) { c.SyncMinRouters = 3 }), static, admin, true},
		{"multi-tenant, 3 routers, valkey without password", multi(func(c *config.Config) { c.SyncMinRouters, c.SyncStore = 3, "valkey" }), static, admin, true},
		{"multi-tenant, 3 routers, valkey with password", multi(func(c *config.Config) { c.SyncMinRouters, c.SyncStore, c.SyncValkeyPassword = 3, "valkey", "pw" }), static, admin, false},
		{"multi-tenant, one router, valkey without password", multi(func(c *config.Config) { c.SyncStore = "valkey" }), static, admin, true},
		{"single-tenant, 3 routers, no sync store", func(c *config.Config) { c.SyncMinRouters = 3 }, static, admin, false},
		// An explicit opt-out of cache isolation needs no salt secret.
		{"multi-tenant, salting explicitly off", func(c *config.Config) {
			c.MultiTenant, c.CacheSaltSet, c.CacheSalt, c.MetricsBearerToken = true, true, false, "metrics-token"
			router.ApplyMultiTenant(c)
		}, static, admin, false},
		// Without multi-tenant mode none of these is required.
		{"single-tenant, open metrics and insecure admin", nil, static, auth.NewNoAuthProvider(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.JWTSecret = strings.Repeat("j", 40)
			cfg.KeyStoreSecret = strings.Repeat("k", 40)
			if tc.mutate != nil {
				tc.mutate(cfg)
			}
			adminProv := tc.admin
			if adminProv == nil {
				adminProv = admin
			}
			if err := router.ValidateIsolationConfig(cfg, tc.api, adminProv); (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

// TestApplyMultiTenant: multi-tenant mode turns on every isolation switch.
// The allowlist and agent-identity settings cannot be turned back off;
// prefix-cache isolation can, but only when set off explicitly. Without
// multi-tenant mode the settings are left as configured.
func TestApplyMultiTenant(t *testing.T) {
	cfg := config.DefaultConfig()
	router.ApplyMultiTenant(cfg)
	if cfg.RequireModelAllowlist || cfg.CacheSalt || cfg.HideAgentIdentity {
		t.Errorf("without multi-tenant: %+v, want every switch off", cfg)
	}
	cfg.MultiTenant = true
	cfg.RequireModelAllowlist, cfg.CacheSalt, cfg.HideAgentIdentity = false, false, false
	router.ApplyMultiTenant(cfg)
	if !cfg.RequireModelAllowlist || !cfg.CacheSalt || !cfg.HideAgentIdentity {
		t.Errorf("multi-tenant: allowlist=%v salt=%v hide=%v, want all on", cfg.RequireModelAllowlist, cfg.CacheSalt, cfg.HideAgentIdentity)
	}

	optOut := config.DefaultConfig()
	optOut.MultiTenant, optOut.CacheSaltSet, optOut.CacheSalt = true, true, false
	router.ApplyMultiTenant(optOut)
	if optOut.CacheSalt || !optOut.RequireModelAllowlist || !optOut.HideAgentIdentity {
		t.Errorf("explicit --cache-salt=false: salt=%v allowlist=%v hide=%v, want salt off and the rest on", optOut.CacheSalt, optOut.RequireModelAllowlist, optOut.HideAgentIdentity)
	}
}
