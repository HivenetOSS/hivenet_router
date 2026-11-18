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
	// secret, a metrics token, and Valkey with a password.
	multi := func(more ...func(*config.Config)) func(*config.Config) {
		return func(c *config.Config) {
			c.MultiTenant = true
			c.CacheSaltSecret = strings.Repeat("s", 32)
			c.MetricsBearerToken = "metrics-token"
			c.SyncStore, c.SyncValkeyAddrs, c.SyncValkeyPassword = "valkey", "valkey.internal:6379", "valkey-password"
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
		// Salting needs its own secret: at least 32 bytes, and none of the
		// router's other secrets (agents hold the JWT secret, every site the
		// redirect secrets, the shared store the Valkey password).
		{"salt without secret", salt(""), static, nil, true},
		{"salt with short secret", salt(strings.Repeat("s", 31)), static, nil, true},
		{"salt with the JWT secret", func(c *config.Config) { c.CacheSalt, c.CacheSaltSecret = true, c.JWTSecret }, static, nil, true},
		{"salt with the key-store secret", func(c *config.Config) { c.CacheSalt, c.CacheSaltSecret = true, c.KeyStoreSecret }, static, nil, true},
		{"salt with a redirect secret", func(c *config.Config) {
			c.RedirectSecrets = "old:" + strings.Repeat("o", 32) + "\nnew:" + strings.Repeat("r", 32)
			c.CacheSalt, c.CacheSaltSecret = true, strings.Repeat("r", 32)
		}, static, nil, true},
		{"salt with the Valkey password", func(c *config.Config) {
			c.SyncValkeyPassword = strings.Repeat("v", 32)
			c.CacheSalt, c.CacheSaltSecret = true, strings.Repeat("v", 32)
		}, static, nil, true},
		{"salt with the metrics token", func(c *config.Config) {
			c.MetricsBearerToken = strings.Repeat("m", 32)
			c.CacheSalt, c.CacheSaltSecret = true, strings.Repeat("m", 32)
		}, static, nil, true},
		{"salt with its own secret", salt(strings.Repeat("s", 32)), static, nil, false},
		{"secret without salting", func(c *config.Config) { c.CacheSaltSecret = "short" }, static, nil, false},
		// Multi-tenant mode: keys on both surfaces, a protected /metrics, and
		// the salt rules (salting is forced on).
		{"multi-tenant, complete", multi(), static, admin, false},
		{"multi-tenant, dynamic keys", multi(), auth.NewDynamicKeyProvider(), admin, false},
		{"multi-tenant, no API auth", multi(), auth.NewNoAuthProvider(), admin, true},
		{"multi-tenant, insecure admin", multi(), static, auth.NewNoAuthProvider(), true},
		{"multi-tenant, open metrics", multi(func(c *config.Config) { c.MetricsBearerToken = "" }), static, admin, true},
		// Without a token /metrics must stay on a loopback or private address:
		// every interface, a public IP and a hostname are all refused.
		{"multi-tenant, metrics on 0.0.0.0", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "0.0.0.0:2112" }), static, admin, true},
		{"multi-tenant, metrics on a public IP", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "203.0.113.7:2112" }), static, admin, true},
		{"multi-tenant, metrics on a hostname", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "metrics.example.com:2112" }), static, admin, true},
		{"multi-tenant, metrics on a private address", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "10.0.0.5:2112" }), static, admin, false},
		{"multi-tenant, metrics on loopback", multi(func(c *config.Config) { c.MetricsBearerToken, c.MetricsPort = "", "127.0.0.1:2112" }), static, admin, false},
		{"multi-tenant, no salt secret", multi(func(c *config.Config) { c.CacheSaltSecret = "" }), static, admin, true},
		// Shared counters with a password, however many routers the site
		// runs: --sync-min-routers is left at its default of 1 here.
		{"multi-tenant, no sync store", multi(func(c *config.Config) { c.SyncStore = "" }), static, admin, true},
		{"multi-tenant, valkey without password", multi(func(c *config.Config) { c.SyncValkeyPassword = "" }), static, admin, true},
		{"multi-tenant, local valkey", multi(func(c *config.Config) { c.SyncValkeyAddrs = "127.0.0.1:6379" }), static, admin, false},
		{"single-tenant, 3 routers, no sync store", func(c *config.Config) { c.SyncMinRouters = 3 }, static, admin, false},
		// An explicit opt-out of cache isolation needs no salt secret.
		{"multi-tenant, salting explicitly off", multi(func(c *config.Config) {
			c.CacheSaltSet, c.CacheSalt, c.CacheSaltSecret = true, false, ""
		}), static, admin, false},
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

// TestIsolation_MultiTenantWarnsOnPlaintextValkey: a multi-tenant router
// warns, but starts, when it reaches Valkey over the network without TLS:
// the password and every tenant's counters would cross it in clear. A local
// Valkey, or one reached with TLS, gets no warning, nor does a single-tenant
// router.
func TestIsolation_MultiTenantWarnsOnPlaintextValkey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		warns  bool
	}{
		{"remote, no TLS", func(c *config.Config) { c.SyncValkeyAddrs = "valkey.internal:6379" }, true},
		{"one of several remote", func(c *config.Config) { c.SyncValkeyAddrs = "127.0.0.1:6379,10.0.0.9:26379" }, true},
		{"remote with TLS", func(c *config.Config) { c.SyncValkeyAddrs, c.SyncValkeyTLS = "valkey.internal:6379", true }, false},
		{"loopback", func(c *config.Config) { c.SyncValkeyAddrs = "127.0.0.1:6379" }, false},
		{"localhost", func(c *config.Config) { c.SyncValkeyAddrs = "localhost:6379" }, false},
		{"single-tenant", func(c *config.Config) { c.MultiTenant, c.SyncValkeyAddrs = false, "valkey.internal:6379" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.MultiTenant, cfg.SyncStore = true, "valkey"
			tc.mutate(cfg)
			if got := len(router.IsolationWarnings(cfg)) > 0; got != tc.warns {
				t.Errorf("warnings %v, want a warning %v", router.IsolationWarnings(cfg), tc.warns)
			}
		})
	}
}
