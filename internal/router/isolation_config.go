// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"fmt"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/config"
	"hivenet_router/internal/metrics"
)

// ApplyMultiTenant turns on every isolation switch when cfg.MultiTenant is
// set: model allowlists required, per-tenant prefix-cache isolation, agent
// identity hidden from clients. The allowlist and agent-identity settings
// cannot be switched back off. Prefix-cache isolation can, but only
// explicitly (--cache-salt=false or HIVENET_ROUTER_CACHE_SALT=false), and the
// router then warns at startup and counts every LLM agent in
// hivenet_cache_isolation_gaps. router.New calls it first; it is idempotent.
func ApplyMultiTenant(cfg *config.Config) {
	if !cfg.MultiTenant {
		return
	}
	cfg.RequireModelAllowlist = true
	if !cfg.CacheSaltSet {
		cfg.CacheSalt = true
	}
	cfg.HideAgentIdentity = true
}

// cacheSaltOptedOut reports a multi-tenant router whose prefix-cache
// isolation was explicitly switched off.
func cacheSaltOptedOut(cfg *config.Config) bool {
	return cfg.MultiTenant && !cfg.CacheSalt
}

// ValidateIsolationConfig checks the tenant-isolation settings against the
// auth providers. It runs at startup (router.New) and on SIGHUP before the
// new providers are swapped in, so a reload that breaks a rule keeps the
// previous providers. apiProv is the provider as built from the config,
// before the redirect wrapper.
func ValidateIsolationConfig(cfg *config.Config, apiProv, adminProv auth.Provider) error {
	_, apiNoAuth := apiProv.(*auth.NoAuthProvider)
	_, adminNoAuth := adminProv.(*auth.NoAuthProvider)
	if cfg.MultiTenant {
		switch {
		case apiNoAuth:
			return fmt.Errorf("--multi-tenant needs API keys: with API auth mode none every caller is the same tenant")
		case adminNoAuth:
			return fmt.Errorf("--multi-tenant refuses unauthenticated admin endpoints (HIVENET_ROUTER_ALLOW_INSECURE_ADMIN): any caller could read and change every customer's keys")
		case cfg.MetricsBearerToken == "" && metrics.IsWildcardAddr(cfg.MetricsPort):
			return fmt.Errorf("--multi-tenant refuses an open /metrics on every interface (%s): its series name every tenant and key; set --metrics-bearer-token-file (HIVENET_ROUTER_METRICS_BEARER_TOKEN) or bind --metrics-port to a private address", cfg.MetricsPort)
		case cfg.SyncMinRouters > 1 && cfg.SyncStore != "valkey":
			// Without shared counters each of N routers enforces every
			// per-key limit on its own: a key gets N times its limits.
			return fmt.Errorf("--multi-tenant with %d routers in the site (--sync-min-routers) needs --sync-store=valkey: without shared counters every router enforces each key's limits on its own", cfg.SyncMinRouters)
		case cfg.SyncStore == "valkey" && cfg.SyncValkeyPassword == "":
			return fmt.Errorf("--multi-tenant needs a Valkey password (--sync-valkey-password-file or HIVENET_ROUTER_SYNC_VALKEY_PASSWORD): the shared store holds every tenant's usage")
		}
	}
	if apiNoAuth && cfg.RequireModelAllowlist {
		return fmt.Errorf("--require-model-allowlist (HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST) needs API keys: with API auth mode none no request carries an allowlist, so every model would be refused")
	}
	if cfg.CacheSalt {
		switch secret := cfg.CacheSaltSecret; {
		case len(secret) < cachesalt.MinSecretBytes:
			return fmt.Errorf("--cache-salt needs a cache-salt secret of at least %d bytes (HIVENET_ROUTER_CACHE_SALT_SECRET or --cache-salt-secret-file), got %d", cachesalt.MinSecretBytes, len(secret))
		case secret == cfg.JWTSecret:
			// Every agent holds the JWT secret; salts must not be computable
			// outside the router.
			return fmt.Errorf("the cache-salt secret must differ from the JWT secret, which agents hold")
		case secret == cfg.KeyStoreSecret:
			return fmt.Errorf("the cache-salt secret must differ from the key-store secret")
		}
	}
	return nil
}

// newCacheSalter returns the per-tenant salter, or nil when salting is off.
// ValidateIsolationConfig has already checked the secret.
func newCacheSalter(cfg *config.Config) (*cachesalt.Salter, error) {
	if !cfg.CacheSalt {
		return nil, nil
	}
	return cachesalt.New([]byte(cfg.CacheSaltSecret), cachesalt.DefaultCapabilities())
}
