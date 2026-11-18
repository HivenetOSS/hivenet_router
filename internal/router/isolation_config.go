// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"fmt"
	"net"
	"strings"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/config"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/redirect"
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

// ValidateIsolationConfig runs every tenant-isolation check:
// ValidateIsolationSettings, then ValidateIsolationProviders.
func ValidateIsolationConfig(cfg *config.Config, apiProv, adminProv auth.Provider) error {
	if err := ValidateIsolationSettings(cfg); err != nil {
		return err
	}
	return ValidateIsolationProviders(cfg, apiProv, adminProv)
}

// ValidateIsolationSettings checks the tenant-isolation settings that need no
// auth provider. router.New runs it first, right after ApplyMultiTenant and
// before anything it cannot undo (--reset-disk-db), so a refused start
// changes nothing. They are startup-only, so a SIGHUP does not recheck them.
func ValidateIsolationSettings(cfg *config.Config) error {
	if cfg.MultiTenant {
		switch {
		case cfg.MetricsBearerToken == "" && !metrics.IsPrivateBindAddr(cfg.MetricsPort):
			return fmt.Errorf("--multi-tenant refuses an open /metrics on %q, which is not a loopback or private address: its series name every tenant and key; set --metrics-bearer-token-file (HIVENET_ROUTER_METRICS_BEARER_TOKEN) or bind --metrics-port to a loopback or private IP", cfg.MetricsPort)
		case cfg.SyncStore != "valkey":
			// Without shared counters each router of a site enforces every
			// per-key and tenant limit on its own: with N routers a key gets
			// N times its limits. The router cannot know how many routers a
			// site runs, so it requires shared counters always; a site with
			// one router runs a local Valkey.
			return fmt.Errorf("--multi-tenant needs shared counters (--sync-store=valkey), however many routers the site runs: without them every router enforces each key's and tenant's limits on its own; a site with one router runs a local Valkey")
		case cfg.SyncValkeyPassword == "":
			return fmt.Errorf("--multi-tenant needs a Valkey password (--sync-valkey-password-file or HIVENET_ROUTER_SYNC_VALKEY_PASSWORD): the shared store holds every tenant's usage")
		}
	}
	if cfg.CacheSalt {
		secret := cfg.CacheSaltSecret
		if len(secret) < cachesalt.MinSecretBytes {
			return fmt.Errorf("--cache-salt needs a cache-salt secret of at least %d bytes (HIVENET_ROUTER_CACHE_SALT_SECRET or --cache-salt-secret-file), got %d", cachesalt.MinSecretBytes, len(secret))
		}
		for _, other := range otherSecrets(cfg) {
			if other.value != "" && secret == other.value {
				return fmt.Errorf("the cache-salt secret must differ from the %s: whoever holds that one could compute every tenant's salt", other.name)
			}
		}
	}
	return nil
}

// ValidateIsolationProviders checks the tenant-isolation settings against the
// auth providers. It runs at startup (router.New) and on SIGHUP before the
// new providers are swapped in, so a reload that breaks a rule keeps the
// previous providers. apiProv is the provider as built from the config,
// before the redirect wrapper.
func ValidateIsolationProviders(cfg *config.Config, apiProv, adminProv auth.Provider) error {
	_, apiNoAuth := apiProv.(*auth.NoAuthProvider)
	_, adminNoAuth := adminProv.(*auth.NoAuthProvider)
	if cfg.MultiTenant {
		switch {
		case apiNoAuth:
			return fmt.Errorf("--multi-tenant needs API keys: with API auth mode none every caller is the same tenant")
		case adminNoAuth:
			return fmt.Errorf("--multi-tenant refuses unauthenticated admin endpoints (HIVENET_ROUTER_ALLOW_INSECURE_ADMIN): any caller could read and change every customer's keys")
		}
	}
	if apiNoAuth && cfg.RequireModelAllowlist {
		return fmt.Errorf("--require-model-allowlist (HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST) needs API keys: with API auth mode none no request carries an allowlist, so every model would be refused")
	}
	return nil
}

// IsolationWarnings returns what a multi-tenant router allows but should not
// run with: a Valkey reached over the network without TLS, which sends the
// password and every tenant's counters in clear. router.New logs them.
func IsolationWarnings(cfg *config.Config) []string {
	if !cfg.MultiTenant || cfg.SyncStore != "valkey" || cfg.SyncValkeyTLS {
		return nil
	}
	for _, addr := range strings.Split(cfg.SyncValkeyAddrs, ",") {
		if addr = strings.TrimSpace(addr); addr != "" && !isLoopbackAddr(addr) {
			return []string{fmt.Sprintf("--multi-tenant with Valkey at %s without --sync-valkey-tls: the Valkey password and every tenant's usage cross the network in clear", addr)}
		}
	}
	return nil
}

// isLoopbackAddr reports whether a host:port names this host.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// newCacheSalter returns the per-tenant salter, or nil when salting is off.
// ValidateIsolationConfig has already checked the secret.
func newCacheSalter(cfg *config.Config) (*cachesalt.Salter, error) {
	if !cfg.CacheSalt {
		return nil, nil
	}
	return cachesalt.New([]byte(cfg.CacheSaltSecret), cachesalt.DefaultCapabilities())
}

// namedSecret is one of the router's secrets, named for an error message.
type namedSecret struct{ name, value string }

// otherSecrets lists every secret of the router other than the cache-salt
// secret. The salt secret must differ from all of them: each is held by more
// parties than the router (the JWT secret by every agent, a redirect secret by
// every site that verifies redirect tokens, the Valkey password by the shared
// store, the metrics token by every scraper) or protects something else, and
// with it a salt could be computed.
func otherSecrets(cfg *config.Config) []namedSecret {
	out := []namedSecret{
		{"JWT secret, which agents hold", cfg.JWTSecret},
		{"key-store secret", cfg.KeyStoreSecret},
		{"Valkey password", cfg.SyncValkeyPassword},
		{"/metrics bearer token, which every scraper holds", cfg.MetricsBearerToken},
	}
	if secrets, err := redirect.ParseSecrets(cfg.RedirectSecrets); err == nil {
		for _, s := range secrets {
			out = append(out, namedSecret{"redirect secret " + s.ID + ", which every site verifying redirect tokens holds", string(s.Key)})
		}
	}
	return out
}
