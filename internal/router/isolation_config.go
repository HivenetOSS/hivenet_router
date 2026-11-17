// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"fmt"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/config"
	"hivenet_router/internal/redirect"
)

// ValidateIsolationConfig checks the tenant-isolation settings against the
// API auth provider. It runs at startup (router.New) and on SIGHUP before the
// new providers are swapped in, so a reload that breaks a rule keeps the
// previous providers. apiProv is the provider as built from the config,
// before the redirect wrapper.
func ValidateIsolationConfig(cfg *config.Config, apiProv auth.Provider) error {
	if _, noAuth := apiProv.(*auth.NoAuthProvider); noAuth && cfg.RequireModelAllowlist {
		return fmt.Errorf("--require-model-allowlist (HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST) needs API keys: with API auth mode none no request carries an allowlist, so every model would be refused")
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
