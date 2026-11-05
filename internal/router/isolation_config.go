// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"fmt"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/config"
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
