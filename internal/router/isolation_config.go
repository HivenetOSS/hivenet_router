// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"fmt"

	"hivenet_router/internal/auth"
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
	return nil
}
