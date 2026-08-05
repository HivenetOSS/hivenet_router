// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
)

// With the default config both /v1/* and /admin/* are mode: none. Because admin
// controls policy and API-key management, the loader must refuse to start rather
// than silently expose it — unless the operator explicitly opts in.
func TestProvidersFromConfig_AdminNoAuth_FailsClosed(t *testing.T) {
	// Ensure the opt-out is off for this case even if the ambient env sets it.
	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "false")

	_, _, _, err := auth.ProvidersFromConfig(config.DefaultConfig())
	if err == nil {
		t.Fatal("expected ProvidersFromConfig to error when admin auth is none without opt-in, got nil")
	}
}

// The escape hatch lets an operator deliberately run with no admin auth.
func TestProvidersFromConfig_AdminNoAuth_AllowedWithOptIn(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")

	_, admin, _, err := auth.ProvidersFromConfig(config.DefaultConfig())
	if err != nil {
		t.Fatalf("expected success with HIVENET_ROUTER_ALLOW_INSECURE_ADMIN=true, got %v", err)
	}
	if admin == nil {
		t.Fatal("expected a non-nil admin provider")
	}
}
