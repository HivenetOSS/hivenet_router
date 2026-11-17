// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

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
	for _, tc := range []struct {
		name    string
		mutate  func(*config.Config)
		api     auth.Provider
		wantErr bool
	}{
		// Defaults accept every auth mode.
		{"defaults, no auth", nil, auth.NewNoAuthProvider(), false},
		{"defaults, static keys", nil, static, false},
		// Requiring an allowlist without API keys would refuse every model.
		{"require allowlist, no auth", func(c *config.Config) { c.RequireModelAllowlist = true }, auth.NewNoAuthProvider(), true},
		{"require allowlist, static keys", func(c *config.Config) { c.RequireModelAllowlist = true }, static, false},
		{"require allowlist, dynamic keys", func(c *config.Config) { c.RequireModelAllowlist = true }, auth.NewDynamicKeyProvider(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			if tc.mutate != nil {
				tc.mutate(cfg)
			}
			if err := router.ValidateIsolationConfig(cfg, tc.api); (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}
