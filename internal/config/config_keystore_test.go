// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package config

import (
	"testing"
)

// The key-store max age is a security bound: invalid env values must be
// reported (startup stops), never silently replaced by the default.
func TestLoadFromEnv_InvalidKeyStoreValuesReported(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_KEY_STORE_MAX_AGE", "15min")
	t.Setenv("HIVENET_ROUTER_RESET_KEY_STORE", "yes please")
	cfg := LoadFromEnv()
	if len(cfg.EnvErrors) != 2 {
		t.Fatalf("EnvErrors = %v, want 2", cfg.EnvErrors)
	}
}

func TestLoadFromEnv_NegativeMaxAgeReported(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_KEY_STORE_MAX_AGE", "-1m")
	if cfg := LoadFromEnv(); len(cfg.EnvErrors) != 1 {
		t.Fatalf("EnvErrors = %v, want 1", cfg.EnvErrors)
	}
}

// The env secret is trimmed like --key-store-secret-file: a trailing newline
// (common when a Secret is created from a file) must not change the MAC key.
func TestLoadFromEnv_KeyStoreSecretTrimmed(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_KEY_STORE_SECRET", "  s3cret\n")
	if cfg := LoadFromEnv(); cfg.KeyStoreSecret != "s3cret" {
		t.Fatalf("KeyStoreSecret = %q, want %q", cfg.KeyStoreSecret, "s3cret")
	}
}

func TestLoadFromEnv_ValidKeyStoreValues(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_KEY_STORE_MAX_AGE", "0")
	t.Setenv("HIVENET_ROUTER_RESET_KEY_STORE", "true")
	t.Setenv("HIVENET_ROUTER_KEY_STORE_SECRET", "s3cret")
	cfg := LoadFromEnv()
	if len(cfg.EnvErrors) != 0 || cfg.KeyStoreMaxAge != 0 || !cfg.ResetKeyStore || cfg.KeyStoreSecret != "s3cret" {
		t.Fatalf("cfg = maxAge %v reset %v secret %q errs %v", cfg.KeyStoreMaxAge, cfg.ResetKeyStore, cfg.KeyStoreSecret, cfg.EnvErrors)
	}
}
