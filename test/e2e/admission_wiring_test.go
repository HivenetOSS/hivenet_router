// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Integration tests that prove the cross-config admission check is actually
// wired into router.New — not merely unit-tested in isolation. They construct a
// real router from on-disk policy + auth files and assert startup is rejected
// (or accepted) based on the input-bucket-covers-context invariant.
package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

const admissionServerlessPolicy = `models: [gemma-4-31b]
mode: serverless
routing_policy:
  strategy: least-loaded
max_input_tokens: 262144
`

// admissionAuthYAML returns an api-key auth config with one serverless key whose
// input token bucket is the given size.
func admissionAuthYAML(itpm int) string {
	return fmt.Sprintf(`api:
  mode: api-key
  keys:
    - key_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      metadata:
        name: test-key
        owner: acme
      models: [gemma-4-31b]
      quota:
        input_tokens_per_minute: %d
admin:
  mode: none
`, itpm)
}

// writeAdmissionFile writes content into dir/name and returns the path.
func writeAdmissionFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

// admissionRouterCfg builds a minimal config that reaches router.New's admission
// check: a JWT secret, a temp Badger dir, and the given policy + auth files.
func admissionRouterCfg(t *testing.T, itpm int) *config.Config {
	t.Helper()
	// api-key mode with admin=none requires the explicit insecure-admin opt-in.
	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.JWTSecret = jwtSecret
	cfg.DiskDBPath = filepath.Join(dir, "badger")
	cfg.PolicyFile = writeAdmissionFile(t, dir, "policy.yaml", admissionServerlessPolicy)
	cfg.AuthConfigFile = writeAdmissionFile(t, dir, "auth.yaml", admissionAuthYAML(itpm))
	return cfg
}

// TestAdmissionWiring_StartupRejectsUndersizedBucket proves the check is wired
// into router.New: a serverless key whose input bucket cannot hold one
// max-size prompt fails startup, with an error naming both numbers.
func TestAdmissionWiring_StartupRejectsUndersizedBucket(t *testing.T) {
	_, err := router.New(admissionRouterCfg(t, 100))
	if err == nil {
		t.Fatal("expected router.New to reject the undersized input bucket, got nil")
	}
	if !strings.Contains(err.Error(), "input_tokens_per_minute") || !strings.Contains(err.Error(), "262144") {
		t.Errorf("startup error should name both values, got: %v", err)
	}
}

// TestAdmissionWiring_StartupAcceptsCoveringBucket is the positive control: the
// same policy with a bucket that covers max_input_tokens starts cleanly.
func TestAdmissionWiring_StartupAcceptsCoveringBucket(t *testing.T) {
	r, err := router.New(admissionRouterCfg(t, 519_540))
	if err != nil {
		t.Fatalf("router.New rejected a valid config: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
