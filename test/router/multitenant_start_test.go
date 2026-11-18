// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

// multiTenantConfig is a multi-tenant config whose settings pass
// ValidateIsolationSettings (it is never connected to a real Valkey).
func multiTenantConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.JWTSecret = strings.Repeat("j", 40)
	cfg.MultiTenant = true
	cfg.CacheSaltSecret = strings.Repeat("s", 32)
	cfg.MetricsBearerToken = "metrics-token"
	cfg.SyncStore, cfg.SyncValkeyAddrs, cfg.SyncValkeyPassword = "valkey", "127.0.0.1:1", "valkey-password"
	cfg.RouterID = "router-test"
	cfg.DiskDBPath = filepath.Join(t.TempDir(), "badger")
	return cfg
}

// TestRouterNew_RefusedStartKeepsDiskDB: every isolation rule is checked
// before anything router.New cannot undo, so a multi-tenant start refused by
// one (a setting, or the API auth mode) leaves the disk database as it was,
// even with --reset-disk-db.
func TestRouterNew_RefusedStartKeepsDiskDB(t *testing.T) {
	// API auth mode none, with admin keys: refused by the isolation check on
	// the providers, not by the admin-auth check that runs while building them.
	noAPIAuth := filepath.Join(t.TempDir(), "auth.yaml")
	if err := os.WriteFile(noAPIAuth, []byte("api:\n  mode: none\nadmin:\n  mode: api-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HIVENET_ROUTER_ADMIN_API_KEYS", "admin-key")
	for name, mutate := range map[string]func(*config.Config){
		"open metrics":    func(c *config.Config) { c.MetricsBearerToken = "" },
		"no shared store": func(c *config.Config) { c.SyncStore = "" },
		"short salt":      func(c *config.Config) { c.CacheSaltSecret = "short" },
		"no API auth":     func(c *config.Config) { c.AuthConfigFile = noAPIAuth },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := multiTenantConfig(t)
			cfg.ResetDiskDB = true
			mutate(cfg)
			if err := os.MkdirAll(cfg.DiskDBPath, 0o700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(cfg.DiskDBPath, "keep-me")
			if err := os.WriteFile(marker, []byte("history"), 0o600); err != nil {
				t.Fatal(err)
			}
			r, err := router.New(cfg)
			if err == nil {
				r.Close() //nolint:errcheck
				t.Fatal("router.New accepted the setting")
			}
			if name == "no API auth" && !strings.Contains(err.Error(), "--multi-tenant needs API keys") {
				t.Fatalf("refused for another reason: %v", err)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Errorf("a refused start wiped the disk database: %v", err)
			}
		})
	}
}

// TestRouterNew_MultiTenantRefusesUnopenableAuditLog: a multi-tenant router
// whose audit log path cannot be opened refuses to start, instead of writing
// every tenant's audit lines to stdout with the application output. The audit
// log is opened once per process, so the check runs in a child process.
func TestRouterNew_MultiTenantRefusesUnopenableAuditLog(t *testing.T) {
	if os.Getenv("HIVENET_TEST_AUDIT_CHILD") == "1" {
		cfg := multiTenantConfig(t)
		r, err := router.New(cfg)
		if err == nil {
			r.Close() //nolint:errcheck
			t.Fatal("router.New started with an audit log it cannot open")
		}
		if !strings.Contains(err.Error(), "audit") {
			t.Fatalf("error %q does not name the audit log", err)
		}
		return
	}
	// A directory: opening it for writing fails.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRouterNew_MultiTenantRefusesUnopenableAuditLog$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "HIVENET_TEST_AUDIT_CHILD=1", "HIVENET_ROUTER_AUDIT_LOG_PATH="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	// A child that matched no test would exit 0 too.
	if !strings.Contains(string(out), "--- PASS: TestRouterNew_MultiTenantRefusesUnopenableAuditLog") {
		t.Fatalf("the child did not run the check:\n%s", out)
	}
}
