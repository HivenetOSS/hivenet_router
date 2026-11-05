// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain points the audit log at a temporary file before any test, so
// router.New opens it wherever the tests run: the default path,
// /var/log/hivenet-router, is not writable by a non-root CI user, and a
// multi-tenant router refuses to start without its audit log. A value
// already set is kept: TestRouterNew_MultiTenantRefusesUnopenableAuditLog
// runs a child with a path that cannot be opened.
func TestMain(m *testing.M) {
	if os.Getenv("HIVENET_ROUTER_AUDIT_LOG_PATH") != "" {
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "hivenet-router-test-audit-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Setenv("HIVENET_ROUTER_AUDIT_LOG_PATH", filepath.Join(dir, "audit.jsonl")) //nolint:errcheck,usetesting // process-wide, before any test
	code := m.Run()
	os.RemoveAll(dir) //nolint:errcheck
	os.Exit(code)
}
