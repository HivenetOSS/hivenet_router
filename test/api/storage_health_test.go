// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Storage health probe tests (HAI-404): GET /health must report 503 while the
// volume backing the router's data directory is faulted, detached, or
// read-only, so the pod drops out of Ready and the endpoint leaves Active.
package api_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hivenet_router/internal/api"

	"github.com/gin-gonic/gin"
)

// serveLiveness runs the /health handler once and returns the response.
func serveLiveness(t *testing.T, h *api.Handlers) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/health", h.Liveness)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	return w
}

func TestStorageHealthVerifyHealthy(t *testing.T) {
	dir := t.TempDir()
	sh := api.NewStorageHealth(dir, "badger-test-endpoint", "longhorn-1r")

	if err := sh.Verify(); err != nil {
		t.Fatalf("Verify on a healthy directory: %v", err)
	}

	// The probe file must be cleaned up: a healthy check leaves the data
	// directory exactly as it found it.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read data dir after check: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Verify left %d file(s) behind in the data directory", len(entries))
	}
}

func TestStorageHealthVerifyMissingDir(t *testing.T) {
	// A detached volume detaches its mount point: the data directory
	// disappears and the probe must fail, not 500 or hang.
	dir := filepath.Join(t.TempDir(), "gone")
	sh := api.NewStorageHealth(dir, "vol", "cls")

	if err := sh.Verify(); err == nil {
		t.Fatal("Verify on a missing directory: want error, got nil")
	}
}

func TestStorageHealthVerifyNotADirectory(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	sh := api.NewStorageHealth(file, "vol", "cls")

	if err := sh.Verify(); err == nil {
		t.Fatal("Verify on a non-directory: want error, got nil")
	}
}

func TestStorageHealthVerifyReadonlyDir(t *testing.T) {
	// A volume that faults in place typically remounts read-only: writes
	// must fail with EROFS/EACCES and the probe must report the fault.
	// Root bypasses permission bits, so the test only runs as non-root.
	if os.Geteuid() == 0 {
		t.Skip("running as root — permission bits do not block writes")
	}

	dir := t.TempDir()
	sh := api.NewStorageHealth(dir, "vol", "cls")
	if err := sh.Verify(); err != nil {
		t.Fatalf("precondition — Verify on a writable directory: %v", err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	if err := sh.Verify(); err == nil {
		t.Fatal("Verify on a read-only directory: want error, got nil")
	}
}

func TestStorageHealthVolumeIdentity(t *testing.T) {
	dir := t.TempDir()

	sh := api.NewStorageHealth(dir, "badger-abc123", "longhorn-1r")
	if got := sh.VolumeName(); got != "badger-abc123" {
		t.Errorf("VolumeName = %q, want the configured name", got)
	}
	if got := sh.StorageClass(); got != "longhorn-1r" {
		t.Errorf("StorageClass = %q, want the configured class", got)
	}

	// Unconfigured identity must degrade gracefully (mount source or
	// "unknown"), never an empty string in the operator log line.
	unset := api.NewStorageHealth(dir, "", "")
	if got := unset.VolumeName(); got == "" {
		t.Error("VolumeName with no configured name: want fallback, got empty")
	}
	if got := unset.StorageClass(); got == "" {
		t.Error("StorageClass with no configured class: want \"unknown\", got empty")
	}
}

func TestLivenessHealthyStorage(t *testing.T) {
	h := newPolicyHandlers()
	h.SetStorageHealth(api.NewStorageHealth(t.TempDir(), "badger-abc123", "longhorn-1r"))

	w := serveLiveness(t, h)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("Liveness with healthy storage = %d %q, want 200 ok", w.Code, w.Body.String())
	}
}

func TestLivenessStorageUnavailable(t *testing.T) {
	// Data directory gone (volume detached) → 503, not 200.
	h := newPolicyHandlers()
	h.SetStorageHealth(api.NewStorageHealth(filepath.Join(t.TempDir(), "detached"), "badger-abc123", "longhorn-1r"))

	w := serveLiveness(t, h)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("Liveness with detached storage = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), "storage unavailable") {
		t.Errorf("Liveness body = %q, want a storage-unavailable message", w.Body.String())
	}
}

func TestLivenessReadonlyStorage(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root — permission bits do not block writes")
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	h := newPolicyHandlers()
	h.SetStorageHealth(api.NewStorageHealth(dir, "badger-abc123", "longhorn-1r"))

	w := serveLiveness(t, h)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("Liveness with read-only storage = %d, want 503", w.Code)
	}
}

func TestLivenessWithoutStorageCheck(t *testing.T) {
	// Handlers without a wired storage check keep the legacy behaviour:
	// an unconditional 200 (existing test doubles and /live routes).
	h := newPolicyHandlers()

	w := serveLiveness(t, h)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("Liveness without storage check = %d %q, want 200 ok", w.Code, w.Body.String())
	}
}
