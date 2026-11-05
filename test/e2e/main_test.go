// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	logging "github.com/ipfs/go-log/v2"
)

// logDir holds every log the router and agents in this test binary write:
// the audit log, the application log (all subsystems, debug level) and the
// gin access log. The log-isolation tests scan them.
var logDir string

func auditLogPath() string  { return filepath.Join(logDir, "audit.jsonl") }
func appLogPath() string    { return filepath.Join(logDir, "app.log") }
func accessLogPath() string { return filepath.Join(logDir, "access.log") }

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "hivenet-e2e-logs-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logDir = dir
	// Pre-create the audit log with a wide mode: the router must narrow it.
	if err := os.WriteFile(auditLogPath(), nil, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Setenv("HIVENET_ROUTER_AUDIT_LOG_PATH", auditLogPath()) //nolint:errcheck,usetesting // process-wide, before any test
	logging.SetupLogging(logging.Config{
		Format: logging.PlaintextOutput,
		Level:  logging.LevelDebug, // the content checks must see debug lines too
		File:   appLogPath(),
	})
	access, err := os.Create(accessLogPath())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	gin.DefaultWriter = access

	code := m.Run()
	if code != 0 {
		// Logs no longer go to stderr; show the end of the application log.
		if b, err := os.ReadFile(appLogPath()); err == nil {
			lines := strings.Split(string(b), "\n")
			fmt.Fprintf(os.Stderr, "--- last application log lines (%s) ---\n", appLogPath())
			fmt.Fprintln(os.Stderr, strings.Join(lines[max(0, len(lines)-200):], "\n"))
		}
	}
	access.Close()    //nolint:errcheck
	os.RemoveAll(dir) //nolint:errcheck
	os.Exit(code)
}
