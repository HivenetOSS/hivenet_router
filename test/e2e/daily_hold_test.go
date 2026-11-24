// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestDailyBudgetCountsExactUsage: end to end, a key's daily budget falls by
// exactly the tokens the backend reports per request (prompt + completion),
// however far the router's prompt estimate was from them: a long prompt the
// backend counts as few tokens gives the rest of its estimate back.
func TestDailyBudgetCountsExactUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	const key = "sk-exact-usage-key"
	auth := writeAuthYAML(testKey{Token: key, Owner: "tenant-x", Name: "x", RPM: 100, TPD: 100_000})
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: auth, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, startUsageStubBackend(t, 30, 10).URL, filepath.Join(t.TempDir(), "agent.key"), nil)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool { return modelHealthyAs(base, key, stubModel) })

	// ~2,500 estimated prompt tokens; the backend counts 30 + 10.
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":10}`, stubModel, strings.Repeat("word ", 2000))
	for i, want := range []int{100_000 - 40, 100_000 - 80, 100_000 - 120} {
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		got, _ := strconv.Atoi(resp.Header.Get("X-RateLimit-Remaining-Tokens"))
		if resp.StatusCode != http.StatusOK || got != want {
			t.Errorf("request %d: status %d, %d tokens left, want 200 and %d", i+1, resp.StatusCode, got, want)
		}
	}
}
