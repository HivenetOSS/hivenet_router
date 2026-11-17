// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/config"

	"github.com/alicebob/miniredis/v2"
)

// TestSaltFingerprintMismatchReported: the routers of a site publish a
// fingerprint of their cache-salt secret in the shared store and compare
// them. A router started with another secret is counted, by every router, in
// hivenet_cache_salt_secret_mismatch, while the fingerprints reveal neither
// the secret nor a salt.
func TestSaltFingerprintMismatchReported(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	mr := miniredis.RunT(t)
	secrets := []string{strings.Repeat("a", 32), strings.Repeat("a", 32), strings.Repeat("b", 32)}
	var metricsURLs []string
	for i, secret := range secrets {
		_, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey, Mutate: func(c *config.Config) {
			c.CacheSalt, c.CacheSaltSecret = true, secret
			c.SyncStore, c.SyncValkeyAddrs = "valkey", mr.Addr()
			c.SyncInterval = 50 * time.Millisecond // a comparison every 1.5 s
			c.RouterID = fmt.Sprintf("router-fp-%d", i)
		}})
		metricsURLs = append(metricsURLs, "http://127.0.0.1"+rcfg.MetricsPort+"/metrics")
	}
	mismatch := func(url string) string {
		resp, err := http.Get(url)
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "hivenet_cache_salt_secret_mismatch ") {
				return strings.TrimPrefix(line, "hivenet_cache_salt_secret_mismatch ")
			}
		}
		return ""
	}
	// The two routers with secret a each see one other secret; the one with
	// secret b sees two.
	for i, want := range []string{"1", "1", "2"} {
		waitFor(t, 15*time.Second, fmt.Sprintf("router %d to report %s mismatching router(s)", i, want), func() bool {
			return mismatch(metricsURLs[i]) == want
		})
	}
	// One field per router, holding a 16-hex-character fingerprint (and the
	// time it was written), never the secret.
	const key = "hivenet:{default}:shared:cache-salt-fingerprint"
	fields, err := mr.HKeys(key)
	if err != nil || len(fields) != len(secrets) {
		t.Fatalf("%s holds fields %v (%v), want one per router", key, fields, err)
	}
	fingerprint := regexp.MustCompile(`^[0-9a-f]{16}\|[0-9]+$`)
	for _, f := range fields {
		v := mr.HGet(key, f)
		if !fingerprint.MatchString(v) {
			t.Errorf("field %s = %q, want a 16-hex-character fingerprint", f, v)
		}
		for _, s := range secrets {
			if strings.Contains(v, s) {
				t.Errorf("field %s holds a cache-salt secret", f)
			}
		}
	}
}
