// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

// Dedicated replicas: a key's reservation sends its requests to its pool
// only. Such a request is never redirected to another site, never held for
// a wake, and carries its pool to the processor.

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
)

// reservedKeys: key-1 (redirectKey) reserved for acme; key-2 (otherKey) shared.
func reservedKeys(t *testing.T) *auth.DynamicKeyProvider {
	t.Helper()
	keys := auth.NewDynamicKeyProvider()
	for i, k := range []struct{ id, raw, res string }{{"key-1", redirectKey, "acme"}, {"key-2", otherKey, ""}} {
		h := sha256.Sum256([]byte(k.raw))
		if err := keys.Upsert(string(rune('1'+i)), auth.DynamicKeyEntry{
			ID: k.id, KeyHash: hex.EncodeToString(h[:]), Owner: "tenant-" + k.id, Name: k.id, Enabled: true,
			Quota: auth.QuotaLimits{RequestsPerMinute: 1000}, Reservation: k.res,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestDedicated_PoolReachesTheProcessor(t *testing.T) {
	for key, want := range map[string]string{redirectKey: "acme", otherKey: domain.SharedPool} {
		s := newSite(t, "a", map[string]int{"m": 1}, reservedKeys(t))
		done := sendAsync(s, chatPath, chatBody, map[string]string{"Authorization": "Bearer " + key})
		p := serveOne(t, s)
		if w := <-done; w.Code != http.StatusOK || p.Pool != want {
			t.Fatalf("status %d pool %q, want 200 and %q", w.Code, p.Pool, want)
		}
	}
}

// With no agent for the model here and a sibling serving it, a shared key is
// redirected, a reserved key is not: its replicas are its own.
func TestDedicated_NeverRedirected(t *testing.T) {
	a := newSite(t, "a", nil, reservedKeys(t))
	push(t, fleetDoc(1), a)
	if w := send(a, chatPath, chatBody, map[string]string{"Authorization": "Bearer " + otherKey}); w.Code != http.StatusTemporaryRedirect {
		t.Fatalf("shared key: status %d, want 307", w.Code)
	}
	if w := send(a, chatPath, chatBody, map[string]string{"Authorization": "Bearer " + redirectKey}); w.Code == http.StatusTemporaryRedirect {
		t.Fatalf("reserved key redirected to %s", w.Header().Get("Location"))
	}
	if len(a.redirs) != 1 {
		t.Errorf("redirects %v, want only the shared key's", a.redirs)
	}
}
