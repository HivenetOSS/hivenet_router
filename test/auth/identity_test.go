// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/auth"
)

func hashOf(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// wantKeyID is the derived key ID for a raw token: prefix + 16 hex of
// SHA-256("hivenet-router/key-id/v1" NUL + hex SHA-256 of the token).
func wantKeyID(prefix, token string) string {
	h := sha256.Sum256([]byte("hivenet-router/key-id/v1\x00" + hashOf(token)))
	return prefix + hex.EncodeToString(h[:8])
}

func staticEntry(token, owner string) auth.APIKeyEntry {
	return auth.APIKeyEntry{KeyHash: hashOf(token), Metadata: auth.KeyMetadata{Owner: owner, Name: "k"}}
}

func authenticate(t *testing.T, p auth.Provider, token string) *auth.AuthResult {
	t.Helper()
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	res, err := p.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate(%q): %v", token, err)
	}
	return res
}

// TestStaticKeyProvider_DerivesStableKeyID: every static auth.yaml key gets
// KeyID "static-" + 16 hex characters derived one-way from its key_hash. It
// is the same across provider rebuilds (restart, SIGHUP, another router),
// different for every key (including two keys of one tenant), and not a
// substring of the key_hash.
func TestStaticKeyProvider_DerivesStableKeyID(t *testing.T) {
	entries := []auth.APIKeyEntry{
		staticEntry("sk-a1", "tenant-a"),
		staticEntry("sk-a2", "tenant-a"),
		staticEntry("sk-b1", "tenant-b"),
	}
	p1, err := auth.NewStaticKeyProvider(entries)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := auth.NewStaticKeyProvider(entries)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]string{}
	for _, tc := range []struct{ token, owner string }{
		{"sk-a1", "tenant-a"}, {"sk-a2", "tenant-a"}, {"sk-b1", "tenant-b"},
	} {
		got := authenticate(t, p1, tc.token)
		want := wantKeyID("static-", tc.token)
		if got.KeyID != want || got.TenantID != tc.owner {
			t.Errorf("%s: KeyID %q tenant %q, want %q / %q", tc.token, got.KeyID, got.TenantID, want, tc.owner)
		}
		if strings.Contains(hashOf(tc.token), strings.TrimPrefix(got.KeyID, "static-")) {
			t.Errorf("%s: KeyID %q is a substring of key_hash; it must not reveal the hash", tc.token, got.KeyID)
		}
		if again := authenticate(t, p2, tc.token); again.KeyID != got.KeyID {
			t.Errorf("%s: KeyID %q after rebuild, want the same %q", tc.token, again.KeyID, got.KeyID)
		}
		if other, dup := seen[got.KeyID]; dup {
			t.Errorf("%s and %s share KeyID %q", other, tc.token, got.KeyID)
		}
		seen[got.KeyID] = tc.token
	}

}

// TestStaticKeyProvider_RejectsDerivedIDCollision: two entries whose
// key_hash differs only in letter case are distinct map entries but derive
// the same key ID (and with it counters and audit identity), so loading
// fails.
func TestStaticKeyProvider_RejectsDerivedIDCollision(t *testing.T) {
	h := hashOf("sk-x")
	_, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: h, Metadata: auth.KeyMetadata{Owner: "a", Name: "n"}},
		{KeyHash: strings.ToUpper(h), Metadata: auth.KeyMetadata{Owner: "b", Name: "n"}},
	})
	if err == nil || !strings.Contains(err.Error(), "same key ID") {
		t.Fatalf("err = %v, want a derived key ID collision error", err)
	}
}

// TestStaticAdminKeyProvider_KeyIDPerKey: each admin key authenticates with
// its own KeyID, so the audit log shows which admin key acted.
func TestStaticAdminKeyProvider_KeyIDPerKey(t *testing.T) {
	p, err := auth.NewStaticAdminKeyProvider([]string{"adm-1", "adm-2"})
	if err != nil {
		t.Fatal(err)
	}
	a, b := authenticate(t, p, "adm-1"), authenticate(t, p, "adm-2")
	if a.TenantID != "admin" || a.KeyID != wantKeyID("admin-", "adm-1") || a.KeyID == b.KeyID {
		t.Errorf("admin results %+v / %+v, want tenant admin and distinct derived admin- key IDs", a, b)
	}
}

// TestKeyIdentifiers_RejectControlChars: key IDs and owners may not contain
// NUL, other control characters, DEL or invalid UTF-8. Counters join
// identifiers with NUL, so such a value could address another key's bucket.
// Spaces and non-ASCII text stay valid.
func TestKeyIdentifiers_RejectControlChars(t *testing.T) {
	bad := map[string]string{
		"NUL":          "tenant\x00model",
		"newline":      "tenant\nevil",
		"tab":          "tenant\tx",
		"DEL":          "tenant\x7f",
		"invalid UTF8": "tenant\xff",
	}
	good := []string{"Hivenet Router", "équipe-α", "org_42.prod"}

	t.Run("static owner", func(t *testing.T) {
		for name, v := range bad {
			if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{staticEntry("sk-1", v)}); err == nil {
				t.Errorf("%s owner accepted", name)
			}
		}
		for _, v := range good {
			if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{staticEntry("sk-1", v)}); err != nil {
				t.Errorf("owner %q rejected: %v", v, err)
			}
		}
	})

	dyn := func(id, owner string) auth.DynamicKeyEntry {
		return auth.DynamicKeyEntry{ID: id, KeyHash: hashOf(id + owner), Owner: owner, Name: "n", Enabled: true}
	}
	t.Run("dynamic id and owner", func(t *testing.T) {
		for name, v := range bad {
			if err := auth.NewDynamicKeyProvider().Upsert("v1", dyn(v, "tenant")); err == nil {
				t.Errorf("%s id accepted", name)
			}
			if err := auth.NewDynamicKeyProvider().Upsert("v1", dyn("key-1", v)); err == nil {
				t.Errorf("%s owner accepted", name)
			}
		}
		for _, v := range good {
			if err := auth.NewDynamicKeyProvider().Upsert("v1", dyn("key-1", v)); err != nil {
				t.Errorf("owner %q rejected: %v", v, err)
			}
		}
	})

	// ReplaceAll is all-or-nothing: one bad entry rejects the whole set and
	// the registry keeps its previous keys.
	t.Run("replace is all-or-nothing", func(t *testing.T) {
		p := auth.NewDynamicKeyProvider()
		if err := p.Upsert("v1", dyn("key-1", "tenant-a")); err != nil {
			t.Fatal(err)
		}
		err := p.ReplaceAll("v2", []auth.DynamicKeyEntry{dyn("key-2", "tenant-b"), dyn("key-3", "bad\x00owner")})
		if err == nil {
			t.Fatal("replace with a bad owner accepted")
		}
		if keys := p.ListKeys(); len(keys) != 1 || keys[0].ID != "key-1" {
			t.Errorf("registry after a rejected replace: %+v, want only key-1", keys)
		}
	})
}
