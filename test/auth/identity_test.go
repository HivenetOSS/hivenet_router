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
// SHA-256("hivenet-router/key-id/v1" NUL + prefix + NUL + hex SHA-256 of the
// token).
func wantKeyID(prefix, token string) string {
	h := sha256.Sum256([]byte("hivenet-router/key-id/v1\x00" + prefix + "\x00" + hashOf(token)))
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

// TestStaticKeyProvider_KeyHashNormalized: a static key_hash is checked like
// a dynamic one. Either case loads and authenticates (keys are looked up by
// the lowercase hex of the presented token's SHA-256); two spellings of one
// hash are one key, so loading both fails; anything that is not 64 hex
// characters could never authenticate and fails at load instead of
// answering 401 forever.
func TestStaticKeyProvider_KeyHashNormalized(t *testing.T) {
	h := hashOf("sk-x")
	p, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{{KeyHash: strings.ToUpper(h), Metadata: auth.KeyMetadata{Owner: "a", Name: "n"}}})
	if err != nil {
		t.Fatalf("uppercase key_hash refused: %v", err)
	}
	if got := authenticate(t, p, "sk-x"); got.KeyID != wantKeyID("static-", "sk-x") {
		t.Errorf("uppercase key_hash: KeyID %q, want the same ID as the lowercase hash", got.KeyID)
	}
	_, err = auth.NewStaticKeyProvider([]auth.APIKeyEntry{
		{KeyHash: h, Metadata: auth.KeyMetadata{Owner: "a", Name: "n"}},
		{KeyHash: strings.ToUpper(h), Metadata: auth.KeyMetadata{Owner: "b", Name: "n"}},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate key_hash") {
		t.Errorf("two spellings of one hash: err = %v, want a duplicate key_hash error", err)
	}
	for name, bad := range map[string]string{
		"empty":     "",
		"too short": h[:63],
		"too long":  h + "0",
		"not hex":   "g" + h[1:],
		"a token":   "sk-live-not-a-hash",
	} {
		if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{{KeyHash: bad, Metadata: auth.KeyMetadata{Owner: "a", Name: "n"}}}); err == nil {
			t.Errorf("%s key_hash accepted", name)
		}
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
	// One token listed both as an admin key and in auth.yaml gets two
	// unrelated IDs, so the two are not linked in logs.
	sp, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{staticEntry("adm-1", "tenant-a")})
	if err != nil {
		t.Fatal(err)
	}
	if st := authenticate(t, sp, "adm-1"); strings.TrimPrefix(st.KeyID, "static-") == strings.TrimPrefix(a.KeyID, "admin-") {
		t.Errorf("static ID %q and admin ID %q share their hash part", st.KeyID, a.KeyID)
	}
}

// TestKeyIdentifiers_RejectControlChars: key IDs and owners may not contain
// NUL, other control characters (C0, DEL, C1), characters that render as
// nothing (format characters, variation selectors, tag characters, Hangul
// fillers, the combining grapheme joiner), line or paragraph separators,
// whitespace other than a plain space, a space at either end, or invalid
// UTF-8. Counters join identifiers with NUL, so such a value could address
// another key's bucket; the others make two tenants look identical in logs or
// change how a log line reads. Plain spaces inside and other non-ASCII text
// stay valid.
func TestKeyIdentifiers_RejectControlChars(t *testing.T) {
	bad := map[string]string{
		"NUL":                    "tenant\x00model",
		"newline":                "tenant\nevil",
		"tab":                    "tenant\tx",
		"DEL":                    "tenant\x7f",
		"NEL (C1)":               "tenant\u0085x",
		"line separator":         "tenant\u2028x",
		"paragraph separator":    "tenant\u2029x",
		"right-to-left override": "tenant\u202ex",
		"zero-width space":       "tena\u200bnt",
		"byte-order mark":        "\ufefftenant",
		"invalid UTF8":           "tenant\xff",
		// Characters that render as nothing (Default_Ignorable_Code_Point).
		"combining grapheme joiner": "ten\u034fant",
		"Hangul choseong filler":    "\u115ftenant",
		"Hangul jungseong filler":   "tenant\u1160",
		"Hangul filler":             "ten\u3164ant",
		"halfwidth Hangul filler":   "tenant\uffa0",
		"variation selector":        "tenant\ufe0f",
		"tag character":             "tenant\U000e0041",
		"soft hyphen":               "ten\u00adant",
		// Whitespace that reads as a space, and spaces at either end.
		"no-break space":    "Acme\u00a0Corp",
		"ideographic space": "tenant\u3000",
		"en quad":           "a\u2000b",
		"trailing space":    "tenant ",
		"leading space":     " tenant",
	}
	good := []string{"Hivenet Router", "équipe-α", "org_42.prod", "株式会社テスト", "Acme Corp"}

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

// TestKeyIdentifiers_RejectReserved: no key can claim an identity the router
// assigns itself. Owners admin (admin keys), default (auth mode none) and
// anonymous are refused, in any letter case, for static and dynamic keys; so
// are the dynamic key ID anonymous and IDs starting like the ones the router
// derives (static-, admin-). Otherwise a key would share those callers'
// counters, cache salt, metric labels and audit identity. Names that only
// contain a reserved word stay valid.
func TestKeyIdentifiers_RejectReserved(t *testing.T) {
	dyn := func(id, owner string) auth.DynamicKeyEntry {
		return auth.DynamicKeyEntry{ID: id, KeyHash: hashOf(id + owner), Owner: owner, Name: "n", Enabled: true}
	}
	// Letter case is folded as Unicode does ("anonymouſ" has a long s), and a
	// space around a reserved name is refused as a space at either end.
	for _, owner := range []string{"admin", "default", "anonymous", "Admin", "DEFAULT", "anonymou\u017f", "admin ", " default", "admin\u00a0"} {
		if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{staticEntry("sk-1", owner)}); err == nil {
			t.Errorf("static owner %q accepted", owner)
		}
		if err := auth.NewDynamicKeyProvider().Upsert("v1", dyn("key-1", owner)); err == nil {
			t.Errorf("dynamic owner %q accepted", owner)
		}
	}
	for _, id := range []string{"anonymous", "Anonymous", "static-0123456789abcdef", "admin-0123456789abcdef", "Static-x", "\u017ftatic-x", "anonymou\u017f", " anonymous"} {
		if err := auth.NewDynamicKeyProvider().Upsert("v1", dyn(id, "tenant-a")); err == nil {
			t.Errorf("dynamic key ID %q accepted", id)
		}
	}
	for _, owner := range []string{"admins", "default-org", "org-admin"} {
		if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{staticEntry("sk-1", owner)}); err != nil {
			t.Errorf("static owner %q refused: %v", owner, err)
		}
	}
	for _, id := range []string{"key-admin-1", "staticky", "anonymous-2"} {
		if err := auth.NewDynamicKeyProvider().Upsert("v1", dyn(id, "tenant-a")); err != nil {
			t.Errorf("dynamic key ID %q refused: %v", id, err)
		}
	}
}
