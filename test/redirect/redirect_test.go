// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package redirect_test covers cross-site redirects: the sibling map, the
// signed redirect token, the deterministic sibling pick and the token-based
// authentication a sibling site uses for a redirected request.
package redirect_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/redirect"
)

var secret = []byte("0123456789abcdef0123456789abcdef")

func site(id string, models ...string) redirect.Site {
	return redirect.Site{ID: id, URL: "https://" + id + ".example.com", Models: models}
}

// TestMapReplace_RejectsInvalid: an invalid push is refused and leaves the
// previous map untouched.
func TestMapReplace_RejectsInvalid(t *testing.T) {
	cases := []struct {
		name    string
		version string
		sites   []redirect.Site
	}{
		{"empty version", "", []redirect.Site{site("a", "m")}},
		{"empty id", "v2", []redirect.Site{{URL: "https://a.example.com"}}},
		// Clients follow https→http and then send the token in clear text.
		{"http url", "v2", []redirect.Site{{ID: "a", URL: "http://a.example.com"}}},
		{"url with path", "v2", []redirect.Site{{ID: "a", URL: "https://a.example.com/v1"}}},
		{"url with query", "v2", []redirect.Site{{ID: "a", URL: "https://a.example.com?x=1"}}},
		{"url with credentials", "v2", []redirect.Site{{ID: "a", URL: "https://u:p@a.example.com"}}},
		{"relative url", "v2", []redirect.Site{{ID: "a", URL: "/a"}}},
		{"duplicate id", "v2", []redirect.Site{site("a", "m"), site("a", "n")}},
		{"empty model", "v2", []redirect.Site{site("a", "")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := redirect.NewMap()
			if err := m.Replace("v1", []redirect.Site{site("keep", "m")}); err != nil {
				t.Fatal(err)
			}
			if err := m.Replace(tc.version, tc.sites); err == nil {
				t.Fatal("expected an error")
			}
			if v, sites := m.Snapshot(); v != "v1" || len(sites) != 1 || sites[0].ID != "keep" {
				t.Fatalf("map changed after a rejected push: %s %v", v, sites)
			}
		})
	}
}

// TestMapReplace_Versions: an older version is refused with ErrStaleVersion
// (a delayed push cannot roll the map back); an equal version is a replay.
func TestMapReplace_Versions(t *testing.T) {
	m := redirect.NewMap()
	if err := m.Replace("v2", []redirect.Site{site("a", "m")}); err != nil {
		t.Fatal(err)
	}
	if err := m.Replace("v1", nil); !errors.Is(err, redirect.ErrStaleVersion) {
		t.Fatalf("older version: got %v, want ErrStaleVersion", err)
	}
	if err := m.Replace("v2", []redirect.Site{site("b", "m")}); err != nil {
		t.Fatalf("equal version replay: %v", err)
	}
	if _, sites := m.Snapshot(); sites[0].ID != "b" {
		t.Fatalf("replay not applied: %v", sites)
	}
}

// TestMapReplace_TrimsTrailingSlash: a site URL is stored without a trailing
// slash so Location never contains "//v1".
func TestMapReplace_TrimsTrailingSlash(t *testing.T) {
	m := redirect.NewMap()
	if err := m.Replace("v1", []redirect.Site{{ID: "a", URL: "https://a.example.com/", Models: []string{"m"}}}); err != nil {
		t.Fatal(err)
	}
	s, _ := m.Target("m", "self", "")
	if s.URL != "https://a.example.com" {
		t.Fatalf("got %q", s.URL)
	}
}

// TestTarget_FiltersSelfAndModel: only other sites that serve the model are
// candidates.
func TestTarget_FiltersSelfAndModel(t *testing.T) {
	m := redirect.NewMap()
	if err := m.Replace("v1", []redirect.Site{site("self", "m"), site("b", "m"), site("c", "other")}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		s, ok := m.Target("m", "self", fmt.Sprint(i))
		if !ok || s.ID != "b" {
			t.Fatalf("affinity %d: got %v %v, want site b", i, s.ID, ok)
		}
	}
	if _, ok := m.Target("missing", "self", ""); ok {
		t.Fatal("a model no site serves must not have a target")
	}
	if _, ok := m.Target("other", "c", ""); ok {
		t.Fatal("this site is never its own target")
	}
}

// TestTarget_RendezvousProperties: the pick is deterministic, spreads keys
// over every site, and when one site leaves only the keys it was winning move.
func TestTarget_RendezvousProperties(t *testing.T) {
	var sites []redirect.Site
	for i := 0; i < 10; i++ {
		sites = append(sites, site(fmt.Sprintf("s%d", i), "m"))
	}
	m := redirect.NewMap()
	if err := m.Replace("v1", sites); err != nil {
		t.Fatal(err)
	}
	before := map[string]string{}
	used := map[string]int{}
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key\x00session-%d", i)
		s, _ := m.Target("m", "", key)
		again, _ := m.Target("m", "", key)
		if s.ID != again.ID {
			t.Fatalf("pick not deterministic for %q", key)
		}
		before[key] = s.ID
		used[s.ID]++
	}
	if len(used) != 10 {
		t.Fatalf("2000 keys reached %d of 10 sites", len(used))
	}

	if err := m.Replace("v2", sites[1:]); err != nil { // s0 leaves
		t.Fatal(err)
	}
	for key, was := range before {
		now, _ := m.Target("m", "", key)
		if was != "s0" && now.ID != was {
			t.Fatalf("key %q moved %s→%s although its site stayed", key, was, now.ID)
		}
		if now.ID == "s0" {
			t.Fatal("a removed site is still picked")
		}
	}
}

// TestLocation keeps the request path and query and replaces an old token.
func TestLocation(t *testing.T) {
	u, _ := url.Parse("/v1/chat/completions?api-version=2024&rt=old")
	got := redirect.Location("https://b.example.com", u, "new")
	want := "https://b.example.com/v1/chat/completions?api-version=2024&rt=new"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// TestSigner covers every way a token can be wrong.
func TestSigner(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	s := redirect.NewSigner(secret, 120*time.Second)
	claims := redirect.Claims{KeyID: "k1", RequestID: "r1", Model: "m", Site: "b", Path: "/v1/chat/completions", Hop: 1, BodySHA: "ab"}
	tok := s.Sign(claims, now)

	got, err := s.Verify(tok, now.Add(119*time.Second))
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	claims.Exp = now.Add(120 * time.Second).Unix()
	if got != claims {
		t.Fatalf("claims round trip: got %+v, want %+v", got, claims)
	}

	payload, sig := tok[:strings.LastIndexByte(tok, '.')], tok[strings.LastIndexByte(tok, '.')+1:]
	flip := func(s string, i int) string {
		b := []byte(s)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		return string(b)
	}
	cases := []struct {
		name  string
		token string
		at    time.Time
		want  error
	}{
		{"expired", tok, now.Add(121 * time.Second), redirect.ErrTokenExpired},
		{"payload tampered", flip(payload, len(payload)-3) + "." + sig, now, redirect.ErrTokenSignature},
		{"signature tampered", payload + "." + flip(sig, 3), now, redirect.ErrTokenSignature},
		{"wrong secret", redirect.NewSigner([]byte("another-secret-another-secret-xx"), time.Minute).Sign(claims, now), now, redirect.ErrTokenSignature},
		{"empty signature", payload + ".", now, redirect.ErrTokenSignature},
		{"no version prefix", strings.TrimPrefix(tok, "v1."), now, redirect.ErrTokenMalformed},
		{"no signature part", "v1.abc", now, redirect.ErrTokenMalformed},
		{"empty", "", now, redirect.ErrTokenMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Verify(tc.token, tc.at); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

// ---- token authentication -------------------------------------------------

const rawKey = "sk-test-key-1"

// recorder is a Provider that counts calls; it stands in for the API key
// provider the redirect wrapper delegates to.
type recorder struct {
	calls int
	res   *auth.AuthResult
	err   error
}

func (r *recorder) Authenticate(*http.Request) (*auth.AuthResult, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	res := *r.res
	return &res, nil
}

func keyHash(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])
}

func registry(t *testing.T, enabled bool, expires *time.Time) *auth.DynamicKeyProvider {
	t.Helper()
	keys := auth.NewDynamicKeyProvider()
	if err := keys.Upsert("v1", auth.DynamicKeyEntry{
		ID: "key-1", KeyHash: keyHash(rawKey), Owner: "tenant-1", Name: "test",
		Enabled: enabled, ExpiresAt: expires, AllowedModels: []string{"m"},
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

func newRedirector(rejects *[]string) *redirect.Redirector {
	return &redirect.Redirector{
		Self:     "b",
		Map:      redirect.NewMap(),
		Signer:   redirect.NewSigner(secret, 120*time.Second),
		OnReject: func(reason string) { *rejects = append(*rejects, reason) },
	}
}

func request(token, bearer string) *http.Request {
	target := "/v1/chat/completions"
	if token != "" {
		target += "?rt=" + url.QueryEscape(token)
	}
	r := httptest.NewRequest(http.MethodPost, target, nil)
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	return r
}

// TestTokenAuth exercises the redirect wrapper around the API auth provider.
func TestTokenAuth(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	valid := redirect.Claims{KeyID: "key-1", Site: "b", Path: "/v1/chat/completions", Hop: 1, BodySHA: "bh"}
	sign := func(mod func(*redirect.Claims)) string {
		c := valid
		if mod != nil {
			mod(&c)
		}
		return redirect.NewSigner(secret, 120*time.Second).Sign(c, now)
	}
	keyResult := &auth.AuthResult{TenantID: "tenant-1", KeyID: "key-1"}

	cases := []struct {
		name       string
		req        *http.Request
		keys       *auth.DynamicKeyProvider
		next       *recorder
		wantErr    bool
		wantReason string
		wantNext   int // calls to the wrapped provider
	}{
		{"no token: delegated unchanged", request("", rawKey), registry(t, true, nil),
			&recorder{res: keyResult}, false, "", 1},
		{"token only: key resolved by id", request(sign(nil), ""), registry(t, true, nil),
			&recorder{err: auth.ErrMissingCredentials}, false, "", 0},
		{"token + the same key (curl back at its first host)", request(sign(nil), rawKey), registry(t, true, nil),
			&recorder{res: keyResult}, false, "", 1},
		{"token + another key", request(sign(nil), "sk-other"), registry(t, true, nil),
			&recorder{res: &auth.AuthResult{TenantID: "t2", KeyID: "key-2"}}, true, "key", 1},
		{"token + a key that does not authenticate", request(sign(nil), "sk-bad"), registry(t, true, nil),
			&recorder{err: auth.ErrInvalidCredentials}, true, "", 1},
		{"key disabled since the redirect", request(sign(nil), ""), registry(t, false, nil),
			&recorder{}, true, "key", 0},
		{"key expired since the redirect", request(sign(nil), ""), registry(t, true, &past),
			&recorder{}, true, "key", 0},
		{"unknown key id", request(sign(func(c *redirect.Claims) { c.KeyID = "nope" }), ""), registry(t, true, nil),
			&recorder{}, true, "key", 0},
		{"token for another site", request(sign(func(c *redirect.Claims) { c.Site = "c" }), ""), registry(t, true, nil),
			&recorder{}, true, "site", 0},
		{"token for another path (e.g. embeddings)", request(sign(func(c *redirect.Claims) { c.Path = "/v1/embeddings" }), ""),
			registry(t, true, nil), &recorder{}, true, "path", 0},
		{"expired token", request(redirect.NewSigner(secret, time.Second).Sign(valid, now.Add(-time.Minute)), ""),
			registry(t, true, nil), &recorder{}, true, "expired", 0},
		{"forged token", request(redirect.NewSigner([]byte("not-the-secret-not-the-secret-xx"), time.Minute).Sign(valid, now), ""),
			registry(t, true, nil), &recorder{}, true, "signature", 0},
		{"garbage token", request("v1.garbage", ""), registry(t, true, nil), &recorder{}, true, "malformed", 0},
		{"auth mode none", request(sign(func(c *redirect.Claims) { c.KeyID = "" }), ""), nil,
			&recorder{res: &auth.AuthResult{TenantID: "default"}}, false, "", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rejects []string
			p := redirect.Wrap(newRedirector(&rejects), tc.next, tc.keys)
			res, err := p.Authenticate(tc.req)
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.next.calls != tc.wantNext {
				t.Errorf("wrapped provider called %d times, want %d", tc.next.calls, tc.wantNext)
			}
			if tc.wantReason != "" && (len(rejects) != 1 || rejects[0] != tc.wantReason) {
				t.Errorf("reject reasons %v, want [%s]", rejects, tc.wantReason)
			}
			if err != nil {
				if !errors.Is(err, auth.ErrInvalidCredentials) {
					t.Errorf("a refused token must look like any bad credential (401), got %v", err)
				}
				return
			}
			if tc.req.URL.Query().Has("rt") && (res.RedirectHop != 1 || res.RedirectBodySHA != "bh") {
				t.Errorf("redirect claims not passed on: hop=%d bh=%q", res.RedirectHop, res.RedirectBodySHA)
			}
			if !tc.req.URL.Query().Has("rt") && res.RedirectHop != 0 {
				t.Errorf("a request without a token must not look redirected")
			}
		})
	}
}

// TestWrap_KeepsTenantListing: the router publishes per-tenant quota gauges by
// type-asserting the installed API provider to Tenants(); the wrapper must
// pass that through, or enabling redirect silently stops the gauges.
func TestWrap_KeepsTenantListing(t *testing.T) {
	keys := registry(t, true, nil)
	var rejects []string
	wrapped := redirect.Wrap(newRedirector(&rejects), keys, keys)
	lister, ok := wrapped.(interface {
		Tenants() map[string]auth.QuotaLimits
	})
	if !ok {
		t.Fatal("the wrapped provider must still list tenants")
	}
	if got := lister.Tenants(); !reflect.DeepEqual(got, keys.Tenants()) || len(got) != 1 {
		t.Fatalf("Tenants() = %v, want the wrapped registry's %v", got, keys.Tenants())
	}
	if got := redirect.Wrap(newRedirector(&rejects), auth.NewNoAuthProvider(), nil).(interface {
		Tenants() map[string]auth.QuotaLimits
	}).Tenants(); got != nil {
		t.Fatalf("a provider without tenants lists none, got %v", got)
	}
}

// TestWrap_Disabled: with redirect disabled the API provider is installed
// unwrapped, so a ?rt= parameter has no meaning.
func TestWrap_Disabled(t *testing.T) {
	next := &recorder{res: &auth.AuthResult{TenantID: "t"}}
	if got := redirect.Wrap(nil, next, nil); got != auth.Provider(next) {
		t.Fatal("Wrap(nil, p) must return p itself")
	}
}

// TestCheckAuthMode: static auth.yaml keys have no key ID, so redirect is
// refused with them; any mode is fine while redirect is disabled.
func TestCheckAuthMode(t *testing.T) {
	static, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{{KeyHash: keyHash(rawKey), Metadata: auth.KeyMetadata{Owner: "t", Name: "n"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := redirect.CheckAuthMode("a", static); err == nil {
		t.Error("static keys with redirect enabled must be refused")
	}
	if err := redirect.CheckAuthMode("", static); err != nil {
		t.Errorf("static keys with redirect disabled: %v", err)
	}
	if err := redirect.CheckAuthMode("a", auth.NewDynamicKeyProvider()); err != nil {
		t.Errorf("dynamic keys: %v", err)
	}
	if err := redirect.CheckAuthMode("a", auth.NewNoAuthProvider()); err != nil {
		t.Errorf("auth mode none: %v", err)
	}
}
