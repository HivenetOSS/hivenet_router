// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package redirect_test covers cross-site redirects (contract 3 v2): the
// sibling map, the redirect choice, scopes, the signed single-use token and
// the token-based authentication a sibling policloud uses.
package redirect_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/redirect"
)

var (
	yes = func() *bool { b := true; return &b }()
	no  = func() *bool { b := false; return &b }()
)

func u64(v uint64) *uint64 { return &v }

func pc(id, country string, eu *bool) redirect.Policloud {
	return redirect.Policloud{ID: id, URL: "https://" + id + ".example.com", Country: country, EU: eu}
}

func place(model, kind, policloud string, sfz *bool, hint *redirect.Hint) redirect.Placement {
	return redirect.Placement{Model: model, Kind: kind, Policloud: policloud, ScaleFromZero: sfz, Hint: hint}
}

// doc is a valid map: fr-1 (entry), fr-2, de-1, us-1; model m (chat) on all
// but fr-1, embedding e on de-1 only.
func doc(version uint64) redirect.MapDoc {
	return redirect.MapDoc{
		Schema: 2, Version: u64(version), Region: "eu",
		Policlouds: []redirect.Policloud{pc("fr-1", "FR", yes), pc("fr-2", "FR", yes), pc("de-1", "DE", yes), pc("us-1", "US", no)},
		Placements: []redirect.Placement{
			place("m", redirect.KindChat, "fr-2", yes, nil),
			place("m", redirect.KindChat, "de-1", yes, nil),
			place("m", redirect.KindChat, "us-1", yes, nil),
			place("e", redirect.KindEmbedding, "de-1", yes, nil),
		},
	}
}

// TestMapReplace_RejectsInvalid: an invalid push is refused (400 at the API)
// and leaves the previous map untouched.
func TestMapReplace_RejectsInvalid(t *testing.T) {
	future := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	cases := []struct {
		name string
		mod  func(*redirect.MapDoc)
	}{
		{"unknown schema", func(d *redirect.MapDoc) { d.Schema = 3 }},
		{"missing schema", func(d *redirect.MapDoc) { d.Schema = 0 }},
		{"missing version", func(d *redirect.MapDoc) { d.Version = nil }},
		{"missing region", func(d *redirect.MapDoc) { d.Region = "" }},
		{"empty policloud id", func(d *redirect.MapDoc) { d.Policlouds[0].ID = "" }},
		{"duplicate policloud id", func(d *redirect.MapDoc) { d.Policlouds[1].ID = "fr-1" }},
		// Clients follow https→http and then send the token in clear text.
		{"http url", func(d *redirect.MapDoc) { d.Policlouds[0].URL = "http://fr-1.example.com" }},
		{"url with path", func(d *redirect.MapDoc) { d.Policlouds[0].URL = "https://fr-1.example.com/v1" }},
		{"url with query", func(d *redirect.MapDoc) { d.Policlouds[0].URL = "https://fr-1.example.com?x=1" }},
		{"url with credentials", func(d *redirect.MapDoc) { d.Policlouds[0].URL = "https://u:p@fr-1.example.com" }},
		{"lowercase country", func(d *redirect.MapDoc) { d.Policlouds[0].Country = "fr" }},
		{"3-letter country", func(d *redirect.MapDoc) { d.Policlouds[0].Country = "FRA" }},
		{"missing eu", func(d *redirect.MapDoc) { d.Policlouds[0].EU = nil }},
		{"empty model", func(d *redirect.MapDoc) { d.Placements[0].Model = "" }},
		{"unknown kind", func(d *redirect.MapDoc) { d.Placements[0].Kind = "image" }},
		{"unknown policloud", func(d *redirect.MapDoc) { d.Placements[0].Policloud = "nope" }},
		{"duplicate placement", func(d *redirect.MapDoc) { d.Placements[1].Policloud = "fr-2" }},
		{"missing scale_from_zero", func(d *redirect.MapDoc) { d.Placements[0].ScaleFromZero = nil }},
		{"unknown hint load", func(d *redirect.MapDoc) {
			d.Placements[0].Hint, d.HintsExpireAt = &redirect.Hint{Load: "hot"}, future
		}},
		{"hint without hints_expire_at", func(d *redirect.MapDoc) { d.Placements[0].Hint = &redirect.Hint{Load: "ok"} }},
		{"unparsable hints_expire_at", func(d *redirect.MapDoc) { d.HintsExpireAt = "soon" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := redirect.NewMap()
			if err := m.Replace(doc(1)); err != nil {
				t.Fatal(err)
			}
			bad := doc(2)
			tc.mod(&bad)
			if err := m.Replace(bad); err == nil {
				t.Fatal("expected an error")
			}
			if m.Version() != 1 {
				t.Fatalf("map changed after a rejected push: version %d", m.Version())
			}
		})
	}
}

// TestMapReplace_NumericVersions: versions compare as numbers ("10" > "9"),
// a lower version is ErrStaleVersion (409), an equal one is a replay.
func TestMapReplace_NumericVersions(t *testing.T) {
	m := redirect.NewMap()
	for _, v := range []uint64{9, 10, 10} {
		if err := m.Replace(doc(v)); err != nil {
			t.Fatalf("version %d: %v", v, err)
		}
	}
	if err := m.Replace(doc(9)); !errors.Is(err, redirect.ErrStaleVersion) {
		t.Fatalf("lower version: got %v, want ErrStaleVersion", err)
	}
	if m.Version() != 10 {
		t.Fatalf("version %d, want 10", m.Version())
	}
}

// TestMapReplace_TrimsTrailingSlash: a policloud URL is stored without a
// trailing slash so Location never contains "//v1".
func TestMapReplace_TrimsTrailingSlash(t *testing.T) {
	d := doc(1)
	d.Policlouds[1].URL = "https://fr-2.example.com/"
	m := redirect.NewMap()
	if err := m.Replace(d); err != nil {
		t.Fatal(err)
	}
	if p, _ := m.Policloud("fr-2"); p.URL != "https://fr-2.example.com" {
		t.Fatalf("got %q", p.URL)
	}
}

// h is the contract's H(a, model, policloud), written independently of the
// package so the test checks the formula, not the code against itself.
func h(a, model, policloud string) uint64 {
	sum := sha256.Sum256([]byte(a + "\x00" + model + "\x00" + policloud))
	return binary.BigEndian.Uint64(sum[:8])
}

// expected computes the contract's choice without hints: top-2 home set by
// H(key, model, pc), then the session order inside it.
func expected(key, session, model string, cands []string) []string {
	sort.Slice(cands, func(i, j int) bool { return h(key, model, cands[i]) > h(key, model, cands[j]) })
	if len(cands) > 2 {
		cands = cands[:2]
	}
	sess := key + "\x00" + session
	sort.Slice(cands, func(i, j int) bool { return h(sess, model, cands[i]) > h(sess, model, cands[j]) })
	return cands
}

func target(t *testing.T, m *redirect.Map, req redirect.TargetRequest) string {
	t.Helper()
	if req.Scope == "" {
		req.Scope = redirect.ScopeAll
	}
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	p, ok := m.Target(req)
	if !ok {
		return ""
	}
	return p.ID
}

// TestTarget_FollowsTheContractFormula: across many keys and sessions the
// pick is the session's first choice inside the key's top-2 home set.
func TestTarget_FollowsTheContractFormula(t *testing.T) {
	m := redirect.NewMap()
	if err := m.Replace(doc(1)); err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 50; k++ {
		key := fmt.Sprintf("key-%d", k)
		for s := 0; s < 10; s++ {
			session := fmt.Sprintf("s-%d", s)
			want := expected(key, session, "m", []string{"fr-2", "de-1", "us-1"})[0]
			got := target(t, m, redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1", KeyID: key, Session: session})
			if got != want {
				t.Fatalf("key %s session %s: got %s, want %s", key, session, got, want)
			}
		}
	}
}

// TestTarget_HomeSetIsTwo: one key + model only ever reaches two sibling
// policlouds, however many sessions it uses (its rate limits stay meaningful).
func TestTarget_HomeSetIsTwo(t *testing.T) {
	m := redirect.NewMap()
	if err := m.Replace(doc(1)); err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for s := 0; s < 500; s++ {
		used[target(t, m, redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1", KeyID: "key-1", Session: fmt.Sprint(s)})] = true
	}
	if len(used) != 2 {
		t.Fatalf("one key reached %d policlouds (%v), want 2", len(used), used)
	}
}

// TestTarget_Filters: kind, this policloud, the origin and the scope.
func TestTarget_Filters(t *testing.T) {
	m := redirect.NewMap()
	if err := m.Replace(doc(1)); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		req  redirect.TargetRequest
		want []string // allowed results ("" = no redirect)
	}{
		{"embedding only where placed as embedding", redirect.TargetRequest{Model: "e", Kind: redirect.KindEmbedding, Self: "fr-1", From: "fr-1"}, []string{"de-1"}},
		{"kind mismatch: a chat model is not redirected for embeddings", redirect.TargetRequest{Model: "m", Kind: redirect.KindEmbedding, Self: "fr-1", From: "fr-1"}, []string{""}},
		{"unknown model", redirect.TargetRequest{Model: "x", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1"}, []string{""}},
		{"never this policloud", redirect.TargetRequest{Model: "e", Kind: redirect.KindEmbedding, Self: "de-1", From: "fr-1"}, []string{""}},
		{"never the origin (2nd hop)", redirect.TargetRequest{Model: "e", Kind: redirect.KindEmbedding, Self: "fr-2", From: "de-1"}, []string{""}},
		{"country scope", redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1", Scope: "country:FR"}, []string{"fr-2"}},
		{"EU scope excludes us-1", redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1", Scope: "EU"}, []string{"fr-2", "de-1"}},
		{"scope with no policloud left", redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1", Scope: "country:IT"}, []string{""}},
		{"unknown scope fails closed", redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "fr-1", From: "fr-1", Scope: "anywhere"}, []string{""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k := 0; k < 30; k++ {
				tc.req.KeyID = fmt.Sprint("key-", k)
				got := target(t, m, tc.req)
				ok := false
				for _, w := range tc.want {
					ok = ok || got == w
				}
				if !ok {
					t.Fatalf("key %s: got %q, want one of %v", tc.req.KeyID, got, tc.want)
				}
			}
		})
	}
}

// TestTarget_Hints: fresh hints skip full and non-wakeable cold targets and
// prefer warm over cold; when every target is skipped the first is used
// anyway; expired hints are ignored.
func TestTarget_Hints(t *testing.T) {
	key, session := "key-1", "s"
	order := expected(key, session, "m", []string{"a", "b"}) // the session order of the two siblings
	first, second := order[0], order[1]
	build := func(hFirst, hSecond *redirect.Hint, sfzFirst *bool, expire time.Time) *redirect.Map {
		d := redirect.MapDoc{
			Schema: 2, Version: u64(1), Region: "eu",
			Policlouds: []redirect.Policloud{pc("self", "FR", yes), pc("a", "FR", yes), pc("b", "FR", yes)},
			Placements: []redirect.Placement{
				place("m", redirect.KindChat, first, sfzFirst, hFirst),
				place("m", redirect.KindChat, second, yes, hSecond),
			},
			HintsExpireAt: expire.UTC().Format(time.RFC3339),
		}
		m := redirect.NewMap()
		if err := m.Replace(d); err != nil {
			t.Fatal(err)
		}
		return m
	}
	fresh, stale := time.Now().Add(time.Hour), time.Now().Add(-time.Hour)
	cold, warm := &redirect.Hint{Warm: no}, &redirect.Hint{Warm: yes}
	cases := []struct {
		name            string
		hFirst, hSecond *redirect.Hint
		sfzFirst        *bool
		expire          time.Time
		want            string
	}{
		{"no hints: session order", nil, nil, yes, fresh, first},
		{"first full: skipped", &redirect.Hint{Load: redirect.LoadFull}, nil, yes, fresh, second},
		{"first busy: still used", &redirect.Hint{Load: redirect.LoadBusy}, nil, yes, fresh, first},
		{"first cold, cannot wake: skipped", cold, nil, no, fresh, second},
		{"first cold but wakeable, second warm: prefer warm", cold, warm, yes, fresh, second},
		{"both cold and wakeable: first", cold, cold, yes, fresh, first},
		{"every target skipped: first anyway", &redirect.Hint{Load: redirect.LoadFull}, &redirect.Hint{Load: redirect.LoadFull}, yes, fresh, first},
		{"expired hints are ignored", &redirect.Hint{Load: redirect.LoadFull}, nil, yes, stale, first},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := build(tc.hFirst, tc.hSecond, tc.sfzFirst, tc.expire)
			if got := target(t, m, redirect.TargetRequest{Model: "m", Kind: redirect.KindChat, Self: "self", From: "self", KeyID: key, Session: session}); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

// TestHintsExpired backs hivenet_sibling_map_hints_expired.
func TestHintsExpired(t *testing.T) {
	m := redirect.NewMap()
	if !m.HintsExpired(time.Now()) {
		t.Fatal("no map: hints count as expired")
	}
	d := doc(1)
	d.HintsExpireAt = time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	if err := m.Replace(d); err != nil {
		t.Fatal(err)
	}
	if m.HintsExpired(time.Now()) || !m.HintsExpired(time.Now().Add(2*time.Minute)) {
		t.Fatal("hints must be fresh before hints_expire_at and expired after")
	}
}

// TestScope covers parsing and membership.
func TestScope(t *testing.T) {
	for _, s := range []string{"all", "EU", "country:FR"} {
		if _, err := redirect.ParseScope(s); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
	for _, s := range []string{"", "eu", "country:fr", "country:", "FR", "world"} {
		if _, err := redirect.ParseScope(s); err == nil {
			t.Errorf("%q: expected an error", s)
		}
	}
	fr, us := pc("fr", "FR", yes), pc("us", "US", no)
	if !redirect.Scope("EU").Contains(fr) || redirect.Scope("EU").Contains(us) ||
		!redirect.Scope("country:US").Contains(us) || redirect.Scope("country:US").Contains(fr) ||
		!redirect.ScopeAll.Contains(us) {
		t.Fatal("scope membership is wrong")
	}
}

// TestLocation keeps the request path and query and replaces an old token.
func TestLocation(t *testing.T) {
	u, _ := url.Parse("/v1/chat/completions?api-version=2024&rt=old")
	got := redirect.Location("https://b.example.com", u, "new")
	if want := "https://b.example.com/v1/chat/completions?api-version=2024&rt=new"; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// ---- token --------------------------------------------------------------------

var (
	secretA = redirect.Secret{ID: "k1", Key: []byte("0123456789abcdef0123456789abcdef")}
	secretB = redirect.Secret{ID: "k2", Key: []byte("fedcba9876543210fedcba9876543210")}
)

func signer(t *testing.T, ttl time.Duration, secrets ...redirect.Secret) *redirect.Signer {
	t.Helper()
	s, err := redirect.NewSigner(secrets, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSigner covers the v2 format, expiry with clock skew and every way a
// token can be wrong.
func TestSigner(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	s := signer(t, 60*time.Second, secretA)
	claims := redirect.Claims{KeyID: "k-1", RequestID: "r-1", Model: "m", Scope: "all", From: "fr-1", Site: "de-1", Path: "/v1/chat/completions", Hop: 1, BodySHA: "ab"}
	tok := s.Sign(claims, now)
	if !strings.HasPrefix(tok, "v2.k1.") || strings.Count(tok, ".") != 3 {
		t.Fatalf("token %q is not v2.<kid>.<claims>.<sig>", tok)
	}

	got, err := s.Verify(tok, now.Add(65*time.Second)) // exp + 5 s skew: still valid
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	claims.IssuedAt, claims.Exp = now.Unix(), now.Add(60*time.Second).Unix()
	if got != claims {
		t.Fatalf("claims round trip: got %+v, want %+v", got, claims)
	}

	parts := strings.Split(tok, ".")
	flip := func(s string, i int) string {
		b := []byte(s)
		b[i] ^= 1
		return string(b)
	}
	cases := []struct {
		name  string
		token string
		at    time.Time
		want  error
	}{
		{"expired past the skew", tok, now.Add(66 * time.Second), redirect.ErrTokenExpired},
		{"claims tampered", strings.Join([]string{parts[0], parts[1], flip(parts[2], 3), parts[3]}, "."), now, redirect.ErrTokenSignature},
		{"signature tampered", strings.Join([]string{parts[0], parts[1], parts[2], flip(parts[3], 3)}, "."), now, redirect.ErrTokenSignature},
		{"same kid, another secret", signer(t, time.Minute, redirect.Secret{ID: "k1", Key: secretB.Key}).Sign(claims, now), now, redirect.ErrTokenSignature},
		{"unknown kid", signer(t, time.Minute, secretB).Sign(claims, now), now, redirect.ErrTokenMalformed},
		{"v1 token", "v1." + parts[2] + "." + parts[3], now, redirect.ErrTokenMalformed},
		{"missing part", strings.Join(parts[:3], "."), now, redirect.ErrTokenMalformed},
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

// TestSigner_Rotation: the first secret signs, every secret verifies, so a
// secret rotates without breaking redirects in flight.
func TestSigner_Rotation(t *testing.T) {
	now := time.Now()
	old := signer(t, time.Minute, secretA)
	rotating := signer(t, time.Minute, secretB, secretA)
	if _, err := rotating.Verify(old.Sign(redirect.Claims{Site: "x"}, now), now); err != nil {
		t.Fatalf("a token signed with the previous secret must still verify: %v", err)
	}
	if tok := rotating.Sign(redirect.Claims{Site: "x"}, now); !strings.HasPrefix(tok, "v2.k2.") {
		t.Fatalf("the first secret must sign, got %q", tok)
	}
	if _, err := old.Verify(rotating.Sign(redirect.Claims{Site: "x"}, now), now); !errors.Is(err, redirect.ErrTokenMalformed) {
		t.Fatalf("a router without the new secret refuses its tokens, got %v", err)
	}
}

// TestParseSecrets: id:secret lines, trimmed; every rule enforced.
func TestParseSecrets(t *testing.T) {
	got, err := redirect.ParseSecrets("  k2 : fedcba9876543210fedcba9876543210 \n\n k1:0123456789abcdef0123456789abcdef\n")
	if err != nil {
		t.Fatal(err)
	}
	if want := []redirect.Secret{secretB, secretA}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v (order kept, values trimmed)", got, want)
	}
	for name, text := range map[string]string{
		"empty":          " \n ",
		"no colon":       "0123456789abcdef0123456789abcdef",
		"bad id":         "bad.id:0123456789abcdef0123456789abcdef",
		"id too long":    "abcdefghijklmnopq:0123456789abcdef0123456789abcdef",
		"short secret":   "k1:short",
		"duplicate id":   "k1:0123456789abcdef0123456789abcdef\nk1:fedcba9876543210fedcba9876543210",
		"empty secret":   "k1:",
		"empty id":       ":0123456789abcdef0123456789abcdef",
		"secret 31 byte": "k1:0123456789abcdef0123456789abcde",
	} {
		if _, err := redirect.ParseSecrets(text); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// TestNewSigner_Rejects a signer that could not work.
func TestNewSigner_Rejects(t *testing.T) {
	if _, err := redirect.NewSigner(nil, time.Minute); err == nil {
		t.Error("no secrets accepted")
	}
	if _, err := redirect.NewSigner([]redirect.Secret{secretA}, 0); err == nil {
		t.Error("TTL 0 accepted")
	}
}

// TestReplayGuard: a request ID is accepted once until its token expires.
func TestReplayGuard(t *testing.T) {
	g := redirect.NewReplayGuard()
	now := time.Now()
	until := now.Add(time.Minute)
	if !g.First("r-1", until, now) || g.First("r-1", until, now.Add(30*time.Second)) {
		t.Fatal("a request ID must be accepted once")
	}
	if !g.First("r-2", until, now) {
		t.Fatal("another request ID is independent")
	}
	if !g.First("r-1", until, now.Add(2*time.Minute)) {
		t.Fatal("once its token can no longer verify, the entry is gone")
	}
}

// ---- token authentication -----------------------------------------------------

const rawKey = "sk-test-key-1"

// recorder stands in for the API key provider the redirect wrapper delegates to.
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

// newRedirector is policloud de-1 (DE, EU) of the map doc(1).
func newRedirector(t *testing.T, rejects *[]string) *redirect.Redirector {
	t.Helper()
	m := redirect.NewMap()
	if err := m.Replace(doc(1)); err != nil {
		t.Fatal(err)
	}
	return &redirect.Redirector{
		Self: "de-1", Map: m, Signer: signer(t, 60*time.Second, secretA), Replay: redirect.NewReplayGuard(),
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

// TestTokenAuth exercises the redirect wrapper around the API auth provider:
// acceptance, each refusal reason, and the fallback that serves a request on
// its own valid Authorization key when the token cannot be used.
func TestTokenAuth(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	valid := redirect.Claims{KeyID: "key-1", RequestID: "rid-1", Model: "m", Scope: "all", From: "fr-1", Site: "de-1", Path: "/v1/chat/completions", Hop: 1, BodySHA: "bh"}
	sign := func(mod func(*redirect.Claims)) string {
		c := valid
		if mod != nil {
			mod(&c)
		}
		return signer(t, 60*time.Second, secretA).Sign(c, now)
	}
	keyResult := &auth.AuthResult{TenantID: "tenant-1", KeyID: "key-1"}
	other := &auth.AuthResult{TenantID: "t2", KeyID: "key-2"}

	cases := []struct {
		name         string
		req          *http.Request
		keys         *auth.DynamicKeyProvider
		next         *recorder
		wantErr      bool
		wantRedirect bool   // accepted as a redirected request (claims passed on)
		wantKey      string // the key the request is served on
		wantReason   string // the single rejection recorded ("" = none)
	}{
		{"no token: delegated unchanged", request("", rawKey), registry(t, true, nil),
			&recorder{res: keyResult}, false, false, "key-1", ""},
		{"token only: key resolved by id", request(sign(nil), ""), registry(t, true, nil),
			&recorder{err: auth.ErrMissingCredentials}, false, true, "key-1", ""},
		{"token + the same key (curl back at its first host)", request(sign(nil), rawKey), registry(t, true, nil),
			&recorder{res: keyResult}, false, true, "key-1", ""},
		{"token + another valid key: fallback, served fresh on that key", request(sign(nil), "sk-other"), registry(t, true, nil),
			&recorder{res: other}, false, false, "key-2", "key"},
		{"token + a key that does not authenticate: 401", request(sign(nil), "sk-bad"), registry(t, true, nil),
			&recorder{err: auth.ErrInvalidCredentials}, true, false, "", "key"},
		{"key disabled since the redirect", request(sign(nil), ""), registry(t, false, nil), &recorder{}, true, false, "", "key"},
		{"key expired since the redirect", request(sign(nil), ""), registry(t, true, &past), &recorder{}, true, false, "", "key"},
		{"unknown key id", request(sign(func(c *redirect.Claims) { c.KeyID = "nope" }), ""), registry(t, true, nil), &recorder{}, true, false, "", "key"},
		{"token for another policloud", request(sign(func(c *redirect.Claims) { c.Site = "fr-2" }), ""), registry(t, true, nil), &recorder{}, true, false, "", "site"},
		{"token for another path", request(sign(func(c *redirect.Claims) { c.Path = "/v1/embeddings" }), ""), registry(t, true, nil), &recorder{}, true, false, "", "path"},
		{"scope that excludes this policloud (DE)", request(sign(func(c *redirect.Claims) { c.Scope = "country:FR" }), ""), registry(t, true, nil), &recorder{}, true, false, "", "scope"},
		{"EU scope at an EU policloud", request(sign(func(c *redirect.Claims) { c.Scope = "EU" }), ""), registry(t, true, nil), &recorder{}, false, true, "key-1", ""},
		{"malformed scope", request(sign(func(c *redirect.Claims) { c.Scope = "world" }), ""), registry(t, true, nil), &recorder{}, true, false, "", "scope"},
		{"empty request ID", request(sign(func(c *redirect.Claims) { c.RequestID = "" }), ""), registry(t, true, nil), &recorder{}, true, false, "", "replay"},
		{"expired token", request(signer(t, time.Second, secretA).Sign(valid, now.Add(-time.Minute)), ""), registry(t, true, nil), &recorder{}, true, false, "", "expired"},
		{"expired token + valid key: fallback", request(signer(t, time.Second, secretA).Sign(valid, now.Add(-time.Minute)), rawKey), registry(t, true, nil),
			&recorder{res: keyResult}, false, false, "key-1", "expired"},
		{"forged token", request(signer(t, time.Minute, redirect.Secret{ID: "k1", Key: secretB.Key}).Sign(valid, now), ""), registry(t, true, nil), &recorder{}, true, false, "", "signature"},
		{"garbage token", request("v2.garbage", ""), registry(t, true, nil), &recorder{}, true, false, "", "malformed"},
		{"auth mode none", request(sign(func(c *redirect.Claims) { c.KeyID = "" }), ""), nil,
			&recorder{res: &auth.AuthResult{TenantID: "default"}}, false, true, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rejects []string
			p := redirect.Wrap(newRedirector(t, &rejects), tc.next, tc.keys)
			res, err := p.Authenticate(tc.req)
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := strings.Join(rejects, ","); got != tc.wantReason {
				t.Errorf("reject reasons %q, want %q", got, tc.wantReason)
			}
			if err != nil {
				if !errors.Is(err, auth.ErrInvalidCredentials) {
					t.Errorf("a refused token must look like any bad credential (401), got %v", err)
				}
				return
			}
			if res.KeyID != tc.wantKey {
				t.Errorf("served on key %q, want %q", res.KeyID, tc.wantKey)
			}
			if tc.wantRedirect != (res.Redirect != nil) {
				t.Fatalf("redirect info %+v, want present=%v", res.Redirect, tc.wantRedirect)
			}
			if res.Redirect != nil {
				want := auth.RedirectInfo{Hop: 1, BodySHA: "bh", RequestID: "rid-1", Model: "m", Scope: res.Redirect.Scope, From: "fr-1"}
				if *res.Redirect != want {
					t.Errorf("redirect info %+v, want %+v", *res.Redirect, want)
				}
			}
		})
	}
}

// TestTokenAuth_SingleUse: the same token is accepted once; the second use is
// refused as a replay, and with a valid key it falls back to a fresh request.
func TestTokenAuth_SingleUse(t *testing.T) {
	var rejects []string
	rd := newRedirector(t, &rejects)
	keys := registry(t, true, nil)
	p := redirect.Wrap(rd, keys, keys)
	tok := rd.Signer.Sign(redirect.Claims{KeyID: "key-1", RequestID: "rid-9", Model: "m", Scope: "all", From: "fr-1", Site: "de-1", Path: "/v1/chat/completions", Hop: 1}, time.Now())

	if res, err := p.Authenticate(request(tok, "")); err != nil || res.Redirect == nil {
		t.Fatalf("first use: %v %+v", err, res)
	}
	if _, err := p.Authenticate(request(tok, "")); !errors.Is(err, auth.ErrInvalidCredentials) {
		t.Fatalf("second use: got %v, want 401", err)
	}
	res, err := p.Authenticate(request(tok, rawKey))
	if err != nil || res.Redirect != nil || res.KeyID != "key-1" {
		t.Fatalf("replay with a valid key: got %+v %v, want a fresh request on the key", res, err)
	}
	if got := strings.Join(rejects, ","); got != "replay,replay" {
		t.Fatalf("reasons %q, want replay,replay", got)
	}
}

// TestTokenAuth_ScopeNeedsOwnCountry: a country scope is checked against this
// policloud's map entry; without one it fails closed.
func TestTokenAuth_ScopeNeedsOwnCountry(t *testing.T) {
	var rejects []string
	rd := newRedirector(t, &rejects)
	rd.Self = "xx-1" // not in the map
	keys := registry(t, true, nil)
	tok := rd.Signer.Sign(redirect.Claims{KeyID: "key-1", RequestID: "r", Scope: "EU", Site: "xx-1", Path: "/v1/chat/completions", Hop: 1}, time.Now())
	if _, err := redirect.Wrap(rd, keys, keys).Authenticate(request(tok, "")); err == nil || strings.Join(rejects, ",") != "scope" {
		t.Fatalf("got %v %v, want a 401 for scope", err, rejects)
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

// TestWrap_KeepsTenantListing: the router publishes per-tenant quota gauges by
// type-asserting the installed API provider to Tenants(); the wrapper must
// pass that through, or enabling redirect silently stops the gauges.
func TestWrap_KeepsTenantListing(t *testing.T) {
	var rejects []string
	keys := registry(t, true, nil)
	lister, ok := redirect.Wrap(newRedirector(t, &rejects), keys, keys).(interface {
		Tenants() map[string]auth.QuotaLimits
	})
	if !ok {
		t.Fatal("the wrapped provider must still list tenants")
	}
	if got := lister.Tenants(); !reflect.DeepEqual(got, keys.Tenants()) || len(got) != 1 {
		t.Fatalf("Tenants() = %v, want the wrapped registry's %v", got, keys.Tenants())
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
