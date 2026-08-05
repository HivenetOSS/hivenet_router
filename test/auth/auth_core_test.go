// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package auth_test — core authentication primitives: JWT sign/verify, the
// in-memory session manager, static API-key and admin-key providers, expiry
// parsing, and deterministic gRPC credential derivation.
package auth_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hivenet_router/internal/auth"
)

// hashKey mirrors how the provider stores keys: only the SHA-256 digest is
// persisted, never the raw secret, so a leaked config file can't reveal keys.
func hashKey(k string) string {
	h := sha256.Sum256([]byte(k))
	return hex.EncodeToString(h[:])
}

// bearerReq builds a request carrying the standard "Authorization: Bearer <token>"
// header the providers parse; an empty token omits the header entirely (the
// "missing credentials" case).
func bearerReq(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

// --- JWT ------------------------------------------------------------------

func TestJWT_RoundTrip(t *testing.T) {
	// Sign a token, then verify with the same secret: the subject must survive
	// the round-trip intact so downstream code can trust the caller's identity.
	secret := []byte("shared-hmac-secret")
	tok, err := auth.CreateToken("agent-42", time.Hour, secret)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	sub, err := auth.NewJWTValidator(secret).ValidateToken(tok)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if sub != "agent-42" {
		t.Errorf("subject = %q, want agent-42", sub)
	}
}

func TestJWT_WrongSecretRejected(t *testing.T) {
	// Verifying with a different secret must fail the HMAC signature check —
	// otherwise anyone could forge tokens without knowing the signing key.
	tok, _ := auth.CreateToken("a", time.Hour, []byte("secret-A"))
	if _, err := auth.NewJWTValidator([]byte("secret-B")).ValidateToken(tok); err == nil {
		t.Error("token signed with a different secret must be rejected")
	}
}

func TestJWT_ExpiredRejected(t *testing.T) {
	// A negative TTL yields an already-expired token; validation must honour the
	// exp claim so revoked/stale credentials can't be replayed indefinitely.
	secret := []byte("s")
	tok, _ := auth.CreateToken("a", -1*time.Minute, secret) // already expired
	if _, err := auth.NewJWTValidator(secret).ValidateToken(tok); err == nil {
		t.Error("expired token must be rejected")
	}
}

func TestJWT_GarbageRejected(t *testing.T) {
	// Non-JWT input must be rejected rather than parsed leniently — the validator
	// must never treat unparseable junk as an authenticated subject.
	if _, err := auth.NewJWTValidator([]byte("s")).ValidateToken("not.a.jwt"); err == nil {
		t.Error("malformed token must be rejected")
	}
}

// --- SessionManager -------------------------------------------------------

func TestSessionManager_Lifecycle(t *testing.T) {
	// Create → validate → delete: covers the full server-side session lifecycle
	// the router relies on to gate agent connections.
	m := auth.NewSessionManager(time.Hour)
	tok := m.CreateSession("agent-1", nil)
	if tok == "" {
		t.Fatal("CreateSession returned empty token")
	}

	sess, ok := m.ValidateSession(tok)
	if !ok || sess == nil {
		t.Fatal("freshly created session must validate")
	}

	// Unknown token → false.
	if _, ok := m.ValidateSession("nope"); ok {
		t.Error("unknown token must not validate")
	}

	// Delete removes it.
	m.DeleteSession(tok)
	if _, ok := m.ValidateSession(tok); ok {
		t.Error("deleted session must not validate")
	}
}

func TestSessionManager_ExpiryAndCleanup(t *testing.T) {
	// A negative TTL makes every session expire immediately, letting us assert
	// expiry handling without sleeping in the test.
	m := auth.NewSessionManager(-1 * time.Second) // sessions are born expired
	tok := m.CreateSession("agent-1", nil)

	// ValidateSession must report expired (and delete it) so stale tokens are
	// reaped lazily on first access, not just by the background sweep.
	if _, ok := m.ValidateSession(tok); ok {
		t.Error("expired session must not validate")
	}

	// CleanupExpired on a fresh set counts the removals — the return count is
	// what the periodic sweeper reports/metrics.
	m2 := auth.NewSessionManager(-1 * time.Second)
	m2.CreateSession("a", nil)
	m2.CreateSession("b", nil)
	if n := m2.CleanupExpired(); n != 2 {
		t.Errorf("CleanupExpired = %d, want 2", n)
	}
}

// --- StaticKeyProvider ----------------------------------------------------

// validEntry builds a well-formed key entry (hashed key + required owner) for
// the happy-path and as a base to mutate into invalid variants below.
func validEntry(rawKey, owner string) auth.APIKeyEntry {
	return auth.APIKeyEntry{
		KeyHash:    hashKey(rawKey),
		KeyPreview: "sk-...xyz",
		Metadata:   auth.KeyMetadata{Name: "test key", Owner: owner},
	}
}

func TestStaticKeyProvider_Authenticate(t *testing.T) {
	p, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{validEntry("secret-key", "team-a")})
	if err != nil {
		t.Fatalf("NewStaticKeyProvider: %v", err)
	}

	// Correct key → tenant resolved. The owner on the entry becomes the tenant
	// identity carried through the rest of the request.
	res, err := p.Authenticate(bearerReq("secret-key"))
	if err != nil || res == nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	if res.TenantID != "team-a" {
		t.Errorf("TenantID = %q, want team-a", res.TenantID)
	}

	// Wrong key → invalid. Distinguished from "missing" so callers can tell a
	// bad credential (401) from an absent one, without leaking which keys exist.
	if _, err := p.Authenticate(bearerReq("wrong-key")); err != auth.ErrInvalidCredentials {
		t.Errorf("wrong key: got %v, want ErrInvalidCredentials", err)
	}
	// Missing token → distinct sentinel so the middleware can emit the right
	// challenge instead of a generic failure.
	if _, err := p.Authenticate(bearerReq("")); err != auth.ErrMissingCredentials {
		t.Errorf("no token: got %v, want ErrMissingCredentials", err)
	}
}

func TestStaticKeyProvider_ConstructorValidation(t *testing.T) {
	// Fail fast at construction on misconfiguration rather than at request time:
	// a provider with zero keys can never authenticate anyone, so reject it.
	if _, err := auth.NewStaticKeyProvider(nil); err == nil {
		t.Error("empty entries must be rejected")
	}
	// Missing owner: without an owner there is no tenant to attribute the request
	// to, so the entry is unusable and must be rejected up front.
	bad := auth.APIKeyEntry{KeyHash: hashKey("k"), Metadata: auth.KeyMetadata{Name: "n"}}
	if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{bad}); err == nil {
		t.Error("missing owner must be rejected")
	}
	// Duplicate hash: two entries with the same key hash would make tenant
	// resolution ambiguous, so a collision is a config error.
	e := validEntry("k", "o")
	if _, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{e, e}); err == nil {
		t.Error("duplicate key_hash must be rejected")
	}
}

func TestStaticKeyProvider_ExpiredKey(t *testing.T) {
	// A structurally valid key whose ExpiresAt is in the past must still be
	// refused at auth time — expiry is enforced per request, not at load.
	e := validEntry("secret-key", "team-a")
	e.Metadata.ExpiresAt = "01-01-2000" // long past
	p, err := auth.NewStaticKeyProvider([]auth.APIKeyEntry{e})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	if _, err := p.Authenticate(bearerReq("secret-key")); err != auth.ErrInvalidCredentials {
		t.Errorf("expired key: got %v, want ErrInvalidCredentials", err)
	}
}

func TestStaticAdminKeyProvider(t *testing.T) {
	// Admin keys authenticate to the fixed "admin" tenant that gates the
	// privileged /admin endpoints, so the mapping must be exact.
	p, err := auth.NewStaticAdminKeyProvider([]string{"admin-secret"})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	res, err := p.Authenticate(bearerReq("admin-secret"))
	if err != nil || res.TenantID != "admin" {
		t.Errorf("admin auth: res=%v err=%v", res, err)
	}
	// A non-admin key must not slip through to the privileged tenant.
	if _, err := p.Authenticate(bearerReq("nope")); err != auth.ErrInvalidCredentials {
		t.Errorf("bad admin key: got %v", err)
	}
	// Constructor rejects empty set and duplicates (same fail-fast rationale as
	// the API-key provider — no usable/ambiguous admin config).
	if _, err := auth.NewStaticAdminKeyProvider(nil); err == nil {
		t.Error("empty admin keys must be rejected")
	}
	if _, err := auth.NewStaticAdminKeyProvider([]string{"x", "x"}); err == nil {
		t.Error("duplicate admin keys must be rejected")
	}
}

// --- expiry parsing -------------------------------------------------------

func TestParseExpiresAt(t *testing.T) {
	// Empty string means "no expiry": (nil, nil), not an error — keys without an
	// expiry date are valid forever.
	if ts, err := auth.ParseExpiresAt(""); err != nil || ts != nil {
		t.Errorf("empty → (nil,nil), got (%v,%v)", ts, err)
	}
	ts, err := auth.ParseExpiresAt("01-01-2027")
	if err != nil || ts == nil {
		t.Fatalf("valid date: %v", err)
	}
	// Valid through the whole day → expiry is start of the NEXT day UTC.
	want := time.Date(2027, 1, 2, 0, 0, 0, 0, time.UTC)
	if !ts.Equal(want) {
		t.Errorf("expiry = %v, want %v (end-of-day semantics)", ts, want)
	}
	// ISO order (YYYY-MM-DD) is intentionally rejected: only DD-MM-YYYY is
	// accepted, so an ambiguous date can't be silently misinterpreted.
	if _, err := auth.ParseExpiresAt("2027-01-01"); err == nil {
		t.Error("wrong format must error (expects DD-MM-YYYY)")
	}
}

// --- gRPC credential derivation ------------------------------------------

func TestDeriveGRPCCredentials_Deterministic(t *testing.T) {
	// Router and agent each derive the gRPC key pair from the same shared secret
	// independently — determinism is what lets them agree on the TLS identity
	// without exchanging keys, while distinct secrets must yield distinct keys.
	secret := []byte("shared-secret")
	_, pub1, err := auth.DeriveGRPCCredentials(secret)
	if err != nil {
		t.Fatalf("derive 1: %v", err)
	}
	_, pub2, err := auth.DeriveGRPCCredentials(secret)
	if err != nil {
		t.Fatalf("derive 2: %v", err)
	}
	if !pub1.Equal(pub2) {
		t.Error("same secret must derive the same public key (deterministic)")
	}

	_, pub3, _ := auth.DeriveGRPCCredentials([]byte("different-secret"))
	if pub1.Equal(pub3) {
		t.Error("different secrets must derive different public keys")
	}
}
