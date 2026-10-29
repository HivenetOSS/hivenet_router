// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package redirect implements cross-site redirects: when no healthy agent on
// this site serves a model, the router answers HTTP 307 to a sibling site that
// does, so a client configured with one hostname still reaches its model.
//
// Clients drop the Authorization header when a redirect changes host, so the
// 307 carries a short-lived signed token in the ?rt= query parameter. The
// sibling authenticates the repeated request with that token (TokenAuth).
//
// The router only receives the sibling map; an external controller builds it
// and pushes it through POST /admin/sibling-map. With no site ID configured the
// package is never wired in and the router behaves exactly as before.
package redirect

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// QueryParam is the query parameter that carries the redirect token.
	QueryParam = "rt"

	// MaxHops is the loop guard: a request that has already been redirected
	// this many times is never redirected again, so a stale or inconsistent
	// map costs at most one extra hop, never a loop.
	MaxHops = 2

	// tokenPrefix versions the token format so it can change without
	// ambiguity during a rolling upgrade.
	tokenPrefix = "v1."
)

// Errors returned by Map.Replace and Signer.Verify.
var (
	// ErrStaleVersion is returned when a pushed map is older than the current
	// one. Versions compare lexicographically, as API-key registry versions do.
	ErrStaleVersion = errors.New("redirect: stale sibling map version")

	ErrTokenMalformed = errors.New("redirect: malformed token")
	ErrTokenSignature = errors.New("redirect: bad token signature")
	ErrTokenExpired   = errors.New("redirect: token expired")
)

// Site is one entry of the sibling map: a site's ID, its public base URL and
// the models it serves.
type Site struct {
	ID     string   `json:"id"`
	URL    string   `json:"url"`
	Models []string `json:"models"`
}

// Map is the versioned, in-memory sibling map. It is replaced whole by each
// push and is safe for concurrent use. The map is also the redirect allowlist:
// a Location header only ever points at a URL taken from it.
type Map struct {
	mu      sync.RWMutex
	version string
	sites   []Site
	byModel map[string][]Site
}

// NewMap returns an empty map; no model is redirected until the first push.
func NewMap() *Map { return &Map{byModel: map[string][]Site{}} }

// Replace validates sites and swaps them in atomically. A version older than
// the current one is rejected with ErrStaleVersion; an equal version is a
// harmless replay. Nothing changes when any site is invalid.
func (m *Map) Replace(version string, sites []Site) error {
	if version == "" {
		return errors.New("redirect: version is required")
	}
	byModel := map[string][]Site{}
	seen := map[string]bool{}
	clean := make([]Site, 0, len(sites))
	for i, s := range sites {
		u, err := validateSite(s)
		if err != nil {
			return fmt.Errorf("redirect: site[%d]: %w", i, err)
		}
		if seen[s.ID] {
			return fmt.Errorf("redirect: site[%d]: duplicate id %q", i, s.ID)
		}
		seen[s.ID] = true
		s.URL = u
		s.Models = append([]string(nil), s.Models...)
		clean = append(clean, s)
		for _, model := range s.Models {
			byModel[model] = append(byModel[model], s)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if version < m.version {
		return ErrStaleVersion
	}
	m.version, m.sites, m.byModel = version, clean, byModel
	return nil
}

// validateSite checks one entry and returns its URL without a trailing slash.
// Only absolute https URLs without path, query or credentials are accepted:
// clients follow a redirect even from https down to http and then send the
// token (and some send the API key) in clear text.
func validateSite(s Site) (string, error) {
	if s.ID == "" {
		return "", errors.New("id must not be empty")
	}
	u, err := url.Parse(s.URL)
	if err != nil {
		return "", fmt.Errorf("site %q: url: %w", s.ID, err)
	}
	if u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		strings.Trim(u.Path, "/") != "" {
		return "", fmt.Errorf("site %q: url must be an absolute https:// origin with no path, query or credentials, got %q", s.ID, s.URL)
	}
	for _, model := range s.Models {
		if model == "" {
			return "", fmt.Errorf("site %q: model names must not be empty", s.ID)
		}
	}
	return strings.TrimSuffix(s.URL, "/"), nil
}

// Snapshot returns the current version and a copy of the sites.
func (m *Map) Snapshot() (string, []Site) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version, append([]Site(nil), m.sites...)
}

// Target picks the sibling for model, excluding the site self. The choice is
// a rendezvous hash of affinity over the candidate sites: the same affinity
// key always lands on the same site, and a site leaving the map moves only the
// keys it was winning. ok is false when no other site serves the model.
func (m *Map) Target(model, self, affinity string) (site Site, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var best uint64
	for _, s := range m.byModel[model] {
		if s.ID == self {
			continue
		}
		h := sha256.Sum256([]byte(affinity + "\x00" + s.ID))
		if score := binary.BigEndian.Uint64(h[:8]); !ok || score > best {
			site, best, ok = s, score, true
		}
	}
	return site, ok
}

// Location builds the redirect URL: the sibling's origin, the request's own
// path, the request's own query parameters (any previous token replaced) and
// the new token.
func Location(siteURL string, req *url.URL, token string) string {
	q := req.Query()
	q.Set(QueryParam, token)
	return siteURL + req.Path + "?" + q.Encode()
}

// Claims is the body of a redirect token.
type Claims struct {
	KeyID     string `json:"kid"`   // the caller's API-key ID ("" with auth mode none), never the key
	RequestID string `json:"rid"`   // the request ID the first site assigned
	Model     string `json:"model"` // the model the first site routed on
	Site      string `json:"site"`  // the only site that accepts this token
	Path      string `json:"path"`  // the only path the token is valid for
	Hop       int    `json:"hop"`   // redirects so far, including this one
	BodySHA   string `json:"bh"`    // hex SHA-256 of the request body as the client sent it
	Exp       int64  `json:"exp"`   // expiry, Unix seconds
}

// Signer creates and checks redirect tokens with one HMAC-SHA256 key shared
// by every site.
type Signer struct {
	key []byte
	ttl time.Duration
}

// NewSigner returns a Signer whose tokens are valid for ttl.
func NewSigner(key []byte, ttl time.Duration) *Signer {
	return &Signer{key: append([]byte(nil), key...), ttl: ttl}
}

// Sign stamps c with an expiry of now + ttl and returns the token:
// "v1." + base64url(JSON claims) + "." + base64url(HMAC-SHA256).
func (s *Signer) Sign(c Claims, now time.Time) string {
	c.Exp = now.Add(s.ttl).Unix()
	body, _ := json.Marshal(c) // Claims holds only strings and ints: cannot fail
	payload := tokenPrefix + base64.RawURLEncoding.EncodeToString(body)
	return payload + "." + base64.RawURLEncoding.EncodeToString(s.mac(payload))
}

// Verify checks the signature and expiry of token and returns its claims.
// The signature is checked before the payload is decoded.
func (s *Signer) Verify(token string, now time.Time) (Claims, error) {
	var c Claims
	dot := strings.LastIndexByte(token, '.')
	if !strings.HasPrefix(token, tokenPrefix) || dot <= len(tokenPrefix) {
		return c, ErrTokenMalformed
	}
	payload, sig := token[:dot], token[dot+1:]
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, s.mac(payload)) {
		return c, ErrTokenSignature
	}
	body, err := base64.RawURLEncoding.DecodeString(payload[len(tokenPrefix):])
	if err != nil {
		return c, ErrTokenMalformed
	}
	if err := json.Unmarshal(body, &c); err != nil {
		return c, ErrTokenMalformed
	}
	if now.Unix() > c.Exp {
		return c, ErrTokenExpired
	}
	return c, nil
}

func (s *Signer) mac(payload string) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(payload))
	return m.Sum(nil)
}

// BodySHA returns the hex SHA-256 of a request body, as carried in Claims.BodySHA.
func BodySHA(body []byte) string {
	h := sha256.Sum256(body)
	return fmt.Sprintf("%x", h)
}
