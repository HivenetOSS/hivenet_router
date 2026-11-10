// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package redirect implements cross-site redirects: when no healthy agent on
// this site serves a model, the router answers HTTP 307 to a sibling
// site that does, so a client configured with one hostname still reaches
// its model.
//
// Clients drop the Authorization header when a redirect changes host, so the
// 307 carries a short-lived, single-use signed token in the ?rt= query
// parameter. The sibling authenticates the repeated request with that token
// (TokenAuth).
//
// The router only receives the sibling map; an external controller builds it
// and pushes it through POST /admin/sibling-map. With no site ID configured
// the package is never wired in and the router behaves exactly as before.
//
// The map format, the redirect decision and the token are frozen in contract 3
// v2 (hivenet_ai_infra docs/contracts/03-sibling-map.md).
package redirect

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// QueryParam is the query parameter that carries the redirect token.
	QueryParam = "rt"

	// MaxHops is the loop guard: a request that has already been redirected
	// this many times is never redirected again.
	MaxHops = 2

	// Schema is the only sibling-map schema this router understands.
	Schema = 2

	// ClockSkew is the grace a token gets past its expiry, for clock drift
	// between the routers that sign and verify it.
	ClockSkew = 5 * time.Second

	// MaxHoldBudget bounds a placement's hold_budget: how long a router may
	// hold a request while the model wakes.
	MaxHoldBudget = time.Hour

	// homeSize is how many sibling sites one key + model may use, which
	// keeps the key's rate limits meaningful (contract 3 v2 §3 step 4).
	homeSize = 2

	tokenPrefix = "v2."
)

// Placement kinds: which inference endpoints may redirect for a model.
const (
	KindChat      = "chat"
	KindEmbedding = "embedding"
	KindRerank    = "rerank"
)

// Hint loads.
const (
	LoadOK   = "ok"
	LoadBusy = "busy"
	LoadFull = "full"
)

// Errors returned by Map.Replace, NewSigner and Signer.Verify.
var (
	// ErrStaleVersion is returned when a pushed map has a lower version than
	// the current one. Versions compare numerically.
	ErrStaleVersion = errors.New("redirect: stale sibling map version")

	ErrTokenMalformed  = errors.New("redirect: malformed token")
	ErrTokenSignature  = errors.New("redirect: bad token signature")
	ErrTokenExpired    = errors.New("redirect: token expired")
	ErrTokenFromFuture = errors.New("redirect: token issued in the future")
)

// ---- sibling map ------------------------------------------------------------

// Sites, placements and hints as pushed. Pointers mark required fields
// whose zero value is meaningful (a missing bool must not read as false).
type (
	// Site is one site: its public entry URL and where it is.
	Site struct {
		ID      string `json:"id"`
		URL     string `json:"url"`
		Country string `json:"country"`
		EU      *bool  `json:"eu"`
	}

	// Placement says a model of a kind runs in a site. Only Model, Kind,
	// Site and ScaleFromZero decide where a request goes; Hint only lets
	// the router skip a full or cold target. HoldBudget is optional: how long
	// the site's routers hold a request while the model wakes from zero,
	// as a Go duration such as "240s" (the router's default when absent).
	Placement struct {
		Model         string `json:"model"`
		Kind          string `json:"kind"`
		Site          string `json:"site"`
		ScaleFromZero *bool  `json:"scale_from_zero"`
		HoldBudget    string `json:"hold_budget,omitempty"`
		Hint          *Hint  `json:"hint,omitempty"`

		holdBudget time.Duration // HoldBudget, parsed by Map.Replace
	}

	// Hint is advisory load for one placement. A missing field reads as the
	// default: warm, ok.
	Hint struct {
		Warm *bool  `json:"warm,omitempty"`
		Load string `json:"load,omitempty"`
	}

	// MapDoc is the body of POST /admin/sibling-map.
	MapDoc struct {
		Schema        int         `json:"schema"`
		Version       *uint64     `json:"version"`
		Region        string      `json:"region"`
		GeneratedAt   string      `json:"generated_at,omitempty"`
		Sites         []Site      `json:"sites"`
		Placements    []Placement `json:"placements"`
		HintsExpireAt string      `json:"hints_expire_at,omitempty"`
	}
)

func (h *Hint) warm() bool { return h == nil || h.Warm == nil || *h.Warm }

func (h *Hint) load() string {
	if h == nil || h.Load == "" {
		return LoadOK
	}
	return h.Load
}

// Map is the in-memory sibling map, replaced whole by each push and safe for
// concurrent use. It is also the redirect allowlist: a Location header only
// ever points at a site URL taken from it, and every such URL's host
// lies under one of the allowed domains.
type Map struct {
	domains     []string // allowed host suffixes, from ParseDomains
	mu          sync.RWMutex
	doc         MapDoc
	set         bool
	sites       map[string]Site
	byModel     map[string][]Placement
	hintsExpire time.Time
}

// NewMap returns an empty map that accepts site URLs only under
// domains (from ParseDomains; none = every push is refused). Nothing is
// redirected until the first push.
func NewMap(domains []string) *Map { return &Map{domains: domains} }

// ParseDomains parses the allowed host suffixes of site URLs: a
// comma-separated list such as ".inference.hivecompute.ai". Each suffix must
// start with "." so that it can only match whole DNS labels
// ("evilhivecompute.ai" never matches "hivecompute.ai"). At least one is
// required. Matching ignores case.
func ParseDomains(s string) ([]string, error) {
	var out []string
	for _, d := range strings.Split(s, ",") {
		d = strings.ToLower(strings.TrimSpace(d))
		if d == "" {
			continue
		}
		if !allowedDomain.MatchString(d) {
			return nil, fmt.Errorf("redirect: allowed domain %q must be a host suffix starting with \".\", such as .inference.hivecompute.ai", d)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, errors.New("redirect: at least one allowed domain is required")
	}
	return out, nil
}

var allowedDomain = regexp.MustCompile(`^(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$`)

var countryCode = regexp.MustCompile(`^[A-Z]{2}$`)

// Replace validates doc and swaps it in atomically. A lower version is
// rejected with ErrStaleVersion; an equal version is a harmless replay.
// Nothing changes when anything is invalid.
func (m *Map) Replace(doc MapDoc) error {
	if doc.Schema != Schema {
		return fmt.Errorf("redirect: unsupported map schema %d (this router supports %d)", doc.Schema, Schema)
	}
	if doc.Version == nil {
		return errors.New("redirect: version is required")
	}
	if doc.Region == "" {
		return errors.New("redirect: region is required")
	}

	sites := make(map[string]Site, len(doc.Sites))
	clean := make([]Site, 0, len(doc.Sites))
	for i, p := range doc.Sites {
		u, err := m.validateSite(p)
		if err != nil {
			return fmt.Errorf("redirect: sites[%d]: %w", i, err)
		}
		if _, dup := sites[p.ID]; dup {
			return fmt.Errorf("redirect: sites[%d]: duplicate id %q", i, p.ID)
		}
		p.URL = u
		sites[p.ID] = p
		clean = append(clean, p)
	}

	byModel := map[string][]Placement{}
	seen := map[[2]string]bool{}
	hinted := false
	for i, pl := range doc.Placements {
		if err := validatePlacement(pl, sites); err != nil {
			return fmt.Errorf("redirect: placements[%d]: %w", i, err)
		}
		budget, err := parseHoldBudget(pl)
		if err != nil {
			return fmt.Errorf("redirect: placements[%d]: %w", i, err)
		}
		pl.holdBudget = budget
		key := [2]string{pl.Model, pl.Site}
		if seen[key] {
			return fmt.Errorf("redirect: placements[%d]: duplicate placement of %q in %q", i, pl.Model, pl.Site)
		}
		seen[key] = true
		hinted = hinted || pl.Hint != nil
		byModel[pl.Model] = append(byModel[pl.Model], pl)
	}

	var hintsExpire time.Time
	switch {
	case doc.HintsExpireAt != "":
		t, err := time.Parse(time.RFC3339, doc.HintsExpireAt)
		if err != nil {
			return fmt.Errorf("redirect: hints_expire_at: %w", err)
		}
		hintsExpire = t
	case hinted:
		return errors.New("redirect: hints_expire_at is required when any placement carries a hint")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.set && *doc.Version < *m.doc.Version {
		return ErrStaleVersion
	}
	doc.Sites = clean
	doc.Placements = append([]Placement(nil), doc.Placements...)
	m.doc, m.set, m.sites, m.byModel, m.hintsExpire = doc, true, sites, byModel, hintsExpire
	return nil
}

// validateSite checks one entry and returns its URL without a trailing
// slash. Only absolute https origins are accepted: clients follow a redirect
// from https down to http and then send the token (and some the API key) in
// clear text.
func (m *Map) validateSite(p Site) (string, error) {
	if p.ID == "" {
		return "", errors.New("id must not be empty")
	}
	u, err := url.Parse(p.URL)
	if err != nil {
		return "", fmt.Errorf("site %q: url: %w", p.ID, err)
	}
	if u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		strings.Trim(u.Path, "/") != "" {
		return "", fmt.Errorf("site %q: url must be an absolute https:// origin with no path, query or credentials, got %q", p.ID, p.URL)
	}
	if !m.allowedHost(u.Hostname()) {
		return "", fmt.Errorf("site %q: host %q is not under the allowed domains %v", p.ID, u.Hostname(), m.domains)
	}
	if !countryCode.MatchString(p.Country) {
		return "", fmt.Errorf("site %q: country must be an ISO 3166-1 alpha-2 code, got %q", p.ID, p.Country)
	}
	if p.EU == nil {
		return "", fmt.Errorf("site %q: eu is required", p.ID)
	}
	return strings.TrimSuffix(p.URL, "/"), nil
}

func validatePlacement(pl Placement, sites map[string]Site) error {
	if pl.Model == "" {
		return errors.New("model must not be empty")
	}
	switch pl.Kind {
	case KindChat, KindEmbedding, KindRerank:
	default:
		return fmt.Errorf("model %q: unknown kind %q", pl.Model, pl.Kind)
	}
	if _, ok := sites[pl.Site]; !ok {
		return fmt.Errorf("model %q: unknown site %q", pl.Model, pl.Site)
	}
	if pl.ScaleFromZero == nil {
		return fmt.Errorf("model %q in %q: scale_from_zero is required", pl.Model, pl.Site)
	}
	if pl.Hint != nil {
		switch pl.Hint.Load {
		case "", LoadOK, LoadBusy, LoadFull:
		default:
			return fmt.Errorf("model %q in %q: unknown hint load %q", pl.Model, pl.Site, pl.Hint.Load)
		}
	}
	return nil
}

// parseHoldBudget returns a placement's hold budget, or 0 when it has none.
func parseHoldBudget(pl Placement) (time.Duration, error) {
	if pl.HoldBudget == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(pl.HoldBudget)
	if err != nil || d <= 0 || d > MaxHoldBudget {
		return 0, fmt.Errorf("model %q in %q: hold_budget %q must be a duration such as \"240s\", greater than 0 and at most %v",
			pl.Model, pl.Site, pl.HoldBudget, MaxHoldBudget)
	}
	return d, nil
}

// allowedHost reports whether host ends with one of the allowed domains.
func (m *Map) allowedHost(host string) bool {
	host = strings.ToLower(host)
	for _, d := range m.domains {
		if strings.HasSuffix(host, d) {
			return true
		}
	}
	return false
}

// Snapshot returns the current map (zero MapDoc before the first push).
func (m *Map) Snapshot() MapDoc {
	m.mu.RLock()
	defer m.mu.RUnlock()
	doc := m.doc
	doc.Sites = append([]Site(nil), m.doc.Sites...)
	doc.Placements = append([]Placement(nil), m.doc.Placements...)
	return doc
}

// Version returns the current map version (0 before the first push).
func (m *Map) Version() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.set {
		return 0
	}
	return *m.doc.Version
}

// HintsExpired reports whether the router currently ignores every hint:
// none were pushed, or hints_expire_at has passed.
func (m *Map) HintsExpired(now time.Time) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !now.Before(m.hintsExpire)
}

// Site returns a site of the map by ID.
func (m *Map) Site(id string) (Site, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.sites[id]
	return p, ok
}

// TargetRequest is the input of Map.Target.
type TargetRequest struct {
	Model, Kind string
	Self, From  string // this site and the request's origin; both excluded
	KeyID       string
	Session     string // the X-Session-ID header ("" when absent)
	Scope       Scope
	Now         time.Time
}

// Target picks the sibling site for a request (contract 3 v2 §3 steps
// 3–7): placements of the model and kind, inside the scope, minus this
// site and the origin; the key's home set (top 2 by H(keyID, model,
// site)); ranked by the session; then, while the hints are fresh, no
// redirect when every candidate is cold and the model may be woken here
// (placed here with scale_from_zero: it is held and woken here instead),
// else skipping full or non-wakeable cold targets. ok is
// false when there is no target.
func (m *Map) Target(req TargetRequest) (Site, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type cand struct {
		pl       Placement
		pc       Site
		home, by uint64
	}
	var cands []cand
	for _, pl := range m.byModel[req.Model] {
		if pl.Kind != req.Kind || pl.Site == req.Self || pl.Site == req.From {
			continue
		}
		pc := m.sites[pl.Site]
		if !req.Scope.Contains(pc) {
			continue
		}
		cands = append(cands, cand{pl: pl, pc: pc, home: score(req.KeyID, req.Model, pc.ID)})
	}
	if len(cands) == 0 {
		return Site{}, false
	}

	sort.Slice(cands, func(i, j int) bool { return better(cands[i].home, cands[j].home, cands[i].pc.ID, cands[j].pc.ID) })
	if len(cands) > homeSize {
		cands = cands[:homeSize]
	}
	session := req.KeyID + "\x00" + req.Session
	for i := range cands {
		cands[i].by = score(session, req.Model, cands[i].pc.ID)
	}
	sort.Slice(cands, func(i, j int) bool { return better(cands[i].by, cands[j].by, cands[i].pc.ID, cands[j].pc.ID) })

	if !req.Now.Before(m.hintsExpire) {
		return cands[0].pc, true // no fresh hints: the session order alone
	}
	if _, here := m.wakeable(req.Model, req.Kind, req.Self); here && !slices.ContainsFunc(cands, func(c cand) bool { return c.pl.Hint.warm() }) {
		return Site{}, false // every sibling is cold and the model may be woken here: wake it here
	}
	firstEligible := -1
	for i, c := range cands {
		if c.pl.Hint.load() == LoadFull || (!c.pl.Hint.warm() && !*c.pl.ScaleFromZero) {
			continue
		}
		if c.pl.Hint.warm() {
			return c.pc, true // the first eligible warm target
		}
		if firstEligible < 0 {
			firstEligible = i
		}
	}
	if firstEligible >= 0 {
		return cands[firstEligible].pc, true // only cold, wakeable targets
	}
	return cands[0].pc, true // every target skipped: a stale hint must not block service
}

// Wakeable reports whether a request for model, as kind, may be held on
// site while the model wakes: the map places it there with
// scale_from_zero. budget is the placement's hold_budget, 0 when it has none.
func (m *Map) Wakeable(model, kind, site string) (budget time.Duration, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if pl, ok := m.wakeable(model, kind, site); ok {
		return pl.holdBudget, true
	}
	return 0, false
}

// WakeableModels returns, sorted, the models the map places on site
// with scale_from_zero: the models whose requests may be held there.
func (m *Map) WakeableModels(site string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for model, pls := range m.byModel {
		if slices.ContainsFunc(pls, func(pl Placement) bool { return pl.Site == site && *pl.ScaleFromZero }) {
			out = append(out, model)
		}
	}
	sort.Strings(out)
	return out
}

// wakeable is the one "may be held here" rule, used by Wakeable and Target:
// the placement of model, as kind, on site with scale_from_zero. The
// caller holds m.mu.
func (m *Map) wakeable(model, kind, site string) (Placement, bool) {
	for _, pl := range m.byModel[model] {
		if pl.Kind == kind && pl.Site == site && *pl.ScaleFromZero {
			return pl, true
		}
	}
	return Placement{}, false
}

// score is H(a, model, site): the first 8 bytes, big-endian, of
// SHA-256(a + "\x00" + model + "\x00" + site).
func score(a, model, site string) uint64 {
	h := sha256.Sum256([]byte(a + "\x00" + model + "\x00" + site))
	return binary.BigEndian.Uint64(h[:8])
}

// better orders by score, highest first; the ID only breaks an (unlikely) tie
// so every router still computes the same order.
func better(a, b uint64, idA, idB string) bool {
	if a != b {
		return a > b
	}
	return idA < idB
}

// Location builds the redirect URL: the site's origin, the request's own
// path, the request's own query parameters (any previous token replaced) and
// the new token.
func Location(siteURL string, req *url.URL, token string) string {
	q := req.Query()
	q.Set(QueryParam, token)
	return siteURL + req.Path + "?" + q.Encode()
}

// ---- scope ------------------------------------------------------------------

// Scope bounds where a request may be served: "all", "EU" or "country:XX".
type Scope string

// ScopeAll is the widest scope. Until scope_max lands on keys (W5.5), every
// request's effective scope is ScopeAll.
const ScopeAll Scope = "all"

// ParseScope validates a scope string.
func ParseScope(s string) (Scope, error) {
	switch {
	case s == string(ScopeAll), s == "EU":
		return Scope(s), nil
	case strings.HasPrefix(s, "country:") && countryCode.MatchString(strings.TrimPrefix(s, "country:")):
		return Scope(s), nil
	}
	return "", fmt.Errorf("redirect: invalid scope %q", s)
}

// Contains reports whether site p is inside the scope. An unknown scope
// contains nothing (fail-closed).
func (s Scope) Contains(p Site) bool {
	switch {
	case s == ScopeAll:
		return true
	case s == "EU":
		return p.EU != nil && *p.EU
	case strings.HasPrefix(string(s), "country:"):
		return p.Country == strings.TrimPrefix(string(s), "country:")
	}
	return false
}

// ---- token ------------------------------------------------------------------

// Claims is the body of a redirect token (contract 3 v2 §4).
type Claims struct {
	KeyID     string `json:"key"` // the caller's API-key ID ("" with auth mode none), never the key
	RequestID string `json:"rid"` // the first router's request ID; the target adopts it
	// ClientRequestID is the ID the client sent, if any. The target adopts it
	// too, so a client retry is still recognised after a redirect.
	ClientRequestID string `json:"crid,omitempty"`
	Model           string `json:"model"` // the concrete model the first router routed on
	Scope           Scope  `json:"scope"` // the effective scope at the first router
	From            string `json:"from"`  // the first router's site ID
	Site            string `json:"site"`  // the only site that accepts this token
	Path            string `json:"path"`  // the only path the token is valid for
	Hop             int    `json:"hop"`   // redirects so far, including this one
	BodySHA         string `json:"bh"`    // hex SHA-256 of the request body as the client sent it
	IssuedAt        int64  `json:"iat"`   // Unix seconds
	Exp             int64  `json:"exp"`   // Unix seconds, IssuedAt + TTL
	JTI             string `json:"jti"`   // 16 random bytes, base64url: this token's single-use ID, set by Sign
}

// jtiLen is the length of a token ID, in random bytes.
const jtiLen = 16

// ValidJTI reports whether jti is a token ID as Sign makes it.
func ValidJTI(jti string) bool {
	b, err := base64.RawURLEncoding.DecodeString(jti)
	return err == nil && len(b) == jtiLen
}

// Secret is one signing secret and its ID (the token's kid).
type Secret struct {
	ID  string
	Key []byte
}

var secretID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,16}$`)

// ParseSecrets parses "id:secret" entries, one per line; blank lines are
// skipped and every value is trimmed. The first entry signs, every entry
// verifies. Each secret must be at least 32 bytes and IDs unique.
func ParseSecrets(text string) ([]Secret, error) {
	var out []Secret
	seen := map[string]bool{}
	for n, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		id, key, ok := strings.Cut(line, ":")
		id, key = strings.TrimSpace(id), strings.TrimSpace(key)
		switch {
		case !ok:
			return nil, fmt.Errorf("redirect secret %d: want id:secret", n+1)
		case !secretID.MatchString(id):
			return nil, fmt.Errorf("redirect secret %d: id %q must match [A-Za-z0-9_-]{1,16}", n+1, id)
		case len(key) < 32:
			return nil, fmt.Errorf("redirect secret %q: must be at least 32 bytes", id)
		case seen[id]:
			return nil, fmt.Errorf("redirect secret %q: duplicate id", id)
		}
		seen[id] = true
		out = append(out, Secret{ID: id, Key: []byte(key)})
	}
	if len(out) == 0 {
		return nil, errors.New("redirect: no secrets")
	}
	return out, nil
}

// Signer creates and checks redirect tokens. The first secret signs; every
// secret verifies, so a secret can be rotated without breaking redirects.
type Signer struct {
	sign Secret
	byID map[string][]byte
	ttl  time.Duration
}

// NewSigner returns a Signer whose tokens are valid for ttl.
func NewSigner(secrets []Secret, ttl time.Duration) (*Signer, error) {
	if len(secrets) == 0 {
		return nil, errors.New("redirect: no secrets")
	}
	if ttl <= 0 {
		return nil, errors.New("redirect: token TTL must be > 0")
	}
	s := &Signer{sign: secrets[0], byID: make(map[string][]byte, len(secrets)), ttl: ttl}
	for _, sec := range secrets {
		s.byID[sec.ID] = append([]byte(nil), sec.Key...)
	}
	return s, nil
}

// Sign stamps c with iat = now, exp = now + ttl and a fresh random jti, and
// returns the token:
// "v2." + kid + "." + base64url(JSON claims) + "." + base64url(HMAC-SHA256).
func (s *Signer) Sign(c Claims, now time.Time) string {
	c.IssuedAt, c.Exp = now.Unix(), now.Add(s.ttl).Unix()
	id := make([]byte, jtiLen)
	_, _ = rand.Read(id) // crypto/rand.Read never fails (it crashes the program instead)
	c.JTI = base64.RawURLEncoding.EncodeToString(id)
	body, _ := json.Marshal(c) // Claims holds only strings and ints: cannot fail
	payload := tokenPrefix + s.sign.ID + "." + base64.RawURLEncoding.EncodeToString(body)
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac(s.sign.Key, payload))
}

// Verify checks token's format, signature (before decoding the claims),
// expiry and issue time (both with ClockSkew) and returns its claims.
func (s *Signer) Verify(token string, now time.Time) (Claims, error) {
	var c Claims
	parts := strings.Split(strings.TrimPrefix(token, tokenPrefix), ".")
	if !strings.HasPrefix(token, tokenPrefix) || len(parts) != 3 {
		return c, ErrTokenMalformed
	}
	key, ok := s.byID[parts[0]]
	if !ok {
		return c, ErrTokenMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(sig, mac(key, tokenPrefix+parts[0]+"."+parts[1])) {
		return c, ErrTokenSignature
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(body, &c) != nil {
		return c, ErrTokenMalformed
	}
	if now.After(time.Unix(c.Exp, 0).Add(ClockSkew)) {
		return c, ErrTokenExpired
	}
	if time.Unix(c.IssuedAt, 0).After(now.Add(ClockSkew)) {
		return c, ErrTokenFromFuture // this clock is behind: the token would outlive its TTL
	}
	return c, nil
}

func mac(key []byte, payload string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(payload))
	return m.Sum(nil)
}

// BodySHA returns the hex SHA-256 of a request body, as carried in Claims.BodySHA.
func BodySHA(body []byte) string {
	h := sha256.Sum256(body)
	return fmt.Sprintf("%x", h)
}

// ---- single use ---------------------------------------------------------------

// ReplayGuard makes tokens single-use: a token ID (jti) is accepted once until
// its token can no longer verify (exp + ClockSkew). It is keyed on the jti,
// which identifies the token. The request ID (rid) is minted by the router
// and identifies the request: one request may be redirected more than once,
// each time with a new token.
//
// ponytail: in memory, so a token can be used once per router of the target
// site, not once per site. Contract 3 v2 moves this to the
// site's shared Valkey with Epic A.
type ReplayGuard struct {
	mu        sync.Mutex
	seen      map[string]time.Time
	lastSweep time.Time
}

// NewReplayGuard returns an empty guard.
func NewReplayGuard() *ReplayGuard { return &ReplayGuard{seen: map[string]time.Time{}} }

// First records id until the given time and reports whether this is its
// first use. Expired entries are swept at most every 10 seconds.
func (g *ReplayGuard) First(id string, until, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now.Sub(g.lastSweep) > 10*time.Second {
		for k, t := range g.seen {
			if now.After(t) {
				delete(g.seen, k)
			}
		}
		g.lastSweep = now
	}
	if t, ok := g.seen[id]; ok && !now.After(t) {
		return false
	}
	g.seen[id] = until
	return true
}
