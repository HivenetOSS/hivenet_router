// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package redirect

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("redirect")

// Redirector is one policloud's redirect state: who this policloud is, the
// sibling map, the token signer, the single-use guard and optional metric
// hooks.
type Redirector struct {
	Self   string // this policloud's ID (the routers' --site-id)
	Map    *Map
	Signer *Signer
	Replay Replayer

	// OnRedirect, if set, is called for every 307 written.
	OnRedirect func(model, site string, hop int)
	// OnReject, if set, is called when a redirect token is refused, with
	// reason ∈ {malformed, signature, expired, clock, site, path, scope,
	// replay, key, body}.
	OnReject func(reason string)
}

// Use marks the token jti, valid until exp, as used and reports whether this
// was its first use. It is the last token check (contract 3 v2 §4), made only
// once the key and the body have passed.
func (r *Redirector) Use(jti string, exp int64, now time.Time) bool {
	return r.Replay.First(jti, time.Unix(exp, 0).Add(ClockSkew), now)
}

// Reject reports a refused redirect token.
func (r *Redirector) Reject(reason string) {
	log.Debugf("redirect token rejected: %s", reason)
	if r.OnReject != nil {
		r.OnReject(reason)
	}
}

// Redirected reports a 307 written.
func (r *Redirector) Redirected(model, site string, hop int) {
	if r.OnRedirect != nil {
		r.OnRedirect(model, site, hop)
	}
}

// Wrap returns the API auth provider to install: next itself when rd is nil
// (redirect disabled), otherwise next wrapped so a request carrying a redirect
// token is authenticated by that token, and requests without ?rt= go to next
// unchanged. keys is the dynamic key registry the token's key ID resolves
// against; nil means auth mode none. Every place that installs an API
// provider (startup and SIGHUP reload) must go through Wrap, or redirected
// requests start failing with 401.
func Wrap(rd *Redirector, next auth.Provider, keys *auth.DynamicKeyProvider) auth.Provider {
	if rd == nil {
		return next
	}
	return &tokenAuth{r: rd, next: next, keys: keys}
}

// CheckAuthMode rejects cross-site redirect (a non-empty siteID) with static
// auth.yaml keys: they carry no key ID, so a sibling policloud could not
// resolve a token to its key. Dynamic keys and auth mode none both work.
func CheckAuthMode(siteID string, apiProv auth.Provider) error {
	if _, static := apiProv.(*auth.StaticKeyProvider); static && siteID != "" {
		return fmt.Errorf("cross-site redirect (site ID %q) requires dynamic API keys or auth mode none; static auth.yaml keys have no key ID", siteID)
	}
	return nil
}

type tokenAuth struct {
	r    *Redirector
	next auth.Provider
	keys *auth.DynamicKeyProvider
}

// Authenticate accepts a redirected request when its token passes the checks
// of contract 3 v2 §4 up to the key, in order. The last two, the body hash
// and single use, cannot run here (the body is not read yet): the API layer
// (RedirectTarget) runs them from AuthResult.Redirect before quota, with the
// same fallback, and only then marks the token used.
//
// Fallback: when the token fails any check but the request carries an
// Authorization header, the token is ignored and the request is served on
// that key as a fresh request (hop 0) — including a valid token whose key is
// not the presented one. Without a usable key a refused token is a 401.
func (t *tokenAuth) Authenticate(req *http.Request) (*auth.AuthResult, error) {
	tok := req.URL.Query().Get(QueryParam)
	if tok == "" {
		return t.next.Authenticate(req)
	}
	c, reason := t.check(req, tok, time.Now())
	if reason == "" {
		res, keyReason, err := t.resolveKey(req, c)
		if err != nil {
			return nil, err
		}
		if keyReason == "" {
			res.Redirect = &auth.RedirectInfo{
				Hop: c.Hop, BodySHA: c.BodySHA, RequestID: c.RequestID,
				ClientRequestID: c.ClientRequestID,
				Model:           c.Model, Scope: string(c.Scope), From: c.From,
				TokenID: c.JTI, Exp: c.Exp,
			}
			return res, nil
		}
		reason = keyReason
	}
	t.r.Reject(reason)
	if req.Header.Get("Authorization") != "" {
		return t.next.Authenticate(req) // fresh request on the presented key
	}
	return nil, auth.ErrInvalidCredentials
}

// check runs the token checks that need no key lookup, in the contract's
// order, and returns the claims or the rejection reason. It does not mark the
// token used: that is the last check (Redirector.Use), once the key and the
// body have passed too, so a refused request never uses up a token.
func (t *tokenAuth) check(req *http.Request, tok string, now time.Time) (Claims, string) {
	c, err := t.r.Signer.Verify(tok, now)
	switch {
	case errors.Is(err, ErrTokenExpired):
		return c, "expired"
	case errors.Is(err, ErrTokenFromFuture):
		return c, "clock"
	case errors.Is(err, ErrTokenSignature):
		return c, "signature"
	case err != nil, !normalizeIDs(&c), !ValidJTI(c.JTI):
		return c, "malformed"
	case c.Site != t.r.Self:
		return c, "site"
	case c.Path != req.URL.Path:
		return c, "path"
	case !t.inScope(c.Scope):
		return c, "scope"
	}
	return c, ""
}

// normalizeIDs puts the token's request IDs in canonical form, so a token
// signed with any UUID spelling is accepted. It reports false when the
// request ID is not a UUID, or a client request ID is set and is not one.
// The signature is already verified, so rewriting the claims is safe.
func normalizeIDs(c *Claims) bool {
	rid, ok := domain.CanonicalRequestID(c.RequestID)
	if !ok {
		return false
	}
	c.RequestID = rid
	if c.ClientRequestID != "" {
		crid, ok := domain.CanonicalRequestID(c.ClientRequestID)
		if !ok {
			return false
		}
		c.ClientRequestID = crid
	}
	return true
}

// inScope reports whether this policloud lies inside scope. Anything but
// "all" needs this policloud's country from the map: fail-closed without it.
func (t *tokenAuth) inScope(scope Scope) bool {
	if scope == ScopeAll {
		return true
	}
	if _, err := ParseScope(string(scope)); err != nil {
		return false
	}
	self, ok := t.r.Map.Policloud(t.r.Self)
	return ok && scope.Contains(self)
}

// resolveKey authenticates the key the token names. A request that also sent
// Authorization must authenticate as that same key (keyReason "key"
// otherwise); an Authorization that does not authenticate at all is returned
// as err. Without Authorization the key is looked up by ID.
func (t *tokenAuth) resolveKey(req *http.Request, c Claims) (res *auth.AuthResult, keyReason string, err error) {
	if req.Header.Get("Authorization") != "" || t.keys == nil {
		res, err = t.next.Authenticate(req)
		if err != nil {
			t.r.Reject("key")
			return nil, "", err
		}
		if res.KeyID != c.KeyID {
			return nil, "key", nil
		}
		return res, "", nil
	}
	if res, err = t.keys.AuthenticateKeyID(c.KeyID); err != nil {
		return nil, "key", nil
	}
	return res, "", nil
}

// Tenants passes through the wrapped provider's per-tenant quota listing,
// which the router reads (by type assertion) to publish quota gauges.
// Without it, enabling redirect would silently stop those gauges.
func (t *tokenAuth) Tenants() map[string]auth.QuotaLimits {
	if l, ok := t.next.(interface {
		Tenants() map[string]auth.QuotaLimits
	}); ok {
		return l.Tenants()
	}
	return nil
}
