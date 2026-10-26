// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package redirect

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"hivenet_router/internal/auth"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("redirect")

// Redirector is one site's redirect state: who this site is, the sibling
// map, the token signer and optional metric hooks.
type Redirector struct {
	Self   string // this site's ID, as it appears in the sibling map
	Map    *Map
	Signer *Signer

	// OnRedirect, if set, is called for every 307 written.
	OnRedirect func(model, site string, hop int)
	// OnReject, if set, is called when a redirected request is refused, with
	// reason ∈ {malformed, signature, expired, site, path, key, body}.
	OnReject func(reason string)
}

// Reject reports a refused redirected request.
func (r *Redirector) Reject(reason string) {
	log.Debugf("redirected request rejected: %s", reason)
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
// auth.yaml keys: they carry no key ID, so a sibling site could not resolve a
// token to its key. Dynamic keys and auth mode none both work.
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

// Authenticate accepts a redirected request when its token is validly signed,
// unexpired, issued for this site and for this request path. The key comes
// from the token's key ID. If the client also sent an Authorization header
// (curl re-sends it when a chain returns to the original host), that key must
// authenticate and must be the key the token names.
//
// The body hash cannot be checked here (the body is not read yet); the
// handler checks it against AuthResult.RedirectBodySHA.
func (t *tokenAuth) Authenticate(req *http.Request) (*auth.AuthResult, error) {
	tok := req.URL.Query().Get(QueryParam)
	if tok == "" {
		return t.next.Authenticate(req)
	}
	c, err := t.r.Signer.Verify(tok, time.Now())
	switch {
	case errors.Is(err, ErrTokenExpired):
		return t.refuse("expired")
	case errors.Is(err, ErrTokenSignature):
		return t.refuse("signature")
	case err != nil:
		return t.refuse("malformed")
	case c.Site != t.r.Self:
		return t.refuse("site")
	case c.Path != req.URL.Path:
		return t.refuse("path")
	}

	var res *auth.AuthResult
	if req.Header.Get("Authorization") != "" || t.keys == nil {
		res, err = t.next.Authenticate(req)
		if err != nil {
			return nil, err
		}
		if res.KeyID != c.KeyID {
			return t.refuse("key")
		}
	} else if res, err = t.keys.AuthenticateKeyID(c.KeyID); err != nil {
		return t.refuse("key")
	}
	res.RedirectHop, res.RedirectBodySHA = c.Hop, c.BodySHA
	return res, nil
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

func (t *tokenAuth) refuse(reason string) (*auth.AuthResult, error) {
	t.r.Reject(reason)
	return nil, auth.ErrInvalidCredentials
}
