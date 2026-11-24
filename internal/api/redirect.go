// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/redirect"

	"github.com/gin-gonic/gin"
)

// Context keys for cross-site redirects. AuthMiddleware sets ctxKeyRedirect
// only for a request authenticated by a redirect token (RedirectTarget clears
// it again when the body check refuses the token); ctxKeyClientBody holds
// the body exactly as the client sent it whenever a middleware rewrites the
// cached body (AliasMiddleware, RedirectTarget).
const (
	ctxKeyRedirect   = "redirect"
	ctxKeyClientBody = "client_body"
)

// endpointKinds maps each redirectable inference path to the placement kind
// that may serve it (contract 3 v3 §3).
var endpointKinds = map[string]string{
	"/v1/chat/completions":      redirect.KindChat,
	"/v1/messages":              redirect.KindChat,
	"/v1/messages/count_tokens": redirect.KindChat,
	"/v1/embeddings":            redirect.KindEmbedding,
	"/v1/rerank":                redirect.KindRerank,
}

// SetRedirect enables cross-site redirects (see internal/redirect). Call
// once during wiring, before the server starts. nil leaves them disabled.
func (h *Handlers) SetRedirect(r *redirect.Redirector) { h.redirect = r }

// redirectInfo returns the verified redirect token's claims, or nil when the
// request did not arrive with one.
func redirectInfo(c *gin.Context) *auth.RedirectInfo {
	v, _ := c.Get(ctxKeyRedirect)
	info, _ := v.(*auth.RedirectInfo)
	return info
}

// RedirectTarget completes the checks of a request that arrived with a
// redirect token. It runs after BodyLimitMiddleware (which bounds the read)
// and before AliasMiddleware and QuotaMiddleware:
//
//   - the body must be the one the token was issued for, checked before any
//     quota is spent, so a leaked token cannot carry another prompt or drain
//     the key's quota; then the token is marked used (single use by its jti).
//     On either failure the token is refused: a request that also carries its
//     key (already verified as the token's key) is served as a fresh request
//     on it, as for any refused token; otherwise 401;
//   - an accepted request adopts the first router's request ID (logs, audit,
//     X-Request-ID), so both hops can be matched, and the client's own ID if
//     it sent one (X-Client-Request-ID);
//   - it uses the token's model, the concrete model the first router
//     resolved, so an alias such as "auto" is not resolved again here. The
//     body is still hashed as sent (ctxKeyClientBody keeps those bytes).
//
// Requests without a redirect token pass through untouched.
func (h *Handlers) RedirectTarget() gin.HandlerFunc {
	return func(c *gin.Context) {
		info := redirectInfo(c)
		if h.redirect == nil || info == nil {
			c.Next()
			return
		}
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
				"failed to read request body", domain.SourceRouter)
			return
		}
		// The last two token checks (contract 3 v3 §4): the body, then single
		// use. The token is marked used only here, once every check passed, so
		// a request refused on any check never uses up the client's token.
		reason, msg := "", ""
		switch {
		case info.BodySHA != redirect.BodySHA(body):
			reason, msg = "body", "the request body does not match the redirect token"
		case !h.redirect.Use(c.Request.Context(), info.TokenID, info.Exp, time.Now()):
			reason, msg = "replay", "the redirect token was already used"
		}
		if reason != "" {
			h.redirect.Reject(reason)
			if c.GetHeader("Authorization") == "" {
				abortWithRouterError(c, http.StatusUnauthorized, domain.ErrCodeUnauthorized, msg, domain.SourceRouter)
				return
			}
			c.Set(ctxKeyRedirect, (*auth.RedirectInfo)(nil)) // fresh request: hop 0, own ID, own model
			c.Set(gin.BodyBytesKey, body)
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			c.Next()
			return
		}
		setRequestID(c, info.RequestID)
		if info.ClientRequestID != "" {
			setClientRequestID(c, info.ClientRequestID)
		}
		routed := body
		var peek struct {
			Model string `json:"model"`
		}
		if json.Unmarshal(body, &peek) == nil && peek.Model != info.Model {
			if routed, err = replaceTopLevelModel(body, info.Model); err != nil {
				abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
					"Invalid request: "+err.Error(), domain.SourceRouter)
				return
			}
		}
		c.Set(ctxKeyClientBody, body)
		c.Set(gin.BodyBytesKey, routed)
		c.Request.Body = io.NopCloser(bytes.NewReader(routed))
		c.Set(ctxKeyPeekedModel, info.Model)
		c.Next()
	}
}

// RedirectDecision answers 307 to a sibling site when no healthy agent
// here serves the model and the sibling map lists one that does (contract 3
// v3 §3), or when the model is full here and a sibling has room (the
// overload spill). It runs after AliasMiddleware (so it routes on the
// concrete model) and before QuotaMiddleware: the redirecting router does no
// inference and charges no quota. It covers every inference endpoint, by the placement kind
// that matches the path.
//
// The scope comes from the Scope middleware (X-Hivenet-Scope, bounded by the
// key's scope_max, or the key's home_country / scope_max). Without one it is
// this site's country, so by default a request stays in the country it
// landed in. A redirected request keeps the scope of its token. A request
// with a scope is served here only if a healthy agent here is inside it.
//
// Overload spill: when a healthy agent here serves the model but the model is
// full here (admission.Controller.Saturated on the shared pool), the request
// goes to a sibling the map's fresh hints show warm and not full
// (redirect.Map.SpillTarget), so it runs on capacity that already exists
// instead of waiting here. Scaling stays the fleet controller's: with no
// such sibling the request is handled here as before (queue, then 429).
//
// A request is served here as before when: redirect is disabled, the key may
// not use the model (the handler answers 403), a healthy agent here serves
// it and it is not full here (or the spill is off or finds no sibling), it
// has already been redirected redirect.MaxHops times, or the map gives no
// target (none inside the scope, or every one cold while the model is
// placed here); the normal handling then applies (hold-at-zero or 404).
// A redirect takes precedence over a policy's fallback_provider.
func (h *Handlers) RedirectDecision() gin.HandlerFunc {
	return func(c *gin.Context) {
		kind := endpointKinds[c.Request.URL.Path]
		// A dedicated request stays on its reserved replicas: never redirected.
		if h.redirect == nil || h.healthyAgentCount == nil || kind == "" || requestPool(c) != domain.SharedPool {
			c.Next()
			return
		}
		model := peekModel(c)
		if model == "" || !h.modelAllowed(c, model) {
			c.Next()
			return
		}
		info := redirectInfo(c)
		hop, from := 0, h.redirect.Self
		if info != nil {
			hop, from = info.Hop, info.From
		}
		reqScope := requestScope(c)
		if hop >= redirect.MaxHops {
			c.Next()
			return
		}
		// Served here: redirected only by the overload spill, when the model
		// is full here and a sibling has room.
		reason := redirect.ReasonMiss
		if h.healthyForRequest(c, model) > 0 {
			if !h.redirect.Spill || h.admission == nil || !h.admission.Saturated(domain.PoolKey(model, domain.SharedPool)) {
				c.Next()
				return
			}
			reason = redirect.ReasonOverload
		}
		// A scope from the client or its key bounds both the sites and, at
		// the target, the agents. Without one the request may go only to
		// sites in this site's country (or the first router's, after a
		// redirect), and the target's agents are not filtered.
		scope := redirect.Scope(reqScope)
		scopeAgents := scope != ""
		switch {
		case scope != "":
		case info != nil:
			scope = redirect.Scope(info.Scope)
		default:
			scope = h.defaultRedirectScope()
		}
		now := time.Now()
		keyID := c.GetString("key_id")
		treq := redirect.TargetRequest{
			Model: model, Kind: kind, Self: h.redirect.Self, From: from,
			KeyID: keyID, Session: c.GetHeader("X-Session-ID"), Scope: scope, Now: now,
		}
		var target redirect.Site
		var ok bool
		if reason == redirect.ReasonOverload {
			var skipped string
			if target, skipped, ok = h.redirect.Map.SpillTarget(treq); !ok {
				h.redirect.SpillSkipped(model, skipped)
			}
		} else {
			target, ok = h.redirect.Map.Target(treq)
		}
		if !ok {
			c.Next()
			return
		}
		token := h.redirect.Signer.Sign(redirect.Claims{
			KeyID:           keyID,
			RequestID:       c.GetString("request_id"),
			ClientRequestID: c.GetString(ctxKeyClientRequestID),
			Model:           model,
			Scope:           scope,
			ScopeAgents:     scopeAgents,
			Strategy:        requestedRoutingStrategy(c),
			From:            from,
			Site:            target.ID,
			Path:            c.Request.URL.Path,
			Hop:             hop + 1,
			BodySHA:         redirect.BodySHA(clientBody(c)),
		}, now)
		c.Header("Location", redirect.Location(target.URL, c.Request.URL, token))
		c.Header("Cache-Control", "no-store")
		// Not abortWithRouterError: a redirect is normal routing, not a failure
		// for the audit log or the trace. The body explains it to clients that
		// do not follow redirects (for example plain httpx).
		msg := fmt.Sprintf("model %s is served by site %s: repeat this request at the Location URL (enable redirect following in your HTTP client)", model, target.ID)
		if reason == redirect.ReasonOverload {
			msg = fmt.Sprintf("model %s is at capacity on site %s and has room on site %s: repeat this request at the Location URL (enable redirect following in your HTTP client)", model, h.redirect.Self, target.ID)
		}
		c.AbortWithStatusJSON(http.StatusTemporaryRedirect, domain.RouterErrorResponse{Error: domain.NewRouterError(
			domain.ErrCodeModelRedirected, msg, domain.SourceRouter)})
		h.redirect.Redirected(model, target.ID, hop+1, reason)
	}
}

// defaultRedirectScope is the redirect scope of a request that has none
// (no X-Hivenet-Scope, and a key without scope_max or home_country): this
// site's country, so by default a request stays in the country it landed in,
// or empty — matching no sites — while this site is not in the map.
func (h *Handlers) defaultRedirectScope() redirect.Scope {
	if self, ok := h.redirect.Map.Site(h.redirect.Self); ok {
		return redirect.Scope("country:" + self.Country)
	}
	return ""
}

// scopeHeader is the request header that asks for a redirect scope.
const scopeHeader = "X-Hivenet-Scope"

// clientBody returns the request body as the client sent it: the copy kept
// before a middleware rewrote the model, else the cached body.
func clientBody(c *gin.Context) []byte {
	if b, ok := c.Get(ctxKeyClientBody); ok {
		if body, ok := b.([]byte); ok {
			return body
		}
	}
	body, _ := cachedBody(c)
	return body
}

const errRedirectNotActive = "cross-site redirect is not enabled on this router (set HIVENET_ROUTER_SITE_ID)"

// GetSiblingMap handles GET /admin/sibling-map: this router's site ID and the
// current map (version 0 and no sites before the first push). The map
// writer compares the version to detect drift.
func (h *Handlers) GetSiblingMap(c *gin.Context) {
	if h.redirect == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errRedirectNotActive})
		return
	}
	doc := h.redirect.Map.Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"site_id":         h.redirect.Self,
		"schema":          redirect.Schema,
		"version":         h.redirect.Map.Version(),
		"region":          doc.Region,
		"sites":           orEmpty(doc.Sites),
		"placements":      orEmpty(doc.Placements),
		"hints_expire_at": doc.HintsExpireAt,
	})
}

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// ReplaceSiblingMap handles POST /admin/sibling-map: replace the whole map.
// A lower version is rejected with 409 so a delayed push cannot roll the map
// back; an equal version is accepted as a replay; an unknown schema or any
// invalid entry is a 400 and the current map is kept. The map is kilobytes,
// so the body is capped at 1 MB.
func (h *Handlers) ReplaceSiblingMap(c *gin.Context) {
	if h.redirect == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errRedirectNotActive})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	// Unknown fields are refused: a body in an older format (contract 3 v2
	// "policlouds") would otherwise decode as an empty map.
	var doc redirect.MapDoc
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	// The map must list this router's own site: without it, the default
	// scope and token checks have nothing to compare against.
	if !slices.ContainsFunc(doc.Sites, func(s redirect.Site) bool { return s.ID == h.redirect.Self }) {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("sibling map does not list this router's site %q", h.redirect.Self)})
		return
	}
	if err := h.redirect.Map.Replace(doc); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, redirect.ErrStaleVersion) {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	log.Infof("sibling map replaced via POST /admin/sibling-map (version %d, %d sites, %d placements)",
		*doc.Version, len(doc.Sites), len(doc.Placements))
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": *doc.Version, "sites": len(doc.Sites), "placements": len(doc.Placements)})
}

// redactPath removes the redirect token from a logged request path: the
// access log prints path?query, and a token in a log line is a usable
// credential until it expires.
func redactPath(p string) string {
	u, err := url.Parse(p)
	if err != nil || !u.Query().Has(redirect.QueryParam) {
		return p
	}
	q := u.Query()
	q.Set(redirect.QueryParam, "REDACTED")
	u.RawQuery = q.Encode()
	return u.String()
}
