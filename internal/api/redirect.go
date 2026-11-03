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
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/redirect"

	"github.com/gin-gonic/gin"
)

// Context keys for cross-site redirects. AuthMiddleware sets ctxKeyRedirect
// only for a request authenticated by a redirect token; ctxKeyClientBody holds
// the body exactly as the client sent it whenever a middleware rewrites the
// cached body (AliasMiddleware, RedirectTarget).
const (
	ctxKeyRedirect   = "redirect"
	ctxKeyClientBody = "client_body"
)

// endpointKinds maps each redirectable inference path to the placement kind
// that may serve it (contract 3 v2 §3).
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

// RedirectTarget prepares a request that arrived with a redirect token. It
// runs after BodyLimitMiddleware (which bounds the read) and before
// AliasMiddleware and QuotaMiddleware:
//
//   - the body must be the one the token was issued for, else 401 — before any
//     quota is spent, so a leaked token cannot carry another prompt or drain
//     the key's quota;
//   - the request then uses the token's model, the concrete model the first
//     router resolved, so an alias such as "auto" is not resolved again here.
//     The body is still hashed as sent (ctxKeyClientBody keeps those bytes).
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
		if info.BodySHA != redirect.BodySHA(body) {
			h.redirect.Reject("body")
			abortWithRouterError(c, http.StatusUnauthorized, domain.ErrCodeUnauthorized,
				"the request body does not match the redirect token", domain.SourceRouter)
			return
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

// RedirectDecision answers 307 to a sibling policloud when no healthy agent
// here serves the model and the sibling map lists one that does (contract 3
// v2 §3). It runs after AliasMiddleware (so it routes on the concrete model)
// and before QuotaMiddleware: the redirecting router does no inference and
// charges no quota. It covers every inference endpoint, by the placement kind
// that matches the path.
//
// A request is served here as before when: redirect is disabled, the key may
// not use the model (the handler answers 403), a healthy agent here serves
// it, it has already been redirected redirect.MaxHops times, or no sibling is
// left after the scope filter (the normal handling then applies, usually
// 404). A redirect takes precedence over a policy's fallback_provider.
func (h *Handlers) RedirectDecision() gin.HandlerFunc {
	return func(c *gin.Context) {
		kind := endpointKinds[c.Request.URL.Path]
		if h.redirect == nil || h.healthyAgentCount == nil || kind == "" {
			c.Next()
			return
		}
		model := peekModel(c)
		if model == "" || !modelAllowed(c, model) {
			c.Next()
			return
		}
		hop, from, scope := 0, h.redirect.Self, redirect.ScopeAll // scope_max on keys is W5.5: until then "all"
		if info := redirectInfo(c); info != nil {
			hop, from, scope = info.Hop, info.From, redirect.Scope(info.Scope)
		}
		if hop >= redirect.MaxHops || h.healthyAgentCount(model) > 0 {
			c.Next()
			return
		}
		now := time.Now()
		keyID := c.GetString("key_id")
		target, ok := h.redirect.Map.Target(redirect.TargetRequest{
			Model: model, Kind: kind, Self: h.redirect.Self, From: from,
			KeyID: keyID, Session: c.GetHeader("X-Session-ID"), Scope: scope, Now: now,
		})
		if !ok {
			c.Next()
			return
		}
		token := h.redirect.Signer.Sign(redirect.Claims{
			KeyID:     keyID,
			RequestID: c.GetString("request_id"),
			Model:     model,
			Scope:     scope,
			From:      from,
			Site:      target.ID,
			Path:      c.Request.URL.Path,
			Hop:       hop + 1,
			BodySHA:   redirect.BodySHA(clientBody(c)),
		}, now)
		c.Header("Location", redirect.Location(target.URL, c.Request.URL, token))
		c.Header("Cache-Control", "no-store")
		// Not abortWithRouterError: a redirect is normal routing, not a failure
		// for the audit log or the trace. The body explains it to clients that
		// do not follow redirects (for example plain httpx).
		c.AbortWithStatusJSON(http.StatusTemporaryRedirect, domain.RouterErrorResponse{Error: domain.NewRouterError(
			domain.ErrCodeModelRedirected,
			fmt.Sprintf("model %s is served by policloud %s: repeat this request at the Location URL (enable redirect following in your HTTP client)", model, target.ID),
			domain.SourceRouter)})
		h.redirect.Redirected(model, target.ID, hop+1)
	}
}

// modelAllowed reports whether the authenticated key may use model.
func modelAllowed(c *gin.Context, model string) bool {
	allowSet, unrestricted := effectiveAllowedModels(c)
	if unrestricted {
		return true
	}
	_, ok := allowSet[model]
	return ok
}

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
// current map (version 0 and no policlouds before the first push). The map
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
		"policlouds":      orEmpty(doc.Policlouds),
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
	var doc redirect.MapDoc
	if err := c.ShouldBindJSON(&doc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
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
	log.Infof("sibling map replaced via POST /admin/sibling-map (version %d, %d policlouds, %d placements)",
		*doc.Version, len(doc.Policlouds), len(doc.Placements))
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": *doc.Version, "policlouds": len(doc.Policlouds), "placements": len(doc.Placements)})
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
