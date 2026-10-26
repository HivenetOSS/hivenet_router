// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/redirect"

	"github.com/gin-gonic/gin"
)

// Context keys for cross-site redirects. AuthMiddleware sets the first two
// only for a request authenticated by a redirect token; AliasMiddleware sets
// the third when it rewrites the cached body, keeping the client's bytes.
const (
	ctxKeyRedirectHop     = "redirect_hop"
	ctxKeyRedirectBodySHA = "redirect_body_sha"
	ctxKeyClientBody      = "client_body"
)

// SetRedirect enables cross-site redirects (see internal/redirect). Call
// once during wiring, before the server starts. nil leaves them disabled.
func (h *Handlers) SetRedirect(r *redirect.Redirector) { h.redirect = r }

// redirectStep runs in Passthrough once the model is authorized for the key.
// It returns true when it has written the response:
//
//   - a redirected request whose body differs from the one its token was
//     issued for is refused (401), so a token leaked from a log cannot carry
//     another prompt;
//   - a request for a model with no healthy agent here, which the sibling map
//     lists on another site, is answered 307 to that site, unless it has
//     already been redirected redirect.MaxHops times.
//
// Otherwise the request is served here as before (including the existing
// 404 when the model exists nowhere). A redirect takes precedence over a
// policy's fallback_provider: the model is served by a sibling rather than
// by an external provider.
func (h *Handlers) redirectStep(c *gin.Context, model string) bool {
	if h.redirect == nil {
		return false
	}
	body := clientBody(c)
	hop := c.GetInt(ctxKeyRedirectHop)
	if hop > 0 && c.GetString(ctxKeyRedirectBodySHA) != redirect.BodySHA(body) {
		h.redirect.Reject("body")
		writeRouterError(c, http.StatusUnauthorized, domain.ErrCodeUnauthorized,
			"the request body does not match the redirect token", domain.SourceRouter)
		return true
	}
	if hop >= redirect.MaxHops || h.healthyAgentCount == nil || h.healthyAgentCount(model) > 0 {
		return false
	}
	keyID := c.GetString("key_id")
	// The affinity key keeps one caller's session on one site, so its KV
	// cache is reused there. X-Session-ID is optional; without it every
	// request of a key goes to the same site.
	site, ok := h.redirect.Map.Target(model, h.redirect.Self, keyID+"\x00"+c.GetHeader("X-Session-ID"))
	if !ok {
		return false
	}

	// ponytail: a key on the legacy flat quota shape is charged RPM here and
	// again at the sibling; per-model keys are not charged here (no healthy
	// agent → QuotaMiddleware skips). Exempt redirected requests here if it
	// matters.
	token := h.redirect.Signer.Sign(redirect.Claims{
		KeyID:     keyID,
		RequestID: c.GetString("request_id"),
		Model:     model,
		Site:      site.ID,
		Path:      c.Request.URL.Path,
		Hop:       hop + 1,
		BodySHA:   redirect.BodySHA(body),
	}, time.Now())
	c.Header("Location", redirect.Location(site.URL, c.Request.URL, token))
	c.Header("Cache-Control", "no-store")
	// Not writeRouterError: a redirect is normal routing, not a failure for the
	// audit log or the trace. The body explains it to clients that do not
	// follow redirects (for example plain httpx, which defaults to not following).
	c.JSON(http.StatusTemporaryRedirect, domain.RouterErrorResponse{Error: domain.NewRouterError(
		domain.ErrCodeModelRedirected,
		fmt.Sprintf("model %s is served by site %s: repeat this request at the Location URL (enable redirect following in your HTTP client)", model, site.ID),
		domain.SourceRouter)})
	h.redirect.Redirected(model, site.ID, hop+1)
	return true
}

// clientBody returns the request body as the client sent it: the copy
// AliasMiddleware kept before rewriting the model, else the cached body.
func clientBody(c *gin.Context) []byte {
	if b, ok := c.Get(ctxKeyClientBody); ok {
		if body, ok := b.([]byte); ok {
			return body
		}
	}
	body, _ := cachedBody(c)
	return body
}

// siblingMapRequest is the body of POST /admin/sibling-map.
type siblingMapRequest struct {
	Version string          `json:"version"`
	Sites   []redirect.Site `json:"sites"`
}

const errRedirectNotActive = "cross-site redirect is not enabled on this router (set HIVENET_ROUTER_SITE_ID)"

// GetSiblingMap handles GET /admin/sibling-map: this site's ID, the current
// map version and its sites.
func (h *Handlers) GetSiblingMap(c *gin.Context) {
	if h.redirect == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errRedirectNotActive})
		return
	}
	version, sites := h.redirect.Map.Snapshot()
	c.JSON(http.StatusOK, gin.H{"site_id": h.redirect.Self, "version": version, "sites": sites})
}

// ReplaceSiblingMap handles POST /admin/sibling-map: replace the whole map.
// An older version is rejected with 409 so a delayed push cannot roll the map
// back; an equal version is accepted as a replay. The map is kilobytes, so
// the body is capped at 1 MB.
func (h *Handlers) ReplaceSiblingMap(c *gin.Context) {
	if h.redirect == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errRedirectNotActive})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	var req siblingMapRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if err := h.redirect.Map.Replace(req.Version, req.Sites); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, redirect.ErrStaleVersion) {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	log.Infof("sibling map replaced via POST /admin/sibling-map (version %s, %d sites)", req.Version, len(req.Sites))
	c.JSON(http.StatusOK, gin.H{"ok": true, "version": req.Version, "count": len(req.Sites)})
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
