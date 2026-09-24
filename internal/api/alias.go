// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/semantic"

	"github.com/gin-gonic/gin"
)

// Semantic alias routing: model-level routing for virtual model names (e.g. "auto").
//
// AliasMiddleware runs in the inference group after BodyLimitMiddleware and
// before QuotaMiddleware. For a request whose model is a configured alias it
// resolves a concrete model, rewrites "model" in the cached request body, and
// lets the rest of the chain (quota, authorizeModel, admission, tokenizer,
// forwarding) run unchanged on the resolved model — they all read the body
// through gin's ShouldBindBodyWith cache. For every other request the only
// added work is one map lookup; the model it peeks is cached for peekModel so
// the body is not decoded an extra time.

const (
	// ctxKeyPeekedModel caches the top-level "model" for peekModel.
	ctxKeyPeekedModel = "peeked_model"

	// Audit fields set on alias requests.
	auditKeyAlias         = "audit_alias"
	auditKeySemanticRoute = "audit_semantic_route"

	// Response headers exposing the decision to the client.
	HeaderRoutedModel = "X-Hivenet-Routed-Model"
	HeaderRoute       = "X-Hivenet-Route"
	HeaderRouteSource = "X-Hivenet-Route-Source"
)

// semanticPaths are the inference paths an alias can be used on.
var semanticPaths = map[string]bool{
	"/v1/chat/completions":      true,
	"/v1/messages":              true,
	"/v1/messages/count_tokens": true,
}

// SetDecisionLog enables the JSONL decision log (nil disables it). Call before
// the server starts serving.
func (h *Handlers) SetDecisionLog(l *semantic.DecisionLog) { h.decisionLog = l }

// Resolver exposes the alias resolver (tests / diagnostics).
func (h *Handlers) Resolver() *semantic.Resolver { return h.resolver }

// AliasMiddleware resolves semantic aliases; see the file comment.
func (h *Handlers) AliasMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		model := peekModel(c)
		if model == "" || h.executor == nil || !semanticPaths[c.Request.URL.Path] || !h.executor.IsAlias(model) {
			c.Next()
			return
		}
		if h.resolveAlias(c, model) {
			c.Next()
		}
	}
}

// resolveAlias resolves model (an alias) for this request and patches the
// cached body. Returns false after writing an error response.
func (h *Handlers) resolveAlias(c *gin.Context, alias string) bool {
	start := time.Now()
	path := c.Request.URL.Path
	sv := h.executor.Semantic()
	spec := sv.Aliases[alias]
	if spec == nil { // removed by a concurrent reload: treat as a plain model name
		return true
	}
	c.Set(auditKeyAlias, alias)
	c.Set(auditKeyModel, alias) // overwritten with the resolved model by parsePassthroughRequest

	body, ok := cachedBody(c)
	if !ok {
		abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "request body is required", domain.SourceRouter)
		return false
	}
	view, err := semantic.ParseView(body, semantic.DialectForPath(path))
	if err != nil {
		abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "Invalid request: "+err.Error(), domain.SourceRouter)
		return false
	}

	allowSet, unrestricted := effectiveAllowedModels(c)
	env := semantic.Env{
		Allowed: func(m string) bool {
			if unrestricted {
				return true
			}
			_, ok := allowSet[m]
			return ok
		},
		IsAlias:    h.executor.IsAlias,
		KeyID:      callerIdentity(c),
		TaskHeader: c.GetHeader(semantic.TaskIDHeader),
		CountOnly:  path == "/v1/messages/count_tokens",
	}
	if h.healthyAgentCount != nil {
		env.Healthy = func(m string) bool { return h.healthyAgentCount(m) > 0 }
	}
	if h.estimator != nil {
		n := len(view.Messages)
		env.EstimateTokens = func(m string) int { return h.estimator.Estimate(m, view.PromptBytes, n) }
	}

	d, resolveErr := h.resolver.Resolve(alias, spec, sv.Profiles, view, env)
	if resolveErr != nil {
		h.logDecision(c, path, d, resolveErr, time.Since(start))
		status, code := http.StatusServiceUnavailable, domain.ErrCodeBackendUnavailable
		if allReasons(d.FilteredOut, "not_allowed") {
			status, code = http.StatusForbidden, domain.ErrCodeModelForbidden
		}
		abortWithRouterError(c, status, code,
			fmt.Sprintf("no model available for %q (route %s): %s", alias, d.Route, describeFiltered(d.FilteredOut)),
			domain.SourceRouter)
		return false
	}

	patched, err := replaceTopLevelModel(body, d.Model)
	// decision_us covers everything the alias adds: parse, resolve and rewrite.
	h.logDecision(c, path, d, err, time.Since(start))
	if err != nil {
		abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid, "Invalid request: "+err.Error(), domain.SourceRouter)
		return false
	}
	c.Set(gin.BodyBytesKey, patched)
	c.Set(ctxKeyPeekedModel, d.Model)
	c.Set(auditKeySemanticRoute, d.Route)
	c.Header(HeaderRoutedModel, d.Model)
	c.Header(HeaderRoute, d.Route)
	c.Header(HeaderRouteSource, d.Source)
	return true
}

func (h *Handlers) logDecision(c *gin.Context, path string, d semantic.Decision, err error, took time.Duration) {
	if h.decisionLog == nil {
		return
	}
	rec := semantic.DecisionRecord{
		TS:            time.Now().UTC(),
		RequestID:     c.GetString("request_id"),
		Path:          path,
		KeyID:         callerIdentity(c),
		Alias:         d.Alias,
		ResolvedModel: d.Model,
		Route:         d.Route,
		Source:        d.Source,
		RouteScores:   d.RouteScores,
		RouteMatches:  d.RouteMatches,
		TaskKey:       d.TaskKey,
		DecisionUS:    took.Microseconds(),
	}
	if d.Features != nil {
		rec.Features = d.Features.Values()
	}
	if len(d.FilteredOut) > 0 {
		rec.FilteredOut = d.FilteredOut
	}
	if err != nil {
		rec.Error = err.Error()
	}
	h.decisionLog.Write(rec)
}

// cachedBody returns the body bytes cached by ShouldBindBodyWith (peekModel
// populated it).
func cachedBody(c *gin.Context) ([]byte, bool) {
	cb, ok := c.Get(gin.BodyBytesKey)
	if !ok {
		return nil, false
	}
	b, ok := cb.([]byte)
	return b, ok
}

// callerIdentity scopes task pins and fingerprints to one API key. Dynamic
// keys carry a key_id; static auth.yaml keys do not, so fall back to the
// tenant plus the key's masked preview, which distinguishes keys of one tenant.
func callerIdentity(c *gin.Context) string {
	if id := c.GetString("key_id"); id != "" {
		return id
	}
	return c.GetString("tenant_id") + "/" + c.GetString("key_preview")
}

func allReasons(filtered map[string]string, reason string) bool {
	if len(filtered) == 0 {
		return false
	}
	for _, r := range filtered {
		if r != reason {
			return false
		}
	}
	return true
}

func describeFiltered(filtered map[string]string) string {
	if len(filtered) == 0 {
		return "no candidates"
	}
	parts := make([]string, 0, len(filtered))
	for m, r := range filtered {
		parts = append(parts, m+"="+r)
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

// replaceTopLevelModel returns body with the value of the top-level "model"
// key replaced by model. Every other byte is preserved, so the forwarded
// request differs from the client's only in the model name. When the key is
// duplicated the LAST occurrence is replaced, matching encoding/json (which
// peekModel and the handler use), so quota and routing see the same model.
//
// It is a byte scanner rather than a json.Decoder walk: the body was already
// validated by semantic.ParseView, and agentic bodies are hundreds of KB with
// "model" often serialised after "messages".
func replaceTopLevelModel(body []byte, model string) ([]byte, error) {
	i := skipWS(body, 0)
	if i >= len(body) || body[i] != '{' {
		return nil, errors.New("request body must be a JSON object")
	}
	i++
	valStart, valEnd := -1, -1
	for {
		i = skipWS(body, i)
		if i < len(body) && body[i] == '}' {
			break
		}
		if i >= len(body) || body[i] != '"' {
			return nil, errors.New("malformed JSON object")
		}
		keyStart := i
		i = skipString(body, i)
		key := body[keyStart:i]
		i = skipWS(body, i)
		if i >= len(body) || body[i] != ':' {
			return nil, errors.New("malformed JSON object")
		}
		i = skipWS(body, i+1)
		vs := i
		i = skipValue(body, i)
		if i < 0 {
			return nil, errors.New("malformed JSON value")
		}
		if isModelKey(key) {
			valStart, valEnd = vs, i
		}
		i = skipWS(body, i)
		if i < len(body) && body[i] == ',' {
			i++
			continue
		}
		if i < len(body) && body[i] == '}' {
			break
		}
		return nil, errors.New("malformed JSON object")
	}
	if valStart < 0 {
		return nil, errors.New("request body has no top-level model field")
	}
	quoted, _ := json.Marshal(model)
	out := make([]byte, 0, len(body)-(valEnd-valStart)+len(quoted))
	out = append(out, body[:valStart]...)
	out = append(out, quoted...)
	return append(out, body[valEnd:]...), nil
}

// isModelKey reports whether a quoted JSON key decodes to "model" (escaped
// spellings such as "mod\u0065l" included, as encoding/json would decode them).
func isModelKey(quoted []byte) bool {
	if string(quoted) == `"model"` {
		return true
	}
	if !bytes.ContainsRune(quoted, '\\') {
		return false
	}
	var k string
	return json.Unmarshal(quoted, &k) == nil && k == "model"
}

func skipWS(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// skipString returns the index just past the string starting at b[i] == '"'.
func skipString(b []byte, i int) int {
	for i++; i < len(b); i++ {
		switch b[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return len(b)
}

// skipValue returns the index just past the JSON value starting at b[i], or -1.
func skipValue(b []byte, i int) int {
	if i >= len(b) {
		return -1
	}
	switch b[i] {
	case '"':
		return skipString(b, i)
	case '{', '[':
		depth := 0
		for ; i < len(b); i++ {
			switch b[i] {
			case '"':
				i = skipString(b, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return -1
	default: // number, true, false, null
		for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != ']' && b[i] != ' ' && b[i] != '\t' && b[i] != '\n' && b[i] != '\r' {
			i++
		}
		return i
	}
}

// aliasModelObjects returns /v1/models entries for aliases the caller may use:
// at least one candidate is allowed for the key and has a healthy agent.
// applyAllowSet=false (admin listing) skips the key filter.
func (h *Handlers) aliasModelObjects(c *gin.Context, models map[string]domain.ModelObject, applyAllowSet bool) []domain.ModelObject {
	if h.executor == nil {
		return nil
	}
	aliases := h.executor.Semantic().Aliases
	if len(aliases) == 0 {
		return nil
	}
	allowSet, unrestricted := effectiveAllowedModels(c)
	out := make([]domain.ModelObject, 0, len(aliases))
	for name, spec := range aliases {
		visible := false
		var routes []string
		for _, r := range spec.Routes {
			routes = append(routes, r.Name)
			for _, cand := range r.Candidates {
				if applyAllowSet && !unrestricted {
					if _, ok := allowSet[cand.Model]; !ok {
						continue
					}
				}
				if m, ok := models[cand.Model]; ok && m.Agents.Healthy > 0 {
					visible = true
				}
			}
		}
		if !visible {
			continue
		}
		out = append(out, domain.ModelObject{
			ID:         name,
			Object:     "model",
			Capability: domain.CapabilityLLM,
			Info:       "semantic alias; routes: " + strings.Join(routes, ", "),
			Agents:     domain.ModelAgents{Engines: []string{}, Regions: []string{}},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
