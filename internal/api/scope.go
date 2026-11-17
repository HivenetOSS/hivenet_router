// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"hivenet_router/internal/domain"
)

// Context keys set by AuthMiddleware and Scope.
const (
	ctxKeyScopeMax    = "scope_max"
	ctxKeyHomeCountry = "home_country"
	ctxKeyScope       = "scope"
)

// Scope resolves where an inference request may be served, from its API key
// (scope_max, home_country) and the X-Hivenet-Scope header:
//
//   - the header asks for a scope: it must be no wider than the key's
//     scope_max ("all" when unset), else 403 scope_forbidden; an invalid
//     value is a 400;
//   - no header: the key's home_country, else its scope_max when narrower
//     than "all", else no scope (served by any agent, redirected inside
//     this site's country, as before keys carried a scope);
//   - a request repeated at a sibling site after a 307 keeps the scope of its
//     token, checked again against the key here.
//
// The scope is a hard filter on the agents that may serve the request (by
// the country they declare) and on the sites it may be redirected to. It
// runs before quota and the redirect decision.
func (h *Handlers) Scope() gin.HandlerFunc {
	return func(c *gin.Context) {
		max := domain.ScopeAll
		if v := c.GetString(ctxKeyScopeMax); v != "" {
			max = domain.Scope(v) // validated when the key was loaded
		}
		var scope domain.Scope
		if info := redirectInfo(c); info != nil {
			scope = domain.Scope(info.Scope)
		} else if v := strings.TrimSpace(c.GetHeader(scopeHeader)); v != "" {
			s, err := domain.ParseScope(v)
			if err != nil {
				abortWithRouterError(c, http.StatusBadRequest, domain.ErrCodeRequestInvalid,
					fmt.Sprintf("invalid %s header %q: use country:XX (ISO 3166-1 alpha-2), EU or all", scopeHeader, v), domain.SourceRouter)
				return
			}
			scope = s
		} else if hc := c.GetString(ctxKeyHomeCountry); hc != "" {
			scope = domain.CountryScope(hc)
		} else if max != domain.ScopeAll {
			scope = max
		}
		if scope != "" && !scope.Within(max) {
			abortWithRouterError(c, http.StatusForbidden, domain.ErrCodeScopeForbidden,
				fmt.Sprintf("scope %q is wider than this API key allows (scope_max: %s)", scope, max), domain.SourceRouter)
			return
		}
		if scope != "" {
			c.Set(ctxKeyScope, scope)
		}
		c.Next()
	}
}

// requestScope returns the scope Scope resolved, or "" when none.
func requestScope(c *gin.Context) domain.Scope {
	v, _ := c.Get(ctxKeyScope)
	s, _ := v.(domain.Scope)
	return s
}
