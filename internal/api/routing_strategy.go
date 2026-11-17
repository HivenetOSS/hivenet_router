// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"

	"hivenet_router/internal/domain"
)

// RoutingStrategyHeader lets a request pick the ranking strategy it is routed
// with, among the ones its API key allows (routing_strategies).
const RoutingStrategyHeader = "X-Hivenet-Routing-Strategy"

// Context keys set by RoutingStrategy.
const (
	ctxKeyRoutingStrategies      = "routing_strategies" // the key's list, set by AuthMiddleware
	ctxKeyRoutingStrategy        = "routing_strategy"
	ctxKeyRoutingStrategySource  = "routing_strategy_source"
	routingStrategySourceHeader  = "header"
	routingStrategySourceKey     = "key"
	routingStrategyNoneAllowedTx = "none"
)

// RoutingStrategy resolves the ranking strategy of an inference request from
// its API key and the X-Hivenet-Routing-Strategy header:
//
//   - the header names a strategy in the key's routing_strategies: that one;
//   - the header names any other (or the key has no list): 403
//     routing_strategy_forbidden — a request can only narrow what its key
//     allows, never widen it;
//   - no header: the key's first strategy, or, when the key has no list, the
//     model's policy decides (nothing is set).
//
// A request repeated at a sibling site after a 307 carries the strategy the
// client asked for at the first router in its signed token, and is checked
// again against the key here.
//
// It runs before quota and the redirect decision, so a refused request
// spends nothing and a redirect carries the choice. The strategy replaces
// only the ranking of the model's policy steps (policy.NewSessionWithStrategy).
func (h *Handlers) RoutingStrategy() gin.HandlerFunc {
	return func(c *gin.Context) {
		allowed := c.GetStringSlice(ctxKeyRoutingStrategies)
		requested := strings.TrimSpace(c.GetHeader(RoutingStrategyHeader))
		if info := redirectInfo(c); info != nil {
			requested = info.Strategy // the first router's choice, never this hop's header
		}
		switch {
		case requested != "":
			if !slices.Contains(allowed, requested) {
				list := routingStrategyNoneAllowedTx
				if len(allowed) > 0 {
					list = strings.Join(allowed, ", ")
				}
				abortWithRouterError(c, http.StatusForbidden, domain.ErrCodeRoutingStrategyForbidden,
					fmt.Sprintf("routing strategy %q is not allowed for this API key (allowed: %s)", requested, list),
					domain.SourceRouter)
				return
			}
			c.Set(ctxKeyRoutingStrategy, requested)
			c.Set(ctxKeyRoutingStrategySource, routingStrategySourceHeader)
		case len(allowed) > 0:
			c.Set(ctxKeyRoutingStrategy, allowed[0])
			c.Set(ctxKeyRoutingStrategySource, routingStrategySourceKey)
		}
		c.Next()
	}
}

// setRoutingStrategy copies the strategy RoutingStrategy resolved onto the
// request handed to the processor.
func setRoutingStrategy(c *gin.Context, pending *domain.PendingRequest) {
	pending.RoutingStrategy = c.GetString(ctxKeyRoutingStrategy)
	pending.RoutingStrategySource = c.GetString(ctxKeyRoutingStrategySource)
}

// requestedRoutingStrategy returns the strategy the client asked for with the
// header (not the key's default), for a redirect token to carry.
func requestedRoutingStrategy(c *gin.Context) string {
	if c.GetString(ctxKeyRoutingStrategySource) != routingStrategySourceHeader {
		return ""
	}
	return c.GetString(ctxKeyRoutingStrategy)
}
