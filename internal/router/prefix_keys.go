// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/config"
	"hivenet_router/internal/prefixhash"
)

// prefixAnchorLabel names the cache-salt sub-key that keys prefix anchors.
const prefixAnchorLabel = "prefix-anchor"

// prefixKeys returns the keys of prefix-aware routing. Block hashes are
// computed by agents, so their key comes from the site's agent secret. With
// the cache salt on, anchors are keyed by a sub-key of the cache-salt secret,
// which agents do not hold: an agent cannot check a guessed text against an
// anchor. With it off, the agent secret keys them too.
func prefixKeys(cfg *config.Config, salter *cachesalt.Salter) prefixhash.Keys {
	keys := prefixhash.DeriveKeys([]byte(cfg.JWTSecret))
	if salter != nil {
		keys = keys.WithAnchorSecret(salter.SubKey(prefixAnchorLabel))
	}
	return keys
}

// prefixScope returns the scope of a tenant's anchors: the tenant when the
// cache salt is on, so tenants never share a memo entry or a pending note,
// as they never share an engine cache entry; nil (one scope) when it is off
// and tenants share the engine's cache.
func prefixScope(salter *cachesalt.Salter) func(tenantID string) string {
	if salter == nil {
		return nil
	}
	return func(tenantID string) string { return tenantID }
}
