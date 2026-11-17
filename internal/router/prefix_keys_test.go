// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"slices"
	"testing"

	"hivenet_router/internal/cachesalt"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixhash"
)

// With the cache salt on, anchors are keyed by the cache-salt secret (which
// agents do not hold) and scoped per tenant; block hashes stay on the agent
// secret. With it off, both come from the agent secret and every tenant
// shares one scope.
func TestPrefixKeysAndScope(t *testing.T) {
	cfg := &config.Config{JWTSecret: "agent-secret-at-least-32-bytes-long!!"}
	salter, err := cachesalt.New([]byte("0123456789abcdef0123456789abcdef"), cachesalt.DefaultCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	req := &domain.ChatRequest{Messages: []domain.ChatCompletionMessage{{Role: "user", Content: "hello"}}}
	agentKeys := prefixhash.DeriveKeys([]byte(cfg.JWTSecret))

	off := prefixKeys(cfg, nil)
	on := prefixKeys(cfg, salter)
	aOff, _ := off.Anchors("m", "", req)
	aAgent, _ := agentKeys.Anchors("m", "", req)
	aOn, _ := on.Anchors("m", "", req)
	if !slices.Equal(aOff, aAgent) {
		t.Error("salt off: anchors not keyed by the agent secret")
	}
	if slices.Equal(aOn, aAgent) {
		t.Error("salt on: anchors still keyed by the agent secret")
	}
	tokens := make([]int, 2*prefixhash.BlockTokens)
	if !slices.Equal(on.Blocks("m", "s", tokens), agentKeys.Blocks("m", "s", tokens)) {
		t.Error("salt on: block key differs from the agents'")
	}

	if prefixScope(nil) != nil {
		t.Error("salt off: tenants should share one scope")
	}
	scope := prefixScope(salter)
	if scope == nil || scope("tenant-a") == scope("tenant-b") {
		t.Fatal("salt on: tenants share a scope")
	}
	a, _ := on.Anchors("m", scope("tenant-a"), req)
	b, _ := on.Anchors("m", scope("tenant-b"), req)
	if slices.Equal(a, b) {
		t.Error("salt on: two tenants' identical prompts share anchors")
	}
}

// Routers compare their cache-salt setting: another secret, salt off among
// routers with it, and salt on among routers without it all count.
func TestCompareFingerprints(t *testing.T) {
	vals := map[string]string{"r1": "aaaa", "r2": "aaaa", "r3": "bbbb", "r4": saltOff}
	if got, want := compareFingerprints("aaaa", vals), (saltMismatch{secret: 1, off: 1}); got != want {
		t.Errorf("salt on: %+v, want %+v", got, want)
	}
	if got, want := compareFingerprints(saltOff, vals), (saltMismatch{on: 3}); got != want {
		t.Errorf("salt off: %+v, want %+v", got, want)
	}
	if got := compareFingerprints(saltOff, map[string]string{"r1": saltOff}); got != (saltMismatch{}) {
		t.Errorf("all off: %+v", got)
	}
}
