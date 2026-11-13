// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"bytes"
	"encoding/json"
	"testing"

	"hivenet_router/internal/domain"
)

func keyPending(t *testing.T, tenant, path string) *domain.PendingRequest {
	t.Helper()
	var req domain.ChatRequest
	body := `{"model":"m","messages":[{"role":"system","content":"S"},{"role":"user","content":"hi"}]}`
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	p := domain.NewPendingRequest("r", &req, 0)
	p.TenantID = tenant
	p.Path = path
	return p
}

// Without a scope the same prompt gets the same key whoever sends it; with
// one, tenants never share a key.
func TestRoutingKey_Scope(t *testing.T) {
	shared := &RequestProcessor{}
	a := shared.routingKey(keyPending(t, "a", ""), domain.CapabilityLLM, 4096)
	b := shared.routingKey(keyPending(t, "b", ""), domain.CapabilityLLM, 4096)
	if a == nil || !bytes.Equal(a, b) {
		t.Errorf("no scope: tenants got keys %x and %x, want one non-nil key", a, b)
	}

	scoped := &RequestProcessor{}
	WithRoutingKeyScope(func(tenant string) []byte { return []byte(tenant) })(scoped)
	a = scoped.routingKey(keyPending(t, "a", ""), domain.CapabilityLLM, 4096)
	b = scoped.routingKey(keyPending(t, "b", ""), domain.CapabilityLLM, 4096)
	if bytes.Equal(a, b) {
		t.Error("scoped: two tenants share a key")
	}
}

// Requests with no KV cache to reuse get no key.
func TestRoutingKey_NoKey(t *testing.T) {
	p := &RequestProcessor{}
	if k := p.routingKey(keyPending(t, "a", ""), domain.CapabilityEmbedding, 4096); k != nil {
		t.Errorf("embedding: key %x, want nil", k)
	}
	if k := p.routingKey(keyPending(t, "a", countTokensPath), domain.CapabilityLLM, 4096); k != nil {
		t.Errorf("count_tokens: key %x, want nil", k)
	}
	if k := p.routingKey(keyPending(t, "a", "/v1/messages"), domain.CapabilityLLM, 4096); k == nil {
		t.Error("/v1/messages: want a key")
	}
}
