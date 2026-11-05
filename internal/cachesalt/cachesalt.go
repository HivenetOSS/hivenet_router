// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package cachesalt gives each tenant its own prefix-cache namespace on the
// inference engine. Engines such as vLLM share cached KV blocks between every
// request with the same prompt prefix, so a cache hit's faster first token
// can reveal that another tenant sent that prefix (vLLM CVE-2025-46570).
// vLLM's per-request "cache_salt" field mixes a value into the first block's
// hash: only requests with the same salt share cached blocks.
//
// The router sets the salt itself, per tenant, on every request it forwards
// to an engine that supports it, and always removes a salt sent by the
// client, which could otherwise pick another tenant's salt.
package cachesalt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"hivenet_router/internal/jsonbody"
)

// Field is the request-body field that carries the salt.
const Field = "cache_salt"

// MinSecretBytes is the shortest secret New accepts (256 bits).
const MinSecretBytes = 32

// saltDomain separates salt HMACs from any other use of the secret.
const saltDomain = "hivenet-router/cache-salt/v1\x00"

// Capabilities lists, per engine name (as the agent reports it in
// AgentMetadata.Engine), the forward paths whose request accepts a salt.
type Capabilities map[string]map[string]bool

// DefaultCapabilities is vLLM, on every path the router forwards to it except
// count_tokens, which runs no prefill and so has no cache to salt. vLLM
// accepts cache_salt on chat completions, the Anthropic messages endpoint and
// its pooling endpoints (embeddings, rerank). Other engines document no salt;
// supporting one is a line here.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		"vllm": {
			"/v1/chat/completions": true,
			"/v1/messages":         true,
			"/v1/embeddings":       true,
			"/v1/rerank":           true,
		},
	}
}

// Salter derives tenant salts and applies them to request bodies. Safe for
// concurrent use.
type Salter struct {
	secret []byte
	caps   Capabilities
}

// New returns a Salter keyed by secret, which must be at least
// MinSecretBytes long and the same on every router in front of the same
// engines (otherwise a tenant's requests through different routers do not
// share its own cache).
func New(secret []byte, caps Capabilities) (*Salter, error) {
	if len(secret) < MinSecretBytes {
		return nil, fmt.Errorf("cache salt secret must be at least %d bytes, got %d", MinSecretBytes, len(secret))
	}
	return &Salter{secret: append([]byte(nil), secret...), caps: caps}, nil
}

// Salt returns the tenant's salt: base64url (no padding) of
// HMAC-SHA256(secret, domain + tenantID), 43 characters from [A-Za-z0-9_-].
// It is unpredictable without the secret, stable for a tenant, and avoids
// the characters vLLM rejects in a salt ('@', '/', '\', NUL).
func (s *Salter) Salt(tenantID string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(saltDomain + tenantID))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Apply returns the body to forward to an agent running engine, on path, for
// tenantID. Any cache_salt the client sent is removed. When the engine
// accepts a salt on that path, the tenant's salt is set. body is never
// modified; the result may be body itself when there was nothing to change.
func (s *Salter) Apply(body []byte, engine, path, tenantID string) ([]byte, error) {
	if !s.caps[engine][path] {
		return jsonbody.RemoveTopLevel(body, Field)
	}
	quoted, _ := json.Marshal(s.Salt(tenantID))
	return jsonbody.SetTopLevel(body, Field, quoted)
}
