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
// client, which could otherwise pick another tenant's salt. An engine with no
// salt but a per-request switch to skip its prompt cache (llama.cpp) gets
// that switch instead: no request reuses another's prefix, at the cost of
// cache reuse within a tenant too.
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

// Action is how a forwarded request is kept out of other tenants' prefix
// cache on one engine and path.
type Action int

const (
	// None: the engine has no way to isolate this path; any client-sent
	// cache_salt is still removed.
	None Action = iota
	// Salt sets cache_salt to the tenant's salt.
	Salt
	// NoCache sets cache_prompt to false: the engine reuses no cached
	// prefix for the request (llama.cpp).
	NoCache
)

// Capabilities lists, per engine name (as the agent reports it in
// AgentMetadata.Engine) and forward path, how a request is isolated. A
// missing entry is None.
type Capabilities map[string]map[string]Action

// LLMPaths are the generation paths an LLM agent serves; a path an engine
// cannot isolate is a gap (Gaps).
var LLMPaths = []string{"/v1/chat/completions", "/v1/messages"}

// DefaultCapabilities lists what each engine supports. count_tokens runs no
// prefill and has no cache to isolate, so no engine lists it.
//
//   - vLLM accepts cache_salt on chat completions, the Anthropic messages
//     endpoint and its pooling endpoints (embeddings, rerank).
//   - SGLang (v0.5.11 or later) accepts cache_salt on chat completions and
//     namespaces its radix cache by it. Its messages endpoint drops the salt
//     in released versions, and its embedding requests have no salt field.
//   - llama.cpp has no salt, but cache_prompt=false on chat completions stops
//     the request from reusing any cached prefix. Its embeddings and rerank
//     never reuse the prompt cache.
//
// Ollama and custom backends document neither; supporting an engine is a
// line here.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		"vllm": {
			"/v1/chat/completions": Salt,
			"/v1/messages":         Salt,
			"/v1/embeddings":       Salt,
			"/v1/rerank":           Salt,
		},
		"sglang": {
			"/v1/chat/completions": Salt,
		},
		"llamacpp": {
			"/v1/chat/completions": NoCache,
		},
	}
}

// Gaps returns the LLM paths an agent running engine serves without
// prefix-cache isolation.
func (c Capabilities) Gaps(engine string) []string {
	var out []string
	for _, p := range LLMPaths {
		if c[engine][p] == None {
			out = append(out, p)
		}
	}
	return out
}

// Gaps is Capabilities.Gaps for this Salter's table.
func (s *Salter) Gaps(engine string) []string { return s.caps.Gaps(engine) }

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

// cachePromptField is llama.cpp's per-request prompt-cache switch.
const cachePromptField = "cache_prompt"

// Apply returns the body to forward to an agent running engine, on path, for
// tenantID. Any cache_salt the client sent is removed. Then, as the engine
// supports on that path, the tenant's salt is set, or cache_prompt is forced
// to false. body is never modified; the result may be body itself when there
// was nothing to change.
func (s *Salter) Apply(body []byte, engine, path, tenantID string) ([]byte, error) {
	switch s.caps[engine][path] {
	case Salt:
		quoted, _ := json.Marshal(s.Salt(tenantID))
		return jsonbody.SetTopLevel(body, Field, quoted)
	case NoCache:
		stripped, err := jsonbody.RemoveTopLevel(body, Field)
		if err != nil {
			return nil, err
		}
		return jsonbody.SetTopLevel(stripped, cachePromptField, []byte("false"))
	default:
		return jsonbody.RemoveTopLevel(body, Field)
	}
}
