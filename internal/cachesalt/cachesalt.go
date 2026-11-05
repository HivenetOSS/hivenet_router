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
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	// prefix for the request (llama.cpp chat completions).
	NoCache
	// NoCacheExtraParams sets extra_params.cache_prompt to false: llama.cpp's
	// /v1/messages merges extra_params into the request it runs, so this is
	// NoCache for an endpoint whose top level only takes Anthropic fields.
	// The top-level cache_prompt is forced to false as well.
	NoCacheExtraParams
)

// Support is how an engine isolates one path, and from which version.
type Support struct {
	Action Action
	// Since is the first engine version, as the agent reports it
	// (AgentMetadata.EngineVersion), whose Action isolates the path; empty
	// means every version. An older or unknown version still gets the Action
	// (it ignores the field), but the path counts as a gap: nothing shows
	// the engine isolates it.
	Since string
	// UnknownMeets counts an agent that reports no version as meeting Since.
	// It is for an engine and path where every version still in use is
	// known to meet Since, so agents built before version reporting do not
	// all count as gaps during a rollout. A version that is reported but
	// below Since, or unreadable, is a gap either way.
	UnknownMeets bool
}

// Capabilities lists, per engine name (as the agent reports it in
// AgentMetadata.Engine) and forward path, how a request is isolated. A
// missing entry is None.
type Capabilities map[string]map[string]Support

// LLMPaths are the generation paths an LLM agent serves; a path an engine
// cannot isolate is a gap (Gaps).
var LLMPaths = []string{"/v1/chat/completions", "/v1/messages"}

// DefaultCapabilities lists what each engine supports. count_tokens runs no
// prefill and has no cache to isolate, so no engine lists it.
//
//   - vLLM accepts cache_salt on chat completions from v0.9.0 (vLLM #17045),
//     on its pooling endpoints (embeddings, rerank) from v0.16.0 (#31127),
//     and on the Anthropic messages endpoint from v0.27.0 (#49498): that
//     endpoint exists from v0.11.1 and ignores the field before. An agent
//     that reports no version counts as isolated on chat and pooling, which
//     every vLLM in use supports, but not on /v1/messages, where v0.25 and
//     v0.26 are still deployed.
//   - SGLang accepts cache_salt on chat completions and namespaces its radix
//     cache by it from v0.5.11; older versions accept the field and ignore
//     it. Its messages endpoint drops the salt in released versions, and its
//     embedding requests have no salt field.
//   - llama.cpp has no salt, but cache_prompt=false stops a request from
//     reusing any cached prefix: on chat completions at the top level, on
//     /v1/messages through extra_params, which it takes from build b11795.
//     Its embeddings and rerank never reuse the prompt cache.
//
// Ollama and custom backends document neither; supporting an engine is a
// line here.
func DefaultCapabilities() Capabilities {
	return Capabilities{
		"vllm": {
			"/v1/chat/completions": {Action: Salt, Since: "0.9.0", UnknownMeets: true},
			"/v1/messages":         {Action: Salt, Since: "0.27.0"},
			"/v1/embeddings":       {Action: Salt, Since: "0.16.0", UnknownMeets: true},
			"/v1/rerank":           {Action: Salt, Since: "0.16.0", UnknownMeets: true},
		},
		"sglang": {
			"/v1/chat/completions": {Action: Salt, Since: "0.5.11"},
		},
		"llamacpp": {
			"/v1/chat/completions": {Action: NoCache},
			"/v1/messages":         {Action: NoCacheExtraParams, Since: "b11795"},
		},
	}
}

// Gaps returns the LLM paths an agent running engine at version serves
// without prefix-cache isolation: the paths the engine cannot isolate, and
// those it isolates only from a later version than version (or when version
// is unknown, unless the path's UnknownMeets says otherwise).
func (c Capabilities) Gaps(engine, version string) []string {
	var out []string
	for _, p := range LLMPaths {
		if sup := c[engine][p]; sup.Action == None || !sup.isolates(version) {
			out = append(out, p)
		}
	}
	return out
}

// isolates reports whether an engine at version isolates the path.
func (s Support) isolates(version string) bool {
	if version == "" && s.UnknownMeets {
		return true
	}
	return AtLeast(version, s.Since)
}

// Gaps is Capabilities.Gaps for this Salter's table.
func (s *Salter) Gaps(engine, version string) []string { return s.caps.Gaps(engine, version) }

// AtLeast reports whether an engine version is since or newer. Both compare
// as their leading dot-separated numbers after an optional "v" or "b" prefix:
// "0.5.11rc1" reads as 0.5.11, "b11795-1a2b3c" (llama.cpp) as 11795. An empty
// since is always met; an empty or unreadable version never meets a
// non-empty since.
func AtLeast(version, since string) bool {
	if since == "" {
		return true
	}
	have, ok := versionNumbers(version)
	if !ok {
		return false
	}
	want, ok := versionNumbers(since)
	if !ok {
		return false
	}
	for i := 0; i < max(len(have), len(want)); i++ {
		var h, w int
		if i < len(have) {
			h = have[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if h != w {
			return h > w
		}
	}
	return true
}

// versionNumbers returns the leading dot-separated numbers of v.
func versionNumbers(v string) ([]int, bool) {
	if v != "" && (v[0] == 'v' || v[0] == 'b') {
		v = v[1:]
	}
	var out []int
	n, digits := 0, 0
	for i := 0; i <= len(v); i++ {
		if i < len(v) && v[i] >= '0' && v[i] <= '9' {
			if digits >= 9 {
				return nil, false
			}
			n = n*10 + int(v[i]-'0')
			digits++
			continue
		}
		if digits == 0 {
			break
		}
		out = append(out, n)
		n, digits = 0, 0
		if i >= len(v) || v[i] != '.' {
			break
		}
	}
	return out, len(out) > 0
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

// fingerprintDomain separates the secret's fingerprint from the salts.
const fingerprintDomain = "hivenet-router/cache-salt-fingerprint/v1"

// Fingerprint identifies the secret without revealing it or any salt: 16 hex
// characters of HMAC-SHA256(secret, a fixed label). The routers of a site
// compare theirs to find one started with another secret, which would give
// each tenant two prefix-cache namespaces.
func (s *Salter) Fingerprint() string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(fingerprintDomain))
	return hex.EncodeToString(mac.Sum(nil)[:8])
}

// ErrExtraParams: a body's extra_params is neither absent, null nor an
// object, so it cannot carry cache_prompt (see CheckBody).
var ErrExtraParams = errors.New("extra_params must be a JSON object")

// CheckBody refuses what Apply could not prepare for some engine on path, so
// the router refuses it up front, whichever agent would serve it: today an
// extra_params that is not an object (or null), on the paths where an
// engine needs cache_prompt inside it (NoCacheExtraParams). body must
// already be one JSON object.
func CheckBody(path string, body []byte) error {
	if !extraParamsPaths[path] {
		return nil
	}
	extra, ok, err := jsonbody.GetTopLevel(body, extraParamsField)
	if err != nil || !ok || isNull(extra) {
		return err
	}
	if len(extra) == 0 || extra[0] != '{' {
		return ErrExtraParams
	}
	return nil
}

// extraParamsPaths are the paths where some engine of DefaultCapabilities
// sets cache_prompt inside extra_params.
var extraParamsPaths = func() map[string]bool {
	out := map[string]bool{}
	for _, paths := range DefaultCapabilities() {
		for p, sup := range paths {
			if sup.Action == NoCacheExtraParams {
				out[p] = true
			}
		}
	}
	return out
}()

// isNull reports whether a raw JSON value is null.
func isNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

// cachePromptField is llama.cpp's per-request prompt-cache switch, and
// extraParamsField the object llama.cpp's /v1/messages merges into the
// request it runs.
const (
	cachePromptField = "cache_prompt"
	extraParamsField = "extra_params"
)

// Apply returns the body to forward to an agent running engine, on path, for
// tenantID. Any cache_salt the client sent is removed. Then, as the engine
// supports on that path, the tenant's salt is set, or cache_prompt is forced
// to false (at the top level, or inside extra_params). body is never
// modified; the result may be body itself when there was nothing to change.
// It fails only for a body that is not one JSON object, or whose
// extra_params is neither null nor an object (refused up front: CheckBody).
func (s *Salter) Apply(body []byte, engine, path, tenantID string) ([]byte, error) {
	switch s.caps[engine][path].Action {
	case Salt:
		quoted, _ := json.Marshal(s.Salt(tenantID))
		return jsonbody.SetTopLevel(body, Field, quoted)
	case NoCache:
		stripped, err := jsonbody.RemoveTopLevel(body, Field)
		if err != nil {
			return nil, err
		}
		return jsonbody.SetTopLevel(stripped, cachePromptField, []byte("false"))
	case NoCacheExtraParams:
		stripped, err := jsonbody.RemoveTopLevel(body, Field)
		if err != nil {
			return nil, err
		}
		// At the top level too, so no client-sent cache_prompt survives
		// on a build that passes top-level llama.cpp parameters through.
		if stripped, err = jsonbody.SetTopLevel(stripped, cachePromptField, []byte("false")); err != nil {
			return nil, err
		}
		extra, ok, err := jsonbody.GetTopLevel(stripped, extraParamsField)
		if err != nil {
			return nil, err
		}
		// A missing or null extra_params is an empty one: null means "no
		// extra parameters" to llama.cpp as much as an absent field does.
		if !ok || isNull(extra) {
			extra = []byte("{}")
		}
		extra, err = jsonbody.SetTopLevel(extra, cachePromptField, []byte("false"))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", extraParamsField, err)
		}
		return jsonbody.SetTopLevel(stripped, extraParamsField, extra)
	default:
		return jsonbody.RemoveTopLevel(body, Field)
	}
}
