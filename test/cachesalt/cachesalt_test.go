// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package cachesalt_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"hivenet_router/internal/cachesalt"
)

var secret = []byte("0123456789abcdef0123456789abcdef")

func newSalter(t *testing.T) *cachesalt.Salter {
	t.Helper()
	s, err := cachesalt.New(secret, cachesalt.DefaultCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// saltRe is the salt format: 43 base64url characters (256 bits), none of
// which vLLM rejects in a cache_salt ('@', '/', '\', NUL; at most 128).
var saltRe = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

// TestSalt_FormatAndStability: a tenant's salt is stable (across Salters
// built from the same secret, as on several routers), differs between
// tenants and between secrets, and has the format vLLM accepts.
func TestSalt_FormatAndStability(t *testing.T) {
	a, b := newSalter(t), newSalter(t)
	other, err := cachesalt.New([]byte(strings.Repeat("z", 32)), nil)
	if err != nil {
		t.Fatal(err)
	}
	sa := a.Salt("tenant-a")
	if !saltRe.MatchString(sa) {
		t.Errorf("salt %q does not match %s", sa, saltRe)
	}
	if b.Salt("tenant-a") != sa {
		t.Error("same secret and tenant gave different salts")
	}
	if a.Salt("tenant-b") == sa {
		t.Error("two tenants share a salt")
	}
	if other.Salt("tenant-a") == sa {
		t.Error("two secrets gave the same salt")
	}
	if strings.Contains(sa, "tenant") {
		t.Error("salt reveals the tenant ID")
	}
}

// TestApply_EngineCapabilities: on an engine and path that accept a salt,
// the body carries exactly the tenant's salt (a client salt is replaced); on
// llama.cpp, cache_prompt is forced to false instead, at the top level on chat
// and inside extra_params on /v1/messages; everywhere else any client salt is
// removed and nothing is added. The rest of the body is unchanged either way.
func TestApply_EngineCapabilities(t *testing.T) {
	s := newSalter(t)
	body := []byte(`{"model":"m","cache_salt":"guess-of-another-tenant","cache_prompt":true,"messages":[{"role":"user","content":"hi"}]}`)
	orig := bytes.Clone(body)
	type want int
	const (
		stripped want = iota
		salted
		noCache
		noCacheExtra
	)
	for _, tc := range []struct {
		engine, path string
		want         want
	}{
		{"vllm", "/v1/chat/completions", salted},
		{"vllm", "/v1/messages", salted},
		{"vllm", "/v1/embeddings", salted},
		{"vllm", "/v1/rerank", salted},
		{"vllm", "/v1/messages/count_tokens", stripped}, // no prefill, no cache
		{"sglang", "/v1/chat/completions", salted},
		{"sglang", "/v1/messages", stripped},   // released SGLang drops the salt here
		{"sglang", "/v1/embeddings", stripped}, // no salt field
		{"llamacpp", "/v1/chat/completions", noCache},
		{"llamacpp", "/v1/messages", noCacheExtra},
		{"ollama", "/v1/chat/completions", stripped},
		{"infinity", "/v1/embeddings", stripped},
		{"custom", "/v1/chat/completions", stripped},
		{"", "/v1/chat/completions", stripped},
	} {
		got, err := s.Apply(body, tc.engine, tc.path, "tenant-a")
		if err != nil {
			t.Fatalf("%s %s: %v", tc.engine, tc.path, err)
		}
		var m map[string]any
		if err := json.Unmarshal(got, &m); err != nil {
			t.Fatalf("%s %s: result is not JSON: %v", tc.engine, tc.path, err)
		}
		salt, hasSalt := m["cache_salt"]
		switch tc.want {
		case salted:
			if salt != s.Salt("tenant-a") {
				t.Errorf("%s %s: cache_salt %v, want the tenant's salt", tc.engine, tc.path, salt)
			}
		case noCache:
			if hasSalt || m["cache_prompt"] != false {
				t.Errorf("%s %s: cache_salt %v cache_prompt %v, want no salt and cache_prompt false", tc.engine, tc.path, salt, m["cache_prompt"])
			}
		case noCacheExtra:
			extra, _ := m["extra_params"].(map[string]any)
			if hasSalt || extra == nil || extra["cache_prompt"] != false || m["cache_prompt"] != false {
				t.Errorf("%s %s: cache_salt %v extra_params %v, want no salt and extra_params.cache_prompt false", tc.engine, tc.path, salt, m["extra_params"])
			}
		case stripped:
			if hasSalt {
				t.Errorf("%s %s: cache_salt %v present, want none", tc.engine, tc.path, salt)
			}
		}
		prompts := 1 // at the top level
		if tc.want == noCacheExtra {
			prompts = 2 // and inside extra_params
		}
		if bytes.Count(got, []byte(`"cache_salt"`)) > 1 || bytes.Count(got, []byte(`"cache_prompt"`)) > prompts {
			t.Errorf("%s %s: a field appears more than once in %s", tc.engine, tc.path, got)
		}
		if m["model"] != "m" || len(m["messages"].([]any)) != 1 {
			t.Errorf("%s %s: other members changed: %s", tc.engine, tc.path, got)
		}
	}
	if !bytes.Equal(body, orig) {
		t.Error("Apply modified its input")
	}

	// A body without a client salt is returned as is where none is added.
	plain := []byte(`{"model":"m"}`)
	if got, _ := s.Apply(plain, "custom", "/v1/chat/completions", "t"); !bytes.Equal(got, plain) {
		t.Errorf("unsalted engine changed a body without cache_salt: %s", got)
	}
	if _, err := s.Apply([]byte(`[1,2]`), "vllm", "/v1/chat/completions", "t"); err == nil {
		t.Error("a non-object body was accepted")
	}

	// llama.cpp /v1/messages: a client's own extra_params are kept, and a
	// client asking for the prompt cache there is overridden; extra_params
	// that is not an object is refused.
	got, err := s.Apply([]byte(`{"model":"m","extra_params":{"seed":42,"cache_prompt":true},"messages":[]}`), "llamacpp", "/v1/messages", "t")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		ExtraParams map[string]any `json:"extra_params"`
	}
	if err := json.Unmarshal(got, &m); err != nil || m.ExtraParams["seed"] != float64(42) || m.ExtraParams["cache_prompt"] != false {
		t.Errorf("client extra_params: got %s, want seed kept and cache_prompt false", got)
	}
	// extra_params null is an empty one: the prompt cache is switched off
	// inside a fresh object.
	got, err = s.Apply([]byte(`{"model":"m","extra_params":null,"messages":[]}`), "llamacpp", "/v1/messages", "t")
	if err != nil {
		t.Fatalf("extra_params null: %v", err)
	}
	var withNull struct {
		ExtraParams map[string]any `json:"extra_params"`
	}
	if json.Unmarshal(got, &withNull) != nil || withNull.ExtraParams["cache_prompt"] != false {
		t.Errorf("extra_params null: got %s, want extra_params.cache_prompt false", got)
	}
	if _, err := s.Apply([]byte(`{"model":"m","extra_params":[1]}`), "llamacpp", "/v1/messages", "t"); err == nil {
		t.Error("extra_params that is not an object was accepted")
	}
}

// TestCapabilities_Gaps: the generation paths each engine serves without
// per-tenant isolation, for its version; the router warns about and counts
// LLM agents with a gap. SGLang chat is isolated only from v0.5.11 and
// llama.cpp /v1/messages only from build b11795: an older or unknown version
// is a gap there. vLLM needs no version.
func TestCapabilities_Gaps(t *testing.T) {
	caps := cachesalt.DefaultCapabilities()
	both := []string{"/v1/chat/completions", "/v1/messages"}
	for _, tc := range []struct {
		engine, version string
		want            []string
	}{
		// vLLM: chat salted from 0.9.0, /v1/messages from 0.27.0. An agent
		// that reports no version counts as isolated on chat, not on
		// /v1/messages, where 0.25 and 0.26 are still in use.
		{"vllm", "", []string{"/v1/messages"}},
		{"vllm", "0.31.0", nil},
		{"vllm", "0.27.0", nil},
		{"vllm", "0.26.0", []string{"/v1/messages"}},
		{"vllm", "0.11.1", []string{"/v1/messages"}},
		{"vllm", "0.9.0", []string{"/v1/messages"}},
		{"vllm", "0.8.5", both},
		{"vllm", "garbage", both},
		{"sglang", "0.5.11", []string{"/v1/messages"}},
		{"sglang", "0.5.12rc1", []string{"/v1/messages"}},
		{"sglang", "0.6.0", []string{"/v1/messages"}},
		{"sglang", "0.5.10", both},
		{"sglang", "", both},
		{"sglang", "garbage", both},
		{"llamacpp", "b11795-261700b", nil},
		{"llamacpp", "b12277-0a1ef30", nil},
		{"llamacpp", "b11794-0a1ef30", []string{"/v1/messages"}},
		{"llamacpp", "", []string{"/v1/messages"}},
		{"ollama", "0.12.0", both},
		{"custom", "", both},
	} {
		if got := caps.Gaps(tc.engine, tc.version); strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("Gaps(%q, %q) = %v, want %v", tc.engine, tc.version, got, tc.want)
		}
	}
}

// TestAtLeast: engine versions compare as their leading dot-separated
// numbers, after an optional v or b prefix.
func TestAtLeast(t *testing.T) {
	for _, tc := range []struct {
		version, since string
		want           bool
	}{
		{"0.5.11", "0.5.11", true},
		{"v0.5.11", "0.5.11", true},
		{"0.5.11.post1", "0.5.11", true},
		{"0.5.10", "0.5.11", false},
		{"0.10.0", "0.5.11", true},
		{"1", "0.5.11", true},
		{"0.5", "0.5.11", false},
		{"b11795-abc", "b11795", true},
		{"b9999", "b11795", false},
		{"", "0.5.11", false},
		{"unknown", "0.5.11", false},
		{"", "", true},
		{"anything", "", true},
		{"99999999999.0", "0.5.11", false}, // too long to be a version
	} {
		if got := cachesalt.AtLeast(tc.version, tc.since); got != tc.want {
			t.Errorf("AtLeast(%q, %q) = %v, want %v", tc.version, tc.since, got, tc.want)
		}
	}
}

// TestNew_RejectsShortSecret: a secret under 32 bytes is refused.
func TestNew_RejectsShortSecret(t *testing.T) {
	if _, err := cachesalt.New(secret[:31], nil); err == nil {
		t.Error("31-byte secret accepted")
	}
}

// TestCheckBody: on /v1/messages, where llama.cpp needs cache_prompt inside
// extra_params, a body whose extra_params is not an object (or null) is
// refused for every engine, so the answer does not depend on which agent is
// picked. Other paths, and absent, null or object extra_params, pass.
func TestCheckBody(t *testing.T) {
	for _, tc := range []struct {
		path, body string
		ok         bool
	}{
		{"/v1/messages", `{"model":"m"}`, true},
		{"/v1/messages", `{"model":"m","extra_params":null}`, true},
		{"/v1/messages", `{"model":"m","extra_params":{"seed":1}}`, true},
		{"/v1/messages", `{"model":"m","extra_params":"x"}`, false},
		{"/v1/messages", `{"model":"m","extra_params":[1]}`, false},
		{"/v1/messages", `{"model":"m","extra_params":1}`, false},
		{"/v1/chat/completions", `{"model":"m","extra_params":"x"}`, true},
	} {
		err := cachesalt.CheckBody(tc.path, []byte(tc.body))
		if (err == nil) != tc.ok {
			t.Errorf("CheckBody(%s, %s) = %v, want ok %v", tc.path, tc.body, err, tc.ok)
		}
		if err != nil && !errors.Is(err, cachesalt.ErrExtraParams) {
			t.Errorf("CheckBody(%s, %s) = %v, want ErrExtraParams", tc.path, tc.body, err)
		}
	}
}

// A sub-key is stable, differs per label and per secret, and is neither a
// salt nor the fingerprint.
func TestSubKey(t *testing.T) {
	s := newSalter(t)
	a := s.SubKey("prefix-anchor")
	if len(a) != 32 || !bytes.Equal(a, newSalter(t).SubKey("prefix-anchor")) {
		t.Fatalf("sub-key %x: want 32 stable bytes", a)
	}
	if bytes.Equal(a, s.SubKey("other")) {
		t.Error("two labels gave the same sub-key")
	}
	other, err := cachesalt.New([]byte("fedcba9876543210fedcba9876543210"), cachesalt.DefaultCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, other.SubKey("prefix-anchor")) {
		t.Error("two secrets gave the same sub-key")
	}
	if base64.RawURLEncoding.EncodeToString(a) == s.Salt("prefix-anchor") {
		t.Error("the sub-key is the salt of a tenant named like its label")
	}
	if hex.EncodeToString(a[:8]) == s.Fingerprint() {
		t.Error("the sub-key starts with the fingerprint")
	}
}
