// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package cachesalt_test

import (
	"bytes"
	"encoding/json"
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
// the body carries exactly the tenant's salt (a client salt is replaced);
// everywhere else any client salt is removed and none is added. The rest of
// the body is unchanged either way.
func TestApply_EngineCapabilities(t *testing.T) {
	s := newSalter(t)
	body := []byte(`{"model":"m","cache_salt":"guess-of-another-tenant","messages":[{"role":"user","content":"hi"}]}`)
	orig := bytes.Clone(body)
	for _, tc := range []struct {
		engine, path string
		salted       bool
	}{
		{"vllm", "/v1/chat/completions", true},
		{"vllm", "/v1/messages", true},
		{"vllm", "/v1/embeddings", true},
		{"vllm", "/v1/rerank", true},
		{"vllm", "/v1/messages/count_tokens", false}, // no prefill, no cache
		{"sglang", "/v1/chat/completions", false},
		{"llamacpp", "/v1/chat/completions", false},
		{"ollama", "/v1/chat/completions", false},
		{"infinity", "/v1/embeddings", false},
		{"custom", "/v1/chat/completions", false},
		{"", "/v1/chat/completions", false},
	} {
		got, err := s.Apply(body, tc.engine, tc.path, "tenant-a")
		if err != nil {
			t.Fatalf("%s %s: %v", tc.engine, tc.path, err)
		}
		var m map[string]any
		if err := json.Unmarshal(got, &m); err != nil {
			t.Fatalf("%s %s: result is not JSON: %v", tc.engine, tc.path, err)
		}
		salt, has := m["cache_salt"]
		switch {
		case tc.salted && salt != s.Salt("tenant-a"):
			t.Errorf("%s %s: cache_salt %v, want the tenant's salt", tc.engine, tc.path, salt)
		case !tc.salted && has:
			t.Errorf("%s %s: cache_salt %v present, want none", tc.engine, tc.path, salt)
		}
		if bytes.Count(got, []byte(`"cache_salt"`)) > 1 {
			t.Errorf("%s %s: more than one cache_salt in %s", tc.engine, tc.path, got)
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
}

// TestNew_RejectsShortSecret: a secret under 32 bytes is refused.
func TestNew_RejectsShortSecret(t *testing.T) {
	if _, err := cachesalt.New(secret[:31], nil); err == nil {
		t.Error("31-byte secret accepted")
	}
}
