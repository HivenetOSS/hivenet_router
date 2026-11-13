// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package prefixkey_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixkey"
)

func parse(t testing.TB, body string) *domain.ChatRequest {
	t.Helper()
	var req domain.ChatRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("parse %s: %v", body, err)
	}
	return &req
}

func key(t testing.TB, body string) []byte {
	t.Helper()
	return prefixkey.Key(nil, prefixkey.Prefix(parse(t, body), 4096))
}

// The turns of one conversation repeat the system prompt and the first user
// message, so every turn gets the same key.
func TestPrefix_TurnsOfOneConversationShareTheKey(t *testing.T) {
	turn1 := `{"model":"m","messages":[
		{"role":"system","content":"You are a coding assistant."},
		{"role":"user","content":"Write a sort function."}]}`
	turn2 := `{"model":"m","messages":[
		{"role":"system","content":"You are a coding assistant."},
		{"role":"user","content":"Write a sort function."},
		{"role":"assistant","content":"def sort(xs): ..."},
		{"role":"user","content":"Now in Go."}]}`
	if !bytes.Equal(key(t, turn1), key(t, turn2)) {
		t.Error("two turns of one conversation got different keys")
	}
}

// Two conversations of one application differ at the first user message.
func TestPrefix_ConversationsDiffer(t *testing.T) {
	a := `{"messages":[{"role":"system","content":"S"},{"role":"user","content":"question A"}]}`
	b := `{"messages":[{"role":"system","content":"S"},{"role":"user","content":"question B"}]}`
	if bytes.Equal(key(t, a), key(t, b)) {
		t.Error("two conversations got the same key")
	}
}

// The Anthropic top-level system prompt counts, as a string or as text
// blocks, and both spellings give the same key.
func TestPrefix_AnthropicSystem(t *testing.T) {
	str := `{"system":"Be brief.","messages":[{"role":"user","content":"hi"}]}`
	blocks := `{"system":[{"type":"text","text":"Be brief."}],
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`
	noSystem := `{"messages":[{"role":"user","content":"hi"}]}`
	if !bytes.Equal(key(t, str), key(t, blocks)) {
		t.Error("string and block system prompts got different keys")
	}
	if bytes.Equal(key(t, str), key(t, noSystem)) {
		t.Error("the system prompt does not change the key")
	}
}

// Tool definitions are part of the prompt; images are not hashed.
func TestPrefix_ToolsAndImages(t *testing.T) {
	withTools := func(tools string) string {
		return `{"tools":` + tools + `,"messages":[{"role":"user","content":"hi"}]}`
	}
	if bytes.Equal(key(t, withTools(`[{"name":"a"}]`)), key(t, withTools(`[{"name":"b"}]`))) {
		t.Error("different tools got the same key")
	}
	img := func(url string) string {
		return `{"messages":[{"role":"user","content":[{"type":"text","text":"describe"},` +
			`{"type":"image_url","image_url":{"url":"` + url + `"}}]}]}`
	}
	if !bytes.Equal(key(t, img("https://a/1.png")), key(t, img("https://a/2.png"))) {
		t.Error("the image changed the key: only text should count")
	}
}

// No text to hash → no key.
func TestPrefix_NothingToHash(t *testing.T) {
	for _, body := range []string{
		`{"model":"m"}`,
		`{"messages":[]}`,
		`{"messages":[{"role":"user","content":""}]}`,
		`{"tools":null,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`,
	} {
		if k := key(t, body); k != nil {
			t.Errorf("%s: key %x, want nil", body, k)
		}
	}
	if prefixkey.Prefix(nil, 4096) != nil {
		t.Error("nil request: want nil prefix")
	}
}

// The prefix stops at maxBytes: prompts that differ only after it share the
// key, and a long prompt is never copied whole.
func TestPrefix_TruncatedToMaxBytes(t *testing.T) {
	long := strings.Repeat("x", 10_000)
	a := parse(t, `{"messages":[{"role":"system","content":"`+long+`"},{"role":"user","content":"A"}]}`)
	b := parse(t, `{"messages":[{"role":"system","content":"`+long+`"},{"role":"user","content":"B"}]}`)
	pa := prefixkey.Prefix(a, 256)
	if len(pa) != 256 {
		t.Errorf("prefix length %d, want 256", len(pa))
	}
	if !bytes.Equal(pa, prefixkey.Prefix(b, 256)) {
		t.Error("prompts equal for the first 256 bytes got different prefixes")
	}
}

// The scope separates tenants; fields cannot be shifted into one another.
func TestKey_ScopeAndSeparators(t *testing.T) {
	p := []byte("prefix")
	if bytes.Equal(prefixkey.Key([]byte("tenant-a"), p), prefixkey.Key([]byte("tenant-b"), p)) {
		t.Error("two scopes got the same key")
	}
	if bytes.Equal(prefixkey.Key(nil, p), prefixkey.Key([]byte("t"), p)) {
		t.Error("a scope does not change the key")
	}
	if bytes.Equal(prefixkey.Key([]byte("ab"), []byte("c")), prefixkey.Key([]byte("a"), []byte("bc"))) {
		t.Error("scope and prefix are not separated")
	}
	if prefixkey.Key([]byte("t"), nil) != nil {
		t.Error("empty prefix: want nil key")
	}
	a := `{"messages":[{"role":"system","content":"ab"},{"role":"user","content":"c"}]}`
	b := `{"messages":[{"role":"system","content":"a"},{"role":"user","content":"bc"}]}`
	if bytes.Equal(key(t, a), key(t, b)) {
		t.Error("messages are not separated")
	}
}

// Any request body: no panic, the prefix never exceeds maxBytes.
func FuzzPrefix(f *testing.F) {
	f.Add([]byte(`{"system":"s","tools":[1],"messages":[{"role":"user","content":"hi"}]}`), 64)
	f.Add([]byte(`{"messages":[{"role":"system","content":[{"type":"text","text":"a"}]}]}`), 1)
	f.Fuzz(func(t *testing.T, body []byte, maxBytes int) {
		var req domain.ChatRequest
		if json.Unmarshal(body, &req) != nil {
			return
		}
		maxBytes %= 1 << 16
		p := prefixkey.Prefix(&req, maxBytes)
		if len(p) > max(maxBytes, 0) {
			t.Fatalf("prefix of %d bytes exceeds maxBytes %d", len(p), maxBytes)
		}
	})
}
