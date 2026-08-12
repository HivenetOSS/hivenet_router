// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"encoding/json"
	"testing"

	"hivenet_router/internal/domain"
)

// TestPromptTextBytes_IncludesAnthropicSystem verifies the prompt-byte count
// covers the top-level Anthropic system prompt — previously invisible to the
// estimate because it is not part of Messages — alongside message text, while a
// non-text (image) block contributes no estimable bytes.
func TestPromptTextBytes_IncludesAnthropicSystem(t *testing.T) {
	system := "You are a precise coding assistant. Follow the style guide exactly."
	body := `{
		"model": "gemma",
		"system": ` + jsonString(system) + `,
		"messages": [
			{"role": "user", "content": [
				{"type": "text", "text": "refactor this"},
				{"type": "image_url", "image_url": {"url": "http://x/y.png"}}
			]}
		]
	}`
	var req domain.ChatRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	bytes, msgs := domain.PromptTextBytes(&req)
	wantAtLeast := len(system) + len("refactor this")
	if bytes < wantAtLeast {
		t.Errorf("prompt bytes = %d, want >= %d (system + message text)", bytes, wantAtLeast)
	}
	if msgs != 1 {
		t.Errorf("message count = %d, want 1", msgs)
	}

	// Dropping the system prompt must lower the count by exactly the system bytes,
	// proving the system text is what's included (and the image adds nothing).
	req.System = nil
	noSystem, _ := domain.PromptTextBytes(&req)
	if bytes-noSystem != len(system) {
		t.Errorf("system contributed %d bytes, want %d", bytes-noSystem, len(system))
	}
}

// TestSystemText_StringOrBlocks verifies SystemText decodes both the plain-string
// and the array-of-text-blocks forms of the Anthropic system field.
func TestSystemText_StringOrBlocks(t *testing.T) {
	var strReq domain.ChatRequest
	_ = json.Unmarshal([]byte(`{"system":"hello system"}`), &strReq)
	if got := domain.SystemText(&strReq); got != "hello system" {
		t.Errorf("string system = %q, want %q", got, "hello system")
	}

	var blkReq domain.ChatRequest
	_ = json.Unmarshal([]byte(`{"system":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}`), &blkReq)
	if got := domain.SystemText(&blkReq); got != "ab" {
		t.Errorf("block system = %q, want %q", got, "ab")
	}

	var noneReq domain.ChatRequest
	if got := domain.SystemText(&noneReq); got != "" {
		t.Errorf("absent system = %q, want empty", got)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// TestPromptTextBytes_IncludesTools verifies the prompt-byte count covers the
// raw tool-definition JSON: the backend renders tool schemas into the prompt
// and tokenizes them, so leaving them out would both under-estimate agentic
// requests at admission and inflate the learned tokens-per-byte ratio when the
// exact prompt_tokens (which include the schemas) are divided by the bytes.
func TestPromptTextBytes_IncludesTools(t *testing.T) {
	tools := `[{"type":"function","function":{"name":"read_file","description":"Read a file from disk","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}}]`
	body := `{
		"model": "gemma",
		"messages": [{"role": "user", "content": "hi"}],
		"tools": ` + tools + `
	}`
	var req domain.ChatRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	withTools, _ := domain.PromptTextBytes(&req)

	req.Tools = nil
	withoutTools, _ := domain.PromptTextBytes(&req)

	if withTools <= withoutTools {
		t.Errorf("tool definitions must contribute bytes: with=%d without=%d", withTools, withoutTools)
	}
	if got := withTools - withoutTools; got < len(tools)-10 {
		t.Errorf("tools contributed %d bytes, want ~%d", got, len(tools))
	}
}

// TestCountImages_AnthropicImageBlocks verifies the image cap sees the
// Anthropic /v1/messages image shape ({"type":"image","source":...}) as well as
// the OpenAI image_url shape — otherwise an Anthropic-dialect request could
// carry unlimited images past images_max.
func TestCountImages_AnthropicImageBlocks(t *testing.T) {
	body := `{
		"model": "gemma",
		"messages": [
			{"role": "user", "content": [
				{"type": "text", "text": "what is in these?"},
				{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}},
				{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}},
				{"type": "image_url", "image_url": {"url": "http://x/y.png"}}
			]}
		]
	}`
	var req domain.ChatRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := domain.CountImages(req.Messages); got != 3 {
		t.Errorf("CountImages = %d, want 3 (2 Anthropic blocks + 1 OpenAI part)", got)
	}
}
