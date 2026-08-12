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
