// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"encoding/json"
	"strings"
)

// AffinityFingerprint extracts the three content fields that key agent-level
// prefix affinity (see policy.AffinityContext and pin.TaskKeyInput):
//
//	endUser       — the declared end user (see below)
//	system        — top-level "system" plus system/developer turns, '\n'-joined
//	firstUserText — the first human turn's text (tool-only turns skipped)
//
// body is the raw client request (pending.Request.RawBytes) and path selects the
// dialect exactly like semantic.DialectForPath ("/v1/messages*" = Anthropic,
// anything else OpenAI). The dialect decides which declared-user field wins:
// OpenAI's top-level "user" vs Anthropic's metadata.user_id — the other field is
// still accepted as a fallback, so a proxy that rewrites the shape keeps a stable
// key. Both dialects are read by one lenient parser because every other
// fingerprint field exists in both (Anthropic carries the system prompt
// top-level; OpenAI carries it as system/developer turns).
//
// This is a targeted parse: no marker stripping (that is an alias-path feature),
// no token estimation, no feature extraction. It never fails — a malformed body
// yields empty fields, which pin.TaskKey still hashes deterministically.
// role:"tool" messages are excluded entirely, and a user turn carrying only tool
// results is the harness talking, not a human, so it is skipped.
func AffinityFingerprint(body []byte, path string) (endUser, system, firstUserText string) {
	var raw struct {
		System   json.RawMessage `json:"system"`
		User     json.RawMessage `json:"user"`
		Metadata json.RawMessage `json:"metadata"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if len(body) == 0 || json.Unmarshal(body, &raw) != nil {
		return "", "", ""
	}
	endUser = affinityEndUser(strings.HasPrefix(path, "/v1/messages"), raw.User, raw.Metadata)

	var sys strings.Builder
	appendSystem := func(s string) {
		if s == "" {
			return
		}
		if sys.Len() > 0 {
			sys.WriteByte('\n')
		}
		sys.WriteString(s)
	}
	appendSystem(affinityText(raw.System))

	for _, m := range raw.Messages {
		text, toolResults := affinityContent(m.Content)
		switch m.Role {
		case "system", "developer":
			appendSystem(text)
		case "tool":
			// OpenAI tool result: neither system nor a human turn.
		case "user":
			// A turn that only carries tool results back to the model is the
			// harness talking, not the human (mirrors Message.IsUserTurn, minus
			// the marker stripping the alias path does). The first human turn
			// wins; later system/developer turns still fold into the system text.
			if firstUserText == "" && (toolResults == 0 || text != "") {
				firstUserText = text
			}
		default: // assistant, or anything a dialect adds later
		}
	}
	return endUser, sys.String(), firstUserText
}

// affinityEndUser decodes the declared end user leniently: an odd shape must
// not fail — it only refines a hash key, so it degrades to "". anthropic picks
// metadata.user_id first, OpenAI picks top-level "user" first; whichever the
// dialect does not define is still tried as a fallback.
func affinityEndUser(anthropic bool, user, metadata json.RawMessage) string {
	var u string
	if len(user) > 0 && json.Unmarshal(user, &u) == nil && u != "" {
		// fall through to metadata only when the dialect prefers it
		if !anthropic {
			return u
		}
	}
	var mdUserID string
	if len(metadata) > 0 {
		var md struct {
			UserID string `json:"user_id"`
		}
		if json.Unmarshal(metadata, &md) == nil {
			mdUserID = md.UserID
		}
	}
	if anthropic {
		if mdUserID != "" {
			return mdUserID
		}
		return u
	}
	if u != "" {
		return u
	}
	return mdUserID
}

// affinityText renders a raw content value (string or typed parts) as text:
// top-level system prompts and system/developer turns use it. Malformed content
// yields "".
func affinityText(c json.RawMessage) string {
	text, _ := affinityContent(c)
	return text
}

// affinityContent joins the text parts of a content value with '\n' and counts
// tool_result parts, so a user turn can be recognised as tool-only. A plain
// string counts as text with no tool results; anything malformed is empty.
func affinityContent(c json.RawMessage) (text string, toolResults int) {
	if len(c) == 0 || string(c) == "null" {
		return "", 0
	}
	if c[0] == '"' {
		_ = json.Unmarshal(c, &text)
		return text, 0
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(c, &parts) != nil {
		return "", 0
	}
	var b strings.Builder
	for _, p := range parts {
		switch p.Type {
		case "tool_result":
			toolResults++
		case "text", "input_text":
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(p.Text)
		}
	}
	return b.String(), toolResults
}
