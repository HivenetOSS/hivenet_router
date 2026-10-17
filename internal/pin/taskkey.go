// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package pin

import (
	"crypto/sha256"
	"encoding/hex"
)

// TaskKeyInput holds the raw fields TaskKey hashes. It is deliberately a plain
// struct rather than a request-parsing type: the pin package must not import
// the request views of its callers (semantic, policy), which would create an
// import cycle. Callers extract the fields themselves — semantic from its
// RequestView, the policy executor from its AffinityContext fingerprint.
//
// Exactly one key source is hashed: HeaderValue when non-empty, otherwise the
// fingerprint triple (EndUser, System, FirstUserText). Alias and KeyID are
// always mixed in so a pin never leaks across aliases or API keys.
type TaskKeyInput struct {
	// Alias scopes the key: the alias (semantic path) or the concrete model
	// being routed (agent-affinity path).
	Alias string
	// KeyID is the caller's API key identity.
	KeyID string
	// HeaderValue is the client's explicit task header (X-Hivenet-Task-ID or,
	// for agent affinity, x-litellm-session-id). When set it replaces the
	// fingerprint entirely.
	HeaderValue string
	// EndUser is the declared end user (OpenAI "user", Anthropic
	// metadata.user_id); separates users that share one API key and one system
	// prompt. Empty when the client declared none.
	EndUser string
	// System is the system prompt (top-level system + system/developer turns).
	System string
	// FirstUserText is the first human turn. Two tasks from the same key
	// differ here even with an identical system prompt.
	FirstUserText string
}

// TaskKey returns the pinning key for a request: a hash of the alias and the
// caller's key id plus either the client's task header (when present) or a
// fingerprint of (end user, system prompt, first human turn). Agent harnesses
// keep the fingerprint inputs stable for a whole task, while two tasks from the
// same key differ in their first user message. The alias is part of the key so
// a pin never leaks from one alias (or model) to another. Raw client values
// never reach the pin store; the key is a fixed 34 bytes ("h:"/"f:" + 32 hex).
func TaskKey(in TaskKeyInput) string {
	h := sha256.New()
	field := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	field(in.Alias)
	field(in.KeyID)
	prefix := "f:"
	if in.HeaderValue != "" {
		prefix = "h:"
		field(in.HeaderValue)
	} else {
		field(in.EndUser)
		field(in.System)
		field(in.FirstUserText)
	}
	return prefix + hex.EncodeToString(h.Sum(nil)[:16])
}
