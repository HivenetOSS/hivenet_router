// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package prefixkey derives the routing key of a request from the start of its
// prompt. Requests that start the same way get the same key, so a routing
// strategy can send them to the replica that already holds that prefix in its
// KV cache.
//
// The prompt and the key are never logged.
package prefixkey

import (
	"crypto/sha256"

	"hivenet_router/internal/domain"
)

// Field and record separators keep "ab"+"c" and "a"+"bc" apart.
const (
	fieldSep  = 0x1f
	recordSep = 0x1e
)

// Prefix returns the bytes that identify the start of the prompt of req,
// truncated to maxBytes, or nil when there is nothing to hash.
//
// The prefix is, in order: the top-level system prompt (Anthropic
// /v1/messages), the raw tool definitions, then every message up to and
// including the first user message (system and developer instructions in the
// OpenAI dialect). All turns of a conversation repeat these, so they share the
// prefix; two conversations of one application differ at the first user
// message. Only text counts: images and audio are ignored.
func Prefix(req *domain.ChatRequest, maxBytes int) []byte {
	if req == nil || maxBytes <= 0 {
		return nil
	}
	b := prefixBuilder{max: maxBytes}
	if s := domain.SystemText(req); s != "" {
		b.add("system", s)
	}
	if len(req.Tools) > 0 && string(req.Tools) != "null" {
		b.add("tools", string(req.Tools))
	}
	for _, m := range req.Messages {
		if b.full() {
			break
		}
		if text := domain.GetMessageTextContent(m); text != "" {
			b.add(m.Role, text)
		}
		if m.Role == "user" {
			break
		}
	}
	if b.n == 0 {
		return nil
	}
	return b.buf
}

// Key hashes scope and prefix into the routing key: sha256(scope ‖ 0x00 ‖
// prefix). scope separates the keys of different tenants (empty when the
// engine cache is shared). It returns nil when prefix is empty.
func Key(scope, prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	h := sha256.New()
	h.Write(scope)
	h.Write([]byte{0})
	h.Write(prefix)
	return h.Sum(nil)
}

// prefixBuilder appends role/text records up to max bytes.
type prefixBuilder struct {
	buf []byte
	max int
	n   int // records with text
}

func (b *prefixBuilder) full() bool { return len(b.buf) >= b.max }

func (b *prefixBuilder) add(role, text string) {
	if b.full() {
		return
	}
	b.n++
	b.put(role)
	b.put(string(rune(fieldSep)))
	b.put(text)
	b.put(string(rune(recordSep)))
}

// put appends as much of s as fits; a long prompt is never copied whole.
func (b *prefixBuilder) put(s string) {
	if room := b.max - len(b.buf); len(s) > room {
		s = s[:room]
	}
	b.buf = append(b.buf, s...)
}
