// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package prefixhash computes the keyed fingerprints that prefix-aware routing
// works with, so that neither the router nor the agent ever stores prompt text
// or token IDs:
//
//   - Anchors identify the text of a request's message prefixes (messages[0..k]).
//     Only routers compute them.
//   - Block hashes identify fixed-size blocks of a prompt's tokens, chained so a
//     block's hash covers every token before it. Only agents compute them, from
//     the tokens the engine returns.
//
// Both are HMAC-SHA256 truncated to 64 bits under a key derived from a site
// secret: without the key nobody can check whether a guessed text was sent.
package prefixhash

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash"

	"hivenet_router/internal/domain"
)

// BlockTokens is the number of tokens in a block. Routers and agents must
// agree on it, so it is not configurable.
const BlockTokens = 64

// MaxAnchors caps the anchors of one request (and so the header that carries
// them): a conversation longer than this is matched on its first MaxAnchors
// messages.
const MaxAnchors = 256

// MaxBlocks caps the blocks of one prompt (256k tokens).
const MaxBlocks = 4096

// Keys holds the two keys derived from a site secret.
type Keys struct {
	anchor []byte
	block  []byte
}

// DeriveKeys derives the anchor and block keys from secret. Every router and
// agent of a site must use the same secret.
func DeriveKeys(secret []byte) Keys {
	return Keys{anchor: derive(secret, "hivenet-prefix-anchor/v1"), block: derive(secret, "hivenet-prefix-block/v1")}
}

func derive(secret []byte, label string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(label))
	return m.Sum(nil)
}

// Valid reports whether k was derived from a secret.
func (k Keys) Valid() bool { return len(k.anchor) > 0 && len(k.block) > 0 }

// Anchors returns one anchor per logical message of req: the top-level system
// prompt of /v1/messages first when present, then each message. anchor[k]
// covers model, scope, the tool definitions and messages[0..k]; equal anchors
// mean equal text so far. sizes[k] estimates the tokens of message k (bytes/4).
// scope separates tenants when the engine's cache does (cache salt); it is
// empty otherwise.
func (k Keys) Anchors(model, scope string, req *domain.ChatRequest) (anchors []uint64, sizes []int) {
	if req == nil || len(k.anchor) == 0 {
		return nil, nil
	}
	m := hmac.New(sha256.New, k.anchor)
	m.Write([]byte("anchor/v1\x00"))
	writeField(m, model)
	writeField(m, scope)
	if len(req.Tools) > 0 && string(req.Tools) != "null" {
		writeField(m, string(req.Tools))
	} else {
		writeField(m, "")
	}
	prev := m.Sum(nil)

	add := func(role, text string) {
		m.Reset()
		m.Write(prev)
		writeField(m, role)
		writeField(m, text)
		prev = m.Sum(prev[:0])
		anchors = append(anchors, binary.LittleEndian.Uint64(prev))
		sizes = append(sizes, len(text)/4+4)
	}
	if s := domain.SystemText(req); s != "" {
		add("system", s)
	}
	for _, msg := range req.Messages {
		if len(anchors) == MaxAnchors {
			break
		}
		add(msg.Role, domain.GetMessageTextContent(msg))
	}
	return anchors, sizes
}

// writeField writes s with its length, so fields cannot shift into each other.
func writeField(h hash.Hash, s string) {
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(len(s)))
	h.Write(n[:])
	h.Write([]byte(s))
}

// Blocks returns the chained hashes of the full BlockTokens-token blocks of
// tokens (a trailing partial block is not hashed). model and scope seed the
// chain: the same tokens under another model or cache salt give other hashes.
func (k Keys) Blocks(model, scope string, tokens []int) []uint64 {
	if len(k.block) == 0 {
		return nil
	}
	n := min(len(tokens)/BlockTokens, MaxBlocks)
	out := make([]uint64, 0, n)
	m := hmac.New(sha256.New, k.block)
	m.Write([]byte("block/v1\x00"))
	writeField(m, model)
	writeField(m, scope)
	prev := m.Sum(nil)
	buf := make([]byte, 4*BlockTokens)
	for b := range n {
		for i, t := range tokens[b*BlockTokens : (b+1)*BlockTokens] {
			binary.LittleEndian.PutUint32(buf[4*i:], uint32(t))
		}
		m.Reset()
		m.Write(prev)
		m.Write(buf)
		prev = m.Sum(prev[:0])
		out = append(out, binary.LittleEndian.Uint64(prev))
	}
	return out
}

// MessageEnds returns, for each of the n logical messages of a prompt, how
// many full blocks of tokens end before the next message starts, using the
// positions of startToken (the token that opens every message in the model's
// chat template, e.g. <|im_start|>). The last n starts belong to the n
// messages (a default system message the template adds comes before them);
// when genPrompt is set, the last start opens the generation prompt and the
// messages are the n starts before it. It returns nil when the tokens do not
// hold that many starts.
func MessageEnds(tokens []int, startToken, n int, genPrompt bool) []int {
	if n <= 0 || startToken < 0 {
		return nil
	}
	var starts []int
	for i, t := range tokens {
		if t == startToken {
			starts = append(starts, i)
		}
	}
	gen := len(tokens)
	if genPrompt {
		if len(starts) == 0 {
			return nil
		}
		gen = starts[len(starts)-1]
		starts = starts[:len(starts)-1]
	}
	if len(starts) < n {
		return nil
	}
	starts = starts[len(starts)-n:]
	ends := make([]int, n)
	for k := range n {
		end := gen
		if k+1 < n {
			end = starts[k+1]
		}
		ends[k] = min(end/BlockTokens, MaxBlocks)
	}
	return ends
}

// EncodeAnchors encodes anchors for a header value.
func EncodeAnchors(anchors []uint64) string {
	buf := make([]byte, 8*len(anchors))
	for i, a := range anchors {
		binary.LittleEndian.PutUint64(buf[8*i:], a)
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

// DecodeAnchors decodes a header value written by EncodeAnchors.
func DecodeAnchors(s string) ([]uint64, error) {
	buf, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(buf)%8 != 0 || len(buf)/8 > MaxAnchors {
		return nil, errors.New("prefixhash: bad anchor header")
	}
	out := make([]uint64, len(buf)/8)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(buf[8*i:])
	}
	return out, nil
}
