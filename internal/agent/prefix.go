// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixhash"
)

// Prefix-aware routing, agent side. The router sends a request's anchors
// (keyed hashes of its message prefixes) in domain.HeaderPrefixAnchors. When
// the engine can return the prompt's tokens (vLLM: return_token_ids), the
// agent asks for them, strips them from the response, hashes the prompt into
// keyed blocks and tells every router which blocks each anchor maps to
// (/prefix-state). Token IDs never leave the agent: they are reversible to
// text with the model's public tokenizer, block hashes are not.

// promptTokenEngine is implemented by engines that can return a prompt's
// token IDs in the response.
type promptTokenEngine interface {
	// MessageStartToken returns the token that opens every message in the
	// model's chat template, or an error when the template has none.
	MessageStartToken(ctx context.Context, baseURL string, httpClient *http.Client, model string) (int, error)
}

// prefixState is the agent's prefix-aware routing state.
type prefixState struct {
	keys       prefixhash.Keys
	startToken int // -1: unknown, only the whole prompt is reported

	mu      sync.Mutex
	nextSeq uint64
	outbox  []outboxEntry // oldest first, at most prefixOutboxCap
}

type outboxEntry struct {
	seq   uint64
	entry prefixServedEntry
}

// prefixServedEntry mirrors the router's /prefix-state entry.
type prefixServedEntry struct {
	Anchors []uint64 `json:"anchors"`
	Ends    []int    `json:"ends"`
	Blocks  []uint64 `json:"blocks"`
}

const (
	prefixOutboxCap   = 4096
	prefixBatchMax    = 1024
	prefixPushEvery   = 100 * time.Millisecond
	prefixProbeTimout = 10 * time.Second
)

// initPrefix enables prefix reporting when the engine can return prompt
// tokens, and learns the model's message-start token.
func (a *Agent) initPrefix() {
	pe, ok := a.engine.(promptTokenEngine)
	if !ok || len(a.cfg.JWTSecret) == 0 {
		return
	}
	st := &prefixState{keys: prefixhash.DeriveKeys([]byte(a.cfg.JWTSecret)), startToken: -1}
	ctx, cancel := context.WithTimeout(context.Background(), prefixProbeTimout)
	defer cancel()
	if t, err := pe.MessageStartToken(ctx, a.cfg.BackendURL, a.httpClient, a.cfg.Model); err == nil {
		st.startToken = t
	} else {
		log.Infof("prefix-aware routing: no message-start token found (%v); only whole prompts are reported", err)
	}
	a.prefix = st
}

// add queues one served prompt for every router.
func (s *prefixState) add(e prefixServedEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	s.outbox = append(s.outbox, outboxEntry{seq: s.nextSeq, entry: e})
	if len(s.outbox) > prefixOutboxCap {
		s.outbox = append(s.outbox[:0], s.outbox[len(s.outbox)-prefixOutboxCap:]...)
	}
}

// since returns up to prefixBatchMax entries after seq, and the seq of the last.
func (s *prefixState) since(seq uint64) ([]prefixServedEntry, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []prefixServedEntry
	last := seq
	for _, o := range s.outbox {
		if o.seq <= seq {
			continue
		}
		out = append(out, o.entry)
		last = o.seq
		if len(out) == prefixBatchMax {
			break
		}
	}
	return out, last
}

// runPrefixStatePusher sends what this agent served to one router every
// prefixPushEvery, until done or stop. A failed batch is sent again on the
// next tick; entries that fall out of the bounded outbox meanwhile are lost
// (they are hints).
func (a *Agent) runPrefixStatePusher(rs *routerSession, done <-chan struct{}) {
	if a.prefix == nil {
		return
	}
	t := time.NewTicker(prefixPushEvery)
	defer t.Stop()
	var cursor uint64
	for {
		select {
		case <-t.C:
			batch, last := a.prefix.since(cursor)
			if len(batch) == 0 {
				continue
			}
			if err := a.pushPrefixState(rs, batch); err != nil {
				log.Debugf("prefix-state push: %v", err)
				continue
			}
			cursor = last
		case <-done:
			return
		case <-a.stopCh:
			return
		}
	}
}

func (a *Agent) pushPrefixState(rs *routerSession, batch []prefixServedEntry) error {
	body, err := json.Marshal(struct {
		SessionToken string              `json:"session_token"`
		Served       []prefixServedEntry `json:"served"`
	}{rs.sessionToken(), batch})
	if err != nil {
		return err
	}
	resp, err := rs.client.Post("http://router/prefix-state", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// prefixRequest is the prefix work of one chat request.
type prefixRequest struct {
	anchors   []uint64
	messages  int    // logical messages of the request
	genPrompt bool   // the template appends a generation prompt
	scope     string // the request's cache salt, if any
	strip     bool   // the agent asked for the tokens: remove them from the response

	once sync.Once
}

// requestFlags are the request fields the agent reads for prefix reporting.
type requestFlags struct {
	ReturnTokenIDs       *bool  `json:"return_token_ids"`
	AddGenerationPrompt  *bool  `json:"add_generation_prompt"`
	ContinueFinalMessage *bool  `json:"continue_final_message"`
	CacheSalt            string `json:"cache_salt"`
}

// preparePrefix decides whether to report this request's prompt and, if the
// client did not ask for token IDs itself, adds return_token_ids to the body.
// It returns the body to forward and the request's prefix work (nil: none).
func (a *Agent) preparePrefix(header string, req *domain.ChatRequest) ([]byte, *prefixRequest) {
	if a.prefix == nil || header == "" {
		return req.RawBytes, nil
	}
	anchors, err := prefixhash.DecodeAnchors(header)
	if err != nil || len(anchors) == 0 {
		return req.RawBytes, nil
	}
	n := len(req.Messages)
	if domain.SystemText(req) != "" {
		n++
	}
	if n != len(anchors) {
		return req.RawBytes, nil // truncated anchors: nothing reliable to report
	}
	var f requestFlags
	_ = json.Unmarshal(req.RawBytes, &f)
	pr := &prefixRequest{
		anchors:   anchors,
		messages:  n,
		genPrompt: (f.AddGenerationPrompt == nil || *f.AddGenerationPrompt) && (f.ContinueFinalMessage == nil || !*f.ContinueFinalMessage),
		scope:     f.CacheSalt,
	}
	if f.ReturnTokenIDs != nil {
		if !*f.ReturnTokenIDs {
			return req.RawBytes, nil // the client asked not to get them: respect it
		}
		return req.RawBytes, pr // the client wants them: read, do not strip
	}
	body := bytes.TrimLeft(req.RawBytes, " \t\r\n")
	if len(body) == 0 || body[0] != '{' {
		return req.RawBytes, nil
	}
	out := make([]byte, 0, len(body)+24)
	out = append(out, `{"return_token_ids":true`...)
	if rest := bytes.TrimLeft(body[1:], " \t\r\n"); len(rest) > 0 && rest[0] != '}' {
		out = append(out, ',')
	}
	out = append(out, body[1:]...)
	pr.strip = true
	return out, pr
}

// learn hashes a prompt's tokens and queues what it maps to. Once per request.
func (a *Agent) learn(pr *prefixRequest, tokens []int) {
	pr.once.Do(func() {
		blocks := a.prefix.keys.Blocks(a.cfg.Model, pr.scope, tokens)
		if len(blocks) == 0 {
			return
		}
		var ends []int
		if a.prefix.startToken >= 0 {
			ends = prefixhash.MessageEnds(tokens, a.prefix.startToken, pr.messages, pr.genPrompt)
		}
		if ends == nil {
			// Only the whole prompt is known: the next turn of the conversation
			// starts with these tokens.
			ends = make([]int, pr.messages)
			ends[pr.messages-1] = len(blocks)
		}
		a.prefix.add(prefixServedEntry{Anchors: pr.anchors, Ends: ends, Blocks: blocks})
	})
}

// handleTokenFields reads the prompt's token IDs from one response body or
// SSE data line (once per request) and, when the agent asked for them,
// removes every token-ID field from it.
func (a *Agent) handleTokenFields(pr *prefixRequest, b []byte) []byte {
	if v, ok := tokenField(b, `"prompt_token_ids"`); ok {
		var toks []int
		if json.Unmarshal(v, &toks) == nil && len(toks) > 0 {
			a.learn(pr, toks)
		}
	}
	if !pr.strip {
		return b
	}
	b = removeField(b, `"prompt_token_ids"`)
	for {
		nb := removeField(b, `"token_ids"`)
		if len(nb) == len(b) {
			return nb
		}
		b = nb
	}
}

// tokenField returns the raw value of the first JSON object member named key
// (quoted) in b. In JSON a quote inside a string is escaped, so an unescaped
// `"key":` is always a member name.
func tokenField(b []byte, key string) ([]byte, bool) {
	i := memberIndex(b, key)
	if i < 0 {
		return nil, false
	}
	start, end := valueBounds(b, i+len(key))
	if start < 0 {
		return nil, false
	}
	return b[start:end], true
}

// removeField removes the first member named key (quoted) and its value from
// b, with its separating comma. b is returned unchanged when key is absent.
func removeField(b []byte, key string) []byte {
	i := memberIndex(b, key)
	if i < 0 {
		return b
	}
	_, end := valueBounds(b, i+len(key))
	if end < 0 {
		return b
	}
	// Drop the comma before the member, or else the one after it.
	from, to := i, end
	j := from - 1
	for j >= 0 && isSpace(b[j]) {
		j--
	}
	if j >= 0 && b[j] == ',' {
		from = j
	} else {
		k := to
		for k < len(b) && isSpace(b[k]) {
			k++
		}
		if k < len(b) && b[k] == ',' {
			to = k + 1
		}
	}
	out := make([]byte, 0, len(b)-(to-from))
	out = append(out, b[:from]...)
	return append(out, b[to:]...)
}

// memberIndex finds key (quoted) used as an object member name in b.
func memberIndex(b []byte, key string) int {
	off := 0
	for {
		i := bytes.Index(b[off:], []byte(key))
		if i < 0 {
			return -1
		}
		i += off
		j := i + len(key)
		for j < len(b) && isSpace(b[j]) {
			j++
		}
		if (i == 0 || b[i-1] != '\\') && j < len(b) && b[j] == ':' {
			return i
		}
		off = i + 1
	}
}

// valueBounds returns where the value after the member name ending at i
// starts and ends: an array of numbers, a number or null (what token-ID
// fields hold). -1 when it is something else.
func valueBounds(b []byte, i int) (int, int) {
	for i < len(b) && (isSpace(b[i]) || b[i] == ':') {
		i++
	}
	if i >= len(b) {
		return -1, -1
	}
	switch {
	case b[i] == '[':
		j := bytes.IndexByte(b[i:], ']')
		if j < 0 || bytes.ContainsAny(b[i+1:i+j], "[{\"") {
			return -1, -1
		}
		return i, i + j + 1
	case bytes.HasPrefix(b[i:], []byte("null")):
		return i, i + 4
	}
	return -1, -1
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// tokenFieldWriter filters an SSE stream line by line through
// Agent.handleTokenFields before it reaches dst.
type tokenFieldWriter struct {
	a   *Agent
	pr  *prefixRequest
	dst io.Writer
	buf []byte
}

func (w *tokenFieldWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			return len(p), nil
		}
		line := w.buf[:i+1]
		if bytes.HasPrefix(line, []byte("data:")) {
			line = w.a.handleTokenFields(w.pr, line)
		}
		if _, err := w.dst.Write(line); err != nil {
			return 0, err
		}
		w.buf = append(w.buf[:0], w.buf[i+1:]...)
	}
}

// flush writes what is left of an unterminated last line.
func (w *tokenFieldWriter) flush() {
	if len(w.buf) == 0 {
		return
	}
	line := w.buf
	if bytes.HasPrefix(line, []byte("data:")) {
		line = w.a.handleTokenFields(w.pr, line)
	}
	w.dst.Write(line) //nolint:errcheck
	w.buf = nil
}
