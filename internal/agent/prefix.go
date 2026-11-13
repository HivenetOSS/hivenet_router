// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
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

// responseTokenEngine is implemented by engines that return a prompt's token
// IDs in the response when asked (vLLM: return_token_ids).
type responseTokenEngine interface {
	promptTokensInResponse()
}

// tokenSource is how the agent learns a served prompt's tokens.
type tokenSource int

const (
	// sourceResponse: the engine returns them in the response (vLLM).
	sourceResponse tokenSource = iota
	// sourceBackground: the agent tokenizes the request after forwarding it,
	// off the request path (SGLang, llama.cpp).
	sourceBackground
	// sourceText: no tokens; the agent reports the text anchors it served.
	sourceText
)

// prefixState is the agent's prefix-aware routing state.
type prefixState struct {
	keys       prefixhash.Keys
	source     tokenSource
	startToken int           // -1: unknown, only the whole prompt is reported
	bgSlots    chan struct{} // bounds background tokenizations

	mu      sync.Mutex
	nextSeq uint64
	outbox  []outboxEntry // oldest first, at most prefixOutboxCap
}

type outboxEntry struct {
	seq   uint64
	entry prefixServedEntry
}

// prefixServedEntry mirrors the router's /prefix-state entry. Text entries
// carry anchors only (the engine gave no tokens).
type prefixServedEntry struct {
	Anchors []uint64 `json:"anchors"`
	Ends    []int    `json:"ends,omitempty"`
	Blocks  []uint64 `json:"blocks,omitempty"`
	Text    bool     `json:"text,omitempty"`
}

const (
	prefixOutboxCap   = 4096
	prefixBatchMax    = 1024
	prefixPushEvery   = 100 * time.Millisecond
	prefixProbeTimout = 10 * time.Second
	prefixBgTokenize  = 2 // background tokenizations at once; more are skipped
	prefixBgTimeout   = 30 * time.Second
)

// initPrefix sets up prefix reporting for a chat agent: how it learns the
// prompts' tokens, and the model's message-start token. It also starts the
// KV-event subscriber when an endpoint is configured.
func (a *Agent) initPrefix() {
	if len(a.cfg.JWTSecret) == 0 || !a.servesChat() {
		return
	}
	st := &prefixState{keys: prefixhash.DeriveKeys([]byte(a.cfg.JWTSecret)), startToken: -1, source: sourceText, bgSlots: make(chan struct{}, prefixBgTokenize)}
	_, inResponse := a.engine.(responseTokenEngine)
	_, tokenizer := a.engine.(chatTokenizer)
	switch {
	case inResponse:
		st.source = sourceResponse
	case tokenizer:
		st.source = sourceBackground
	}
	if pr, ok := a.engine.(messageStartProber); ok && st.source != sourceText {
		ctx, cancel := context.WithTimeout(context.Background(), prefixProbeTimout)
		if t, err := pr.MessageStartToken(ctx, a.cfg.BackendURL, a.httpClient, a.cfg.Model); err == nil {
			st.startToken = t
		} else {
			log.Infof("prefix-aware routing: no message-start token found (%v); only whole prompts are reported", err)
		}
		cancel()
	}
	a.prefix = st
	if ep := a.cfg.KVEventsEndpoint; ep != "" && st.source != sourceText {
		warnIfRemote(ep)
		a.kv = newKVMirror(st.keys, a.cfg.Model)
		go a.runKVEvents(ep)
	}
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
	var cursor, kvCursor uint64
	// A new session: the router does not know this agent's cache yet. Every
	// snapshot waits a random delay, so the agents of a site do not all send
	// theirs at once when a router restarts.
	needSnapshot, snapshotAt := true, time.Now().Add(snapshotJitter())
	for {
		select {
		case <-t.C:
			batch, last := a.prefix.since(cursor)
			var kv *kvPayload
			if a.kv != nil {
				p, ok := a.kv.since(kvCursor)
				if !ok && !needSnapshot {
					needSnapshot, snapshotAt = true, time.Now().Add(snapshotJitter())
				}
				switch {
				case needSnapshot && time.Now().Before(snapshotAt):
					p = kvPayload{} // not yet: changes wait for the snapshot
				case needSnapshot:
					p = a.kv.snapshot()
				}
				if p.Snapshot || p.Cleared || len(p.Stored) > 0 || len(p.Removed) > 0 {
					kv = &p
				}
			}
			if len(batch) == 0 && kv == nil {
				continue
			}
			err := a.pushPrefixState(rs, batch, kv)
			if errors.Is(err, errKVGap) {
				needSnapshot, snapshotAt = true, time.Now().Add(snapshotJitter())
				cursor = last // the served entries were taken
				continue
			}
			if err != nil {
				log.Debugf("prefix-state push: %v", err)
				continue
			}
			cursor = last
			if kv != nil {
				kvCursor, needSnapshot = kv.To, false
			}
		case <-done:
			return
		case <-a.stopCh:
			return
		}
	}
}

// errKVGap is the router's answer to cache changes it cannot apply in order:
// the next batch must be a snapshot.
var errKVGap = errors.New("router needs a cache snapshot")

func (a *Agent) pushPrefixState(rs *routerSession, batch []prefixServedEntry, kv *kvPayload) error {
	body, err := json.Marshal(struct {
		SessionToken string              `json:"session_token"`
		Served       []prefixServedEntry `json:"served,omitempty"`
		KV           *kvPayload          `json:"kv,omitempty"`
	}{rs.sessionToken(), batch, kv})
	if err != nil {
		return err
	}
	resp, err := rs.client.Post("http://router/prefix-state", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return errKVGap
	}
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
	if a.prefix.source != sourceResponse {
		return req.RawBytes, pr
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

// served is called once a request was served: engines that do not return
// tokens are handled here, off the request path.
func (a *Agent) served(pr *prefixRequest, body []byte) {
	if pr == nil {
		return
	}
	switch a.prefix.source {
	case sourceText:
		a.prefix.add(prefixServedEntry{Anchors: pr.anchors, Text: true})
	case sourceBackground:
		select {
		case a.prefix.bgSlots <- struct{}{}:
		default:
			return // busy: skip this one, it is a hint
		}
		go func() {
			defer func() { <-a.prefix.bgSlots }()
			ctx, cancel := context.WithTimeout(context.Background(), prefixBgTimeout)
			defer cancel()
			if toks, err := a.engine.(chatTokenizer).TokenizeChat(ctx, a.cfg.BackendURL, a.httpClient, a.cfg.Model, body); err == nil {
				a.learn(pr, toks)
			}
		}()
	}
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
		if a.kv != nil {
			a.kv.resolve(tokens, pr.scope)
		}
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

// snapshotJitter is the random delay before a cache snapshot, up to 2 s.
func snapshotJitter() time.Duration {
	return time.Duration(mrand.Int64N(int64(2 * time.Second)))
}
