// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"bytes"
	"encoding/json"
	"strings"

	"hivenet_router/internal/domain"
)

// SSETokenMeter estimates the token usage of a streamed (SSE) chat response
// without disturbing the byte-exact stream forwarded to the client. It is fed a
// copy of the stream via io.TeeReader: it buffers across Write boundaries (a
// single SSE event may be split across reads), parses OpenAI-style "data:"
// events, and either captures an exact usage object (when the backend emits one,
// i.e. the request set stream_options.include_usage) or accumulates the delta
// text so completion tokens can be estimated at end-of-stream.
//
// Streaming accounting is necessarily post-hoc: the bytes are already delivered
// by the time the totals are known, so the caller deducts from the budget and
// records metrics after the stream completes — it cannot reject mid-stream.
//
// Anthropic (/v1/messages) events use a different shape (usage in message_start /
// message_delta); until that is parsed, those streams fall back to the
// text-accumulation estimate, which omits the system prompt and non-text blocks.
//
// Exported so the streaming token metering can be tested in isolation.
type SSETokenMeter struct {
	buf     bytes.Buffer
	content strings.Builder

	haveUsage       bool
	usagePrompt     int
	usageCompletion int
}

// NewSSETokenMeter returns an empty meter ready to be wired into an io.TeeReader.
func NewSSETokenMeter() *SSETokenMeter {
	return &SSETokenMeter{}
}

// Write implements io.Writer. It never returns an error so that the TeeReader
// forwarding the stream to the client is never disrupted by metering.
func (m *SSETokenMeter) Write(p []byte) (int, error) {
	m.buf.Write(p)
	data := m.buf.Bytes()
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		m.parseLine(data[:i])
		data = data[i+1:]
	}
	// Retain any partial trailing line for the next Write (copy: the next
	// buf.Reset would otherwise alias the slice we keep).
	leftover := append([]byte(nil), data...)
	m.buf.Reset()
	m.buf.Write(leftover)
	return len(p), nil
}

// sseChunk is the minimal subset of an OpenAI streaming chunk we need: the
// per-delta text content and the optional terminal usage object.
type sseChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func (m *SSETokenMeter) parseLine(line []byte) {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	payload := bytes.TrimSpace(line[len("data:"):])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	var c sseChunk
	if err := json.Unmarshal(payload, &c); err != nil {
		return // non-JSON comment/keepalive — ignore
	}
	if c.Usage != nil && (c.Usage.PromptTokens > 0 || c.Usage.CompletionTokens > 0) {
		m.haveUsage = true
		m.usagePrompt = c.Usage.PromptTokens
		m.usageCompletion = c.Usage.CompletionTokens
	}
	for _, ch := range c.Choices {
		if ch.Delta.Content != "" {
			m.content.WriteString(ch.Delta.Content)
		}
	}
}

// Tokens returns the prompt and completion token counts for the stream. It
// prefers the backend-reported usage (exact) and otherwise estimates: prompt
// from the original request messages, completion from the accumulated deltas.
func (m *SSETokenMeter) Tokens(req *domain.ChatRequest) (prompt, completion int) {
	if m.haveUsage {
		return m.usagePrompt, m.usageCompletion
	}
	if req != nil {
		prompt = domain.EstimateTokens(domain.GetMessageSlice(req.Messages))
	}
	completion = domain.EstimateTokens([]string{m.content.String()})
	return prompt, completion
}
