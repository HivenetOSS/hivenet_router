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
// Anthropic (/v1/messages) events are parsed too: message_start carries the
// exact input_tokens, message_delta the cumulative output_tokens, and
// content_block_delta the streamed text — so both dialects yield exact usage
// and live content observation.
//
// Exported so the streaming token metering can be tested in isolation.
type SSETokenMeter struct {
	buf     bytes.Buffer
	content strings.Builder

	haveUsage       bool
	usagePrompt     int
	usageCompletion int

	// onContent, when set, is called with each streamed delta's content as it is
	// parsed. It lets the occupancy budget grow an undeclared request's footprint
	// live, before the stream's total is known. Never called after the stream ends.
	onContent func(delta string)
}

// NewSSETokenMeter returns an empty meter ready to be wired into an io.TeeReader.
func NewSSETokenMeter() *SSETokenMeter {
	return &SSETokenMeter{}
}

// NewGrowthObserver returns an SSE content observer that grows res as output
// streams. It charges the increase in floor(cumulativeBytes/4) rather than
// summing floor(len(delta)/4) per chunk: the per-chunk form drops up to ~1 token
// on every delta, so a stream split into many small pieces would badly
// under-count its footprint. res must be non-nil.
func NewGrowthObserver(res domain.Reservation) func(delta string) {
	var bytesSoFar, tokensSoFar int
	return func(delta string) {
		bytesSoFar += len(delta)
		tokens := bytesSoFar / 4
		if tokens > tokensSoFar {
			res.Grow(tokens - tokensSoFar)
			tokensSoFar = tokens
		}
	}
}

// SetContentObserver registers a callback invoked with each streamed delta's
// content as it is parsed. Pass nil to clear. Set it before the meter starts
// receiving bytes.
func (m *SSETokenMeter) SetContentObserver(fn func(delta string)) {
	m.onContent = fn
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

// sseChunk is the minimal union of the OpenAI and Anthropic streaming shapes we
// need. OpenAI: per-delta text under choices[].delta.content plus the optional
// terminal usage object. Anthropic: message_start carries usage.input_tokens
// under "message", content_block_delta carries text under a top-level "delta",
// and message_delta carries the cumulative usage.output_tokens at the top
// level. The field sets do not collide, so one struct decodes both dialects.
type sseChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		InputTokens      int `json:"input_tokens"`  // Anthropic message_delta (rare here)
		OutputTokens     int `json:"output_tokens"` // Anthropic message_delta (cumulative)
	} `json:"usage"`
	Message *struct { // Anthropic message_start
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
	Delta *struct { // Anthropic content_block_delta
		Text string `json:"text"`
	} `json:"delta"`
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
	// Anthropic message_start: the exact prompt count arrives before any output.
	// Usage is exact from this point; output_tokens is refined by message_delta.
	if c.Message != nil && c.Message.Usage.InputTokens > 0 {
		m.haveUsage = true
		m.usagePrompt = c.Message.Usage.InputTokens
		if c.Message.Usage.OutputTokens > m.usageCompletion {
			m.usageCompletion = c.Message.Usage.OutputTokens
		}
	}
	// Anthropic message_delta: cumulative output count; the last one wins.
	if c.Usage != nil && c.Usage.OutputTokens > m.usageCompletion {
		m.usageCompletion = c.Usage.OutputTokens
		if c.Usage.InputTokens > 0 {
			m.usagePrompt = c.Usage.InputTokens
			m.haveUsage = true
		}
	}
	deliver := func(text string) {
		m.content.WriteString(text)
		if m.onContent != nil {
			m.onContent(text)
		}
	}
	for _, ch := range c.Choices {
		if ch.Delta.Content != "" {
			deliver(ch.Delta.Content)
		}
	}
	if c.Delta != nil && c.Delta.Text != "" {
		deliver(c.Delta.Text)
	}
}

// HaveUsage reports whether the stream carried an exact backend usage object
// (OpenAI stream_options.include_usage, or Anthropic message_start/
// message_delta). When false, Tokens returns an estimate, which the caller must
// not feed back into the learned estimator.
func (m *SSETokenMeter) HaveUsage() bool { return m.haveUsage }

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
