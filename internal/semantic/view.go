// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package semantic resolves a virtual model alias (e.g. "auto") to one concrete
// model from the content of the request.
//
// It runs only for requests whose model is an alias; concrete-model requests
// never reach it. Everything here is in-process and stdlib-only (no cgo, no
// network): request parsing, structural signals, route scoring, candidate
// filtering, task pinning and a JSONL decision log.
package semantic

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Dialect identifies the request schema.
type Dialect string

const (
	DialectOpenAI    Dialect = "openai"    // /v1/chat/completions
	DialectAnthropic Dialect = "anthropic" // /v1/messages, /v1/messages/count_tokens
)

// DialectForPath maps a request path to its dialect.
func DialectForPath(path string) Dialect {
	if strings.HasPrefix(path, "/v1/messages") {
		return DialectAnthropic
	}
	return DialectOpenAI
}

// Message is the routing-relevant view of one conversation message.
type Message struct {
	Role        string
	Text        string // concatenated text parts (tool results excluded)
	Images      int
	ToolCalls   int // assistant tool_calls (OpenAI) or tool_use blocks (Anthropic)
	ToolResults int // role "tool" (OpenAI) or tool_result blocks (Anthropic)
	ToolErrors  int // tool results flagged is_error, or whose text looks like a failure
}

// IsUserTurn reports whether the message is a human turn (a user message that
// is not merely carrying tool results back to the model).
func (m *Message) IsUserTurn() bool {
	return m.Role == "user" && (m.ToolResults == 0 || strings.TrimSpace(m.Text) != "")
}

// RequestView is the routing-relevant view of a chat request in either dialect.
// It is parsed from the raw body only on the alias path and is deliberately
// separate from domain.ChatRequest, which drops tool calls, tool results,
// response_format and reasoning parameters.
type RequestView struct {
	Dialect        Dialect
	Messages       []Message
	System         string // top-level system (Anthropic) + system/developer messages (OpenAI)
	FirstUserText  string // first human turn, for the task fingerprint
	ToolCount      int    // entries in "tools"
	StructuredOut  bool   // response_format json_schema / json_object
	ReasoningAsked bool   // reasoning_effort / reasoning / thinking requested
	Stream         bool
	RequestBytes   int
	// PromptBytes approximates the prompt text the backend tokenizes: message
	// text, tool-result text, system prompt and tool schemas. It is at least the
	// router's own domain.PromptTextBytes measure (which skips tool results), so
	// token estimates built from it err on the large side for the context filter.
	PromptBytes int
}

type rawRequest struct {
	Messages        []rawMessage    `json:"messages"`
	System          json.RawMessage `json:"system"`
	Tools           json.RawMessage `json:"tools"`
	ResponseFormat  json.RawMessage `json:"response_format"`
	ReasoningEffort string          `json:"reasoning_effort"`
	Reasoning       json.RawMessage `json:"reasoning"`
	Thinking        json.RawMessage `json:"thinking"`
	Stream          bool            `json:"stream"`
}

type rawMessage struct {
	Role       string            `json:"role"`
	Content    json.RawMessage   `json:"content"`
	ToolCalls  []json.RawMessage `json:"tool_calls"`
	ToolCallID string            `json:"tool_call_id"`
}

type rawPart struct {
	Type    string          `json:"type"`
	Text    string          `json:"text"`
	IsError bool            `json:"is_error"`
	Content json.RawMessage `json:"content"` // tool_result payload: string or parts
}

// ParseView builds a RequestView from a raw request body.
func ParseView(body []byte, dialect Dialect) (*RequestView, error) {
	var raw rawRequest
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("semantic: invalid request body: %w", err)
	}
	v := &RequestView{
		Dialect:      dialect,
		Stream:       raw.Stream,
		RequestBytes: len(body),
		Messages:     make([]Message, 0, len(raw.Messages)),
	}

	var system strings.Builder
	if len(raw.System) > 0 {
		t, _, _ := contentText(raw.System)
		system.WriteString(t)
	}
	v.PromptBytes = len(raw.Tools)
	if len(raw.Tools) > 0 {
		var tools []json.RawMessage
		if json.Unmarshal(raw.Tools, &tools) == nil {
			v.ToolCount = len(tools)
		}
	}
	v.StructuredOut = wantsStructuredOutput(raw.ResponseFormat)
	v.ReasoningAsked = wantsReasoning(raw)

	for _, rm := range raw.Messages {
		m := Message{Role: rm.Role, ToolCalls: len(rm.ToolCalls)}
		switch rm.Role {
		case "system", "developer":
			t, _, _ := contentText(rm.Content)
			if system.Len() > 0 {
				system.WriteByte('\n')
			}
			system.WriteString(t)
			continue
		case "tool": // OpenAI tool result
			t, _, _ := contentText(rm.Content)
			m.ToolResults = 1
			if looksLikeFailure(t) {
				m.ToolErrors = 1
			}
			v.PromptBytes += len(t)
		default:
			var resultBytes int
			m.Text, m.Images, m.ToolResults, m.ToolErrors, m.ToolCalls, resultBytes = parseContent(rm.Content, m.ToolCalls)
			v.PromptBytes += len(m.Text) + resultBytes
		}
		v.Messages = append(v.Messages, m)
	}
	v.System = system.String()
	v.PromptBytes += len(v.System)
	for i := range v.Messages {
		if v.Messages[i].IsUserTurn() {
			v.FirstUserText = v.Messages[i].Text
			break
		}
	}
	return v, nil
}

// parseContent handles a message content that is either a string or an array
// of typed parts (OpenAI multimodal parts or Anthropic content blocks).
func parseContent(c json.RawMessage, toolCalls int) (text string, images, results, errs, calls, resultBytes int) {
	calls = toolCalls
	if len(c) == 0 || string(c) == "null" {
		return
	}
	var s string
	if json.Unmarshal(c, &s) == nil {
		text = s
		return
	}
	var parts []rawPart
	if json.Unmarshal(c, &parts) != nil {
		return
	}
	var b strings.Builder
	for _, p := range parts {
		switch p.Type {
		case "text", "input_text":
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(p.Text)
		case "image_url", "image", "input_image":
			images++
		case "tool_use":
			calls++
		case "tool_result":
			results++
			rt, _, _ := contentText(p.Content)
			resultBytes += len(rt)
			if p.IsError || looksLikeFailure(rt) {
				errs++
			}
		}
	}
	text = b.String()
	return
}

// contentText returns the concatenated text of a string-or-parts content.
func contentText(c json.RawMessage) (string, int, error) {
	if len(c) == 0 || string(c) == "null" {
		return "", 0, nil
	}
	var s string
	if err := json.Unmarshal(c, &s); err == nil {
		return s, 0, nil
	}
	var parts []rawPart
	if err := json.Unmarshal(c, &parts); err != nil {
		return "", 0, err
	}
	var b strings.Builder
	images := 0
	for _, p := range parts {
		switch p.Type {
		case "text", "input_text":
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(p.Text)
		case "image_url", "image", "input_image":
			images++
		}
	}
	return b.String(), images, nil
}

func wantsStructuredOutput(rf json.RawMessage) bool {
	if len(rf) == 0 {
		return false
	}
	var f struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(rf, &f) != nil {
		return false
	}
	return f.Type == "json_schema" || f.Type == "json_object"
}

func wantsReasoning(r rawRequest) bool {
	if e := strings.ToLower(r.ReasoningEffort); e != "" && e != "none" {
		return true
	}
	if len(r.Reasoning) > 0 && string(r.Reasoning) != "null" {
		var o struct {
			Effort string `json:"effort"`
		}
		if json.Unmarshal(r.Reasoning, &o) == nil && strings.ToLower(o.Effort) == "none" {
			return false
		}
		return true
	}
	if len(r.Thinking) > 0 && string(r.Thinking) != "null" {
		var t struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(r.Thinking, &t) == nil && t.Type != "disabled" {
			return true
		}
	}
	return false
}
