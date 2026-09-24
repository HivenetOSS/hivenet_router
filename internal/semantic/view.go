// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package semantic resolves a virtual model alias (e.g. "auto") to one concrete
// model from the content of the request.
//
// It runs only for requests whose model is an alias; concrete-model requests
// never reach it. Everything here is in-process and stdlib-only (no cgo, no
// network): request parsing, structural signals, route scoring, candidate
// filtering and task pinning.
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
	Role string
	// Text is the concatenated text parts (tool results excluded) with the
	// alias's strip markers already removed, so harness-injected blocks such as
	// <system-reminder> never count as something the human wrote.
	Text        string
	Images      int
	ToolCalls   int // assistant tool_calls (OpenAI) or tool_use blocks (Anthropic)
	ToolResults int // role "tool" (OpenAI) or tool_result blocks (Anthropic)
	ToolErrors  int // tool results flagged is_error, or whose text looks like a failure
}

// IsUserTurn reports whether the message is a human turn: a user message that
// is not merely carrying tool results back to the model. Text is already
// stripped, so a tool-result message that only adds a harness reminder is not
// a human turn.
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
	FirstUserText  string // first human turn (stripped), for the task fingerprint
	EndUser        string // OpenAI "user" / Anthropic metadata.user_id, for the task fingerprint
	ToolCount      int    // entries in "tools"
	StructuredOut  bool   // response_format json_schema / json_object
	ReasoningAsked bool   // reasoning_effort / reasoning / thinking requested
	Stream         bool
	RequestBytes   int
	// EstimatorBytes is the prompt size in the exact unit the router's token
	// estimator learns its per-model ratio on (domain.PromptTextBytes): raw
	// message text, the system prompt and the tool schemas, with Anthropic
	// tool_result / tool_use blocks excluded. Feeding it any other byte count
	// would multiply a ratio learned on one measure by another and mis-estimate
	// the prompt, e.g. count tool results twice on agentic traffic.
	EstimatorBytes int
	// MaxOutputTokens is the output the request reserves (max_tokens /
	// max_completion_tokens, whichever is larger; 0 when unset). The context
	// window must hold prompt + output.
	MaxOutputTokens int
}

type rawRequest struct {
	Messages            []rawMessage    `json:"messages"`
	System              json.RawMessage `json:"system"`
	Tools               json.RawMessage `json:"tools"`
	ResponseFormat      json.RawMessage `json:"response_format"`
	ReasoningEffort     string          `json:"reasoning_effort"`
	Reasoning           json.RawMessage `json:"reasoning"`
	Thinking            json.RawMessage `json:"thinking"`
	Stream              bool            `json:"stream"`
	MaxTokens           json.RawMessage `json:"max_tokens"`
	MaxCompletionTokens json.RawMessage `json:"max_completion_tokens"`
	// User and Metadata are decoded leniently (RawMessage): an odd shape must
	// not turn into a 400 on a field that only refines the task fingerprint.
	User     json.RawMessage `json:"user"`
	Metadata json.RawMessage `json:"metadata"`
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

// parsedContent is what one string-or-parts content yields.
type parsedContent struct {
	text        string // text parts joined by '\n'
	estBytes    int    // bytes domain.PromptTextBytes would count ("text" parts / plain string)
	images      int
	toolCalls   int // tool_use blocks
	toolResults int // tool_result blocks
	toolErrors  int // tool_result blocks flagged or looking like a failure
}

// ParseView builds a RequestView from a raw request body. stripMarkers lists
// open/close tag pairs (see policy.AliasSpec.StripMarkers) removed from every
// message's text before it is classified or matched.
func ParseView(body []byte, dialect Dialect, stripMarkers []string) (*RequestView, error) {
	var raw rawRequest
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("semantic: invalid request body: %w", err)
	}
	v := &RequestView{
		Dialect:         dialect,
		Stream:          raw.Stream,
		RequestBytes:    len(body),
		Messages:        make([]Message, 0, len(raw.Messages)),
		EstimatorBytes:  len(raw.Tools),
		MaxOutputTokens: max(jsonInt(raw.MaxTokens), jsonInt(raw.MaxCompletionTokens)),
		EndUser:         endUser(raw),
	}
	if len(raw.Tools) > 0 {
		var tools []json.RawMessage
		if json.Unmarshal(raw.Tools, &tools) == nil {
			v.ToolCount = len(tools)
		}
	}
	v.StructuredOut = wantsStructuredOutput(raw.ResponseFormat)
	v.ReasoningAsked = wantsReasoning(raw)

	var system strings.Builder
	if len(raw.System) > 0 {
		pc := parseContent(raw.System)
		system.WriteString(pc.text)
		v.EstimatorBytes += pc.estBytes
	}
	for _, rm := range raw.Messages {
		pc := parseContent(rm.Content)
		v.EstimatorBytes += pc.estBytes
		switch rm.Role {
		case "system", "developer":
			if system.Len() > 0 {
				system.WriteByte('\n')
			}
			system.WriteString(pc.text)
			continue
		case "tool": // OpenAI tool result: the whole content is the result
			m := Message{Role: rm.Role, ToolResults: 1}
			if looksLikeFailure(pc.text) {
				m.ToolErrors = 1
			}
			v.Messages = append(v.Messages, m)
			continue
		}
		v.Messages = append(v.Messages, Message{
			Role:        rm.Role,
			Text:        stripMarked(pc.text, stripMarkers),
			Images:      pc.images,
			ToolCalls:   len(rm.ToolCalls) + pc.toolCalls,
			ToolResults: pc.toolResults,
			ToolErrors:  pc.toolErrors,
		})
	}
	v.System = system.String()
	for i := range v.Messages {
		if v.Messages[i].IsUserTurn() {
			v.FirstUserText = v.Messages[i].Text
			break
		}
	}
	return v, nil
}

// parseContent handles a content that is either a string or an array of typed
// parts (OpenAI multimodal parts or Anthropic content blocks). Malformed
// content yields the zero value: routing degrades, it does not fail.
func parseContent(c json.RawMessage) parsedContent {
	var pc parsedContent
	if len(c) == 0 || string(c) == "null" {
		return pc
	}
	if c[0] == '"' {
		if json.Unmarshal(c, &pc.text) == nil {
			pc.estBytes = len(pc.text)
		}
		return pc
	}
	var parts []rawPart
	if json.Unmarshal(c, &parts) != nil {
		return pc
	}
	var b strings.Builder
	for _, p := range parts {
		switch p.Type {
		case "text", "input_text":
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(p.Text)
			if p.Type == "text" {
				pc.estBytes += len(p.Text)
			}
		case "image_url", "image", "input_image":
			pc.images++
		case "tool_use":
			pc.toolCalls++
		case "tool_result":
			pc.toolResults++
			if p.IsError || looksLikeFailure(parseContent(p.Content).text) {
				pc.toolErrors++
			}
		}
	}
	pc.text = b.String()
	return pc
}

// jsonInt decodes a JSON number into an int; anything else is 0.
func jsonInt(r json.RawMessage) int {
	var n float64
	if len(r) == 0 || json.Unmarshal(r, &n) != nil || n < 0 {
		return 0
	}
	return int(n)
}

// endUser returns the caller-supplied end-user id, if any: OpenAI "user" or
// Anthropic metadata.user_id.
func endUser(r rawRequest) string {
	var s string
	if len(r.User) > 0 && json.Unmarshal(r.User, &s) == nil && s != "" {
		return s
	}
	if len(r.Metadata) > 0 {
		var md struct {
			UserID string `json:"user_id"`
		}
		if json.Unmarshal(r.Metadata, &md) == nil {
			return md.UserID
		}
	}
	return ""
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
