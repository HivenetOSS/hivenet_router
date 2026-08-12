// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// New ChatRequest that covers text generation, vision and audio, with extensible fields for future use.
// It is meant to decode only the fields relevant to routing and basic processing, and pass through the raw request to the backend engine without modification.
// ChatCompletionRequest is the root object for /v1/chat/completions
type ChatRequest struct {
	Model    string                  `json:"model"`
	Messages []ChatCompletionMessage `json:"messages"`

	// Modalities & Multimedia (VLM/Audio)
	Modalities []string     `json:"modalities,omitempty"`
	Audio      *AudioConfig `json:"audio,omitempty"`

	// max_completion_tokens is preferred over max_tokens in 2026
	MaxCompletionTokens int `json:"max_completion_tokens,omitempty"`
	MaxTokens           int `json:"max_tokens,omitempty"`

	// System is the top-level system prompt used by the Anthropic /v1/messages
	// dialect (absent from Messages). Kept raw because it may be either a plain
	// string or an array of text content blocks; SystemText decodes both.
	System json.RawMessage `json:"system,omitempty"`

	// Tools holds the tool/function definitions (both dialects use "tools").
	// Kept raw: the router never interprets them, but their JSON is rendered
	// into the prompt by the backend and tokenized like any other input, so the
	// token estimator must count these bytes — an agentic request can carry tens
	// of KB of tool schemas that would otherwise be invisible to admission and
	// would poison the learned tokens-per-byte ratio.
	Tools json.RawMessage `json:"tools,omitempty"`

	// Sampling & Controls
	Temperature float64 `json:"temperature,omitempty"`
	TopP        float64 `json:"top_p,omitempty"`
	Stream      bool    `json:"stream,omitempty"`
	ServiceTier string  `json:"service_tier,omitempty"`

	// additional internal fields, not in the OpenAI spec
	// removed when sending the request to the backend engine.
	// Original submitted request Headers.
	// This includes all custom headers added by the openAI client library.
	HttpHeaders http.Header `json:"-"`
	// RawBytes holds the original request body bytes for seamless forwarding to the backend engine.
	RawBytes []byte `json:"-"`
}

type ChatCompletionMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"` // string for text-only, []ContentPart for VLM/audio
}

// UnmarshalJSON ensures Content is decoded as string or []ContentPart, never as
// the generic []interface{} that json.Unmarshal produces for arrays by default.
// Without this, the type switches in GetMessageSlice/GetMessageTextContent would
// never match the []ContentPart case.
func (m *ChatCompletionMessage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.Role = raw.Role
	if len(raw.Content) == 0 {
		return nil
	}
	// Try string first (most common case).
	var s string
	if err := json.Unmarshal(raw.Content, &s); err == nil {
		m.Content = s
		return nil
	}
	// Try array of content parts (VLM / audio).
	var parts []ContentPart
	if err := json.Unmarshal(raw.Content, &parts); err == nil {
		m.Content = parts
		return nil
	}
	return fmt.Errorf("unsupported content type in message: %s", raw.Content)
}

type ContentPart struct {
	Type       string            `json:"type"`                  // "text", "image_url", "input_audio" (OpenAI); "image" (Anthropic)
	Text       string            `json:"text,omitempty"`        // If type is "text"
	ImageURL   *ImageURLConfig   `json:"image_url,omitempty"`   // If type is "image_url"
	InputAudio *InputAudioConfig `json:"input_audio,omitempty"` // If type is "input_audio"
}

type ImageURLConfig struct {
	URL    string `json:"url"`              // HTTPS URL or Base64
	Detail string `json:"detail,omitempty"` // "auto", "low", "high"
}

type InputAudioConfig struct {
	Data   string `json:"data"`   // Base64 encoded audio
	Format string `json:"format"` // "wav", "mp3"
}

type AudioConfig struct {
	Voice  string `json:"voice"`  // "alloy", "echo", etc.
	Format string `json:"format"` // "wav", "mp3", "flac"
}

// ChatResponse represents an OpenAI-compatible chat completion response.
// This is what your service returns after processing a request.
type ChatResponse struct {
	ID                string   `json:"id"`
	Object            string   `json:"object"`
	Created           int64    `json:"created"`
	Model             string   `json:"model"`
	SystemFingerprint string   `json:"system_fingerprint,omitempty"` // Identifies the backend configuration
	Choices           []Choice `json:"choices"`
	Usage             Usage    `json:"usage,omitempty"`
	ProcessedBy       string   `json:"processed_by,omitempty"`
	ServiceTier       string   `json:"service_tier,omitempty"`
	// additional internal fields, not in the OpenAI spec
	// removed when sending the response to the client
	HttpHeaders http.Header   `json:"-"`
	RawBytes    []byte        `json:"-"`
	Body        io.ReadCloser `json:"-"` // non-nil for streaming responses; handler must Close
}

// Choice represents a response choice
type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	FinishReason string   `json:"finish_reason,omitempty"`
}

// Usage represents token usage statistics
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// UnmarshalJSON also accepts the Anthropic usage shape (input_tokens /
// output_tokens, no total), normalizing it into the OpenAI-named fields.
// Without this, every /v1/messages response parsed as zero usage, so the
// Anthropic dialect silently escaped output metering, OTPM charging, the
// occupancy true-up, and estimator learning. Marshaling is unchanged (OpenAI
// names); TotalTokens is synthesized when absent so "usage present" checks
// (TotalTokens > 0) hold for both dialects.
func (u *Usage) UnmarshalJSON(data []byte) error {
	var raw struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
		InputTokens      int `json:"input_tokens"`
		OutputTokens     int `json:"output_tokens"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	u.PromptTokens = raw.PromptTokens
	u.CompletionTokens = raw.CompletionTokens
	u.TotalTokens = raw.TotalTokens
	if u.PromptTokens == 0 && raw.InputTokens > 0 {
		u.PromptTokens = raw.InputTokens
	}
	if u.CompletionTokens == 0 && raw.OutputTokens > 0 {
		u.CompletionTokens = raw.OutputTokens
	}
	if u.TotalTokens == 0 && u.PromptTokens+u.CompletionTokens > 0 {
		u.TotalTokens = u.PromptTokens + u.CompletionTokens
	}
	return nil
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content,omitempty"`
	Refusal string `json:"refusal,omitempty"`
}

// ModelAgentEntry is a single agent entry in the per-model detail response.
type ModelAgentEntry struct {
	PeerID       string `json:"peer_id"`
	Engine       string `json:"engine,omitempty"`
	Version      string `json:"version,omitempty"`
	Region       string `json:"region,omitempty"`
	Organization string `json:"organization,omitempty"`
	Machine      string `json:"machine,omitempty"`
	Capacity     int    `json:"capacity"`
	IsHealthy    bool   `json:"is_healthy"`
	LastSeen     int64  `json:"last_seen"`
}

// ModelAgents holds aggregated agent stats for a model.
// The List field is populated only by the per-model detail endpoint.
type ModelAgents struct {
	Total         int               `json:"total"`
	Healthy       int               `json:"healthy"`
	TotalCapacity int               `json:"total_capacity"`
	Engines       []string          `json:"engines"`
	Regions       []string          `json:"regions"`
	List          []ModelAgentEntry `json:"list,omitempty"`
}

// ModelObject is an entry in the OpenAI-compatible models response.
type ModelObject struct {
	ID         string      `json:"id"`
	Object     string      `json:"object"`
	PrettyName string      `json:"pretty_name,omitempty"`
	Info       string      `json:"info,omitempty"`
	Capability string      `json:"capability,omitempty"`
	HideLLM    bool        `json:"hide_llm,omitempty"`
	Agents     ModelAgents `json:"agents"`
}

// EstimateTokens estimates token count for messages
func EstimateTokens(messages []string) int {
	total := 0
	for _, msg := range messages {
		total += len(msg) / 4
	}
	total += len(messages) * 4
	return total
}

func GetMessageSlice(messages []ChatCompletionMessage) []string {
	var result []string
	for _, message := range messages {
		switch content := message.Content.(type) {
		case string:
			result = append(result, content)
		case []ContentPart:
			for _, part := range content {
				if part.Type == "text" {
					result = append(result, part.Text)
				}
			}
		}
	}
	return result
}

// SystemText extracts the text of a top-level Anthropic system prompt, which is
// absent from Messages. It accepts either a plain string or an array of text
// content blocks; non-text blocks contribute nothing.
func SystemText(req *ChatRequest) string {
	if len(req.System) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(req.System, &s); err == nil {
		return s
	}
	var parts []ContentPart
	if err := json.Unmarshal(req.System, &parts); err == nil {
		var out string
		for _, p := range parts {
			if p.Type == "text" {
				out += p.Text
			}
		}
		return out
	}
	return ""
}

// PromptTextBytes returns the total bytes of estimable prompt text — message
// content, any top-level system prompt, and the raw tool-definition JSON — and
// the message count, for the token estimator. Tool schemas are rendered into
// the prompt and tokenized by the backend, so counting their bytes keeps the
// estimate (and the learned ratio, which divides exact prompt_tokens by these
// bytes) honest for agentic requests. Non-text content (images) is not
// byte-estimable and is bounded separately by the per-request image cap.
func PromptTextBytes(req *ChatRequest) (textBytes, messageCount int) {
	for _, s := range GetMessageSlice(req.Messages) {
		textBytes += len(s)
	}
	textBytes += len(SystemText(req))
	textBytes += len(req.Tools)
	return textBytes, len(req.Messages)
}

// CountImages returns the number of image content parts across all messages:
// OpenAI "image_url" parts and Anthropic "image" blocks (the /v1/messages
// dialect) both count, so neither dialect can slip images past the per-request
// image cap. Text-only messages (Content is a plain string) carry no images and
// contribute zero. Audio and text parts are ignored.
func CountImages(messages []ChatCompletionMessage) int {
	count := 0
	for _, message := range messages {
		if parts, ok := message.Content.([]ContentPart); ok {
			for _, part := range parts {
				if part.Type == "image_url" || part.Type == "image" {
					count++
				}
			}
		}
	}
	return count
}

func GetMessageTextContent(message ChatCompletionMessage) string {
	var result string
	switch content := message.Content.(type) {
	case string:
		result += content
	case []ContentPart:
		for _, part := range content {
			if part.Type == "text" {
				result += part.Text
			}
		}
	}
	return result
}
