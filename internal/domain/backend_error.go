// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"encoding/json"
	"net/http"
	"strings"
)

// knownErrorCodes are the codes the router and its agents produce. A code
// outside this set, read from a response body, did not come from them.
var knownErrorCodes = map[ErrorCode]bool{
	ErrCodeUnauthorized: true, ErrCodeModelForbidden: true, ErrCodeRequestInvalid: true,
	ErrCodeContextLengthExceeded: true, ErrCodeInvalidParameter: true, ErrCodeModelNotFound: true,
	ErrCodeNoAgentsAvailable: true, ErrCodeNoCapacity: true, ErrCodeAgentDisconnected: true,
	ErrCodeBackendUnavailable: true, ErrCodeBackendError: true, ErrCodeQueueFull: true,
	ErrCodeRequestTimeout: true, ErrCodeRateLimitExceeded: true, ErrCodeTokenLimitExceeded: true,
	ErrCodeInputTooLong: true, ErrCodeConcurrencyLimit: true, ErrCodeModelRedirected: true,
	ErrCodeModelWaking: true,
}

// KnownErrorCode reports whether code is one the router or an agent
// produces.
func KnownErrorCode(code ErrorCode) bool { return knownErrorCodes[code] }

// maxBackendMessage bounds the backend message a classified error carries
// to the client.
const maxBackendMessage = 512

// ClassifyBackendError maps a backend's error response (status and body) to
// a RouterError from SourceBackend, so its message reaches the client only:
// the message is the backend's own text (often an echo of the input), read
// from the body when it has one, truncated to 512 bytes. It understands the
// flat vLLM/SGLang shape {"message","type"}, the OpenAI shape
// {"error":{"message","type"}} and the Anthropic shape
// {"type":"error","error":{"type","message"}}, and otherwise classifies by
// status and keywords. The agent uses it for every backend error it
// forwards, and the router for an error body that did not come from an
// agent (an agent from before this change, or an unexpected answer).
func ClassifyBackendError(status int, body []byte) *RouterError {
	msg := string(body)
	var parsed struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Error   struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		if parsed.Message != "" {
			msg = parsed.Message
		} else if parsed.Error.Message != "" {
			msg = parsed.Error.Message
		}
		errType := parsed.Type
		// Anthropic wraps the error: the top-level type is just "error".
		if errType == "" || errType == "error" {
			errType = parsed.Error.Type
		}
		if len(msg) > maxBackendMessage {
			msg = msg[:maxBackendMessage]
		}
		switch errType {
		case "context_length_exceeded":
			return NewRouterError(ErrCodeContextLengthExceeded, msg, SourceBackend)
		case "invalid_request_error", "invalid_parameter":
			return NewRouterError(ErrCodeInvalidParameter, msg, SourceBackend)
		case "overloaded_error", "rate_limit_error":
			return NewRouterError(ErrCodeBackendUnavailable, msg, SourceBackend)
		}
	}
	if len(msg) > maxBackendMessage {
		msg = msg[:maxBackendMessage]
	}
	bodyLower := strings.ToLower(string(body))
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		if strings.Contains(bodyLower, "context") &&
			(strings.Contains(bodyLower, "length") ||
				strings.Contains(bodyLower, "window") ||
				strings.Contains(bodyLower, "token")) {
			return NewRouterError(ErrCodeContextLengthExceeded, msg, SourceBackend)
		}
		return NewRouterError(ErrCodeInvalidParameter, msg, SourceBackend)
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return NewRouterError(ErrCodeBackendUnavailable, msg, SourceBackend)
	default:
		if msg == "" {
			msg = http.StatusText(status)
		}
		return NewRouterError(ErrCodeBackendError, msg, SourceBackend)
	}
}

// maxEnvelopeBytes is how much of an agent's error response is parsed as
// the router's error envelope: structured errors are always short, and
// parsing a truncated longer body would fail anyway.
const maxEnvelopeBytes = 4 << 10

// DecodeAgentError turns an agent's non-200 response into the error the
// router acts on. The agent answers its own and its backend's errors in the
// router's envelope, {"error":{"code","message","source"}}; that envelope is
// trusted only when its code and its source are values the router knows.
// Anything else is a backend's body that reached the router unclassified
// (an agent from before this change forwarded /v1/messages, embeddings and
// rerank errors as is), and a backend body can look like the envelope
// without being one: {"error":{"message":...}} has no source, and an
// OpenAI-style string code is no router code. Those are classified as a
// backend error (ClassifyBackendError), so their text stays out of logs
// and traces.
func DecodeAgentError(status int, body []byte) *RouterError {
	head := body
	if len(head) > maxEnvelopeBytes {
		head = head[:maxEnvelopeBytes]
	}
	var env RouterErrorResponse
	if json.Unmarshal(head, &env) == nil && env.Error != nil &&
		KnownErrorCode(env.Error.Code) && (env.Error.Source == SourceRouter || env.Error.Source == SourceBackend) {
		return env.Error
	}
	if len(body) == 0 {
		return NewRouterError(ErrCodeBackendError, http.StatusText(status), SourceBackend)
	}
	return ClassifyBackendError(status, body)
}
