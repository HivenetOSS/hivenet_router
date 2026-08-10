// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "net/http"

// ErrorCode identifies the category of a routing or backend failure.
type ErrorCode string

const (
	ErrCodeUnauthorized          ErrorCode = "unauthorized"
	ErrCodeModelForbidden        ErrorCode = "model_forbidden"
	ErrCodeRequestInvalid        ErrorCode = "request_invalid"
	ErrCodeContextLengthExceeded ErrorCode = "context_length_exceeded"
	ErrCodeInvalidParameter      ErrorCode = "invalid_parameter"
	ErrCodeModelNotFound         ErrorCode = "model_not_found"
	ErrCodeNoAgentsAvailable     ErrorCode = "no_agents_available"
	ErrCodeNoCapacity            ErrorCode = "no_capacity"
	ErrCodeAgentDisconnected     ErrorCode = "agent_disconnected"
	ErrCodeBackendUnavailable    ErrorCode = "backend_unavailable"
	ErrCodeBackendError          ErrorCode = "backend_error"
	ErrCodeQueueFull             ErrorCode = "queue_full"
	ErrCodeRequestTimeout        ErrorCode = "request_timeout"
	ErrCodeRateLimitExceeded     ErrorCode = "rate_limit_exceeded"
	ErrCodeTokenLimitExceeded    ErrorCode = "token_limit_exceeded"
	ErrCodeInputTooLong          ErrorCode = "input_too_long"
	ErrCodeConcurrencyLimit      ErrorCode = "concurrency_limit_exceeded"
)

// ErrorSource identifies which layer produced the error.
type ErrorSource string

const (
	SourceRouter  ErrorSource = "router"
	SourceBackend ErrorSource = "backend"
)

// RouterError is the structured error propagated from backend/agent through the
// router to the HTTP client. It implements the error interface so it can be sent
// on the existing chan error without changing any channel types.
//
// Usage and AgentID are internal-only audit metadata — excluded from the JSON
// response body so they are never exposed to API clients. They are populated only
// when inference actually completed before the error was raised (e.g. output token
// budget exceeded after a successful agent response).
type RouterError struct {
	Code    ErrorCode   `json:"code"`
	Message string      `json:"message"`
	Source  ErrorSource `json:"source"`

	// Audit metadata — not sent to clients.
	Usage   *Usage `json:"-"`
	AgentID string `json:"-"`
}

func (e *RouterError) Error() string { return e.Message }

// RouterErrorResponse is the top-level JSON envelope returned to clients:
//
//	{"error": {"code": "...", "message": "...", "source": "..."}}
type RouterErrorResponse struct {
	Error *RouterError `json:"error"`
}

// NewRouterError constructs a RouterError.
func NewRouterError(code ErrorCode, message string, source ErrorSource) *RouterError {
	return &RouterError{Code: code, Message: message, Source: source}
}

// HTTPStatusFor maps an ErrorCode to the appropriate HTTP status code.
// Unknown codes return 500.
func HTTPStatusFor(code ErrorCode) int {
	switch code {
	case ErrCodeUnauthorized:
		return http.StatusUnauthorized
	case ErrCodeModelForbidden:
		return http.StatusForbidden
	case ErrCodeRequestInvalid, ErrCodeContextLengthExceeded, ErrCodeInvalidParameter,
		ErrCodeInputTooLong:
		return http.StatusBadRequest
	case ErrCodeModelNotFound:
		return http.StatusNotFound
	case ErrCodeNoAgentsAvailable, ErrCodeNoCapacity, ErrCodeAgentDisconnected,
		ErrCodeBackendUnavailable, ErrCodeQueueFull:
		return http.StatusServiceUnavailable
	case ErrCodeBackendError:
		return http.StatusBadGateway
	case ErrCodeRequestTimeout:
		return http.StatusGatewayTimeout
	case ErrCodeRateLimitExceeded, ErrCodeTokenLimitExceeded, ErrCodeConcurrencyLimit:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}
