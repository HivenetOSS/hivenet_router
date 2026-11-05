// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
)

const echoed = "bad input: <the user's prompt>"

// TestDecodeAgentError: an agent's error envelope is trusted only when its
// code and source are the router's. Any other body, which is what an agent
// from before the passthrough fix forwarded from its backend, is classified
// as the backend's: its text is the client's message, never in Error().
func TestDecodeAgentError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		wantCode domain.ErrorCode
	}{
		{"Anthropic shape, 500", 500, `{"type":"error","error":{"type":"api_error","message":"` + echoed + `"}}`, domain.ErrCodeBackendError},
		{"Anthropic invalid request", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"` + echoed + `"}}`, domain.ErrCodeInvalidParameter},
		{"Anthropic overloaded", 529, `{"type":"error","error":{"type":"overloaded_error","message":"` + echoed + `"}}`, domain.ErrCodeBackendUnavailable},
		{"envelope with no source", 500, `{"error":{"code":"backend_error","message":"` + echoed + `"}}`, domain.ErrCodeBackendError},
		{"router code, no source", 400, `{"error":{"code":"context_length_exceeded","message":"` + echoed + `"}}`, domain.ErrCodeContextLengthExceeded},
		{"unknown string code", 401, `{"error":{"code":"invalid_api_key","message":"` + echoed + `","source":"backend"}}`, domain.ErrCodeBackendError},
		{"unknown source", 500, `{"error":{"code":"backend_error","message":"` + echoed + `","source":"agent"}}`, domain.ErrCodeBackendError},
		{"flat vLLM shape", 400, `{"object":"error","message":"` + echoed + `","type":"BadRequestError"}`, domain.ErrCodeInvalidParameter},
		{"plain text", 502, echoed, domain.ErrCodeBackendError},
	} {
		re := domain.DecodeAgentError(tc.status, []byte(tc.body))
		if re.Source != domain.SourceBackend || re.Code != tc.wantCode {
			t.Errorf("%s: code %q source %q, want %q from the backend", tc.name, re.Code, re.Source, tc.wantCode)
		}
		if strings.Contains(re.Error(), "prompt") {
			t.Errorf("%s: Error() %q carries the backend's text", tc.name, re.Error())
		}
		if !strings.Contains(re.Message, echoed) {
			t.Errorf("%s: message %q, want the backend's text for the client", tc.name, re.Message)
		}
	}
	// The agent's own envelopes are kept as they are.
	for _, src := range []domain.ErrorSource{domain.SourceRouter, domain.SourceBackend} {
		body := `{"error":{"code":"backend_unavailable","message":"engine down","source":"` + string(src) + `"}}`
		if re := domain.DecodeAgentError(503, []byte(body)); re.Code != domain.ErrCodeBackendUnavailable || re.Source != src || re.Message != "engine down" {
			t.Errorf("agent envelope from %s: got %+v", src, re)
		}
	}
	if re := domain.DecodeAgentError(http.StatusBadGateway, nil); re.Code != domain.ErrCodeBackendError || re.Source != domain.SourceBackend {
		t.Errorf("empty body: got %+v", re)
	}
}

// TestRouterError_ErrorShowsOnlyRouterMessages: Error(), what logs and span
// statuses see, carries the message only for the router's own errors.
func TestRouterError_ErrorShowsOnlyRouterMessages(t *testing.T) {
	for src, want := range map[domain.ErrorSource]bool{domain.SourceRouter: true, domain.SourceBackend: false, "": false, "agent": false} {
		re := domain.NewRouterError(domain.ErrCodeBackendError, echoed, src)
		if got := strings.Contains(re.Error(), echoed); got != want {
			t.Errorf("source %q: Error() %q shows the message = %v, want %v", src, re.Error(), got, want)
		}
	}
}

// TestKnownErrorCodes_ListsEveryCode: every ErrCode constant declared in
// internal/domain is a known code, so an agent envelope carrying it is
// trusted (a code missing from the list would turn the agent's errors into
// generic backend errors).
func TestKnownErrorCodes_ListsEveryCode(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "../../internal/domain/errors.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	ast.Inspect(f, func(node ast.Node) bool {
		vs, ok := node.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || !strings.HasPrefix(vs.Names[0].Name, "ErrCode") || len(vs.Values) != 1 {
			return true
		}
		lit, ok := vs.Values[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		n++
		code := domain.ErrorCode(strings.Trim(lit.Value, `"`))
		if !domain.KnownErrorCode(code) {
			t.Errorf("%s (%q) is not a known error code", vs.Names[0].Name, code)
		}
		return true
	})
	if n < 15 {
		t.Fatalf("found only %d ErrCode constants; the parse is wrong", n)
	}
}
