// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/semantic"
	"hivenet_router/internal/tokenizer"
)

// smallWindowProfiles gives both alias candidates a tiny context window.
const smallWindowProfiles = `
models: ["coder", "chat"]
profile:
  context_window: 64
`

// newAliasHandlersWithProfiles is newAliasHandlers plus a profile document and
// the real token estimator, so the context-window filter is live.
func newAliasHandlersWithProfiles(t *testing.T, profileDoc string) *api.Handlers {
	t.Helper()
	exec := policy.NewExecutor(nil, nil, policy.Default(), 3, 0)
	for name, doc := range map[string]string{"auto": aliasDocYAML, "profiles": profileDoc} {
		p, err := policy.LoadModelDocBytes([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		if err := exec.SetNamedPolicy(name, p); err != nil {
			t.Fatal(err)
		}
	}
	return api.NewHandlers(aliasStorage{models: []string{"coder", "chat"}}, nil, make(chan *domain.PendingRequest, 1),
		time.Second, exec, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, tokenizer.NewEstimator())
}

// TestAliasMiddleware_FailureStatus: a failed decision answers with a status
// the client can act on; only an outage is a 503.
func TestAliasMiddleware_FailureStatus(t *testing.T) {
	long := `{"model":"auto","messages":[{"role":"user","content":"` + strings.Repeat("word ", 400) + `"}]}`
	short := `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`
	tests := []struct {
		name     string
		handlers func(t *testing.T) *api.Handlers
		body     string
		wantCode int
		wantErr  string
	}{
		{
			// Every candidate's window is too small: the client must shrink the
			// prompt, exactly like the concrete-model max_input_tokens cap.
			name:     "too long for every window",
			handlers: func(t *testing.T) *api.Handlers { return newAliasHandlersWithProfiles(t, smallWindowProfiles) },
			body:     long, wantCode: http.StatusBadRequest, wantErr: string(domain.ErrCodeInputTooLong),
		},
		{
			name:     "fits",
			handlers: func(t *testing.T) *api.Handlers { return newAliasHandlersWithProfiles(t, smallWindowProfiles) },
			body:     short, wantCode: http.StatusOK,
		},
		{
			// No healthy agent: a real outage, retrying later can help.
			name:     "all down",
			handlers: func(t *testing.T) *api.Handlers { return newAliasHandlers(t, map[string]int{}) },
			body:     short, wantCode: http.StatusServiceUnavailable, wantErr: string(domain.ErrCodeBackendUnavailable),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out seen
			w := post(aliasRouter(tc.handlers(t), noAuth, &out), "/v1/chat/completions", tc.body)
			if w.Code != tc.wantCode || !strings.Contains(w.Body.String(), tc.wantErr) {
				t.Errorf("status %d body %s, want %d %s", w.Code, w.Body.String(), tc.wantCode, tc.wantErr)
			}
		})
	}
}

// TestAliasMiddleware_ForbiddenHidesOtherModels: a restricted key's 403 must
// not name the fleet models it cannot use.
func TestAliasMiddleware_ForbiddenHidesOtherModels(t *testing.T) {
	var out seen
	w := post(aliasRouter(newAliasHandlers(t, nil), allowOnly("other"), &out), "/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d", w.Code)
	}
	if b := w.Body.String(); strings.Contains(b, "coder") || strings.Contains(b, "chat") {
		t.Errorf("403 body discloses models the key cannot use: %s", b)
	}
}

// TestAliasMiddleware_TaskHeaderCap: an oversized task id is rejected before
// it can reach the pin store or the decision log.
func TestAliasMiddleware_TaskHeaderCap(t *testing.T) {
	var out seen
	r := aliasRouter(newAliasHandlers(t, nil), noAuth, &out)
	for _, tc := range []struct {
		name     string
		header   string
		wantCode int
	}{
		{"at the cap", strings.Repeat("t", semantic.MaxTaskIDLen), http.StatusOK},
		{"over the cap", strings.Repeat("t", semantic.MaxTaskIDLen+1), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(semantic.TaskIDHeader, tc.header)
			r.ServeHTTP(w, req)
			if w.Code != tc.wantCode {
				t.Errorf("status %d, want %d: %s", w.Code, tc.wantCode, w.Body.String())
			}
		})
	}
}
