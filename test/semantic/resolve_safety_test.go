// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/semantic"
)

// pinDoc builds a one-route alias that always pins to the given candidates.
func pinDoc(alias, candidates string) string {
	return `
models: ["` + alias + `"]
alias:
  default_route: agentic
  routes:
    - name: agentic
      pin_task: true
      candidates: [` + candidates + `]
`
}

// TestResolve_PinStaysInsideAliasCandidates: a pin can only ever return a model
// that is a current candidate of the alias that is being resolved.
func TestResolve_PinStaysInsideAliasCandidates(t *testing.T) {
	body := `{"tools":[{}],"messages":[{"role":"user","content":"research autonomously"}]}`
	env := semantic.Env{KeyID: "k", TaskHeader: "task-1"}
	tests := []struct {
		name       string
		first      string // doc resolved first (pins its model)
		second     string // doc resolved next with the same task header
		alias2     string
		wantModel  string
		wantReason string // FilteredOut reason recorded for the old pin ("" = none)
	}{
		{
			// Same header, another alias: the first alias's pin must not leak.
			name: "other alias", first: pinDoc("auto", "{ model: big }"),
			second: pinDoc("auto-fast", "{ model: small }"), alias2: "auto-fast",
			wantModel: "small",
		},
		{
			// Hot reload removed big from the route: the pin is dropped.
			name: "candidate removed by reload", first: pinDoc("auto", "{ model: big }"),
			second: pinDoc("auto", "{ model: small }"), alias2: "auto",
			wantModel: "small", wantReason: "pinned_but_not_a_candidate",
		},
		{
			// Unchanged config: the pin holds.
			name: "unchanged config keeps pin", first: pinDoc("auto", "{ model: big }, { model: small }"),
			second: pinDoc("auto", "{ model: small }, { model: big }"), alias2: "auto",
			wantModel: "big",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := semantic.NewResolver(nil)
			v := view(t, body, semantic.DialectOpenAI)
			first := mustAlias(t, tc.first)
			if _, err := r.Resolve("auto", first, nil, v, env); err != nil {
				t.Fatal(err)
			}
			d, err := r.Resolve(tc.alias2, mustAlias(t, tc.second), nil, v, env)
			if err != nil {
				t.Fatal(err)
			}
			if d.Model != tc.wantModel {
				t.Errorf("model = %s, want %s (source %s)", d.Model, tc.wantModel, d.Source)
			}
			if tc.wantReason != "" && d.FilteredOut["big"] != tc.wantReason {
				t.Errorf("FilteredOut[big] = %q, want %q", d.FilteredOut["big"], tc.wantReason)
			}
		})
	}
}

// TestResolve_TriesEveryRoute: when the scored and default routes have no
// usable candidate, another route's candidate is used, so an alias the key can
// see in /v1/models (any candidate allowed) also resolves.
func TestResolve_TriesEveryRoute(t *testing.T) {
	spec := mustAlias(t, `
models: ["auto"]
alias:
  default_route: general
  routes:
    - name: software
      signals: [ { feature: stack_trace, weight: 1 } ]
      candidates: [ { model: coder } ]
    - name: general
      candidates: [ { model: chat } ]
    - name: science
      candidates: [ { model: mathy } ]
`)
	d, err := semantic.NewResolver(nil).Resolve("auto", spec, nil,
		view(t, userMsg("Traceback (most recent call last):"), semantic.DialectOpenAI),
		semantic.Env{Allowed: func(m string) bool { return m == "mathy" }})
	if err != nil || d.Model != "mathy" || d.Route != "science" || d.Source != semantic.SourceFallback {
		t.Fatalf("model=%q route=%q source=%q err=%v", d.Model, d.Route, d.Source, err)
	}
}

// TestClassify: failure reasons map to an actionable class.
func TestClassify(t *testing.T) {
	tests := []struct {
		name     string
		filtered map[string]string
		want     semantic.Failure
	}{
		{"all forbidden", map[string]string{"a": "not_allowed", "b": "not_allowed"}, semantic.FailureForbidden},
		{"forbidden plus dropped pin", map[string]string{"a": "not_allowed", "b": "pinned_but_not_a_candidate"}, semantic.FailureForbidden},
		{"allowed ones too long", map[string]string{"a": "not_allowed", "b": "context_window(est=9,out=0>limit=1)"}, semantic.FailureTooLong},
		{"allowed ones lack capability", map[string]string{"a": "no_vision", "b": "context_window(est=9,out=0>limit=1)"}, semantic.FailureUnsupported},
		{"one down", map[string]string{"a": "no_vision", "b": "no_healthy_agent"}, semantic.FailureUnavailable},
		{"nothing recorded", map[string]string{}, semantic.FailureUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := semantic.Classify(tc.filtered); got != tc.want {
				t.Errorf("Classify = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolve_AllowlistReportedFirst: a model the key may not use is reported
// as not_allowed even when it also lacks a needed capability, so the answer is
// 403, not 503.
func TestResolve_AllowlistReportedFirst(t *testing.T) {
	spec := mustAlias(t, aliasYAML)
	img := `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`
	d, err := semantic.NewResolver(nil).Resolve("auto", spec, testProfiles(), view(t, img, semantic.DialectOpenAI),
		semantic.Env{Allowed: func(string) bool { return false }})
	if !errors.Is(err, semantic.ErrNoEligibleModel) || semantic.Classify(d.FilteredOut) != semantic.FailureForbidden {
		t.Errorf("err=%v filtered=%v", err, d.FilteredOut)
	}
}

// TestResolve_ContextReservesOutput: the context window must hold the prompt
// estimate plus max_tokens, as the backend enforces.
func TestResolve_ContextReservesOutput(t *testing.T) {
	spec := mustAlias(t, aliasYAML) // default route prefers small (8k window)
	est := semantic.Env{EstimateTokens: func(string) int { return 5000 }}
	for _, tc := range []struct {
		name, body, want string
	}{
		{"fits without output", `{"messages":[{"role":"user","content":"hi"}]}`, "small"},
		{"max_tokens overflows small", `{"max_tokens":4000,"messages":[{"role":"user","content":"hi"}]}`, "big"},
		{"max_completion_tokens counts too", `{"max_completion_tokens":4000,"messages":[{"role":"user","content":"hi"}]}`, "big"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := semantic.NewResolver(nil).Resolve("auto", spec, testProfiles(), view(t, tc.body, semantic.DialectOpenAI), est)
			if err != nil || d.Model != tc.want {
				t.Errorf("model=%q err=%v filtered=%v", d.Model, err, d.FilteredOut)
			}
		})
	}
}

// TestEstimatorBytes_MatchesPromptTextBytes locks the unit the context filter
// feeds the token estimator: it must equal domain.PromptTextBytes, the measure
// the estimator learns its per-model ratio on. Counting Anthropic tool_result
// text on one side only multiplied a learned ratio by the wrong byte count and
// over-estimated agentic prompts several times over.
func TestEstimatorBytes_MatchesPromptTextBytes(t *testing.T) {
	bigResult := strings.Repeat("ok  pkg/x 0.1s\n", 2000)
	tests := []struct {
		name    string
		body    string
		dialect semantic.Dialect
	}{
		{"openai tools and tool role", `{"tools":[{"type":"function","function":{"name":"f"}}],"messages":[
			{"role":"system","content":"sys"},{"role":"user","content":[{"type":"text","text":"fix"},{"type":"image_url","image_url":{"url":"x"}}]},
			{"role":"assistant","content":null,"tool_calls":[{"id":"1"}]},{"role":"tool","tool_call_id":"1","content":"result text"}]}`, semantic.DialectOpenAI},
		{"anthropic tool_result excluded", `{"system":[{"type":"text","text":"You are an agent."}],"tools":[{"name":"Bash"}],"messages":[
			{"role":"user","content":"refactor"},
			{"role":"assistant","content":[{"type":"text","text":"ok"},{"type":"tool_use","id":"t","name":"Bash","input":{}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":"` + strings.ReplaceAll(bigResult, "\n", `\n`) + `"},{"type":"text","text":"<system-reminder>r</system-reminder>"}]}]}`, semantic.DialectAnthropic},
		{"plain string system", `{"system":"be brief","messages":[{"role":"user","content":"hi"}]}`, semantic.DialectAnthropic},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req domain.ChatRequest
			if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
				t.Fatal(err)
			}
			want, _ := domain.PromptTextBytes(&req)
			if got := view(t, tc.body, tc.dialect).EstimatorBytes; got != want {
				t.Errorf("EstimatorBytes = %d, domain.PromptTextBytes = %d", got, want)
			}
		})
	}
}

// TestParseView_ReminderOnlyToolResultIsNotATurn: Claude Code appends
// <system-reminder> text to tool_result messages; those are not human turns,
// so they neither inflate turn_count nor push the real request out of the
// recent-turns window.
func TestParseView_ReminderOnlyToolResultIsNotATurn(t *testing.T) {
	msgs := []map[string]any{{"role": "user", "content": "fix the parser bug"}}
	for range 4 {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "t", "name": "Bash", "input": map[string]any{}}}},
			map[string]any{"role": "user", "content": []map[string]any{
				{"type": "tool_result", "tool_use_id": "t", "content": "ok"},
				{"type": "text", "text": "<system-reminder>todo list is empty</system-reminder>"},
			}},
		)
	}
	b, _ := json.Marshal(map[string]any{"tools": []any{map[string]any{"name": "Bash"}}, "messages": msgs})
	f := features(t, view(t, string(b), semantic.DialectAnthropic))
	if n, _ := f.Value("turn_count"); n != 1 {
		t.Errorf("turn_count = %g, want 1", n)
	}
	if !strings.Contains(f.Window(), "parser") || strings.Contains(f.Window(), "todo") {
		t.Errorf("window %q should hold the human request, without the reminder", f.Window())
	}
}
