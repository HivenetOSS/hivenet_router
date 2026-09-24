// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package semantic_test covers the semantic (model-level) alias resolver:
// request parsing in both dialects, structural features, route scoring,
// candidate filtering, task pinning and the decision log.
package semantic_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/policy"
	"hivenet_router/internal/semantic"
)

// aliasYAML is the alias used across tests: an agentic route (pinned), a
// software route, a science route and a general default.
const aliasYAML = `
models: ["auto"]
alias:
  default_route: general
  abstain_below: 0.3
  routes:
    - name: agentic
      pin_task: true
      pin_ttl: 10m
      signals:
        - { feature: has_tools, weight: 0.4 }
        - { feature: tool_result_turns, gte: 2, weight: 0.4 }
        - { keywords: [research, autonomously], weight: 0.2 }
      candidates: [ { model: big }, { model: small } ]
    - name: software
      signals:
        - { feature: code_presence, weight: 0.4 }
        - { feature: stack_trace, weight: 0.4 }
        - { keywords: [bug, compile, refactor], weight: 0.2 }
      candidates: [ { model: small }, { model: big } ]
    - name: science
      signals:
        - { feature: math_presence, weight: 0.5 }
        - { keywords: [physics, derivative, molecule, "quantum mechanics"], weight: 0.5 }
      candidates: [ { model: big } ]
    - name: general
      prefer: smallest
      candidates: [ { model: big }, { model: small } ]
`

func mustAlias(t testing.TB, doc string) *policy.AliasSpec {
	t.Helper()
	p, err := policy.LoadModelDocBytes([]byte(doc))
	if err != nil {
		t.Fatalf("alias: %v", err)
	}
	return p.Alias
}

func bptr(b bool) *bool { return &b }

// profiles: "big" is a 35B model that cannot see images; "small" a 3B model
// without tool calling and a small context window.
func testProfiles() map[string]*policy.ModelProfile {
	return map[string]*policy.ModelProfile{
		"big":   {ContextWindow: 131072, ParamsB: 35, Capabilities: policy.ModelCapabilities{ToolCalling: bptr(true), Vision: bptr(false), Reasoning: bptr(true)}},
		"small": {ContextWindow: 8192, ParamsB: 3, Capabilities: policy.ModelCapabilities{ToolCalling: bptr(false), Vision: bptr(true), StructuredOutput: bptr(true)}},
	}
}

func view(t testing.TB, body string, d semantic.Dialect) *semantic.RequestView {
	t.Helper()
	v, err := semantic.ParseView([]byte(body), d)
	if err != nil {
		t.Fatalf("ParseView: %v", err)
	}
	return v
}

func userMsg(text string) string {
	b, _ := json.Marshal(map[string]any{"model": "auto", "messages": []map[string]any{{"role": "user", "content": text}}})
	return string(b)
}

// TestParseView_OpenAI covers tool definitions, assistant tool_calls, tool
// results (including a failure), images, json_schema and reasoning_effort.
func TestParseView_OpenAI(t *testing.T) {
	body := `{"model":"auto","reasoning_effort":"high","response_format":{"type":"json_schema","json_schema":{}},
	"tools":[{"type":"function"},{"type":"function"}],
	"messages":[
	  {"role":"system","content":"You are a coder."},
	  {"role":"user","content":[{"type":"text","text":"fix this"},{"type":"image_url","image_url":{"url":"data:x"}}]},
	  {"role":"assistant","content":null,"tool_calls":[{"id":"1"}]},
	  {"role":"tool","tool_call_id":"1","content":"Error: file not found"},
	  {"role":"assistant","content":"","tool_calls":[{"id":"2"}]},
	  {"role":"tool","tool_call_id":"2","content":"ok"},
	  {"role":"user","content":"thanks"}]}`
	v := view(t, body, semantic.DialectOpenAI)
	if v.ToolCount != 2 || !v.StructuredOut || !v.ReasoningAsked {
		t.Errorf("tools=%d structured=%v reasoning=%v", v.ToolCount, v.StructuredOut, v.ReasoningAsked)
	}
	if v.System != "You are a coder." || v.FirstUserText != "fix this" {
		t.Errorf("system=%q first=%q", v.System, v.FirstUserText)
	}
	f := features(t, v)
	want := map[string]float64{"has_tools": 1, "tool_count": 2, "tool_result_turns": 2, "tool_error_count": 1,
		"assistant_tool_call_turns": 2, "turn_count": 2, "message_count": 6, "image_count": 1,
		"needs_structured_output": 1, "needs_reasoning": 1}
	for k, w := range want {
		if got, _ := f.Value(k); got != w {
			t.Errorf("%s = %g, want %g", k, got, w)
		}
	}
}

// TestParseView_Anthropic covers a block-array system prompt, tool_use,
// tool_result with is_error, images and thinking.
func TestParseView_Anthropic(t *testing.T) {
	body := `{"model":"auto","thinking":{"type":"enabled","budget_tokens":1024},
	"system":[{"type":"text","text":"You are Claude Code."}],
	"tools":[{"name":"Bash"}],
	"messages":[
	  {"role":"user","content":"refactor the parser"},
	  {"role":"assistant","content":[{"type":"text","text":"ok"},{"type":"tool_use","id":"t1","name":"Bash"}]},
	  {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","is_error":true,"content":"boom"}]},
	  {"role":"user","content":[{"type":"image","source":{}},{"type":"text","text":"see screenshot"}]}]}`
	v := view(t, body, semantic.DialectAnthropic)
	f := features(t, v)
	want := map[string]float64{"has_tools": 1, "tool_result_turns": 1, "tool_error_count": 1,
		"assistant_tool_call_turns": 1, "turn_count": 2, "image_count": 1, "needs_reasoning": 1,
		"system_prompt_bytes": float64(len("You are Claude Code."))}
	for k, w := range want {
		if got, _ := f.Value(k); got != w {
			t.Errorf("%s = %g, want %g", k, got, w)
		}
	}
	if v.FirstUserText != "refactor the parser" {
		t.Errorf("first user text = %q", v.FirstUserText)
	}
}

// TestParseView_ReasoningOff: explicit "none"/"disabled" do not require a
// reasoning model.
func TestParseView_ReasoningOff(t *testing.T) {
	for _, body := range []string{
		`{"messages":[],"reasoning_effort":"none"}`,
		`{"messages":[],"thinking":{"type":"disabled"}}`,
		`{"messages":[],"reasoning":{"effort":"none"}}`,
	} {
		if v := view(t, body, semantic.DialectOpenAI); v.ReasoningAsked {
			t.Errorf("%s: reasoning should not be required", body)
		}
	}
}

func features(t testing.TB, v *semantic.RequestView) *semantic.Features {
	t.Helper()
	f := &semantic.Features{}
	semantic.Structural{}.Extract(v, semantic.ExtractOptions{RecentTurns: 3, StripMarkers: []string{"<system-reminder>", "</system-reminder>"}}, f)
	return f
}

// TestStructural_TextMarkers: lexical detectors over the recent human turns.
func TestStructural_TextMarkers(t *testing.T) {
	tests := []struct {
		name, text, feature string
		want                float64
	}{
		{"fenced code", "look:\n```go\nx := 1\n```", "code_presence", 1},
		{"code lines", "def f(x):\n    return x\nimport os", "code_presence", 1},
		{"prose is not code", "What is the capital of France?", "code_presence", 0},
		{"python traceback", "Traceback (most recent call last):\n  File \"a.py\", line 3", "stack_trace", 1},
		{"go panic", "panic: runtime error: index out of range\ngoroutine 1 [running]:", "stack_trace", 1},
		{"java trace", "java.lang.NullPointerException\n    at com.x.Foo.bar(Foo.java:12)", "stack_trace", 1},
		{"no trace", "the build failed yesterday", "stack_trace", 0},
		{"latex", `Compute \frac{a}{b} and \int_0^1 x dx`, "math_presence", 1},
		{"unicode math", "Show that ∑ n ≤ ∞", "math_presence", 1},
		{"no math", "Plan a trip to Rome", "math_presence", 0},
		{"reminder stripped", "hello <system-reminder>```code```</system-reminder> world", "code_presence", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := features(t, view(t, userMsg(tc.text), semantic.DialectOpenAI))
			if got, _ := f.Value(tc.feature); got != tc.want {
				t.Errorf("%s = %g, want %g", tc.feature, got, tc.want)
			}
		})
	}
}

// TestStructural_RecentTurnsWindow: only the last N human turns feed keyword
// rules, so an old topic does not keep steering the conversation.
func TestStructural_RecentTurnsWindow(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"OLD physics"},{"role":"assistant","content":"a"},
	{"role":"user","content":"two"},{"role":"user","content":"three"},{"role":"user","content":"four"}]}`
	f := features(t, view(t, body, semantic.DialectOpenAI))
	if strings.Contains(f.Window(), "physics") {
		t.Errorf("window %q should only hold the last 3 human turns", f.Window())
	}
	if !strings.Contains(f.Window(), "four") {
		t.Errorf("window %q missing latest turn", f.Window())
	}
}

// TestKnownFeaturesAreAllProduced keeps policy.KnownSignalFeatures and the
// features the resolver produces in sync (the documented sync point).
func TestKnownFeaturesAreAllProduced(t *testing.T) {
	r := semantic.NewResolver(nil)
	spec := mustAlias(t, aliasYAML)
	d, err := r.Resolve("auto", spec, testProfiles(), view(t, userMsg("hi"), semantic.DialectOpenAI), semantic.Env{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range policy.KnownSignalFeatures() {
		if _, ok := d.Features.Value(name); !ok {
			t.Errorf("feature %q is accepted by the loader but never produced", name)
		}
	}
}

// TestResolve_Routing: which route and model each kind of request gets.
func TestResolve_Routing(t *testing.T) {
	spec := mustAlias(t, aliasYAML)
	tools := `"tools":[{"type":"function"}],`
	tests := []struct {
		name       string
		body       string
		env        semantic.Env
		wantRoute  string
		wantModel  string
		wantSource string
	}{
		{
			// Nothing matches → default route; prefer: smallest picks the 3B.
			name: "plain chat abstains to default", body: userMsg("hello there"),
			wantRoute: "general", wantModel: "small", wantSource: semantic.SourceDefault,
		},
		{
			// Stack trace + keyword → software; first candidate is small.
			name: "stack trace goes to software", body: userMsg("got this bug:\nTraceback (most recent call last):\n  File \"x.py\", line 1"),
			wantRoute: "software", wantModel: "small", wantSource: semantic.SourceScored,
		},
		{
			name: "math goes to science", body: userMsg(`What is the derivative of \frac{1}{x}?`),
			wantRoute: "science", wantModel: "big", wantSource: semantic.SourceScored,
		},
		{
			// Tools + two tool results → agentic; small lacks tool calling anyway.
			name: "tool loop goes to agentic",
			body: `{` + tools + `"messages":[{"role":"user","content":"research this"},
			  {"role":"assistant","tool_calls":[{"id":"1"}]},{"role":"tool","content":"r1"},
			  {"role":"assistant","tool_calls":[{"id":"2"}]},{"role":"tool","content":"r2"}]}`,
			wantRoute: "agentic", wantModel: "big", wantSource: semantic.SourceScored,
		},
		{
			// Software route's first candidate (small) cannot call tools → next candidate.
			name:      "capability filter skips small for tools",
			body:      `{` + tools + `"messages":[{"role":"user","content":"refactor this\n` + "```go\\nfunc a(){}\\n```" + `"}]}`,
			wantRoute: "software", wantModel: "big", wantSource: semantic.SourceScored,
		},
		{
			// Default route prefers small, but small is not allowed for this key.
			name: "allowlist excludes small", body: userMsg("hello"),
			env:       semantic.Env{Allowed: func(m string) bool { return m == "big" }},
			wantRoute: "general", wantModel: "big", wantSource: semantic.SourceDefault,
		},
		{
			// Science only lists big; big is unhealthy → fall back to default route.
			name: "unhealthy route falls back to default", body: userMsg(`derivative of \frac{1}{x}`),
			env:       semantic.Env{Healthy: func(m string) bool { return m != "big" }},
			wantRoute: "general", wantModel: "small", wantSource: semantic.SourceFallback,
		},
		{
			// Image → big (no vision) excluded; default prefers small anyway.
			name:      "vision filter",
			body:      `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}},{"type":"text","text":"physics"}]}]}`,
			wantRoute: "general", wantModel: "small", wantSource: semantic.SourceFallback,
		},
		{
			// Long prompt exceeds small's 8k window → big.
			name: "context window filter", body: userMsg("hello"),
			env:       semantic.Env{EstimateTokens: func(string) int { return 20000 }},
			wantRoute: "general", wantModel: "big", wantSource: semantic.SourceDefault,
		},
		{
			name: "count_tokens uses default route", body: userMsg("Traceback (most recent call last):"),
			env:       semantic.Env{CountOnly: true},
			wantRoute: "general", wantModel: "small", wantSource: semantic.SourceCount,
		},
		{
			// A candidate that is itself an alias is skipped.
			name: "alias candidate skipped", body: userMsg("hello"),
			env:       semantic.Env{IsAlias: func(m string) bool { return m == "small" }},
			wantRoute: "general", wantModel: "big", wantSource: semantic.SourceDefault,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := semantic.NewResolver(nil)
			d, err := r.Resolve("auto", spec, testProfiles(), view(t, tc.body, semantic.DialectOpenAI), tc.env)
			if err != nil {
				t.Fatalf("Resolve: %v (filtered=%v)", err, d.FilteredOut)
			}
			if d.Route != tc.wantRoute || d.Model != tc.wantModel || d.Source != tc.wantSource {
				t.Errorf("got route=%s model=%s source=%s, want %s/%s/%s (scores=%v filtered=%v)",
					d.Route, d.Model, d.Source, tc.wantRoute, tc.wantModel, tc.wantSource, d.RouteScores, d.FilteredOut)
			}
		})
	}
}

// TestResolve_NoEligibleModel: every candidate filtered → error with reasons.
func TestResolve_NoEligibleModel(t *testing.T) {
	spec := mustAlias(t, aliasYAML)
	r := semantic.NewResolver(nil)
	_, err := r.Resolve("auto", spec, testProfiles(), view(t, userMsg("hi"), semantic.DialectOpenAI),
		semantic.Env{Allowed: func(string) bool { return false }})
	if !errors.Is(err, semantic.ErrNoEligibleModel) {
		t.Fatalf("err = %v, want ErrNoEligibleModel", err)
	}
}

// TestResolve_UnknownProfilePasses: a model without a profile is never
// excluded on capability grounds.
func TestResolve_UnknownProfilePasses(t *testing.T) {
	spec := mustAlias(t, aliasYAML)
	r := semantic.NewResolver(nil)
	body := `{"tools":[{}],"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`
	d, err := r.Resolve("auto", spec, nil, view(t, body, semantic.DialectOpenAI), semantic.Env{})
	if err != nil || d.Model == "" {
		t.Fatalf("model=%q err=%v", d.Model, err)
	}
}

// TestResolve_Pinning: a pinned task keeps its model even when later turns
// look different; an unhealthy pinned model is re-routed; pins expire; the
// task header separates tasks that share a fingerprint.
func TestResolve_Pinning(t *testing.T) {
	spec := mustAlias(t, aliasYAML)
	pins := semantic.NewPinStore(0)
	now := time.Unix(1_700_000_000, 0)
	pins.SetClock(func() time.Time { return now })
	r := semantic.NewResolver(pins)
	sys := `{"role":"system","content":"agent harness"}`
	first := `{"role":"user","content":"research the market autonomously"}`
	start := `{"tools":[{}],"messages":[` + sys + `,` + first + `]}`
	later := `{"messages":[` + sys + `,` + first + `,{"role":"assistant","content":"done"},{"role":"user","content":"hello"}]}`
	env := semantic.Env{KeyID: "k1"}

	d1, err := r.Resolve("auto", spec, testProfiles(), view(t, start, semantic.DialectOpenAI), env)
	if err != nil || d1.Route != "agentic" || d1.Model != "big" {
		t.Fatalf("first call: route=%s model=%s err=%v", d1.Route, d1.Model, err)
	}
	// Same task, a later turn with no tools: stays pinned to big.
	d2, _ := r.Resolve("auto", spec, testProfiles(), view(t, later, semantic.DialectOpenAI), env)
	if d2.Source != semantic.SourcePinned || d2.Model != "big" || d2.TaskKey != d1.TaskKey {
		t.Errorf("second call: source=%s model=%s key=%s (first key %s)", d2.Source, d2.Model, d2.TaskKey, d1.TaskKey)
	}
	// Different API key → different fingerprint → no pin.
	d3, _ := r.Resolve("auto", spec, testProfiles(), view(t, later, semantic.DialectOpenAI), semantic.Env{KeyID: "k2"})
	if d3.Source == semantic.SourcePinned {
		t.Error("another key must not share the pin")
	}
	// Pinned model becomes unhealthy → re-routed, not pinned.
	d4, _ := r.Resolve("auto", spec, testProfiles(), view(t, later, semantic.DialectOpenAI),
		semantic.Env{KeyID: "k1", Healthy: func(m string) bool { return m != "big" }})
	if d4.Source == semantic.SourcePinned || d4.Model == "big" {
		t.Errorf("unhealthy pin: source=%s model=%s", d4.Source, d4.Model)
	}
	// Re-pin, then let it expire.
	_, _ = r.Resolve("auto", spec, testProfiles(), view(t, start, semantic.DialectOpenAI), env)
	now = now.Add(11 * time.Minute)
	d5, _ := r.Resolve("auto", spec, testProfiles(), view(t, later, semantic.DialectOpenAI), env)
	if d5.Source == semantic.SourcePinned {
		t.Error("pin should have expired after pin_ttl")
	}
	// Explicit task header wins over the fingerprint.
	hdr := semantic.Env{KeyID: "k1", TaskHeader: "task-42"}
	d6, _ := r.Resolve("auto", spec, testProfiles(), view(t, start, semantic.DialectOpenAI), hdr)
	if !strings.HasPrefix(d6.TaskKey, "h:") {
		t.Errorf("task key %q should come from the header", d6.TaskKey)
	}
}

// TestTaskKey_Stability: the fingerprint ignores later turns and differs
// across tasks.
func TestTaskKey_Stability(t *testing.T) {
	a := view(t, `{"messages":[{"role":"system","content":"s"},{"role":"user","content":"task A"}]}`, semantic.DialectOpenAI)
	a2 := view(t, `{"messages":[{"role":"system","content":"s"},{"role":"user","content":"task A"},{"role":"assistant","content":"x"},{"role":"user","content":"more"}]}`, semantic.DialectOpenAI)
	b := view(t, `{"messages":[{"role":"system","content":"s"},{"role":"user","content":"task B"}]}`, semantic.DialectOpenAI)
	if semantic.TaskKey("", "k", a) != semantic.TaskKey("", "k", a2) {
		t.Error("fingerprint must be stable across turns of one task")
	}
	if semantic.TaskKey("", "k", a) == semantic.TaskKey("", "k", b) {
		t.Error("different tasks must not collide")
	}
}

// TestPinStore_Bounded: the store never exceeds its cap.
func TestPinStore_Bounded(t *testing.T) {
	s := semantic.NewPinStore(3)
	for i := range 10 {
		s.Put(string(rune('a'+i)), "m", "r", time.Minute)
	}
	if s.Len() > 3 {
		t.Errorf("len = %d, want <= 3", s.Len())
	}
}

// TestDecisionLog: records land as JSON lines; a nil log is a no-op.
func TestDecisionLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.jsonl")
	l, err := semantic.OpenDecisionLog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	l.Write(semantic.DecisionRecord{Alias: "auto", ResolvedModel: "big", Route: "agentic", Source: "scored",
		RouteScores: map[string]float64{"agentic": 0.8}, DecisionUS: 42})
	l.Write(semantic.DecisionRecord{Alias: "auto", Source: "default", Error: "no eligible"})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	var lines []semantic.DecisionRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec semantic.DecisionRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		lines = append(lines, rec)
	}
	if len(lines) != 2 || lines[0].ResolvedModel != "big" || lines[0].RouteScores["agentic"] != 0.8 {
		t.Errorf("records = %+v", lines)
	}
	var nilLog *semantic.DecisionLog
	nilLog.Write(semantic.DecisionRecord{}) // must not panic
	_ = nilLog.Close()
}

// claudeCodeLikeBody builds a ~200 KB agentic Anthropic request: a large
// system prompt, tool schemas and a long tool-use history.
func claudeCodeLikeBody(tb testing.TB) []byte {
	tb.Helper()
	tools := make([]map[string]any, 30)
	for i := range tools {
		tools[i] = map[string]any{"name": "Tool" + string(rune('A'+i%26)), "input_schema": map[string]any{"type": "object", "description": strings.Repeat("schema text ", 60)}}
	}
	msgs := []map[string]any{{"role": "user", "content": "Refactor the storage layer and add tests"}}
	for len(msgs) < 120 {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "running"}, {"type": "tool_use", "id": "t", "name": "Bash", "input": map[string]any{"cmd": "go test ./..."}}}},
			map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "t", "content": strings.Repeat("ok  \tpkg/x\t0.1s\n", 110)}}},
		)
	}
	b, _ := json.Marshal(map[string]any{"model": "auto", "system": strings.Repeat("You are an agent. ", 1500), "tools": tools, "messages": msgs, "max_tokens": 4096})
	return b
}

// BenchmarkResolve_ClaudeCodeLike measures the full alias decision (parse +
// features + scoring + filter) on a ~200 KB agentic request (PRD N2 target:
// structural path <= 1 ms).
func BenchmarkResolve_ClaudeCodeLike(b *testing.B) {
	body := claudeCodeLikeBody(b)
	spec := mustAlias(b, aliasYAML)
	r := semantic.NewResolver(nil)
	profiles := testProfiles()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		v, err := semantic.ParseView(body, semantic.DialectAnthropic)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := r.Resolve("auto", spec, profiles, v, semantic.Env{KeyID: "k"}); err != nil {
			b.Fatal(err)
		}
	}
}

// TestResolve_ClaudeCodeLikeIsAgentic: the benchmark body routes as agentic.
func TestResolve_ClaudeCodeLikeIsAgentic(t *testing.T) {
	body := claudeCodeLikeBody(t)
	if len(body) < 150_000 {
		t.Fatalf("body only %d bytes", len(body))
	}
	d, err := semantic.NewResolver(nil).Resolve("auto", mustAlias(t, aliasYAML), testProfiles(),
		view(t, string(body), semantic.DialectAnthropic), semantic.Env{})
	if err != nil || d.Route != "agentic" {
		t.Fatalf("route=%s err=%v scores=%v", d.Route, err, d.RouteScores)
	}
}

// TestToolFailureHeuristic: OpenAI tool messages carry no is_error flag, so a
// failure is inferred from the head/tail of the output.
func TestToolFailureHeuristic(t *testing.T) {
	long := strings.Repeat("compiling...\n", 200)
	tests := []struct {
		name, output string
		want         float64
	}{
		{"error prefix", "Error: no such file", 1},
		{"exception prefix", "  Exception: boom", 1},
		{"python traceback", "Traceback (most recent call last):\n  File x", 1},
		{"non-zero exit at end of long output", long + "Process finished with exit code 2", 1},
		{"zero exit", long + "exit code 0", 0},
		{"failure buried mid-output is not probed", long + "Error: x\n" + long, 0},
		{"word starting with error", "errors_total=0 all good", 0},
		{"clean output", "ok  \tpkg\t0.1s", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := json.Marshal(map[string]any{"messages": []map[string]any{
				{"role": "assistant", "tool_calls": []map[string]any{{"id": "1"}}},
				{"role": "tool", "tool_call_id": "1", "content": tc.output}}})
			f := features(t, view(t, string(b), semantic.DialectOpenAI))
			if got, _ := f.Value("tool_error_count"); got != tc.want {
				t.Errorf("tool_error_count = %g, want %g", got, tc.want)
			}
		})
	}
}
