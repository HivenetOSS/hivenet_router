// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/policy"
	"hivenet_router/internal/semantic"
)

// TestLoadModelDoc_ScoringRules: each route's signal weights must sum to 1
// (so scores read the same way in every route), and min_score and the
// affinity knobs are range-checked.
func TestLoadModelDoc_ScoringRules(t *testing.T) {
	doc := func(alias, route string) string {
		return "models: [auto]\nalias:\n  default_route: g\n" + alias +
			"  routes:\n    - { name: g, candidates: [{model: m}]" + route + " }\n"
	}
	tests := []struct {
		name, doc, wantErr string
	}{
		{"weights sum to 1", doc("", ", signals: [{feature: has_tools, weight: 0.35}, {feature: stack_trace, weight: 0.35}, {keywords: [x], weight: 0.3}]"), ""},
		{"weights below 1", doc("", ", signals: [{feature: has_tools, weight: 0.5}, {feature: stack_trace, weight: 0.2}]"), "must sum to 1"},
		{"weights above 1", doc("", ", signals: [{feature: has_tools, weight: 1}, {feature: stack_trace, weight: 1}]"), "must sum to 1"},
		{"omitted weight", doc("", ", signals: [{feature: has_tools}]"), "must sum to 1"},
		{"no signals needs no weights", doc("", ""), ""},
		{"min_score in range", doc("", ", min_score: 0.6"), ""},
		{"min_score out of range", doc("", ", min_score: 1.5"), "min_score"},
		{"affinity ok", doc("  affinity_ttl: 5m\n  affinity_margin: 0.3\n", ""), ""},
		{"affinity bad duration", doc("  affinity_ttl: soon\n", ""), "affinity_ttl"},
		{"affinity margin out of range", doc("  affinity_ttl: 5m\n  affinity_margin: 2\n", ""), "affinity_margin"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := policy.LoadModelDocBytes([]byte(tc.doc))
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
	a := mustAlias(t, doc("", ""))
	if a.AffinityDuration() != 0 || a.AffinityMargin != 0.2 {
		t.Errorf("defaults: affinity_ttl=%v affinity_margin=%g, want off / 0.2", a.AffinityDuration(), a.AffinityMargin)
	}
}

// TestResolve_MinScore: a route can require more evidence than one weak
// signal before it may win; below its min_score the decision abstains.
func TestResolve_MinScore(t *testing.T) {
	spec := mustAlias(t, `
models: ["auto"]
alias:
  default_route: general
  routes:
    - name: helpdesk
      min_score: 0.6
      signals: [ { keywords: [help], weight: 0.5 }, { feature: has_tools, weight: 0.5 } ]
      candidates: [ { model: small } ]
    - name: general
      candidates: [ { model: big } ]
`)
	tests := []struct {
		name, body, wantModel, wantSource string
	}{
		// Keyword only: score 0.5 < min_score 0.6 → abstain to general.
		{"below min_score", userMsg("help me plan a trip"), "big", semantic.SourceDefault},
		// Keyword and tools: score 1.0 → helpdesk wins.
		{"meets min_score", `{"tools":[{}],"messages":[{"role":"user","content":"help me plan a trip"}]}`, "small", semantic.SourceScored},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := semantic.NewResolver(nil).Resolve("auto", spec, nil, view(t, tc.body, semantic.DialectOpenAI), semantic.Env{})
			if err != nil || d.Model != tc.wantModel || d.Source != tc.wantSource {
				t.Errorf("model=%q source=%q err=%v scores=%v", d.Model, d.Source, err, d.RouteScores)
			}
		})
	}
}

// affinityAlias: a coding route (big) and a general default (small), with a
// 5-minute conversation affinity and the default 0.2 switching margin.
const affinityAlias = `
models: ["auto"]
alias:
  default_route: general
  affinity_ttl: 5m
  routes:
    - name: coding
      signals: [ { feature: code_presence, weight: 0.5 }, { keywords: [refactor], weight: 0.5 } ]
      candidates: [ { model: big } ]
    - name: general
      candidates: [ { model: small } ]
`

// conversation builds an OpenAI chat whose first user turn is fixed (so the
// fingerprint is stable) and whose latest user turn is last.
func conversation(last string) string {
	if last == "" {
		return userMsg("hello")
	}
	return `{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"},{"role":"user","content":"` + last + `"}]}`
}

// TestResolve_Affinity: a conversation stays on the model that answered its
// previous turn unless the new route is clearly better (by affinity_margin);
// the sticky entry is dropped when its model is no longer eligible.
func TestResolve_Affinity(t *testing.T) {
	spec := mustAlias(t, affinityAlias)
	code := "```go\\nfunc a(){}\\n```"
	tests := []struct {
		name       string
		turns      []string     // latest user turn per request, in order
		lastEnv    semantic.Env // env of the last request (KeyID is set for all)
		wantModel  string       // model of the last request
		wantSource string
	}{
		{
			// Starts general; a turn with only half the coding evidence (0.5)
			// beats general (0) by more than 0.2, so it switches.
			name: "clear win switches", turns: []string{"", code},
			wantModel: "big", wantSource: semantic.SourceScored,
		},
		{
			// Starts on coding (score 1). A plain follow-up abstains to general
			// (score 0), which is not 0.2 above coding's 0: it stays on big.
			name: "follow-up stays", turns: []string{"refactor " + code, "thanks, what next?"},
			wantModel: "big", wantSource: semantic.SourceAffinity,
		},
		{
			// The sticky model loses its agent: the conversation moves on.
			name: "unhealthy sticky model is dropped", turns: []string{"refactor " + code, "thanks"},
			lastEnv:   semantic.Env{Healthy: func(m string) bool { return m != "big" }},
			wantModel: "small", wantSource: semantic.SourceDefault,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := semantic.NewResolver(nil)
			var d semantic.Decision
			var err error
			for i, turn := range tc.turns {
				env := semantic.Env{KeyID: "k"}
				if i == len(tc.turns)-1 {
					env = tc.lastEnv
					env.KeyID = "k"
				}
				d, err = r.Resolve("auto", spec, nil, view(t, conversation(turn), semantic.DialectOpenAI), env)
				if err != nil {
					t.Fatalf("turn %d: %v", i, err)
				}
			}
			if d.Model != tc.wantModel || d.Source != tc.wantSource {
				t.Errorf("last turn: model=%s source=%s, want %s/%s (scores %v)", d.Model, d.Source, tc.wantModel, tc.wantSource, d.RouteScores)
			}
		})
	}
}

// TestResolve_AffinityOffByDefault: without affinity_ttl every turn is decided
// afresh, and count_tokens never touches affinity.
func TestResolve_AffinityOffByDefault(t *testing.T) {
	spec := mustAlias(t, strings.Replace(affinityAlias, "  affinity_ttl: 5m\n", "", 1))
	r := semantic.NewResolver(nil)
	code := "```go\\nfunc a(){}\\n```"
	if d, _ := r.Resolve("auto", spec, nil, view(t, conversation("refactor "+code), semantic.DialectOpenAI), semantic.Env{KeyID: "k"}); d.Model != "big" {
		t.Fatalf("first turn model = %s", d.Model)
	}
	d, _ := r.Resolve("auto", spec, nil, view(t, conversation("thanks"), semantic.DialectOpenAI), semantic.Env{KeyID: "k"})
	if d.Model != "small" || d.Source == semantic.SourceAffinity {
		t.Errorf("affinity off: model=%s source=%s, want a fresh decision", d.Model, d.Source)
	}
	if r.Pins().Len() != 0 {
		t.Errorf("no route pins and affinity is off, but %d entries were stored", r.Pins().Len())
	}
}
