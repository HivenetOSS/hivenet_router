// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/policy"
)

const profileDoc = `
models: ["m-big"]
profile:
  context_window: 131072
  params_b: 35
  active_params_b: 3
  capabilities: { tool_calling: true, vision: false }
`

const aliasDoc = `
models: ["auto"]
alias:
  default_route: general
  abstain_below: 0.3
  routes:
    - name: agentic
      pin_task: true
      signals:
        - { feature: has_tools, weight: 0.5 }
        - { feature: turn_count, gte: 6, weight: 0.3 }
        - { keywords: ["Research", " pipeline "], weight: 0.2 }
      candidates: [ { model: m-big }, { model: m-small } ]
    - name: general
      prefer: smallest
      candidates: [ { model: m-small } ]
`

// TestLoadModelDoc_ProfileOnly: a profile-only document loads without
// routing_policy and inherits the global routing.
func TestLoadModelDoc_ProfileOnly(t *testing.T) {
	p, err := policy.LoadModelDocBytes([]byte(profileDoc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !p.InheritsRouting() {
		t.Fatal("profile-only document should inherit routing")
	}
	pr := p.Profile
	if pr.ContextWindow != 131072 || pr.SizeB() != 3 {
		t.Errorf("profile = %+v, want context 131072 and size 3 (active params)", pr)
	}
	if pr.Capabilities.ToolCalling == nil || !*pr.Capabilities.ToolCalling {
		t.Error("tool_calling should be true")
	}
	if pr.Capabilities.Vision == nil || *pr.Capabilities.Vision {
		t.Error("vision should be explicitly false")
	}
	if pr.Capabilities.Reasoning != nil {
		t.Error("undeclared reasoning should stay nil (unknown)")
	}
}

// TestLoadModelDoc_AliasDefaults: defaults are filled, keywords normalised,
// pin TTL defaulted when pin_task is set.
func TestLoadModelDoc_AliasDefaults(t *testing.T) {
	p, err := policy.LoadModelDocBytes([]byte(aliasDoc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	a := p.Alias
	if a.ContextBuffer != 0.95 || a.RecentTurns != 3 {
		t.Errorf("defaults: context_buffer=%g recent_turns=%d", a.ContextBuffer, a.RecentTurns)
	}
	if len(a.StripMarkers) != 2 || a.StripMarkers[0] != "<system-reminder>" {
		t.Errorf("strip_markers default = %v", a.StripMarkers)
	}
	if got := a.Routes[0].Signals[2].Keywords; got[0] != "research" || got[1] != "pipeline" {
		t.Errorf("keywords not normalised: %q", got)
	}
	if d := a.Routes[0].PinDuration(); d != 30*time.Minute {
		t.Errorf("pin ttl = %v, want 30m default", d)
	}
	if d := a.Routes[1].PinDuration(); d != 0 {
		t.Errorf("non-pinned route ttl = %v, want 0", d)
	}
}

// TestLoadModelDoc_WithRouting: a document that declares routing and a profile
// validates both and claims routing as before.
func TestLoadModelDoc_WithRouting(t *testing.T) {
	doc := profileDoc + "routing_policy:\n  strategy: least-loaded\n"
	p, err := policy.LoadModelDocBytes([]byte(doc))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if p.InheritsRouting() {
		t.Error("document with routing_policy must not inherit")
	}
}

// TestLoadModelDoc_Errors: each malformed document is rejected with a
// message naming the problem.
func TestLoadModelDoc_Errors(t *testing.T) {
	route := func(extra string) string {
		return "models: [auto]\nalias:\n  default_route: g\n  routes:\n    - name: g\n      candidates: [{model: m}]\n" + extra
	}
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"no routing and no semantic block", "models: [m]\n", "strategy is required"},
		{"inherit with admission fields", "models: [m]\nprofile: {context_window: 10}\nmax_input_tokens: 5\n", "routing_policy.strategy is required"},
		{"negative context window", "models: [m]\nprofile: {context_window: -1}\n", "context_window"},
		{"active exceeds total", "models: [m]\nprofile: {params_b: 3, active_params_b: 7}\n", "exceeds"},
		{"alias with profile", route("profile: {context_window: 1}\n"), "both alias and profile"},
		{"alias with routing", route("routing_policy: {strategy: least-loaded}\n"), "must not declare routing_policy"},
		{"alias without models", "alias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}]}]\n", "models"},
		{"no routes", "models: [auto]\nalias: {default_route: g}\n", "at least one route"},
		{"unknown default route", "models: [auto]\nalias:\n  default_route: nope\n  routes: [{name: g, candidates: [{model: m}]}]\n", "not one of the declared routes"},
		{"duplicate route", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}]}, {name: g, candidates: [{model: m}]}]\n", "duplicate route"},
		{"route without candidates", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g}]\n", "at least one model"},
		{"alias as own candidate", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: auto}]}]\n", "is an alias"},
		{"unknown feature", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}], signals: [{feature: vibes, weight: 1}]}]\n", "unknown feature"},
		{"feature and keywords", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}], signals: [{feature: has_tools, keywords: [x], weight: 1}]}]\n", "exactly one of feature or keywords"},
		{"negative weight", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}], signals: [{feature: has_tools, weight: -1}]}]\n", "weight"},
		{"bounds on keywords", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}], signals: [{keywords: [x], gte: 1, weight: 1}]}]\n", "gte/lte apply to features"},
		{"inverted bounds", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m}], signals: [{feature: turn_count, gte: 5, lte: 2, weight: 1}]}]\n", "gte (5) > lte (2)"},
		{"bad prefer", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, prefer: cheapest, candidates: [{model: m}]}]\n", "unknown prefer"},
		{"bad pin ttl", "models: [auto]\nalias:\n  default_route: g\n  routes: [{name: g, pin_ttl: soon, candidates: [{model: m}]}]\n", "invalid pin_ttl"},
		{"abstain out of range", "models: [auto]\nalias:\n  default_route: g\n  abstain_below: 1.5\n  routes: [{name: g, candidates: [{model: m}]}]\n", "abstain_below"},
		{"odd strip markers", "models: [auto]\nalias:\n  default_route: g\n  strip_markers: [\"<a>\"]\n  routes: [{name: g, candidates: [{model: m}]}]\n", "even count"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := policy.LoadModelDocBytes([]byte(tc.doc))
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

// TestLoadBytes_RejectsSemanticBlocks: the global policy loader refuses
// profile/alias, which are per-model concepts.
func TestLoadBytes_RejectsSemanticBlocks(t *testing.T) {
	for _, doc := range []string{profileDoc, aliasDoc} {
		if _, err := policy.LoadBytes([]byte(doc + "routing_policy: {strategy: least-loaded}\n")); err == nil {
			t.Error("LoadBytes accepted a semantic block")
		}
	}
}

// TestExecutor_SemanticIndexes: profiles and aliases are indexed by model,
// an inheriting document leaves the model on the global policy, and the
// indexes survive every kind of state write.
func TestExecutor_SemanticIndexes(t *testing.T) {
	global := policy.Default()
	exec := policy.NewExecutor(nil, nil, global, 3, 0)
	prof, err := policy.LoadModelDocBytes([]byte(profileDoc))
	if err != nil {
		t.Fatal(err)
	}
	alias, err := policy.LoadModelDocBytes([]byte(aliasDoc))
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.SetNamedPolicy("big", prof); err != nil {
		t.Fatal(err)
	}
	if err := exec.SetNamedPolicy("auto", alias); err != nil {
		t.Fatal(err)
	}

	check := func(stage string) {
		t.Helper()
		sv := exec.Semantic()
		if sv.Profiles["m-big"] == nil || sv.Profiles["m-big"].ContextWindow != 131072 {
			t.Errorf("%s: profile for m-big missing", stage)
		}
		if sv.Aliases["auto"] == nil || !exec.IsAlias("auto") {
			t.Errorf("%s: alias auto missing", stage)
		}
		if exec.IsAlias("m-big") {
			t.Errorf("%s: a model must not be reported as an alias", stage)
		}
		if got := exec.EffectivePolicy("m-big"); got != exec.GetPolicy() {
			t.Errorf("%s: inheriting document must leave m-big on the global policy", stage)
		}
	}
	check("after SetNamedPolicy")

	exec.SetPolicy(policy.Default()) // global reload must keep the semantic indexes
	check("after SetPolicy")

	exec.SetNamedPoliciesFromSnapshot(exec.GetNamedPolicies()) // SIGHUP path
	check("after SetNamedPoliciesFromSnapshot")

	exec.DeleteNamedPolicy("auto")
	if exec.IsAlias("auto") {
		t.Error("alias still present after DeleteNamedPolicy")
	}
}

// TestLoadDirSnapshot_AliasCollision: an alias document claiming a name that a
// model document already owns loses the conflict, like any other document.
func TestLoadDirSnapshot_AliasCollision(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string, age time.Duration) {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		ts := time.Now().Add(-age)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatal(err)
		}
	}
	write("big.yaml", profileDoc, 2*time.Hour)
	write("auto.yaml", aliasDoc, time.Hour)
	// An alias that tries to take over the name of an already-profiled model.
	write("clash.yaml", "models: [\"m-big\"]\nalias:\n  default_route: g\n  routes: [{name: g, candidates: [{model: m-small}]}]\n", 0)

	snap, err := policy.LoadDirSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Named["big"] == nil || snap.Named["auto"] == nil {
		t.Fatalf("expected big and auto to load, got %v", keys(snap.Named))
	}
	if _, ok := snap.Conflicted["clash"]; !ok {
		t.Errorf("clash.yaml should lose the conflict for m-big; conflicted=%v", snap.Conflicted)
	}
}

func keys(m map[string]*policy.Policy) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
