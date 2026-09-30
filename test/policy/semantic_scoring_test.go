// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy_test

import (
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/policy"
)

// scoringDoc builds a one-route alias; alias adds lines under "alias:" and
// route adds fields inside the route's flow mapping.
func scoringDoc(alias, route string) string {
	return "models: [auto]\nalias:\n  default_route: g\n" + alias +
		"  routes:\n    - { name: g, candidates: [{model: m}]" + route + " }\n"
}

// TestLoadModelDoc_ScoringRules: each route's signal weights must sum to 1
// (so scores read the same way in every route); min_score, pin_max_age, the
// affinity knobs and strip_markers are range-checked.
func TestLoadModelDoc_ScoringRules(t *testing.T) {
	tests := []struct {
		name, doc, wantErr string
	}{
		{"weights sum to 1", scoringDoc("", ", signals: [{feature: has_tools, weight: 0.35}, {feature: stack_trace, weight: 0.35}, {keywords: [x], weight: 0.3}]"), ""},
		{"weights below 1", scoringDoc("", ", signals: [{feature: has_tools, weight: 0.5}, {feature: stack_trace, weight: 0.2}]"), "must sum to 1"},
		{"weights above 1", scoringDoc("", ", signals: [{feature: has_tools, weight: 1}, {feature: stack_trace, weight: 1}]"), "must sum to 1"},
		{"omitted weight", scoringDoc("", ", signals: [{feature: has_tools}]"), "must sum to 1"},
		{"no signals needs no weights", scoringDoc("", ""), ""},
		{"min_score in range", scoringDoc("", ", min_score: 0.6"), ""},
		{"min_score out of range", scoringDoc("", ", min_score: 1.5"), "min_score"},
		{"affinity ok", scoringDoc("  affinity_ttl: 5m\n  affinity_margin: 0.3\n", ""), ""},
		{"affinity bad duration", scoringDoc("  affinity_ttl: soon\n", ""), "affinity_ttl"},
		{"affinity margin out of range", scoringDoc("  affinity_ttl: 5m\n  affinity_margin: 2\n", ""), "affinity_margin"},
		{"pin_max_age bad duration", scoringDoc("  pin_max_age: nope\n", ""), "pin_max_age"},
		{"empty strip marker", scoringDoc("  strip_markers: [\"<a>\", \"\"]\n", ""), "strip_markers"},
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
}

// TestLoadModelDoc_SemanticDefaults: pin_max_age defaults to 4h, affinity is
// off with a 0.2 margin.
func TestLoadModelDoc_SemanticDefaults(t *testing.T) {
	p, err := policy.LoadModelDocBytes([]byte(scoringDoc("", "")))
	if err != nil {
		t.Fatal(err)
	}
	a := p.Alias
	if a.PinMaxAgeDuration() != 4*time.Hour || a.AffinityDuration() != 0 || a.AffinityMargin != 0.2 {
		t.Errorf("defaults: pin_max_age=%v affinity_ttl=%v affinity_margin=%g", a.PinMaxAgeDuration(), a.AffinityDuration(), a.AffinityMargin)
	}
}
