// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"strings"
	"testing"

	"hivenet_router/internal/domain"
)

// A key's strategy replaces the ranking of every step — the primary step and
// each fallback_chain step — and nothing else: match filters and max_tries
// stay the policy's, and the stored policy is not changed.
func TestNewSessionWithStrategy_ReplacesOnlyTheRanking(t *testing.T) {
	pol := &Policy{
		RoutingPolicy: PolicyStep{Strategy: "prefix-aware", Match: MatchFilter{Region: "eu"}, MaxTries: 2},
		FallbackChain: []FallbackStep{{Name: "any", PolicyStep: PolicyStep{Strategy: "prefix-aware", MaxTries: 1}}},
	}
	e := NewExecutor(nil, nil, pol, 3, 0)

	s := e.NewSessionWithStrategy("m", "least-loaded", domain.CapabilityLLM)
	if len(s.steps) != 2 {
		t.Fatalf("%d steps, want 2", len(s.steps))
	}
	for i, st := range s.steps {
		if st.Strategy != "least-loaded" {
			t.Errorf("step %d strategy %q, want least-loaded", i, st.Strategy)
		}
	}
	if s.steps[0].Match.Region != "eu" || s.steps[0].maxTries != 2 || s.steps[1].maxTries != 1 {
		t.Errorf("match %+v, max tries %d/%d: want the policy's (eu, 2/1)", s.steps[0].Match, s.steps[0].maxTries, s.steps[1].maxTries)
	}
	if s.WantsPrefix() {
		t.Error("a least-loaded session must not ask for prefix anchors")
	}
	if pol.RoutingPolicy.Strategy != "prefix-aware" || pol.FallbackChain[0].Strategy != "prefix-aware" {
		t.Error("the stored policy was modified")
	}

	if s := e.NewSessionWithStrategy("m", "", domain.CapabilityLLM); s.steps[0].Strategy != "prefix-aware" || !s.WantsPrefix() {
		t.Errorf("empty strategy: got %q, want the policy's prefix-aware", s.steps[0].Strategy)
	}
}

// A key may turn prefix-aware routing on over a least-loaded policy.
func TestNewSessionWithStrategy_PrefixAwareOverLeastLoaded(t *testing.T) {
	e := NewExecutor(nil, nil, &Policy{RoutingPolicy: PolicyStep{Strategy: "least-loaded"}}, 3, 0)
	if s := e.NewSessionWithStrategy("m", "prefix-aware", domain.CapabilityLLM); !s.WantsPrefix() {
		t.Error("a prefix-aware session must ask for prefix anchors")
	}
}

func TestValidateKeyStrategies(t *testing.T) {
	for _, ok := range [][]string{nil, {}, {"least-loaded"}, {"prefix-aware", "least-loaded"}} {
		if err := ValidateKeyStrategies(ok); err != nil {
			t.Errorf("%v: unexpected error %v", ok, err)
		}
	}
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{[]string{"round-robin"}, "unknown strategy"},
		{[]string{""}, "unknown strategy"},
		{[]string{"least-loaded", "least-loaded"}, "listed twice"},
	} {
		err := ValidateKeyStrategies(tc.names)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: error %v, want %q", tc.names, err, tc.want)
		}
	}
}
