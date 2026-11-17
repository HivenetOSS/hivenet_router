// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"context"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

func agentIn(id, country string) *domain.Agent {
	return domain.NewAgent(peer.ID(id), domain.AgentMetadata{Model: "m", Capability: domain.CapabilityLLM, Country: country}, "")
}

// The scope is a hard filter on every step: a fallback step with an empty
// match still cannot pick an agent outside it, and an agent that declares
// no country serves only scope "all".
func TestSessionScope_FiltersEveryStep(t *testing.T) {
	fr, de, us, none := agentIn("fr", "FR"), agentIn("de", "DE"), agentIn("us", "US"), agentIn("none", "")
	stor := testutil.NoopStorage{}
	pol := &Policy{
		RoutingPolicy: PolicyStep{Strategy: "least-loaded", Match: MatchFilter{Region: "nowhere"}, MaxTries: 1},
		FallbackChain: []FallbackStep{{Name: "any", PolicyStep: PolicyStep{Strategy: "least-loaded", MaxTries: 8}}},
	}
	e := NewExecutor(agentList{"m": {fr, de, us, none}}, NewEvaluator(stor, metrics.NewUniversalCounterStore(stor, nil)), pol, 3, 0)

	cases := []struct {
		scope domain.Scope
		allow map[peer.ID]bool
	}{
		{"country:FR", map[peer.ID]bool{"fr": true}},
		{domain.ScopeEU, map[peer.ID]bool{"fr": true, "de": true}},
		{domain.ScopeAll, map[peer.ID]bool{"fr": true, "de": true, "us": true, "none": true}},
		{"", map[peer.ID]bool{"fr": true, "de": true, "us": true, "none": true}},
	}
	for _, tc := range cases {
		s := e.NewSession("m", domain.CapabilityLLM)
		s.SetScope(tc.scope)
		seen := map[peer.ID]bool{}
		for range 8 {
			a, err := s.Select(context.Background())
			if err != nil {
				break
			}
			seen[a.ID] = true
			s.RecordFailure(a.ID)
		}
		for id := range seen {
			if !tc.allow[id] {
				t.Errorf("scope %q: picked %s", tc.scope, id)
			}
		}
		if len(seen) != len(tc.allow) {
			t.Errorf("scope %q: picked %v, want every agent of %v", tc.scope, seen, tc.allow)
		}
	}
}
