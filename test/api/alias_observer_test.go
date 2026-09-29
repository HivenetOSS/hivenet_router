// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"testing"

	"github.com/gin-gonic/gin"
)

// observed is one SemanticObserver call.
type observed struct{ alias, route, source, outcome string }

// TestAliasMiddleware_ObserverOutcomes: every alias decision reaches the
// metrics hook exactly once, with the outcome matching the response.
func TestAliasMiddleware_ObserverOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		auth    gin.HandlerFunc
		healthy map[string]int
		want    observed
	}{
		{"resolved", noAuth, nil, observed{"auto", "general", "default", "ok"}},
		{"forbidden", allowOnly("other"), nil, observed{"auto", "", "", "forbidden"}},
		{"all down", noAuth, map[string]int{}, observed{"auto", "", "", "unavailable"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newAliasHandlers(t, tc.healthy)
			var got []observed
			h.SetSemanticObserver(func(alias, route, source, outcome string, seconds float64) {
				if seconds <= 0 {
					t.Errorf("non-positive decision time %g", seconds)
				}
				got = append(got, observed{alias, route, source, outcome})
			})
			var out seen
			post(aliasRouter(h, tc.auth, &out), "/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`)
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("observer calls = %+v, want [%+v]", got, tc.want)
			}
		})
	}
}
