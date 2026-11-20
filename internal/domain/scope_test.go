// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "testing"

func TestParseScope(t *testing.T) {
	for _, ok := range []string{"all", "EU", "country:FR", "country:US"} {
		if _, err := ParseScope(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "eu", "ALL", "country:fr", "country:FRA", "FR", "mars", "country:"} {
		if _, err := ParseScope(bad); err == nil {
			t.Errorf("%q: accepted", bad)
		}
	}
}

func TestScopeAllowsCountry(t *testing.T) {
	cases := []struct {
		scope Scope
		cc    string
		want  bool
	}{
		{ScopeAll, "", true}, {ScopeAll, "US", true},
		{ScopeEU, "FR", true}, {ScopeEU, "DE", true}, {ScopeEU, "CH", false}, {ScopeEU, "US", false}, {ScopeEU, "", false},
		{"country:FR", "FR", true}, {"country:FR", "DE", false}, {"country:FR", "", false},
		{"mars", "FR", false},
	}
	for _, tc := range cases {
		if got := tc.scope.AllowsCountry(tc.cc); got != tc.want {
			t.Errorf("%s allows %q = %v, want %v", tc.scope, tc.cc, got, tc.want)
		}
	}
}

func TestScopeWithin(t *testing.T) {
	cases := []struct {
		s, max Scope
		want   bool
	}{
		{ScopeAll, ScopeAll, true}, {ScopeEU, ScopeAll, true}, {"country:US", ScopeAll, true},
		{ScopeEU, ScopeEU, true}, {"country:FR", ScopeEU, true}, {"country:US", ScopeEU, false}, {ScopeAll, ScopeEU, false},
		{"country:FR", "country:FR", true}, {"country:DE", "country:FR", false}, {ScopeEU, "country:FR", false}, {ScopeAll, "country:FR", false},
	}
	for _, tc := range cases {
		if got := tc.s.Within(tc.max); got != tc.want {
			t.Errorf("%s within %s = %v, want %v", tc.s, tc.max, got, tc.want)
		}
	}
}
