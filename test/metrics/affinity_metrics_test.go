// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"testing"

	"hivenet_router/internal/metrics"
)

// TestRouterMetrics_AffinityCounter covers hivenet_router_affinity_total:
// construction must register it exactly once (a duplicate name in the
// MustRegister block panics NewRouterMetrics), the {tenant,result} labels must
// resolve, and every outcome must increment independently.
func TestRouterMetrics_AffinityCounter(t *testing.T) {
	m := metrics.NewRouterMetrics() // would panic if the name were registered twice

	for _, res := range []string{"hit", "miss", "margin", "ineligible"} {
		m.AffinityRouted("tenant-a", res)
	}
	m.AffinityRouted("tenant-a", "hit") // second hit on the same tenant

	cases := []struct {
		result string
		want   float64
	}{
		{"hit", 2},
		{"miss", 1},
		{"margin", 1},
		{"ineligible", 1},
	}
	for _, tc := range cases {
		got, ok := metricValue(t, m, "hivenet_router_affinity_total", map[string]string{
			"tenant": "tenant-a",
			"result": tc.result,
		})
		if !ok {
			t.Errorf("no sample for result=%q", tc.result)
			continue
		}
		if got != tc.want {
			t.Errorf("result=%q value = %v, want %v", tc.result, got, tc.want)
		}
	}

	// A tenant that never routed affinity must have no series at all.
	if _, ok := metricValue(t, m, "hivenet_router_affinity_total", map[string]string{
		"tenant": "tenant-b",
		"result": "hit",
	}); ok {
		t.Error("unexpected series for a tenant that never recorded a decision")
	}
}
