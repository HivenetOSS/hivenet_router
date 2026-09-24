// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"testing"

	"hivenet_router/internal/metrics"
)

// TestSemanticDecisionMetrics: decisions count by alias/route/source/outcome,
// and the decision log's drop and write-error counts are exported as read.
func TestSemanticDecisionMetrics(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.SemanticDecision("auto", "general", "default", "ok", 0.0001)
	m.SemanticDecision("auto", "general", "default", "ok", 0.0002)
	m.SemanticDecision("auto", "", "", "forbidden", 0.0001)

	if v, _ := metricValue(t, m, "hivenet_semantic_decisions_total",
		map[string]string{"alias": "auto", "route": "general", "source": "default", "outcome": "ok"}); v != 2 {
		t.Errorf("ok decisions = %g, want 2", v)
	}
	if v, _ := metricValue(t, m, "hivenet_semantic_decisions_total", map[string]string{"outcome": "forbidden"}); v != 1 {
		t.Errorf("forbidden decisions = %g, want 1", v)
	}

	dropped := int64(3)
	m.RegisterSemanticDecisionLog(func() int64 { return dropped }, func() int64 { return 1 })
	if v, ok := metricValue(t, m, "hivenet_semantic_decision_log_dropped_total", nil); !ok || v != 3 {
		t.Errorf("dropped = %g (present %v), want 3", v, ok)
	}
	if v, _ := metricValue(t, m, "hivenet_semantic_decision_log_write_errors_total", nil); v != 1 {
		t.Errorf("write errors = %g, want 1", v)
	}
}
