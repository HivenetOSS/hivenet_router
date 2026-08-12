// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics_test

import (
	"testing"

	"hivenet_router/internal/metrics"

	dto "github.com/prometheus/client_model/go"
)

// metricValue scans a gathered registry for the sample of metric `name` whose
// labels match `want`, returning its counter/gauge value and whether it existed.
func metricValue(t *testing.T, m *metrics.RouterMetrics, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if !labelsMatch(labels, want) {
				continue
			}
			return sampleValue(metric), true
		}
	}
	return 0, false
}

func labelsMatch(got, want map[string]string) bool {
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	default:
		return 0
	}
}

// TestAdmissionRejectionsCounter verifies the reason-labelled counter increments
// per gate/reason and model, as the "429 by reason" dashboards read it.
func TestAdmissionRejectionsCounter(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.AdmissionRejected("b2", "gemma")
	m.AdmissionRejected("b2", "gemma")
	m.AdmissionRejected("b3", "gemma")
	m.AdmissionRejected("b4_rpm", "qwen")

	cases := []struct {
		reason, model string
		want          float64
	}{
		{"b2", "gemma", 2},
		{"b3", "gemma", 1},
		{"b4_rpm", "qwen", 1},
	}
	for _, tc := range cases {
		got, ok := metricValue(t, m, "hivenet_router_admission_rejections_total",
			map[string]string{"reason": tc.reason, "model": tc.model})
		if !ok {
			t.Errorf("no series for reason=%s model=%s", tc.reason, tc.model)
			continue
		}
		if got != tc.want {
			t.Errorf("reason=%s model=%s = %v, want %v", tc.reason, tc.model, got, tc.want)
		}
	}
	// A reason that never fired has no series (rather than a zero we'd have to seed).
	if _, ok := metricValue(t, m, "hivenet_router_admission_rejections_total",
		map[string]string{"reason": "b1", "model": "gemma"}); ok {
		t.Error("expected no b1 series before any b1 rejection")
	}
}

// TestAdmissionOccupancyGauges verifies the occupancy gauges expose the numerator
// (Σw, in-flight) and both denominators (budget, max_inflight) for a model.
func TestAdmissionOccupancyGauges(t *testing.T) {
	m := metrics.NewRouterMetrics()
	m.SetAdmissionOccupancy("gemma", 250_000, 1, 409_000, 64)

	for _, tc := range []struct {
		name string
		want float64
	}{
		{"hivenet_router_admission_occupancy_tokens", 250_000},
		{"hivenet_router_admission_inflight_requests", 1},
		{"hivenet_router_admission_budget_tokens", 409_000},
		{"hivenet_router_admission_max_inflight", 64},
	} {
		got, ok := metricValue(t, m, tc.name, map[string]string{"model": "gemma"})
		if !ok || got != tc.want {
			t.Errorf("%s = %v (present=%v), want %v", tc.name, got, ok, tc.want)
		}
	}

	// A later update overwrites the gauges (occupancy drains as requests finish),
	// and a disabled budget drops to 0 rather than staying stale.
	m.SetAdmissionOccupancy("gemma", 0, 0, 0, 0)
	if got, _ := metricValue(t, m, "hivenet_router_admission_budget_tokens", map[string]string{"model": "gemma"}); got != 0 {
		t.Errorf("budget gauge must drop to 0 when disabled, got %v", got)
	}
}
