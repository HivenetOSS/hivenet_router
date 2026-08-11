// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package router_test contains black-box tests for the aggregate engine-pressure
// helper that feeds the front-door KV-pressure shed gate.
package router_test

import (
	"math"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/router"
)

func fp(v float64) *float64 { return &v }

func approx(got *float64, want float64) bool {
	return got != nil && math.Abs(*got-want) < 1e-9
}

// TestMeanEnginePressure_AveragesReportedMetrics verifies the helper returns the
// mean over the agents that report each metric, skips nil snapshots, and returns
// nil for a dimension no agent reports.
func TestMeanEnginePressure_AveragesReportedMetrics(t *testing.T) {
	metrics := []*domain.BackendMetrics{
		{KVCacheUtilization: fp(0.80), WaitingRequests: fp(10)},
		{KVCacheUtilization: fp(0.90), WaitingRequests: fp(30)},
		nil,                            // skipped
		{KVCacheUtilization: fp(0.70)}, // no waiting reported
	}
	kv, waiting := router.MeanEnginePressure(metrics)
	if !approx(kv, 0.80) { // (0.80+0.90+0.70)/3
		t.Errorf("kv mean = %v, want 0.80", kv)
	}
	if !approx(waiting, 20) { // (10+30)/2
		t.Errorf("waiting mean = %v, want 20", waiting)
	}
}

// TestMeanEnginePressure_NilWhenNoneReported verifies a dimension no agent
// reports is nil (not zero), so the gate skips it instead of reading it as no
// pressure.
func TestMeanEnginePressure_NilWhenNoneReported(t *testing.T) {
	// Waiting reported, KV never reported.
	kv, waiting := router.MeanEnginePressure([]*domain.BackendMetrics{
		{WaitingRequests: fp(5)},
		{WaitingRequests: fp(15)},
	})
	if kv != nil {
		t.Errorf("kv must be nil when no agent reports it, got %v", *kv)
	}
	if !approx(waiting, 10) {
		t.Errorf("waiting mean = %v, want 10", waiting)
	}
}

// TestMeanEnginePressure_EmptyIsNil verifies an empty fleet reports no pressure
// on either dimension.
func TestMeanEnginePressure_EmptyIsNil(t *testing.T) {
	kv, waiting := router.MeanEnginePressure(nil)
	if kv != nil || waiting != nil {
		t.Errorf("empty input must yield nil/nil, got kv=%v waiting=%v", kv, waiting)
	}
}
