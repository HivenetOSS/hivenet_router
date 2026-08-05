// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package metrics_test contains black-box tests for the universal counter store's
// reset behaviour (POST /admin/metrics/reset).
package metrics_test

import (
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/storage"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

// stubStorage is a no-op RoutingStorage that records whether ResetUniversalHistory
// was called, so the test can assert the disk wipe is triggered.
type stubStorage struct {
	testutil.NoopStorage
	resetCalled bool
}

func (s *stubStorage) ResetUniversalHistory() error { s.resetCalled = true; return nil }

var _ storage.RoutingStorage = (*stubStorage)(nil)

// ResetAll must zero a registered agent's in-memory lifetime counters and trigger the
// disk wipe, so dashboards start from a clean baseline.
func TestUniversalCounterStore_ResetAll(t *testing.T) {
	stor := &stubStorage{}
	m := metrics.NewRouterMetrics()
	store := metrics.NewUniversalCounterStore(stor, m)

	id := peer.ID("agent-reset-0001")
	store.Bootstrap(id, "test-model", "vllm", "org", "machine")
	agent := domain.NewAgent(id, domain.AgentMetadata{
		Model:    "test-model",
		Engine:   "vllm",
		Capacity: 10,
	}, "")

	store.RecordSuccess(agent, 10, 20, 12)
	store.RecordSuccess(agent, 10, 20, 14)
	store.RecordFailure(agent, 5)

	before, ok := store.GetAgentStats(id)
	if !ok {
		t.Fatal("expected stats for the registered agent")
	}
	if before.SuccessfulRequests != 2 || before.FailedRequests != 1 {
		t.Fatalf("pre-reset stats = (success=%d, failed=%d), want (2, 1)",
			before.SuccessfulRequests, before.FailedRequests)
	}

	if err := store.ResetAll(); err != nil {
		t.Fatalf("ResetAll returned error: %v", err)
	}

	if !stor.resetCalled {
		t.Fatal("ResetAll must wipe persisted history via storage.ResetUniversalHistory")
	}
	after, ok := store.GetAgentStats(id)
	if !ok {
		t.Fatal("agent state should still exist (zeroed) after reset")
	}
	if after.SuccessfulRequests != 0 || after.FailedRequests != 0 || after.Disconnections != 0 {
		t.Fatalf("post-reset stats = (success=%d, failed=%d, disc=%d), want all 0",
			after.SuccessfulRequests, after.FailedRequests, after.Disconnections)
	}
}
