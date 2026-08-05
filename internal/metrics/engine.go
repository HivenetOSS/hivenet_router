// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics

import (
	"sync"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// EnginePunctualStore manages per-agent engine backend metrics on the router side.
// It writes the BackendMetrics snapshot to BadgerDB memDB and refreshes the
// Prometheus engine gauges. On disconnect it cleans up both.
//
// Update is called from two paths:
//   - handleRoutingSignals every ~500ms (primary freshness path)
//   - handleAgentHeartbeat every ~5s (fallback carrier)
//
// Update and Unregister are fully serialised by mu so that a concurrent Unregister
// cannot interleave with Update and orphan Prometheus series for a dead agent.
//
// It is safe for concurrent use.
type EnginePunctualStore struct {
	storage storage.RoutingStorage
	m       *RouterMetrics
	mu      sync.Mutex
}

// NewEnginePunctualStore creates an EnginePunctualStore backed by the given storage and metrics.
func NewEnginePunctualStore(s storage.RoutingStorage, m *RouterMetrics) *EnginePunctualStore {
	return &EnginePunctualStore{storage: s, m: m}
}

// Update persists the BackendMetrics snapshot to memDB and refreshes all engine
// Prometheus gauges. Nil pointer fields in bm are silently skipped — the gauge
// retains its previous value. The entire operation runs under mu.
// Called from both handleRoutingSignals (~500ms, primary) and handleAgentHeartbeat (~5s, fallback).
func (e *EnginePunctualStore) Update(peerID peer.ID, model, engine, organization, machine string, bm *domain.BackendMetrics) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.m.AgentEnginePunctualUpdated(peerID.String(), model, engine, organization, machine, bm)

	if err := e.storage.SetEnginePunctual(peerID, bm); err != nil {
		log.Warnf("EnginePunctualStore.Update: storage write failed for %s: %v", peerID, err)
	}
}

// Unregister removes the snapshot from memDB and deletes all engine Prometheus label
// sets for peerID. The entire operation runs under mu so that a concurrent Update
// cannot re-create series after Unregister has cleaned them up.
// Called from the health monitor when an agent is removed from the routing table.
func (e *EnginePunctualStore) Unregister(peerID peer.ID, model, engine, organization, machine string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.m.AgentEnginePunctualUnregistered(peerID.String(), model, engine, organization, machine)

	if err := e.storage.DeleteEnginePunctual(peerID); err != nil {
		log.Warnf("EnginePunctualStore.Unregister: storage delete failed for %s: %v", peerID, err)
	}
}
