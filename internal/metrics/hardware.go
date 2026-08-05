// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package metrics

import (
	"strconv"
	"sync"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// HardwareStore manages per-agent hardware snapshots on the router side.
// On every heartbeat it writes the snapshot to BadgerDB memDB and refreshes
// the Prometheus hardware gauges. On disconnect it cleans up both.
//
// Update and Unregister are fully serialised by mu: Prometheus writes and
// storage writes both happen while the lock is held. This prevents a race
// where Unregister deletes all series and then Update (processing the last
// in-flight heartbeat) re-creates them for a dead agent — orphaning the
// series indefinitely.
//
// It is safe for concurrent use.
type HardwareStore struct {
	storage    storage.RoutingStorage
	m          *RouterMetrics
	mu         sync.Mutex        // guards gpuIndices and serialises Prometheus + storage ops
	gpuIndices map[peer.ID][]int // actual GPU indices last seen per agent
}

// NewHardwareStore creates a HardwareStore backed by the given storage and metrics.
func NewHardwareStore(s storage.RoutingStorage, m *RouterMetrics) *HardwareStore {
	return &HardwareStore{
		storage:    s,
		m:          m,
		gpuIndices: make(map[peer.ID][]int),
	}
}

// Update persists the snapshot to memDB and refreshes all Prometheus hardware gauges.
// If the GPU index set changed since the last heartbeat (e.g. CUDA_VISIBLE_DEVICES or MIG
// reconfiguration), stale Prometheus series are deleted before new values are written.
// The entire operation — index diff, Prometheus update, and storage write — runs under
// mu so that a concurrent Unregister cannot interleave and leave orphaned series.
// Called from handleAgentHeartbeat for every heartbeat that carries a non-nil snapshot.
func (h *HardwareStore) Update(peerID peer.ID, region, model, engine, organization, machine string, snap *domain.HardwareSnapshot) {
	newIndices := make([]int, 0, len(snap.GPUs))
	for _, gpu := range snap.GPUs {
		newIndices = append(newIndices, gpu.Index)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if old, ok := h.gpuIndices[peerID]; ok {
		// Delete Prometheus series for GPU indices no longer present in this snapshot.
		// This handles non-contiguous indices (e.g. {0, 2}) and set changes between heartbeats.
		newSet := make(map[int]struct{}, len(newIndices))
		for _, idx := range newIndices {
			newSet[idx] = struct{}{}
		}
		for _, idx := range old {
			if _, stillPresent := newSet[idx]; !stillPresent {
				h.m.deleteGPULabelSet(peerID.String(), region, model, engine, strconv.Itoa(idx), organization, machine)
			}
		}
	}
	h.gpuIndices[peerID] = newIndices
	h.m.AgentHardwareUpdated(peerID.String(), region, model, engine, organization, machine, snap)

	if err := h.storage.SetHardwareSnapshot(peerID, snap); err != nil {
		log.Warnf("HardwareStore.Update: storage write failed for %s: %v", peerID, err)
	}
}

// Unregister removes the snapshot from memDB and deletes all Prometheus label sets.
// The entire operation runs under mu so that a concurrent Update cannot re-create
// series after Unregister has cleaned them up.
// Called from the health monitor when an agent is removed from the routing table.
func (h *HardwareStore) Unregister(peerID peer.ID, region, model, engine, organization, machine string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	indices := h.gpuIndices[peerID]
	delete(h.gpuIndices, peerID)
	h.m.AgentHardwareUnregistered(peerID.String(), region, model, engine, organization, machine, indices)

	if err := h.storage.DeleteHardwareSnapshot(peerID); err != nil {
		log.Warnf("HardwareStore.Unregister: storage delete failed for %s: %v", peerID, err)
	}
}
