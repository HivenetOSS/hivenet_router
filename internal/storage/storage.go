// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package storage

import (
	"time"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
)

// RoutingStorage defines the interface for agent storage operations.
type RoutingStorage interface {
	// RegisterAgent writes a new AgentRegistration under metadata:{peerID} in memDB.
	RegisterAgent(peerID peer.ID, metadata domain.AgentMetadata, sessionToken string) error

	// UnregisterAgent deletes the metadata:{peerID} entry from memDB.
	UnregisterAgent(peerID peer.ID) error

	// UpdateAgentStatus updates IsHealthy and LastSeen for a registered agent.
	// Called by the heartbeat handler (isHealthy=true) and health monitor (isHealthy=false).
	// No-op if the agent is not found (already removed).
	UpdateAgentStatus(peerID peer.ID, isHealthy bool, lastSeen time.Time) error

	// ListAgents scans the metadata: prefix in memDB and returns all registered agents.
	ListAgents() ([]*domain.AgentRegistration, error)

	// GetUniversalPunctual reads session-scoped counters for peerID from memDB.
	// Returns (nil, nil) if no entry exists yet.
	GetUniversalPunctual(peerID peer.ID) (*domain.AgentUniversalPunctual, error)

	// SetUniversalPunctual writes session-scoped counters for peerID to memDB.
	SetUniversalPunctual(peerID peer.ID, p *domain.AgentUniversalPunctual) error

	// DeleteUniversalPunctual removes session-scoped counters for peerID from memDB.
	DeleteUniversalPunctual(peerID peer.ID) error

	// GetUniversalHistory reads lifetime counters for peerID from diskDB.
	// Returns (nil, nil) if no entry exists yet.
	GetUniversalHistory(peerID peer.ID) (*domain.AgentUniversalHistory, error)

	// SetUniversalHistory writes lifetime counters for peerID to diskDB.
	SetUniversalHistory(peerID peer.ID, h *domain.AgentUniversalHistory) error

	// GetHardwareSnapshot reads the latest hardware reading for peerID from memDB.
	// Returns (nil, nil) if no entry exists yet.
	GetHardwareSnapshot(peerID peer.ID) (*domain.HardwareSnapshot, error)

	// SetHardwareSnapshot writes the latest hardware reading for peerID to memDB.
	SetHardwareSnapshot(peerID peer.ID, snap *domain.HardwareSnapshot) error

	// DeleteHardwareSnapshot removes the hardware snapshot for peerID from memDB.
	DeleteHardwareSnapshot(peerID peer.ID) error

	// GetEnginePunctual reads the latest engine backend metrics for peerID from memDB.
	// Returns (nil, nil) if no entry exists yet.
	GetEnginePunctual(peerID peer.ID) (*domain.BackendMetrics, error)

	// SetEnginePunctual writes the latest engine backend metrics for peerID to memDB.
	SetEnginePunctual(peerID peer.ID, m *domain.BackendMetrics) error

	// DeleteEnginePunctual removes the engine backend metrics for peerID from memDB.
	DeleteEnginePunctual(peerID peer.ID) error

	// ResetUniversalHistory deletes ALL persisted lifetime per-agent counters
	// (the universalHistory: prefix) from diskDB. Used by the admin metrics-reset
	// endpoint to clear historical totals after a deploy. Billing/quota state and
	// agent metadata are untouched.
	ResetUniversalHistory() error

	// Close releases any resources held by the storage.
	Close() error
}
