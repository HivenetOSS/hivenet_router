// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package testutil provides shared test doubles for the Hivenet Router test suite.
package testutil

import (
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// NoopStorage is a no-op implementation of storage.RoutingStorage. Embed it in a
// test stub and override only the methods a given test needs:
//
//	type stubStorage struct {
//		testutil.NoopStorage
//		resetCalled bool
//	}
//	func (s *stubStorage) ResetUniversalHistory() error { s.resetCalled = true; return nil }
//
// Methods use value receivers so the embedding struct satisfies the interface via
// either a value or a pointer, and pointer-receiver overrides shadow them correctly.
type NoopStorage struct{}

func (NoopStorage) RegisterAgent(peer.ID, domain.AgentMetadata, string) error { return nil }
func (NoopStorage) UnregisterAgent(peer.ID) error                             { return nil }
func (NoopStorage) UpdateAgentStatus(peer.ID, bool, time.Time) error          { return nil }
func (NoopStorage) ListAgents() ([]*domain.AgentRegistration, error)          { return nil, nil }
func (NoopStorage) GetUniversalPunctual(peer.ID) (*domain.AgentUniversalPunctual, error) {
	return nil, nil
}
func (NoopStorage) SetUniversalPunctual(peer.ID, *domain.AgentUniversalPunctual) error { return nil }
func (NoopStorage) DeleteUniversalPunctual(peer.ID) error                              { return nil }
func (NoopStorage) GetUniversalHistory(peer.ID) (*domain.AgentUniversalHistory, error) {
	return nil, nil
}
func (NoopStorage) SetUniversalHistory(peer.ID, *domain.AgentUniversalHistory) error { return nil }
func (NoopStorage) GetHardwareSnapshot(peer.ID) (*domain.HardwareSnapshot, error)    { return nil, nil }
func (NoopStorage) SetHardwareSnapshot(peer.ID, *domain.HardwareSnapshot) error      { return nil }
func (NoopStorage) DeleteHardwareSnapshot(peer.ID) error                             { return nil }
func (NoopStorage) GetEnginePunctual(peer.ID) (*domain.BackendMetrics, error)        { return nil, nil }
func (NoopStorage) SetEnginePunctual(peer.ID, *domain.BackendMetrics) error          { return nil }
func (NoopStorage) DeleteEnginePunctual(peer.ID) error                               { return nil }
func (NoopStorage) ResetUniversalHistory() error                                     { return nil }
func (NoopStorage) Close() error                                                     { return nil }

var _ storage.RoutingStorage = NoopStorage{}
