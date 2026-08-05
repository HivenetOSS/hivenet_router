// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "time"

// RegistrationEventType is the discriminator for an emitted agent-registration
// change. These string values are a published wire contract consumed outside
// this repository — treat a change to them as a breaking API change.
type RegistrationEventType string

const (
	RegistrationRegistered   RegistrationEventType = "registered"
	RegistrationUnregistered RegistrationEventType = "unregistered"
)

// RegistrationEvent is one settled change broadcast on the registration
// change feed. The (DeploymentID, ReplicaID) pair is the join key a consumer
// uses to map a router row back to the workload it started; AgentID is the
// libp2p peer ID, carried for diagnostics only.
//
// Defined in domain (rather than router or api) so both packages can refer to
// it without circular imports.
type RegistrationEvent struct {
	EventType    RegistrationEventType `json:"event_type"`
	DeploymentID string                `json:"deployment_id"`
	ReplicaID    string                `json:"replica_id"`
	AgentID      string                `json:"agent_id,omitempty"`
	Timestamp    time.Time             `json:"timestamp"`
}
