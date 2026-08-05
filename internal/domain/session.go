// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "hivenet_router/proto"
)

// Session represents an authenticated agent session.
//
// Lifecycle:
//  1. Created by SessionManager.CreateSession() during gRPC Authenticate — PeerID is zero.
//  2. PeerID is set by SessionManager.LinkPeerID() when the agent calls POST /register.
//     After this point the session is fully linked and all three hot handlers
//     (handleAgentHeartbeat, handleRoutingSignals) can resolve the agent in O(1)
//     via agents.Get(session.PeerID) instead of an O(n) ForEach scan.
type Session struct {
	AgentID   string
	PeerID    peer.ID           // Zero until LinkPeerID is called during /register
	Metadata  *pb.AgentMetadata // Immutable after creation (model, engine, region, capacity)
	CreatedAt time.Time
	ExpiresAt time.Time
}

// NewSession creates a new session
func NewSession(agentID string, metadata *pb.AgentMetadata, ttl time.Duration) *Session {
	now := time.Now()
	return &Session{
		AgentID:   agentID,
		Metadata:  metadata,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
}

// IsExpired checks if the session has expired
func (s *Session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}

// Refresh extends the session expiration
func (s *Session) Refresh(ttl time.Duration) {
	s.ExpiresAt = time.Now().Add(ttl)
}
