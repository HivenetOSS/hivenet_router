// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"hivenet_router/internal/domain"

	"github.com/libp2p/go-libp2p/core/peer"
	pb "hivenet_router/proto"
)

// SessionManager manages in-memory sessions for authenticated agents.
// Provides creation, validation, expiration, and cleanup of sessions.
type SessionManager struct {
	sessions map[string]*domain.Session // Map of sessionToken -> Session
	mu       sync.RWMutex               // Protects concurrent access to sessions
	ttl      time.Duration              // Default time-to-live for each session
}

// NewSessionManager creates a new session manager with a TTL for sessions.
func NewSessionManager(ttl time.Duration) *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*domain.Session),
		ttl:      ttl,
	}
}

// CreateSession generates a new session for the given agent and metadata.
// Returns the session token string which will be used for authentication.
func (m *SessionManager) CreateSession(agentID string, metadata *pb.AgentMetadata) string {

	// Generate a random, 32-byte session token
	token := generateSessionToken()

	// Store the session in-memory
	m.mu.Lock()
	m.sessions[token] = domain.NewSession(agentID, metadata, m.ttl)
	m.mu.Unlock()

	return token
}

// ValidateSession checks whether a session token is valid and not expired.
// If expired, the session is deleted. Returns the session object if valid.
// The entire check-and-delete runs under a single write lock to prevent a
// TOCTOU race where CleanupExpired could delete the session between the
// existence check and the expiry-triggered delete.
func (m *SessionManager) ValidateSession(token string) (*domain.Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[token]
	if !exists {
		return nil, false
	}
	if session.IsExpired() {
		delete(m.sessions, token)
		return nil, false
	}
	return session, true
}

// LinkPeerID binds a libp2p peer ID to an existing session.
// Called once from handleAgentRegister after the peer ID is decoded from the
// registration payload. After this call, ValidateSession returns a session with
// a non-zero PeerID, enabling O(1) agent lookup in the hot heartbeat and
// routing-signal handlers without scanning the entire agent registry.
// Returns false if the token is unknown or already expired.
func (m *SessionManager) LinkPeerID(token string, id peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, exists := m.sessions[token]
	if !exists || session.IsExpired() {
		return false
	}
	session.PeerID = id
	return true
}

// DeleteSession removes a session
func (m *SessionManager) DeleteSession(token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, token)
}

// CleanupExpired iterates over all sessions and deletes any that have expired.
func (m *SessionManager) CleanupExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for token, session := range m.sessions {
		if session.IsExpired() {
			delete(m.sessions, token)
			count++
		}
	}
	return count
}

// generateSessionToken creates a cryptographically random 32-byte hexadecimal session token.
// Panics if the OS CSPRNG is unavailable — a broken entropy source is not a recoverable
// condition and issuing a weak or duplicate token would be a silent security failure.
func generateSessionToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand unavailable: %v", err))
	}
	return hex.EncodeToString(b)
}
