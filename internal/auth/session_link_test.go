// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "hivenet_router/proto"
)

// A session belongs to the first peer that registers with it.
func TestLinkPeerIDRefusesADifferentPeer(t *testing.T) {
	m := NewSessionManager(time.Hour)
	token := m.CreateSession("agent-a", &pb.AgentMetadata{Model: "m", Capacity: 1})
	a, b := peer.ID("peer-a"), peer.ID("peer-b")

	if !m.LinkPeerID(token, a) {
		t.Fatal("first link must succeed")
	}
	if !m.LinkPeerID(token, a) {
		t.Fatal("re-linking the same peer must succeed")
	}
	if m.LinkPeerID(token, b) {
		t.Fatal("linking the session to a different peer must be refused")
	}
	if s, ok := m.ValidateSession(token); !ok || s.PeerID != a {
		t.Fatalf("session must stay bound to the first peer, got %q", s.PeerID)
	}
}
