// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	p2phttp "github.com/libp2p/go-libp2p/p2p/http"

	"hivenet_router/internal/auth"
	pb "hivenet_router/proto"
)

// /register must identify the agent by the libp2p peer that sent the request,
// never by the peer_id written in the body.
func TestRegisterUsesTheConnectionPeer(t *testing.T) {
	r := &Router{sessionManager: auth.NewSessionManager(time.Hour)}
	token := r.sessionManager.CreateSession("agent-x", &pb.AgentMetadata{Model: "m", Capacity: 1})

	t.Run("not over libp2p", func(t *testing.T) {
		body := fmt.Sprintf(`{"session_token":%q,"peer_id":"12D3KooWFakeFakeFake"}`, token)
		rec := httptest.NewRecorder()
		r.handleAgentRegister(rec, httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(body)))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401", rec.Code)
		}
	})

	t.Run("body peer differs from the connection peer", func(t *testing.T) {
		server, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		srv := &p2phttp.Host{StreamHost: server}
		mux := http.NewServeMux()
		mux.HandleFunc("/register", r.handleAgentRegister)
		srv.SetHTTPHandler("/test-router/1.0.0", mux)
		go srv.Serve() //nolint:errcheck
		defer srv.Close()

		client, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		victim, err := libp2p.New(libp2p.NoListenAddrs)
		if err != nil {
			t.Fatal(err)
		}
		defer victim.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		info := peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}
		if err := client.Connect(ctx, info); err != nil {
			t.Fatal(err)
		}
		c, err := (&p2phttp.Host{StreamHost: client}).NamespacedClient("/test-router/1.0.0", info)
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"session_token":%q,"peer_id":%q}`, token, victim.ID())
		resp, err := c.Post("/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status %d, want 403", resp.StatusCode)
		}
		if s, _ := r.sessionManager.ValidateSession(token); s.PeerID != "" {
			t.Fatalf("session must not be bound after a refused registration, got %s", s.PeerID)
		}
	})
}
