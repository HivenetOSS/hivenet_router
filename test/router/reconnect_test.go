// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const testProto = "/test/1.0.0"

// connectWithRetry dials target from h, retrying every 5ms until it succeeds
// or ctx expires. Returns an error only on timeout.
func connectWithRetry(ctx context.Context, h host.Host, target peer.AddrInfo) error {
	for {
		if err := h.Connect(ctx, target); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestImmediateReconnectAfterDisconnect verifies that the Notifiee fix allows
// an agent to reconnect immediately after its TCP connection drops, without
// hitting the resource manager's per-peer connection limit while a stale scope
// is still open.
//
// Without the fix, the router holds the peer's libp2p slot open for up to 30s
// (RemoveAfter). A reconnect arriving during that window gets rejected with an
// immediate TCP FIN before the Noise handshake. With the fix, ClosePeer is
// called as soon as the last connection closes, freeing the slot instantly.
func TestImmediateReconnectAfterDisconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Router-side host — mirrors what the real router does in Start().
	routerHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create router host: %v", err)
	}
	defer routerHost.Close()

	// Register the Notifiee — this is the fix under test.
	routerHost.Network().Notify(&libp2pnet.NotifyBundle{
		DisconnectedF: func(n libp2pnet.Network, conn libp2pnet.Conn) {
			peerID := conn.RemotePeer()
			if len(n.ConnsToPeer(peerID)) != 0 {
				return
			}
			go func() {
				if len(n.ConnsToPeer(peerID)) == 0 {
					n.ClosePeer(peerID) //nolint:errcheck
				}
			}()
		},
	})

	// Register a minimal stream handler so the router accepts streams.
	routerHost.SetStreamHandler(testProto, func(s libp2pnet.Stream) { s.Close() })

	// Agent-side host.
	agentHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create agent host: %v", err)
	}
	defer agentHost.Close()

	routerInfo := peer.AddrInfo{ID: routerHost.ID(), Addrs: routerHost.Addrs()}

	// ── First connection ──────────────────────────────────────────────────────
	if err := agentHost.Connect(ctx, routerInfo); err != nil {
		t.Fatalf("first connect: %v", err)
	}

	conns := agentHost.Network().ConnsToPeer(routerHost.ID())
	if len(conns) == 0 {
		t.Fatal("expected at least one connection after first connect")
	}

	// Force-close the connection from the agent side (simulates a TCP drop or
	// VM network blip where the agent detects the loss first).
	if err := conns[0].Close(); err != nil {
		t.Fatalf("close first connection: %v", err)
	}

	// ── Immediate reconnect ───────────────────────────────────────────────────
	// Retry until the reconnect succeeds or 3s elapse. Without the fix the peer
	// slot stays occupied for up to 30s, so a 3s deadline reliably distinguishes
	// "fixed" from "broken" without being sensitive to Notifiee goroutine timing.
	reconnectCtx, reconnectCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reconnectCancel()

	if err := connectWithRetry(reconnectCtx, agentHost, routerInfo); err != nil {
		t.Fatalf("reconnect timed out — peer slot was not released in time: %v", err)
	}

	// Open a stream to confirm the connection is fully functional.
	s, err := agentHost.NewStream(ctx, routerHost.ID(), testProto)
	if err != nil {
		t.Fatalf("open stream after reconnect: %v", err)
	}
	s.Close()
}
