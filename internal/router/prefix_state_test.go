// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Cache changes are applied only in order within one agent run; anything
// else needs a snapshot first.
func TestKVCursors(t *testing.T) {
	var c kvCursors
	a := peer.ID("agent")
	if c.accept(a, &prefixKVPayload{Epoch: 1, From: 1, To: 3}) {
		t.Error("unknown agent: changes must wait for a snapshot")
	}
	if !c.accept(a, &prefixKVPayload{Epoch: 1, To: 3, Snapshot: true}) {
		t.Fatal("snapshot refused")
	}
	if !c.accept(a, &prefixKVPayload{Epoch: 1, From: 4, To: 6}) {
		t.Error("next changes in order refused")
	}
	if c.accept(a, &prefixKVPayload{Epoch: 1, From: 8, To: 9}) {
		t.Error("a gap (7 missing) was accepted")
	}
	if c.accept(a, &prefixKVPayload{Epoch: 2, From: 7, To: 7}) {
		t.Error("a new agent run (epoch) was accepted without a snapshot")
	}
	c.forget(a)
	if c.accept(a, &prefixKVPayload{Epoch: 1, From: 7, To: 7}) {
		t.Error("a forgotten agent was accepted without a snapshot")
	}
}

func TestPrefixRateLimits(t *testing.T) {
	var l prefixRateLimits
	a := peer.ID("agent")
	now := time.Unix(0, 0)
	ok := 0
	for range int(prefixBurst) + 20 {
		if l.allow(a, now) {
			ok++
		}
	}
	if ok != int(prefixBurst) {
		t.Errorf("burst allowed %d, want %d", ok, int(prefixBurst))
	}
	if !l.allow(a, now.Add(100*time.Millisecond)) {
		t.Error("no refill after 100 ms")
	}
	if !l.allow(peer.ID("other"), now) {
		t.Error("buckets are not per agent")
	}
}
