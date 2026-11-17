// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixhash"
	"hivenet_router/internal/prefixindex"
	pb "hivenet_router/proto"

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

// The session comes in the Authorization header and is checked, with the
// rate limit, before the body is read: a batch over the limit is refused
// without decoding it.
func TestPrefixStateAuthenticatesBeforeTheBody(t *testing.T) {
	r := &Router{sessionManager: auth.NewSessionManager(time.Hour), agents: NewAgentRegistry(), prefixIndex: prefixindex.New(prefixindex.Config{})}
	id := peer.ID("agent-x")
	token := r.sessionManager.CreateSession("agent-x", &pb.AgentMetadata{Model: "m", Capacity: 1})
	if !r.sessionManager.LinkPeerID(token, id) {
		t.Fatal("LinkPeerID failed")
	}
	r.agents.Register(domain.NewAgent(id, domain.AgentMetadata{Model: "m", Capacity: 1}, token))
	post := func(auth, body string) int {
		req := httptest.NewRequest(http.MethodPost, "/prefix-state", strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		r.handlePrefixState(rec, req)
		return rec.Code
	}
	if c := post("", `{}`); c != http.StatusUnauthorized {
		t.Errorf("no header: %d, want 401", c)
	}
	if c := post("Bearer nope", `{}`); c != http.StatusUnauthorized {
		t.Errorf("bad token: %d, want 401", c)
	}
	if c := post("Bearer "+token, `{"served":[]}`); c != http.StatusOK {
		t.Errorf("valid batch: %d, want 200", c)
	}
	// A batch with too many cache changes is refused whole: its served
	// entries are not learned (the agent sends them again).
	big := `{"served":[{"anchors":[7],"ends":[1],"blocks":[1]}],"kv":{"epoch":1,"to":1,"snapshot":true,"stored":[` + strings.Repeat("1,", maxKVBlocks) + `1]}}`
	if c := post("Bearer "+token, big); c != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized cache changes: %d, want 413", c)
	}
	if st := r.prefixIndex.Stats(); st.Memo != 0 {
		t.Errorf("a refused batch was partly applied: %+v", st)
	}
	for range int(prefixBurst) {
		post("Bearer "+token, `{}`)
	}
	if c := post("Bearer "+token, `not json`); c != http.StatusTooManyRequests {
		t.Errorf("over the limit: %d, want 429 before the body is read", c)
	}
}

// The largest batch an agent sends fits the router's body limit: a full
// snapshot and a full set of served entries, every hash at its longest.
func TestPrefixStateLargestBatchFits(t *testing.T) {
	hashes := func(n int) []uint64 {
		h := make([]uint64, n)
		for i := range h {
			h[i] = math.MaxUint64
		}
		return h
	}
	body, err := json.Marshal(prefixStatePayload{
		Served: []prefixServedEntry{{Anchors: hashes(prefixhash.MaxBatchHashes / 2), Blocks: hashes(prefixhash.MaxBatchHashes / 2)}},
		KV:     &prefixKVPayload{Stored: hashes(prefixhash.MaxKVBlocks)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) >= maxPrefixStateBody {
		t.Errorf("largest batch %d bytes, body limit %d", len(body), maxPrefixStateBody)
	}
}
