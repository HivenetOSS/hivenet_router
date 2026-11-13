// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"hivenet_router/internal/policy"
	"hivenet_router/internal/prefixhash"

	"github.com/libp2p/go-libp2p/core/peer"
)

// prefixStatePayload is what an agent reports about the prompts it served:
// for each prompt, the request's anchors (as the router sent them), the full
// blocks before each message, and the prompt's block hashes. Hashes only:
// no text, no token IDs.
type prefixStatePayload struct {
	SessionToken string              `json:"session_token"`
	Served       []prefixServedEntry `json:"served"`
	// KV carries the changes of the agent's engine cache (from its KV
	// events), in router block hashes; absent when the agent has none.
	KV *prefixKVPayload `json:"kv,omitempty"`
}

type prefixServedEntry struct {
	Anchors []uint64 `json:"anchors"`
	Ends    []int    `json:"ends"`
	Blocks  []uint64 `json:"blocks"`
	// Text: the engine returned no tokens; the anchors point to the agent.
	Text bool `json:"text"`
}

// prefixKVPayload is a batch of cache changes: deltas From..To of the agent's
// run Epoch, or a Snapshot of everything it holds at To.
type prefixKVPayload struct {
	Epoch    uint64   `json:"epoch"`
	From     uint64   `json:"from"`
	To       uint64   `json:"to"`
	Stored   []uint64 `json:"stored"`
	Removed  []uint64 `json:"removed"`
	Cleared  bool     `json:"cleared"`
	Snapshot bool     `json:"snapshot"`
}

// kvCursor is where an agent's cache changes stand on this router.
type kvCursor struct{ epoch, seq uint64 }

// kvCursors tracks every agent's cache-change cursor.
type kvCursors struct {
	mu sync.Mutex
	m  map[peer.ID]kvCursor
}

// accept reports whether a batch continues the agent's changes in order (or
// is a snapshot), and moves the cursor if so.
func (c *kvCursors) accept(agent peer.ID, p *prefixKVPayload) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[peer.ID]kvCursor{}
	}
	cur, ok := c.m[agent]
	if !p.Snapshot && (!ok || cur.epoch != p.Epoch || p.From != cur.seq+1 || p.To < p.From) {
		return false
	}
	c.m[agent] = kvCursor{epoch: p.Epoch, seq: p.To}
	return true
}

func (c *kvCursors) forget(agent peer.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, agent)
}

// Bounds on one /prefix-state batch.
const (
	maxPrefixStateBody   = 16 << 20
	maxPrefixStateServed = 1024
	maxKVBlocks          = 1 << 20 // 64M tokens of cache per agent
)

// handlePrefixState records what an agent served in the prefix memory: the
// memo learns which blocks each anchor maps to, and the board predicts those
// blocks on the agent. Authenticated like /routing-signals.
func (r *Router) handlePrefixState(w http.ResponseWriter, req *http.Request) {
	var payload prefixStatePayload
	if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, maxPrefixStateBody)).Decode(&payload); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	session, valid := r.sessionManager.ValidateSession(payload.SessionToken)
	if !valid {
		http.Error(w, "invalid or expired session token", http.StatusUnauthorized)
		return
	}
	a := r.agents.Get(session.PeerID)
	if a == nil {
		http.Error(w, "agent not found", http.StatusNotFound)
		return
	}
	if len(payload.Served) > maxPrefixStateServed {
		http.Error(w, "too many entries", http.StatusRequestEntityTooLarge)
		return
	}
	model := a.Metadata.Model
	for _, s := range payload.Served {
		if len(s.Anchors) == 0 || len(s.Anchors) > prefixhash.MaxAnchors {
			continue
		}
		if s.Text {
			r.prefixIndex.LearnText(model, a.ID, s.Anchors, policy.DefaultPrefixPredictedTTL)
			continue
		}
		if len(s.Ends) != len(s.Anchors) || len(s.Blocks) > prefixhash.MaxBlocks {
			continue
		}
		r.prefixIndex.Learn(model, a.ID, s.Anchors, s.Ends, s.Blocks, policy.DefaultPrefixPredictedTTL)
	}
	if kv := payload.KV; kv != nil {
		if len(kv.Stored) > maxKVBlocks || len(kv.Removed) > maxKVBlocks {
			http.Error(w, "too many blocks", http.StatusRequestEntityTooLarge)
			return
		}
		if !r.kvCursors.accept(a.ID, kv) {
			// Out of order, or this router does not know the agent's run:
			// the agent sends a snapshot next.
			http.Error(w, "cache changes out of order: send a snapshot", http.StatusConflict)
			return
		}
		r.prefixIndex.SetEventSource(a.ID)
		if kv.Snapshot {
			r.prefixIndex.ReplaceConfirmed(model, a.ID, kv.Stored)
		} else {
			r.prefixIndex.ApplyEvents(model, a.ID, kv.Stored, kv.Removed, kv.Cleared)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// runPrefixIndex sweeps expired prefix-memory entries and publishes its size
// until ctx ends.
func (r *Router) runPrefixIndex(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.prefixIndex.Sweep()
			st := r.prefixIndex.Stats()
			r.metrics.PrefixIndexSize(st.Memo, st.Blocks, st.Pending)
		case <-ctx.Done():
			return
		}
	}
}
