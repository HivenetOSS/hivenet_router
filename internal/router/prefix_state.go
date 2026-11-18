// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"hivenet_router/internal/policy"
	"hivenet_router/internal/prefixhash"
)

// prefixStatePayload is what an agent reports about the prompts it served:
// for each prompt, the request's anchors (as the router sent them), the full
// blocks before each message, and the prompt's block hashes. Hashes only:
// no text, no token IDs.
type prefixStatePayload struct {
	SessionToken string              `json:"session_token"`
	Served       []prefixServedEntry `json:"served"`
}

type prefixServedEntry struct {
	Anchors []uint64 `json:"anchors"`
	Ends    []int    `json:"ends"`
	Blocks  []uint64 `json:"blocks"`
}

// Bounds on one /prefix-state batch.
const (
	maxPrefixStateBody   = 8 << 20
	maxPrefixStateServed = 1024
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
	for _, s := range payload.Served {
		if len(s.Anchors) == 0 || len(s.Anchors) > prefixhash.MaxAnchors || len(s.Ends) != len(s.Anchors) || len(s.Blocks) > prefixhash.MaxBlocks {
			continue
		}
		r.prefixIndex.Learn(a.Metadata.Model, a.ID, s.Anchors, s.Ends, s.Blocks, policy.DefaultPrefixPredictedTTL)
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
