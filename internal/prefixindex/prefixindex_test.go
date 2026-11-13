// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package prefixindex

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTestIndex(cfg Config) (*Index, *clock) {
	ix := New(cfg)
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	ix.now = c.now
	return ix, c
}

const (
	a1 = peer.ID("agent-1")
	a2 = peer.ID("agent-2")
)

// What an agent served teaches the memo (anchor -> blocks) and predicts the
// blocks on that agent; a later request with the same leading messages finds them.
func TestLearnThenLookup(t *testing.T) {
	ix, _ := newTestIndex(Config{})
	blocks := []uint64{10, 11, 12, 13}
	// Turn 1: [S, Q1] -> S ends after 3 blocks, the whole prompt after 4.
	ix.Learn("m", a1, []uint64{100, 101}, []int{3, 4}, blocks, time.Minute)

	// Turn 2 of the same conversation: [S, Q1, A1, Q2].
	m := ix.Lookup("m", []uint64{100, 101, 102, 103}, []peer.ID{a1, a2})
	if m.Known != 2 || len(m.Blocks) != 4 {
		t.Fatalf("known %d blocks %d, want 2 and 4", m.Known, len(m.Blocks))
	}
	if m.Depth[a1] != 4 || m.Depth[a2] != 0 {
		t.Errorf("depth %v, want a1=4 a2=0", m.Depth)
	}
	// Another conversation of the same app shares only S.
	m = ix.Lookup("m", []uint64{100, 200}, []peer.ID{a1, a2})
	if m.Known != 1 || len(m.Blocks) != 3 || m.Depth[a1] != 3 {
		t.Errorf("shared system prompt: %+v", m)
	}
	if m := ix.Lookup("other-model", []uint64{100}, []peer.ID{a1}); m.Known != 0 {
		t.Error("models must not share entries")
	}
}

// Predictions and memo entries expire; Sweep removes them.
func TestExpiry(t *testing.T) {
	ix, c := newTestIndex(Config{MemoTTL: time.Minute})
	ix.Learn("m", a1, []uint64{100}, []int{2}, []uint64{1, 2}, 10*time.Second)
	c.t = c.t.Add(11 * time.Second)
	if m := ix.Lookup("m", []uint64{100}, []peer.ID{a1}); m.Known != 1 || m.Depth[a1] != 0 {
		t.Errorf("after the block TTL: %+v (memo kept, prediction gone)", m)
	}
	c.t = c.t.Add(time.Minute)
	if m := ix.Lookup("m", []uint64{100}, []peer.ID{a1}); m.Known != 0 {
		t.Error("memo entry must expire")
	}
	ix.Sweep()
	if s := ix.Stats(); s != (Stats{}) {
		t.Errorf("after sweep: %+v", s)
	}
}

// A decision leaves a pending note on new text, so a copy arriving before the
// agent reports goes to the same agent; the report clears the note.
func TestCommitPending(t *testing.T) {
	ix, c := newTestIndex(Config{})
	anchors := []uint64{100, 101}
	ix.Commit("m", a2, anchors, Match{}, time.Minute, 5*time.Second)
	m := ix.Lookup("m", anchors, []peer.ID{a1, a2})
	if m.Pending != a2 || m.PendingTo != 2 {
		t.Fatalf("pending %q to %d, want agent-2 to 2", m.Pending, m.PendingTo)
	}
	ix.Learn("m", a2, anchors, []int{1, 2}, []uint64{1, 2}, time.Minute)
	if m := ix.Lookup("m", anchors, []peer.ID{a1, a2}); m.Pending != "" || m.Depth[a2] != 2 {
		t.Errorf("after the report: %+v", m)
	}
	ix.Commit("m", a1, []uint64{300}, Match{}, time.Minute, 5*time.Second)
	c.t = c.t.Add(6 * time.Second)
	if m := ix.Lookup("m", []uint64{300}, []peer.ID{a1}); m.Pending != "" {
		t.Error("pending note must expire")
	}
}

// A removed agent is forgotten; the per-agent cap bounds its predictions.
func TestRemoveAgentAndCaps(t *testing.T) {
	ix, _ := newTestIndex(Config{BlocksPerAgent: 100, MemoCap: 10})
	blocks := make([]uint64, 150)
	for i := range blocks {
		blocks[i] = uint64(i + 1)
	}
	ix.Learn("m", a1, []uint64{1}, []int{150}, blocks, time.Minute)
	ix.Learn("m", a2, []uint64{1}, []int{150}, blocks, time.Minute)
	ix.mu.RLock()
	n := ix.models["m"].agentBlocks[a1]
	ix.mu.RUnlock()
	if n > 100 {
		t.Errorf("agent-1 holds %d blocks, cap 100", n)
	}
	ix.RemoveAgent(a1)
	if m := ix.Lookup("m", []uint64{1}, []peer.ID{a1, a2}); m.Depth[a1] != 0 {
		t.Errorf("removed agent still holds %d blocks", m.Depth[a1])
	}
	for i := range 30 {
		ix.Learn("m", a2, []uint64{uint64(1000 + i)}, []int{1}, []uint64{7}, time.Minute)
	}
	if s := ix.Stats(); s.Memo > 10 {
		t.Errorf("memo %d entries, cap 10", s.Memo)
	}
}
