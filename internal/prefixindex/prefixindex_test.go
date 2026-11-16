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

// Engine events confirm, remove and clear holdings; a block nobody holds any
// more takes the memo entries ending at it along (the memo forgets what no
// cache holds).
func TestEvents(t *testing.T) {
	ix, c := newTestIndex(Config{})
	ix.SetEventSource(a1)
	ix.Learn("m", a1, []uint64{100, 101}, []int{2, 3}, []uint64{1, 2, 3}, time.Minute)
	// Predictions on an event source last SpeculativeTTL only.
	c.t = c.t.Add(DefaultSpeculativeTTL + time.Second)
	if m := ix.Lookup("m", []uint64{100}, []peer.ID{a1}); m.Depth[a1] != 0 {
		t.Fatalf("prediction outlived the speculative TTL: %+v", m)
	}
	ix.ApplyEvents("m", a1, []uint64{1, 2, 3}, nil, false)
	if m := ix.Lookup("m", []uint64{100, 101}, []peer.ID{a1}); m.Known != 2 || m.Depth[a1] != 3 {
		t.Fatalf("after stored events: %+v", m)
	}
	c.t = c.t.Add(time.Hour - time.Minute) // confirmed holdings do not expire
	ix.Sweep()
	if m := ix.Lookup("m", []uint64{100}, []peer.ID{a1}); m.Depth[a1] != 2 {
		t.Errorf("confirmed blocks expired: %+v", m)
	}
	c.t = time.Unix(1_800_000_000, 0)
	ix.Learn("m", a1, []uint64{100, 101}, []int{2, 3}, []uint64{1, 2, 3}, time.Minute)
	ix.ApplyEvents("m", a1, nil, []uint64{3}, false)
	if m := ix.Lookup("m", []uint64{100, 101}, []peer.ID{a1}); m.Known != 1 {
		t.Errorf("anchor 101 ends at the evicted block 3 and must be forgotten: %+v", m)
	}
	ix.ApplyEvents("m", a1, nil, nil, true)
	if s := ix.Stats(); s.Blocks != 0 || s.Memo != 0 {
		t.Errorf("after cleared: %+v", s)
	}
}

// A snapshot replaces the confirmed holdings.
func TestReplaceConfirmed(t *testing.T) {
	ix, _ := newTestIndex(Config{})
	ix.ApplyEvents("m", a1, []uint64{1, 2, 3}, nil, false)
	ix.ReplaceConfirmed("m", a1, []uint64{2, 4})
	ix.mu.RLock()
	m := ix.models["m"]
	ix.mu.RUnlock()
	for b, want := range map[uint64]bool{1: false, 2: true, 3: false, 4: true} {
		if got := holds(m.board[b], a1, 0); got != want {
			t.Errorf("block %d held: %v, want %v", b, got, want)
		}
	}
}

// Text entries (engines without tokens) point their anchors at the agent.
func TestLearnText(t *testing.T) {
	ix, _ := newTestIndex(Config{})
	ix.LearnText("m", a2, []uint64{100, 101}, time.Minute)
	if m := ix.Lookup("m", []uint64{100, 101, 102}, []peer.ID{a1, a2}); m.Pending != a2 || m.PendingTo != 2 {
		t.Errorf("text entry: %+v", m)
	}
}

// Trust: unjudged until trustMinSamples checks; drops when predictions do not
// come true; small predictions are not checked.
func TestTrust(t *testing.T) {
	ix, _ := newTestIndex(Config{})
	if !ix.Observe(a1, 100, 0) || ix.Trust(a1) != 1 {
		t.Error("a check below trustMinPredicted must be ignored")
	}
	for range trustMinSamples - 1 {
		ix.Observe(a1, 1000, 0)
	}
	if ix.Trust(a1) != 1 {
		t.Error("judged before trustMinSamples checks")
	}
	if ix.Observe(a1, 1000, 0) {
		t.Error("a prediction with nothing cached reported as held")
	}
	if tr := ix.Trust(a1); tr > 0.4 {
		t.Errorf("trust %.2f after %d missed predictions", tr, trustMinSamples)
	}
	for range 20 {
		ix.Observe(a2, 1000, 950)
	}
	if tr := ix.Trust(a2); tr < 0.9 {
		t.Errorf("trust %.2f for an agent whose predictions hold", tr)
	}
	ix.RemoveAgent(a1)
	if ix.Trust(a1) != 1 {
		t.Error("a removed agent keeps its trust")
	}
}

// An agent's KV capacity caps its board entries below the global cap.
func TestAgentCap(t *testing.T) {
	ix, _ := newTestIndex(Config{BlocksPerAgent: 1000})
	ix.SetAgentCap(a1, 50)
	blocks := make([]uint64, 200)
	for i := range blocks {
		blocks[i] = uint64(i + 1)
	}
	ix.ApplyEvents("m", a1, blocks, nil, false)
	ix.mu.RLock()
	n := ix.models["m"].agentBlocks[a1]
	ix.mu.RUnlock()
	if n > 50 {
		t.Errorf("agent holds %d blocks, its cap is 50", n)
	}
}

// Lookup tells apart this router's guesses (Commit), what the agent reported
// (Learn) and what its engine confirmed (events), each as a leading depth.
func TestDepthByOrigin(t *testing.T) {
	ix, _ := newTestIndex(Config{})
	anchors := []uint64{100, 101}
	ix.Learn("m", a1, anchors, []int{2, 4}, blocks4, time.Minute) // a1 reported 4 blocks
	m := ix.Lookup("m", anchors, []peer.ID{a1, a2})
	ix.Commit("m", a2, anchors, m, time.Minute, 5*time.Second) // a2: a guess on the same 4
	ix.ApplyEvents("m", a1, blocks4[:3], nil, false)           // a1's engine confirms 3
	m = ix.Lookup("m", anchors, []peer.ID{a1, a2})
	if m.Depth[a1] != 4 || m.Reported[a1] != 4 || m.Confirmed[a1] != 3 {
		t.Errorf("a1: depth %d reported %d confirmed %d, want 4 4 3", m.Depth[a1], m.Reported[a1], m.Confirmed[a1])
	}
	if m.Depth[a2] != 4 || m.Reported[a2] != 0 || m.Confirmed[a2] != 0 {
		t.Errorf("a2: depth %d reported %d confirmed %d, want 4 0 0 (a guess only)", m.Depth[a2], m.Reported[a2], m.Confirmed[a2])
	}
	// A report turns the guess into a reported holding; a gap stops the depth.
	ix.Learn("m", a2, anchors, []int{2, 4}, blocks4, time.Minute)
	ix.ApplyEvents("m", a2, []uint64{blocks4[0], blocks4[2]}, nil, false)
	m = ix.Lookup("m", anchors, []peer.ID{a2})
	if m.Reported[a2] != 4 || m.Confirmed[a2] != 1 {
		t.Errorf("a2 after its report: reported %d confirmed %d, want 4 1", m.Reported[a2], m.Confirmed[a2])
	}
}

var blocks4 = []uint64{11, 12, 13, 14}

// An agent's text entry is its report; a routing note is this router's.
// A decision to the same agent keeps the agent's (longer) text entry.
func TestTextEntryIsReported(t *testing.T) {
	ix, c := newTestIndex(Config{})
	ix.LearnText("m", a2, []uint64{100}, time.Minute)
	m := ix.Lookup("m", []uint64{100, 101}, []peer.ID{a1, a2})
	if m.Pending != a2 || !m.PendingReported {
		t.Fatalf("text entry: %+v", m)
	}
	ix.Commit("m", a2, []uint64{100, 101}, m, time.Minute, 5*time.Second)
	c.t = c.t.Add(10 * time.Second) // the routing notes expired, the text entry not
	if m := ix.Lookup("m", []uint64{100}, []peer.ID{a2}); m.Pending != a2 || !m.PendingReported {
		t.Errorf("the decision replaced the agent's text entry: %+v", m)
	}
	ix.Commit("m", a1, []uint64{200}, Match{}, time.Minute, 5*time.Second)
	if m := ix.Lookup("m", []uint64{200}, []peer.ID{a1}); m.Pending != a1 || m.PendingReported {
		t.Errorf("routing note: %+v", m)
	}
}

// A cap set from a small KV capacity goes back to the global one when the
// capacity grows past it.
func TestAgentCapResets(t *testing.T) {
	ix, _ := newTestIndex(Config{BlocksPerAgent: 1000})
	ix.SetAgentCap(a1, 50)
	if !ix.HasAgentCap(a1) || ix.capOf(a1) != 50 {
		t.Fatalf("cap %d", ix.capOf(a1))
	}
	ix.SetAgentCap(a1, 5000)
	if ix.HasAgentCap(a1) || ix.capOf(a1) != 1000 {
		t.Errorf("cap %d after the capacity grew, want the global 1000", ix.capOf(a1))
	}
}

// Over its cap, an agent loses this router's guesses before what it reported.
func TestTrimDropsGuessesFirst(t *testing.T) {
	ix, _ := newTestIndex(Config{BlocksPerAgent: 100})
	reported := make([]uint64, 60)
	guessed := make([]uint64, 60)
	for i := range 60 {
		reported[i], guessed[i] = uint64(i+1), uint64(1000+i)
	}
	ix.Learn("m", a1, []uint64{1}, []int{60}, reported, time.Minute)
	ix.Learn("m", a2, []uint64{2}, []int{60}, guessed, time.Minute) // memo for anchor 2
	m := ix.Lookup("m", []uint64{2}, []peer.ID{a1})
	ix.Commit("m", a1, []uint64{2}, m, time.Minute, time.Second) // 120 > 100: trim
	if m := ix.Lookup("m", []uint64{1}, []peer.ID{a1}); m.Reported[a1] != 60 {
		t.Errorf("a1 kept %d of its 60 reported blocks", m.Reported[a1])
	}
}

// A check judges only the blocks the engine kept since the decision: blocks
// it evicted (or evicted and stored again) are not held against the agent;
// blocks it still holds and did not serve from cache are.
func TestCheckClaim(t *testing.T) {
	ix, c := newTestIndex(Config{})
	ix.ApplyEvents("m", a1, blocks12, nil, false)
	at := ix.Lookup("m", []uint64{1}, []peer.ID{a1}).At
	c.t = c.t.Add(time.Second)
	// Too small to judge.
	if r := ix.CheckClaim("m", a1, blocks12[:3], at, 0); r != "" {
		t.Errorf("3 blocks: %q, want no check", r)
	}
	// The engine evicted all but 2 blocks before the request reached it.
	ix.ApplyEvents("m", a1, nil, blocks12[2:], false)
	for range 10 {
		if r := ix.CheckClaim("m", a1, blocks12, at, 128); r != CheckEvicted {
			t.Fatalf("after evictions: %q, want %q", r, CheckEvicted)
		}
	}
	// It stored them again (the request recomputed them): still not held all along.
	ix.ApplyEvents("m", a1, blocks12[2:], nil, false)
	if r := ix.CheckClaim("m", a1, blocks12, at, 128); r != CheckEvicted {
		t.Errorf("evicted then stored again: %q, want %q", r, CheckEvicted)
	}
	if tr := ix.Trust(a1); tr != 1 {
		t.Errorf("trust %.2f after evictions only, want 1", tr)
	}
	// An agent whose engine still holds the blocks but served none of them.
	ix.ApplyEvents("m", a2, blocks12, nil, false)
	at = ix.Lookup("m", []uint64{1}, []peer.ID{a2}).At
	c.t = c.t.Add(time.Second)
	for range trustMinSamples {
		if r := ix.CheckClaim("m", a2, blocks12, at, 0); r != CheckMissed {
			t.Fatalf("held but not served: %q, want %q", r, CheckMissed)
		}
	}
	if tr := ix.Trust(a2); tr > 0.4 {
		t.Errorf("trust %.2f for an agent whose kept blocks were never cached", tr)
	}
	// A repeated store event for blocks it holds changes nothing.
	ix.ApplyEvents("m", a2, blocks12, nil, false)
	if r := ix.CheckClaim("m", a2, blocks12, at, 0); r != CheckMissed {
		t.Errorf("after a repeated store event: %q, want %q", r, CheckMissed)
	}
	// Partly evicted, the rest served: judged on what it kept, and held.
	ix.ApplyEvents("m", a2, nil, blocks12[8:], false)
	if r := ix.CheckClaim("m", a2, blocks12, at, 8*64); r != CheckHeld {
		t.Errorf("8 kept and served: %q, want %q", r, CheckHeld)
	}
}

var blocks12 = []uint64{201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212}
