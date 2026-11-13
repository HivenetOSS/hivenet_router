// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package prefixhash_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixhash"
)

var keys = prefixhash.DeriveKeys([]byte("site-secret-at-least-32-bytes-long!!"))

func chat(t *testing.T, body string) *domain.ChatRequest {
	t.Helper()
	var r domain.ChatRequest
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

// A conversation's next turn repeats its earlier messages, so its anchors
// start with the earlier turn's anchors.
func TestAnchors_NextTurnExtendsThePrevious(t *testing.T) {
	t1 := chat(t, `{"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q1"}]}`)
	t2 := chat(t, `{"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q1"},{"role":"assistant","content":"A1"},{"role":"user","content":"Q2"}]}`)
	a1, s1 := keys.Anchors("m", "", t1)
	a2, _ := keys.Anchors("m", "", t2)
	if len(a1) != 2 || len(s1) != 2 || len(a2) != 4 {
		t.Fatalf("anchors: %d, %d", len(a1), len(a2))
	}
	if !slices.Equal(a1, a2[:2]) {
		t.Error("turn 2 does not start with turn 1's anchors")
	}
	other := chat(t, `{"messages":[{"role":"system","content":"S"},{"role":"user","content":"other"}]}`)
	a3, _ := keys.Anchors("m", "", other)
	if a3[0] != a1[0] || a3[1] == a1[1] {
		t.Error("same system prompt must share anchor 0, different questions must differ at anchor 1")
	}
}

// The key, the model, the scope (cache salt) and the tools all separate anchors.
func TestAnchors_Separation(t *testing.T) {
	r := chat(t, `{"messages":[{"role":"user","content":"hello"}]}`)
	base, _ := keys.Anchors("m", "", r)
	other := prefixhash.DeriveKeys([]byte("another-site-secret-32-bytes-long!!!"))
	for name, a := range map[string]uint64{
		"key":   first(other.Anchors("m", "", r)),
		"model": first(keys.Anchors("m2", "", r)),
		"scope": first(keys.Anchors("m", "tenant-a", r)),
		"tools": first(keys.Anchors("m", "", chat(t, `{"tools":[{"name":"f"}],"messages":[{"role":"user","content":"hello"}]}`))),
		"role":  first(keys.Anchors("m", "", chat(t, `{"messages":[{"role":"system","content":"hello"}]}`))),
	} {
		if a == base[0] {
			t.Errorf("%s does not change the anchor", name)
		}
	}
	// The Anthropic top-level system prompt is the first logical message.
	sys := chat(t, `{"system":"S","messages":[{"role":"user","content":"Q"}]}`)
	inMsgs := chat(t, `{"messages":[{"role":"system","content":"S"},{"role":"user","content":"Q"}]}`)
	as, _ := keys.Anchors("m", "", sys)
	am, _ := keys.Anchors("m", "", inMsgs)
	if !slices.Equal(as, am) {
		t.Error("top-level system and a system message must give the same anchors")
	}
	if a, _ := (prefixhash.Keys{}).Anchors("m", "", r); a != nil {
		t.Error("no key: want no anchors")
	}
}

func first(a []uint64, _ []int) uint64 { return a[0] }

// Blocks are chained: equal leading tokens give equal leading blocks, and a
// trailing partial block is not hashed.
func TestBlocks_Chain(t *testing.T) {
	toks := make([]int, 3*prefixhash.BlockTokens+10)
	for i := range toks {
		toks[i] = i % 1000
	}
	b := keys.Blocks("m", "", toks)
	if len(b) != 3 {
		t.Fatalf("blocks: %d, want 3 (partial block ignored)", len(b))
	}
	longer := append(slices.Clone(toks[:3*prefixhash.BlockTokens]), toks...)
	if !slices.Equal(keys.Blocks("m", "", longer)[:3], b) {
		t.Error("equal leading tokens must give equal leading blocks")
	}
	changed := slices.Clone(toks)
	changed[0] = 999999
	if keys.Blocks("m", "", changed)[2] == b[2] {
		t.Error("a change in block 0 must change every later block")
	}
	if keys.Blocks("m", "salt", toks)[0] == b[0] || keys.Blocks("m2", "", toks)[0] == b[0] {
		t.Error("model and scope must seed the chain")
	}
}

// MessageEnds finds where each message ends from the message-start token,
// skipping a default system message the template adds and the generation prompt.
func TestMessageEnds(t *testing.T) {
	const S = 7 // message-start token
	// [default system][user 130 tokens][assistant 70][user 10][gen prompt]
	var toks []int
	open := func(n int) {
		toks = append(toks, S)
		for range n - 1 {
			toks = append(toks, 1)
		}
	}
	open(20)        // default system, 20 tokens
	u1 := len(toks) // 20
	open(130)
	a1 := len(toks) // 150
	open(70)
	u2 := len(toks) // 220
	open(10)
	gen := len(toks) // 230
	open(3)
	ends := prefixhash.MessageEnds(toks, S, 3, true)
	want := []int{a1 / prefixhash.BlockTokens, u2 / prefixhash.BlockTokens, gen / prefixhash.BlockTokens}
	if !slices.Equal(ends, want) {
		t.Errorf("ends %v, want %v (u1 at %d)", ends, want, u1)
	}
	if prefixhash.MessageEnds(toks, S, 9, true) != nil {
		t.Error("more messages than starts: want nil")
	}
	if prefixhash.MessageEnds(toks, 12345, 3, true) != nil {
		t.Error("unknown start token: want nil")
	}
	noGen := prefixhash.MessageEnds(toks[:gen], S, 3, false)
	if !slices.Equal(noGen, want) {
		t.Errorf("without generation prompt: %v, want %v", noGen, want)
	}
}

func TestAnchorHeader(t *testing.T) {
	a := []uint64{1, 1 << 63, 42}
	got, err := prefixhash.DecodeAnchors(prefixhash.EncodeAnchors(a))
	if err != nil || !slices.Equal(got, a) {
		t.Fatalf("round trip: %v %v", got, err)
	}
	for _, bad := range []string{"%%%", "AAA", strings.Repeat("A", 8*400)} {
		if _, err := prefixhash.DecodeAnchors(bad); err == nil {
			t.Errorf("%.10q: want an error", bad)
		}
	}
}

func FuzzDecodeAnchors(f *testing.F) {
	f.Add(prefixhash.EncodeAnchors([]uint64{1, 2, 3}))
	f.Fuzz(func(t *testing.T, s string) {
		a, err := prefixhash.DecodeAnchors(s)
		if err == nil && len(a) > prefixhash.MaxAnchors {
			t.Fatalf("%d anchors over the cap", len(a))
		}
	})
}
