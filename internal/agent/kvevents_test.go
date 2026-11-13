// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"slices"
	"sort"
	"testing"
	"time"

	"hivenet_router/internal/prefixhash"

	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

var testKeys = prefixhash.DeriveKeys([]byte(testSecret))

func promptTokens(n int) []int {
	t := make([]int, n)
	for i := range t {
		t[i] = 1000 + i%977
	}
	return t
}

// storedEvents describes tokens as an engine with blockSize-token blocks would:
// one BlockStored per chunk of blocks, chained by parent hash.
func storedEvents(tokens []int, blockSize int, salt string, perEvent int) []kvEvent {
	var evs []kvEvent
	var parent any
	n := len(tokens) / blockSize
	for start := 0; start < n; start += perEvent {
		end := min(start+perEvent, n)
		ev := kvEvent{Type: "BlockStored", BlockSize: blockSize, ParentBlockHash: parent, Kind: "full_attention"}
		for b := start; b < end; b++ {
			ev.BlockHashes = append(ev.BlockHashes, int64(b+1)*7919)
		}
		ev.TokenIDs = tokens[start*blockSize : end*blockSize]
		if salt != "" && start == 0 {
			ev.ExtraKeys = make([]any, end-start)
			ev.ExtraKeys[0] = []any{salt}
		}
		parent = ev.BlockHashes[len(ev.BlockHashes)-1]
		evs = append(evs, ev)
	}
	return evs
}

func sorted(v []uint64) []uint64 {
	v = slices.Clone(v)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v
}

// The router blocks built from engine events equal the blocks the agent
// hashes from a whole prompt (what the memo holds), for vLLM's 16-token
// blocks, SGLang's 1-token pages, and a cache salt.
func TestKVMirror_MatchesPromptHashing(t *testing.T) {
	tokens := promptTokens(5*prefixhash.BlockTokens + 30)
	for _, tc := range []struct {
		name      string
		blockSize int
		perEvent  int
		salt      string
		sglang    bool
	}{
		{"vllm", 16, 7, "", false},
		{"vllm salted", 16, 20, "tenant-salt", false},
		{"sglang", 1, 50, "", true},
		{"sglang salted", 1, 1000, "tenant-salt", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newKVMirror(testKeys, "m")
			evs := storedEvents(tokens, tc.blockSize, tc.salt, tc.perEvent)
			if tc.sglang {
				for i := range evs {
					evs[i].ExtraKeys, evs[i].CacheSalt = nil, tc.salt
				}
			}
			m.apply(evs)
			p, ok := m.since(0)
			want := testKeys.Blocks("m", tc.salt, tokens)
			if !ok || !slices.Equal(sorted(p.Stored), sorted(want)) || len(want) != 5 {
				t.Fatalf("stored %d blocks, want the %d prompt blocks", len(p.Stored), len(want))
			}
		})
	}
}

func TestKVMirror_RemoveClearAndMerge(t *testing.T) {
	tokens := promptTokens(4 * prefixhash.BlockTokens)
	m := newKVMirror(testKeys, "m")
	m.apply(storedEvents(tokens, 16, "", 4)) // 16 engine blocks = 4 router blocks
	blocks := testKeys.Blocks("m", "", tokens)

	// Evicting the engine block that completed router block 3 removes it.
	m.apply([]kvEvent{{Type: "BlockRemoved", BlockHashes: []any{int64(16 * 7919)}}})
	p, _ := m.since(1)
	if !slices.Equal(p.Removed, []uint64{blocks[3]}) || len(p.Stored) != 0 {
		t.Fatalf("after one eviction: %+v", p)
	}
	// Merging from the start: stored then removed nets to the three left
	// stored and block 3 removed (harmless for a router that never had it).
	p, _ = m.since(0)
	if len(p.Stored) != 3 || !slices.Equal(p.Removed, []uint64{blocks[3]}) || slices.Contains(p.Stored, blocks[3]) {
		t.Errorf("merged: %d stored, removed %v", len(p.Stored), p.Removed)
	}
	// Store and remove in one batch: the block ends removed.
	m2 := newKVMirror(testKeys, "m")
	evs := storedEvents(tokens[:prefixhash.BlockTokens], 16, "", 4)
	evs = append(evs, kvEvent{Type: "BlockRemoved", BlockHashes: []any{int64(4 * 7919)}})
	m2.apply(evs)
	if p, _ := m2.since(0); len(p.Stored) != 0 {
		t.Errorf("store+remove in one batch left %v stored", p.Stored)
	}

	m.apply([]kvEvent{{Type: "AllBlocksCleared"}})
	p, _ = m.since(0)
	if !p.Cleared || len(p.Stored) != 0 || len(p.Removed) != 0 {
		t.Errorf("after cleared: %+v", p)
	}
	if s := m.snapshot(); len(s.Stored) != 0 || !s.Snapshot {
		t.Errorf("snapshot after cleared: %+v", s)
	}
}

// Blocks a router block hash cannot represent are skipped: LoRA, multimodal
// extra keys, non-full-attention groups, and chains whose parent is unknown.
func TestKVMirror_Skips(t *testing.T) {
	tokens := promptTokens(2 * prefixhash.BlockTokens)
	for name, mod := range map[string]func(*kvEvent){
		"lora":       func(e *kvEvent) { e.LoraName = "adapter" },
		"multimodal": func(e *kvEvent) { e.ExtraKeys = []any{[]any{"img-hash", 3}} },
		"swa":        func(e *kvEvent) { e.Kind = "sliding_window" },
		"orphan":     func(e *kvEvent) { e.ParentBlockHash = int64(424242) },
	} {
		m := newKVMirror(testKeys, "m")
		evs := storedEvents(tokens, 16, "", 100)
		mod(&evs[0])
		m.apply(evs)
		if s := m.snapshot(); len(s.Stored) != 0 {
			t.Errorf("%s: %d blocks stored, want none", name, len(s.Stored))
		}
	}
}

// The delta log is bounded: a router that fell behind gets a snapshot.
func TestKVMirror_SinceNeedsSnapshot(t *testing.T) {
	m := newKVMirror(testKeys, "m")
	for i := range kvDeltaCap + 10 {
		m.apply([]kvEvent{{Type: "BlockRemoved", BlockHashes: []any{int64(i)}}, {Type: "AllBlocksCleared"}})
	}
	if _, ok := m.since(1); ok {
		t.Error("deltas past the log: want a snapshot")
	}
	if p, ok := m.since(m.seq); !ok || p.To != m.seq {
		t.Errorf("up to date: %+v %v", p, ok)
	}
}

// End to end over ZMQ: a publisher shaped like vLLM's (topic, sequence,
// msgpack [ts, events, dp_rank]) reaches the agent's mirror; a sequence gap
// clears it.
func TestKVEvents_ZMQ(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep := fmt.Sprintf("tcp://%s", l.Addr())
	l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub := zmq4.NewPub(ctx)
	defer pub.Close()
	if err := pub.Listen(ep); err != nil {
		t.Fatal(err)
	}
	a := &Agent{cfg: nil, stopCh: make(chan struct{}), kv: newKVMirror(testKeys, "m")}
	defer close(a.stopCh)
	go a.consumeKVEvents(ep)

	tokens := promptTokens(3 * prefixhash.BlockTokens)
	send := func(seq uint64, evs []kvEvent) {
		raw := make([]any, len(evs))
		for i, e := range evs {
			raw[i] = map[string]any{"type": e.Type, "block_hashes": e.BlockHashes, "parent_block_hash": e.ParentBlockHash,
				"token_ids": e.TokenIDs, "block_size": e.BlockSize, "lora_id": nil, "medium": "GPU", "lora_name": nil,
				"extra_keys": e.ExtraKeys, "group_idx": 0, "kv_cache_spec_kind": e.Kind}
		}
		payload, _ := msgpack.Marshal([]any{float64(time.Now().Unix()), raw, 0})
		var s [8]byte
		binary.BigEndian.PutUint64(s[:], seq)
		_ = pub.Send(zmq4.NewMsgFrom([]byte("kv-events"), s[:], payload))
	}
	want := testKeys.Blocks("m", "", tokens)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		send(0, storedEvents(tokens, 16, "", 100)) // resent until the subscription is up
		time.Sleep(50 * time.Millisecond)
		if len(a.kv.snapshot().Stored) == len(want) {
			break
		}
	}
	if got := a.kv.snapshot().Stored; !slices.Equal(sorted(got), sorted(want)) {
		t.Fatalf("over ZMQ: %d blocks, want %d", len(got), len(want))
	}
	send(5, nil) // gap after 0: the mirror is cleared
	time.Sleep(100 * time.Millisecond)
	if n := len(a.kv.snapshot().Stored); n != 0 {
		t.Errorf("after a sequence gap: %d blocks, want 0", n)
	}
}

// The safe setup: the agent listens on a local address and the engine
// connects to it (vLLM and SGLang connect to any non-wildcard endpoint).
func TestKVEvents_EngineConnects(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ep := fmt.Sprintf("tcp://%s", l.Addr())
	l.Close()
	a := &Agent{stopCh: make(chan struct{}), kv: newKVMirror(testKeys, "m")}
	defer close(a.stopCh)
	go a.consumeKVEvents(ep)
	time.Sleep(200 * time.Millisecond) // the agent listens first
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pub := zmq4.NewPub(ctx)
	defer pub.Close()
	if err := pub.Dial(ep); err != nil {
		t.Fatal(err)
	}
	tokens := promptTokens(2 * prefixhash.BlockTokens)
	evs := storedEvents(tokens, 16, "", 100)
	raw := []any{map[string]any{"type": "BlockStored", "block_hashes": evs[0].BlockHashes, "parent_block_hash": nil, "token_ids": evs[0].TokenIDs, "block_size": 16}}
	payload, _ := msgpack.Marshal([]any{1.0, raw, 0})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(a.kv.snapshot().Stored) == 0 {
		_ = pub.Send(zmq4.NewMsgFrom([]byte(""), make([]byte, 8), payload))
		time.Sleep(50 * time.Millisecond)
	}
	if n := len(a.kv.snapshot().Stored); n != 2 {
		t.Errorf("engine connecting to the agent: %d blocks, want 2", n)
	}
}

// A prompt that extends a prefix the engine cached before the agent
// subscribed: its stored event has an unknown parent. Once the agent learns
// the served prompt's tokens, the orphan is placed: the router blocks are
// those of the whole prompt, and evicting the parent releases the prefix.
func TestKVMirror_ResolvesOrphansFromServedPrompts(t *testing.T) {
	prompt := promptTokens(5 * prefixhash.BlockTokens)
	for _, bs := range []int{1, 16} {
		m := newKVMirror(testKeys, "m")
		p := 2*prefixhash.BlockTokens + 16 // the engine already had prompt[:p]
		n := (len(prompt) - p) / bs
		ev := kvEvent{Type: "BlockStored", BlockSize: bs, ParentBlockHash: int64(-99), Kind: "full_attention", TokenIDs: prompt[p : p+n*bs]}
		for i := range n {
			ev.BlockHashes = append(ev.BlockHashes, int64(1000+i))
		}
		m.apply([]kvEvent{ev})
		if s := m.snapshot(); len(s.Stored) != 0 {
			t.Fatalf("bs %d: orphan stored %d blocks before it was placed", bs, len(s.Stored))
		}
		m.resolve(prompt, "")
		want := testKeys.Blocks("m", "", prompt)
		if got := m.snapshot().Stored; !slices.Equal(sorted(got), sorted(want)) {
			t.Fatalf("bs %d: %d blocks after resolve, want the prompt's %d", bs, len(got), len(want))
		}
		// Evicting the leaves and then the parent releases everything.
		var leaves []any
		for i := range n {
			leaves = append(leaves, int64(1000+i))
		}
		m.apply([]kvEvent{{Type: "BlockRemoved", BlockHashes: leaves}, {Type: "BlockRemoved", BlockHashes: []any{int64(-99)}}})
		if n := len(m.snapshot().Stored); n != 0 {
			t.Errorf("bs %d: %d blocks left after evicting the chain", bs, n)
		}
	}
	// Orphans whose tokens are in no served prompt expire.
	m := newKVMirror(testKeys, "m")
	clock := time.Unix(0, 0)
	m.now = func() time.Time { return clock }
	m.apply([]kvEvent{{Type: "BlockStored", BlockSize: 16, ParentBlockHash: int64(5), TokenIDs: promptTokens(16), BlockHashes: []any{int64(6)}}})
	clock = clock.Add(orphanTTL + time.Second)
	m.resolve(promptTokens(64), "")
	if len(m.orphans) != 0 {
		t.Errorf("%d orphans kept past their TTL", len(m.orphans))
	}
}
