// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"hivenet_router/internal/prefixhash"

	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// KV events: vLLM and SGLang publish what their KV cache stores and evicts on
// a ZMQ PUB socket (--kv-events-config). The agent listens for them on a local
// address the engine connects to (or dials an engine that binds), keeps a mirror
// of the engine's blocks, re-chunks them into the router's keyed 64-token
// blocks, and sends every router the changes (/prefix-state, "kv"). The
// events carry token IDs: the engine must publish on a local address only,
// and the agent never forwards token IDs.

// kvMirror maps the engine's blocks to router block hashes.
type kvMirror struct {
	keys  prefixhash.Keys
	model string

	mu      sync.Mutex
	entries map[string]mirrorEntry // engine block hash → state after that block
	held    map[uint64]int         // router block hash → engine blocks that completed it
	epoch   uint64                 // per agent run: a router that sees a new one asks for a snapshot
	seq     uint64                 // last delta
	deltas  []kvDelta              // oldest first, at most kvDeltaCap
	// orphans are stored events whose parent block the mirror does not know
	// (cached before the agent subscribed): kept a few seconds until a served
	// prompt shows which tokens come before them (resolve).
	orphans []orphanEvent
	now     func() time.Time
}

type orphanEvent struct {
	ev    kvEvent
	scope string
	at    time.Time
}

type mirrorEntry struct {
	chain prefixhash.Chain
	buf   []int32  // tokens after the last full router block
	done  []uint64 // router blocks this engine block completed
}

// kvDelta is one change of the agent's holdings, in router block hashes.
type kvDelta struct {
	seq     uint64
	stored  []uint64
	removed []uint64
	cleared bool
}

const (
	kvDeltaCap  = 1024
	orphanCap   = 256
	orphanTTL   = 10 * time.Second
	orphanMatch = 4096 // tokens of a served prompt searched for an orphan's tokens
)

func newKVMirror(keys prefixhash.Keys, model string) *kvMirror {
	var e [8]byte
	_, _ = rand.Read(e[:])
	return &kvMirror{keys: keys, model: model, entries: map[string]mirrorEntry{}, held: map[uint64]int{}, epoch: binary.LittleEndian.Uint64(e[:]) | 1, now: time.Now}
}

// kvEvent is the part of a vLLM / SGLang KV event the agent uses. Unknown
// fields are ignored, so newer engine versions that add fields still decode.
type kvEvent struct {
	Type            string `msgpack:"type"`
	BlockHashes     []any  `msgpack:"block_hashes"`
	ParentBlockHash any    `msgpack:"parent_block_hash"`
	TokenIDs        []int  `msgpack:"token_ids"`
	BlockSize       int    `msgpack:"block_size"`
	LoraName        any    `msgpack:"lora_name"`
	LoraID          any    `msgpack:"lora_id"`
	ExtraKeys       []any  `msgpack:"extra_keys"`
	CacheSalt       string `msgpack:"cache_salt"`
	Kind            string `msgpack:"kv_cache_spec_kind"`
}

// apply applies one batch of engine events and records the change.
func (m *kvMirror) apply(events []kvEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cleared := false
	net := map[uint64]bool{} // in event order: true stored, false removed
	for _, ev := range events {
		switch ev.Type {
		case "BlockStored":
			for _, h := range m.store(ev) {
				net[h] = true
			}
		case "BlockRemoved":
			for _, eh := range ev.BlockHashes {
				for _, h := range m.remove(engineKey(eh)) {
					net[h] = false
				}
			}
		case "AllBlocksCleared":
			m.clearLocked()
			cleared = true
			clear(net)
		}
	}
	if len(net) == 0 && !cleared {
		return
	}
	d := kvDelta{cleared: cleared}
	for h, stored := range net {
		if stored {
			d.stored = append(d.stored, h)
		} else {
			d.removed = append(d.removed, h)
		}
	}
	m.push(d)
}

// store records the blocks of one BlockStored event and returns the router
// blocks they complete. Caller holds m.mu.
func (m *kvMirror) store(ev kvEvent) []uint64 {
	if ev.Kind != "" && ev.Kind != "full_attention" {
		return nil // e.g. the sliding-window or mamba group of a hybrid model
	}
	if ev.LoraName != nil || ev.LoraID != nil {
		return nil // a LoRA adapter's blocks: not the base model's prefixes
	}
	scope, ok := saltOf(ev)
	if !ok {
		return nil // multimodal or unknown extra keys: not a text prefix we can match
	}
	bs := ev.BlockSize
	if bs <= 0 || len(ev.TokenIDs) < bs*len(ev.BlockHashes) {
		return nil
	}
	var st mirrorEntry
	if ev.ParentBlockHash == nil {
		st = mirrorEntry{chain: m.keys.Seed(m.model, scope)}
	} else if p, ok := m.entries[engineKey(ev.ParentBlockHash)]; ok {
		st = p
	} else {
		// Parent unknown: cached before we subscribed (or evicted). Wait for
		// a served prompt that shows the tokens before it.
		m.orphans = append(m.orphans, orphanEvent{ev: ev, scope: scope, at: m.now()})
		if len(m.orphans) > orphanCap {
			m.orphans = m.orphans[len(m.orphans)-orphanCap:]
		}
		return nil
	}
	var out []uint64
	for k, eh := range ev.BlockHashes {
		buf := make([]int32, len(st.buf), len(st.buf)+bs)
		copy(buf, st.buf)
		for _, t := range ev.TokenIDs[k*bs : (k+1)*bs] {
			buf = append(buf, int32(t))
		}
		chain := st.chain
		var done []uint64
		for len(buf) >= prefixhash.BlockTokens {
			var h uint64
			chain, h = m.keys.Next(chain, buf[:prefixhash.BlockTokens])
			buf = buf[prefixhash.BlockTokens:]
			done = append(done, h)
			if m.held[h]++; m.held[h] == 1 {
				out = append(out, h)
			}
		}
		st = mirrorEntry{chain: chain, buf: append([]int32(nil), buf...), done: done}
		key := engineKey(eh)
		if old, ok := m.entries[key]; ok {
			m.release(old.done) // stored again: count it once
		}
		m.entries[key] = st
	}
	return out
}

// resolve uses a served prompt's tokens to place waiting orphan events: an
// orphan's tokens found in the prompt at a block boundary p mean the engine
// holds prompt[:p] (its cache keeps whole prefixes), so the orphan's parent
// is the chain state after prompt[:p]. Those prefix blocks are recorded as
// held under the parent's engine hash, so evicting the parent releases them,
// and the orphan is applied.
func (m *kvMirror) resolve(tokens []int, scope string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.orphans) == 0 {
		return
	}
	now := m.now()
	net := map[uint64]bool{}
	kept := m.orphans[:0]
	for _, o := range m.orphans {
		if now.Sub(o.at) > orphanTTL {
			continue
		}
		parentKey := engineKey(o.ev.ParentBlockHash)
		if _, known := m.entries[parentKey]; !known {
			p := findAt(tokens, o.ev.TokenIDs, o.ev.BlockSize)
			if p <= 0 || o.scope != scope {
				kept = append(kept, o)
				continue
			}
			chain := m.keys.Seed(m.model, scope)
			var done []uint64
			blk := make([]int32, prefixhash.BlockTokens)
			full := p / prefixhash.BlockTokens
			for b := range full {
				for i, t := range tokens[b*prefixhash.BlockTokens : (b+1)*prefixhash.BlockTokens] {
					blk[i] = int32(t)
				}
				var h uint64
				chain, h = m.keys.Next(chain, blk)
				done = append(done, h)
				if m.held[h]++; m.held[h] == 1 {
					net[h] = true
				}
			}
			buf := make([]int32, 0, p-full*prefixhash.BlockTokens)
			for _, t := range tokens[full*prefixhash.BlockTokens : p] {
				buf = append(buf, int32(t))
			}
			m.entries[parentKey] = mirrorEntry{chain: chain, buf: buf, done: done}
		}
		for _, h := range m.store(o.ev) {
			net[h] = true
		}
	}
	m.orphans = kept
	if len(net) == 0 {
		return
	}
	d := kvDelta{}
	for h := range net {
		d.stored = append(d.stored, h)
	}
	m.push(d)
}

// findAt returns the first block-aligned position p > 0 (within the first
// orphanMatch tokens) where tokens continue with want, or -1.
func findAt(tokens, want []int, blockSize int) int {
	if len(want) == 0 || blockSize <= 0 {
		return -1
	}
	limit := min(len(tokens)-len(want), orphanMatch)
	for p := blockSize; p <= limit; p += blockSize {
		if tokens[p] != want[0] {
			continue
		}
		match := true
		for i := 1; i < len(want); i++ {
			if tokens[p+i] != want[i] {
				match = false
				break
			}
		}
		if match {
			return p
		}
	}
	return -1
}

// remove drops one engine block and returns the router blocks no engine
// block holds any more. Caller holds m.mu.
func (m *kvMirror) remove(key string) []uint64 {
	e, ok := m.entries[key]
	if !ok {
		return nil
	}
	delete(m.entries, key)
	return m.release(e.done)
}

func (m *kvMirror) release(done []uint64) []uint64 {
	var gone []uint64
	for _, h := range done {
		if m.held[h]--; m.held[h] <= 0 {
			delete(m.held, h)
			gone = append(gone, h)
		}
	}
	return gone
}

// clear forgets everything (engine cache cleared, or events lost) and
// records it, so every router drops what it had from this agent.
func (m *kvMirror) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clearLocked()
	m.push(kvDelta{cleared: true})
}

func (m *kvMirror) clearLocked() {
	m.entries = map[string]mirrorEntry{}
	m.held = map[uint64]int{}
	m.orphans = nil
}

// push appends a delta. Caller holds m.mu.
func (m *kvMirror) push(d kvDelta) {
	m.seq++
	d.seq = m.seq
	m.deltas = append(m.deltas, d)
	if len(m.deltas) > kvDeltaCap {
		m.deltas = append(m.deltas[:0], m.deltas[len(m.deltas)-kvDeltaCap:]...)
	}
}

// kvPayload is the "kv" part of a /prefix-state batch.
type kvPayload struct {
	Epoch    uint64   `json:"epoch"`
	From     uint64   `json:"from"` // first delta in the batch (snapshot: 0)
	To       uint64   `json:"to"`   // last delta in the batch, or the mirror's seq for a snapshot
	Stored   []uint64 `json:"stored,omitempty"`
	Removed  []uint64 `json:"removed,omitempty"`
	Cleared  bool     `json:"cleared,omitempty"`
	Snapshot bool     `json:"snapshot,omitempty"`
}

// since merges the deltas after seq into one change. ok is false when some
// of them already fell out of the log: the router needs a snapshot.
func (m *kvMirror) since(seq uint64) (p kvPayload, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p = kvPayload{Epoch: m.epoch, To: m.seq}
	if seq >= m.seq {
		return p, true
	}
	if len(m.deltas) == 0 || m.deltas[0].seq > seq+1 {
		return p, false
	}
	p.From = seq + 1
	net := map[uint64]bool{} // true: stored, false: removed
	for _, d := range m.deltas {
		if d.seq <= seq {
			continue
		}
		if d.cleared {
			p.Cleared = true
			clear(net)
		}
		for _, h := range d.removed {
			net[h] = false
		}
		for _, h := range d.stored {
			net[h] = true
		}
	}
	for h, stored := range net {
		if stored {
			p.Stored = append(p.Stored, h)
		} else if !p.Cleared {
			p.Removed = append(p.Removed, h)
		}
	}
	return p, true
}

// snapshot returns every router block the engine holds now.
func (m *kvMirror) snapshot() kvPayload {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := kvPayload{Epoch: m.epoch, To: m.seq, Snapshot: true, Stored: make([]uint64, 0, len(m.held))}
	for h := range m.held {
		p.Stored = append(p.Stored, h)
	}
	return p
}

// saltOf returns the cache salt that scopes an event's blocks (vLLM: the
// first extra key of block 0; SGLang: cache_salt), and false when the extra
// keys hold anything else (multimodal inputs).
func saltOf(ev kvEvent) (string, bool) {
	if ev.CacheSalt != "" {
		return ev.CacheSalt, true
	}
	salt := ""
	for i, ek := range ev.ExtraKeys {
		if ek == nil {
			continue
		}
		list, ok := ek.([]any)
		if !ok || i != 0 || ev.ParentBlockHash != nil || len(list) != 1 {
			return "", false
		}
		s, ok := list[0].(string)
		if !ok {
			return "", false
		}
		salt = s
	}
	return salt, true
}

// engineKey turns an engine block hash (an integer or bytes, by engine and
// version) into a map key.
func engineKey(v any) string {
	switch h := v.(type) {
	case []byte:
		return "b" + string(h)
	case string:
		return "s" + h
	default:
		return fmt.Sprintf("i%v", h)
	}
}

// decodeKVBatch decodes one event message: msgpack [timestamp, [events...], ...].
func decodeKVBatch(payload []byte) ([]kvEvent, error) {
	var batch []msgpack.RawMessage
	if err := msgpack.Unmarshal(payload, &batch); err != nil {
		return nil, err
	}
	if len(batch) < 2 {
		return nil, fmt.Errorf("kv events: short batch")
	}
	var events []kvEvent
	if err := msgpack.Unmarshal(batch[1], &events); err != nil {
		return nil, err
	}
	return events, nil
}

// runKVEvents subscribes to the engine's KV events until the agent stops,
// reconnecting on errors. A sequence gap or a reconnection clears the
// mirror: some events were missed, the cache refills quickly.
func (a *Agent) runKVEvents(endpoint string) {
	for !a.stopped() {
		a.consumeKVEvents(endpoint)
		a.kv.clear()
		if a.sleepOrStop(2 * time.Second) {
			return
		}
	}
}

func (a *Agent) consumeKVEvents(endpoint string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-a.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	sub := zmq4.NewSub(ctx)
	defer sub.Close()
	// vLLM and SGLang bind their publisher only to a wildcard address
	// (tcp://*:port) and connect to any other. The safe setup is the engine
	// connecting to a local address the agent listens on (no port opened by
	// the engine); an engine that binds is dialed instead.
	if lerr := sub.Listen(endpoint); lerr != nil {
		if err := sub.Dial(endpoint); err != nil {
			a.warnKVEndpoint(endpoint, lerr, err)
			return
		}
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, ""); err != nil {
		return
	}
	var last uint64
	have := false
	for {
		msg, err := sub.Recv()
		if err != nil {
			return
		}
		if len(msg.Frames) < 2 {
			continue
		}
		payload := msg.Frames[len(msg.Frames)-1]
		if len(msg.Frames) >= 3 && len(msg.Frames[len(msg.Frames)-2]) == 8 {
			seq := binary.BigEndian.Uint64(msg.Frames[len(msg.Frames)-2])
			if have && seq != last+1 {
				a.kv.clear()
			}
			last, have = seq, true
		}
		events, err := decodeKVBatch(payload)
		if err != nil {
			log.Debugf("kv events: %v", err)
			continue
		}
		a.kv.apply(events)
	}
}

// warnIfRemote logs once when the KV-event endpoint is not a local address:
// the events carry token IDs, which can be turned back into prompt text.
func warnIfRemote(endpoint string) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return
	}
	host := u.Hostname()
	if host == "localhost" {
		return
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return
	}
	log.Warnf("kv events: %s is not a local address; the engine's KV events carry prompt token IDs and must stay on the host (127.0.0.1) or inside the pod", endpoint)
}

// kvWarnEvery throttles the warning for an unusable KV-event endpoint.
const kvWarnEvery = 5 * time.Minute

// warnKVEndpoint logs, at most every kvWarnEvery, that the agent can neither
// listen on nor connect to the KV-event endpoint (e.g. the port belongs to
// another service): no cache events reach the routers.
func (a *Agent) warnKVEndpoint(endpoint string, listenErr, dialErr error) {
	a.kvWarnMu.Lock()
	defer a.kvWarnMu.Unlock()
	if time.Since(a.kvWarnedAt) < kvWarnEvery {
		return
	}
	a.kvWarnedAt = time.Now()
	log.Warnf("kv events: cannot listen on %s (%v) nor connect to it (%v): the routers get no cache events from this agent", endpoint, listenErr, dialErr)
}
