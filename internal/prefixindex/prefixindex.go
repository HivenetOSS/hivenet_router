// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package prefixindex is the router's memory for prefix-aware routing, per model:
//
//   - the memo: anchor (keyed hash of a text message prefix) → the block hashes
//     of that prefix's tokens, learned from what agents served;
//   - the board: block hash → the agents that hold it;
//   - pending notes: anchor → the agent a request with that text was just sent
//     to, so copies of new text that arrive together go together.
//
// Everything in it is advice: a wrong entry only makes a request slower, never
// wrong, because the engine checks its own cache. It holds hashes only, never
// text or tokens, and entries expire.
package prefixindex

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Default bounds; see New.
const (
	DefaultMemoTTL         = 10 * time.Minute
	DefaultMemoCap         = 200_000 // anchors per model
	DefaultBlocksPerAgent  = 65_536  // board entries per agent and model (4M tokens)
	DefaultPendingCap      = 100_000 // pending notes per model
	DefaultSpeculativeTTL  = 5 * time.Second
	maxHoldersPerBlockSoft = 64
)

// Index is safe for concurrent use.
type Index struct {
	mu     sync.RWMutex
	models map[string]*model
	cfg    Config
	now    func() time.Time
	// eventAgents report their engine's cache events: predictions on them
	// only need to bridge the moment until the event confirms them.
	eventAgents map[peer.ID]bool
	// trust is, per agent, how far the cache hits the board predicts on it
	// come true (see Observe).
	trust map[peer.ID]*trustState
	// agentCap bounds an agent's board entries (from its KV capacity), below
	// the global BlocksPerAgent.
	agentCap map[peer.ID]int
}

type trustState struct {
	score   float64
	samples int
}

// Trust bounds: an agent is judged after trustMinSamples checks of at least
// trustMinPredicted predicted tokens each.
const (
	trustMinSamples   = 5
	trustMinPredicted = 256
	trustWeight       = 0.2 // weight of the newest check in the moving average
)

// Config bounds the index. Zero fields take the defaults.
type Config struct {
	MemoTTL        time.Duration
	MemoCap        int
	BlocksPerAgent int
	PendingCap     int
	// SpeculativeTTL caps predictions on agents whose engine reports cache
	// events (the event confirms them within a moment).
	SpeculativeTTL time.Duration
}

type model struct {
	mu          sync.Mutex
	memo        map[uint64]memoEntry
	board       map[uint64][]holder
	agentBlocks map[peer.ID]int
	pending     map[uint64]note
	// memoByEnd lists the anchors whose memo entry ends at a block, so the
	// memo forgets them when no agent holds that block any more.
	memoByEnd map[uint64][]uint64
}

type memoEntry struct {
	blocks []uint64 // a prefix of one served prompt's blocks; shared between that prompt's anchors
	exp    int64
}

type holder struct {
	agent peer.ID
	exp   int64 // unix nanos; 0 = confirmed by the engine (never expires on its own)
}

type note struct {
	agent peer.ID
	exp   int64
}

// New returns an empty index.
func New(cfg Config) *Index {
	if cfg.MemoTTL <= 0 {
		cfg.MemoTTL = DefaultMemoTTL
	}
	if cfg.MemoCap <= 0 {
		cfg.MemoCap = DefaultMemoCap
	}
	if cfg.BlocksPerAgent <= 0 {
		cfg.BlocksPerAgent = DefaultBlocksPerAgent
	}
	if cfg.PendingCap <= 0 {
		cfg.PendingCap = DefaultPendingCap
	}
	if cfg.SpeculativeTTL <= 0 {
		cfg.SpeculativeTTL = DefaultSpeculativeTTL
	}
	return &Index{models: map[string]*model{}, cfg: cfg, now: time.Now, eventAgents: map[peer.ID]bool{}, trust: map[peer.ID]*trustState{}, agentCap: map[peer.ID]int{}}
}

func (ix *Index) model(name string, create bool) *model {
	ix.mu.RLock()
	m := ix.models[name]
	ix.mu.RUnlock()
	if m != nil || !create {
		return m
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if m = ix.models[name]; m == nil {
		m = &model{memo: map[uint64]memoEntry{}, board: map[uint64][]holder{}, agentBlocks: map[peer.ID]int{}, pending: map[uint64]note{}, memoByEnd: map[uint64][]uint64{}}
		ix.models[name] = m
	}
	return m
}

// Learn records what agent served: the block hashes of a prompt's tokens and,
// for each anchor k of the request, the number of full blocks before message
// k+1 (ends[k]; 0 = none). The memo maps each anchor to its blocks, the board
// predicts the blocks on agent for blockTTL, and the pending notes of those
// anchors are cleared (the memo now knows them).
func (ix *Index) Learn(modelName string, agent peer.ID, anchors []uint64, ends []int, blocks []uint64, blockTTL time.Duration) {
	if len(anchors) == 0 || len(anchors) != len(ends) {
		return
	}
	m := ix.model(modelName, true)
	now := ix.now()
	memoExp := now.Add(ix.cfg.MemoTTL).UnixNano()
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, a := range anchors {
		e := min(ends[k], len(blocks))
		delete(m.pending, a)
		if e <= 0 {
			continue
		}
		m.memo[a] = memoEntry{blocks: blocks[:e:e], exp: memoExp}
		m.memoByEnd[blocks[e-1]] = append(m.memoByEnd[blocks[e-1]], a)
	}
	ix.capMemo(m, now)
	ix.predict(m, agent, blocks, now.Add(ix.predictionTTL(agent, blockTTL)).UnixNano(), now)
}

// predictionTTL caps ttl for agents whose engine confirms blocks by events.
func (ix *Index) predictionTTL(agent peer.ID, ttl time.Duration) time.Duration {
	ix.mu.RLock()
	ev := ix.eventAgents[agent]
	ix.mu.RUnlock()
	if ev {
		return min(ttl, ix.cfg.SpeculativeTTL)
	}
	return ttl
}

// LearnText records that agent served a request whose text the memo cannot
// map to blocks (its engine returns no tokens): each anchor points to the
// agent for ttl, as a pending note does.
func (ix *Index) LearnText(modelName string, agent peer.ID, anchors []uint64, ttl time.Duration) {
	if len(anchors) == 0 {
		return
	}
	m := ix.model(modelName, true)
	exp := ix.now().Add(ttl).UnixNano()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range anchors {
		m.pending[a] = note{agent: agent, exp: exp}
	}
}

// Observe records one check of what the board predicted on agent against
// what its engine reported: predicted prompt tokens in cache vs. the cached
// tokens in the response. It returns whether the prediction held (half or
// more came true); checks below trustMinPredicted tokens are ignored and
// count as held.
func (ix *Index) Observe(agent peer.ID, predicted, actual int) bool {
	if predicted < trustMinPredicted {
		return true
	}
	ratio := min(1, float64(max(actual, 0))/float64(predicted))
	ix.mu.Lock()
	defer ix.mu.Unlock()
	t := ix.trust[agent]
	if t == nil {
		t = &trustState{score: 1}
		ix.trust[agent] = t
	}
	t.score = (1-trustWeight)*t.score + trustWeight*ratio
	t.samples++
	return ratio >= 0.5
}

// Trust is the weight of agent's predicted cache hits, 0–1: 1 until it has
// trustMinSamples checks, then the moving share of its predictions that came
// true. An agent whose engine reports cache it does not have loses weight.
func (ix *Index) Trust(agent peer.ID) float64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	t := ix.trust[agent]
	if t == nil || t.samples < trustMinSamples {
		return 1
	}
	return t.score
}

// SetAgentCap bounds agent's board entries per model to n blocks (its KV
// cache cannot hold more), when n is below the global cap.
func (ix *Index) SetAgentCap(agent peer.ID, n int) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if n > 0 && n < ix.cfg.BlocksPerAgent {
		ix.agentCap[agent] = n
	}
}

// capOf is agent's board cap. Caller must not hold ix.mu.
func (ix *Index) capOf(agent peer.ID) int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if n := ix.agentCap[agent]; n > 0 {
		return n
	}
	return ix.cfg.BlocksPerAgent
}

// SetEventSource marks agent as reporting its engine's cache events.
func (ix *Index) SetEventSource(agent peer.ID) {
	ix.mu.Lock()
	ix.eventAgents[agent] = true
	ix.mu.Unlock()
}

// ApplyEvents applies an agent's engine cache events: stored blocks become
// confirmed holdings, removed blocks are dropped, and cleared drops
// everything the agent held in this model first. A block no agent holds any
// more takes the memo entries that end at it along (the memo forgets what no
// cache holds).
func (ix *Index) ApplyEvents(modelName string, agent peer.ID, stored, removed []uint64, cleared bool) {
	m := ix.model(modelName, true)
	now := ix.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if cleared {
		m.dropAgent(agent)
	}
	for _, b := range removed {
		m.unhold(b, agent)
	}
	ix.confirm(m, agent, stored, now)
}

// ReplaceConfirmed makes set the agent's confirmed holdings in this model
// (a snapshot after a gap or a restart); its predictions are kept.
func (ix *Index) ReplaceConfirmed(modelName string, agent peer.ID, set []uint64) {
	m := ix.model(modelName, true)
	now := ix.now()
	keep := make(map[uint64]bool, len(set))
	for _, b := range set {
		keep[b] = true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for b, hs := range m.board {
		if keep[b] {
			continue
		}
		for _, h := range hs {
			if h.agent == agent && h.exp == 0 {
				m.unhold(b, agent)
				break
			}
		}
	}
	ix.confirm(m, agent, set, now)
}

// confirm marks blocks as held by agent for good (until an event removes
// them). Caller holds m.mu.
func (ix *Index) confirm(m *model, agent peer.ID, blocks []uint64, now time.Time) {
	for _, b := range blocks {
		hs := m.board[b]
		found := false
		for i := range hs {
			if hs[i].agent == agent {
				hs[i].exp = 0
				found = true
				break
			}
		}
		if !found {
			m.board[b] = append(hs, holder{agent: agent})
			m.agentBlocks[agent]++
		}
	}
	if m.agentBlocks[agent] > ix.capOf(agent) {
		ix.trimAgent(m, agent, now)
	}
}

// unhold drops agent's holding of block b; when no agent holds it any more,
// the block and the memo entries ending at it go. Caller holds m.mu.
func (m *model) unhold(b uint64, agent peer.ID) {
	hs := m.board[b]
	for i, h := range hs {
		if h.agent == agent {
			hs = append(hs[:i], hs[i+1:]...)
			m.agentBlocks[agent]--
			break
		}
	}
	if len(hs) > 0 {
		m.board[b] = hs
		return
	}
	delete(m.board, b)
	m.forgetEndingAt(b)
}

// forgetEndingAt removes the memo entries whose last block is b. Caller holds m.mu.
func (m *model) forgetEndingAt(b uint64) {
	for _, a := range m.memoByEnd[b] {
		if e, ok := m.memo[a]; ok && len(e.blocks) > 0 && e.blocks[len(e.blocks)-1] == b {
			delete(m.memo, a)
		}
	}
	delete(m.memoByEnd, b)
}

// dropAgent removes every holding of agent in this model. Caller holds m.mu.
func (m *model) dropAgent(agent peer.ID) {
	if m.agentBlocks[agent] == 0 {
		return
	}
	for b, hs := range m.board {
		for _, h := range hs {
			if h.agent == agent {
				m.unhold(b, agent)
				break
			}
		}
	}
	delete(m.agentBlocks, agent)
}

// predict marks blocks as held by agent until exp, unless an engine already
// confirmed them. Caller holds m.mu.
func (ix *Index) predict(m *model, agent peer.ID, blocks []uint64, exp int64, now time.Time) {
	for _, b := range blocks {
		hs := m.board[b]
		found := false
		for i := range hs {
			if hs[i].agent == agent {
				if hs[i].exp != 0 && hs[i].exp < exp {
					hs[i].exp = exp
				}
				found = true
				break
			}
		}
		if found {
			continue
		}
		if len(hs) >= maxHoldersPerBlockSoft {
			hs = m.pruneHolders(hs, now.UnixNano())
		}
		m.board[b] = append(hs, holder{agent: agent, exp: exp})
		m.agentBlocks[agent]++
	}
	if m.agentBlocks[agent] > ix.capOf(agent) {
		ix.trimAgent(m, agent, now)
	}
}

// Match is what the index knows about a request.
type Match struct {
	// Known is the number of leading anchors the memo knows (0 = none); Blocks
	// are the block hashes of messages[0..Known-1].
	Known  int
	Blocks []uint64
	// Depth is, per candidate, how many leading Blocks it holds.
	Depth map[peer.ID]int
	// Pending is the agent a request whose text went past the memo was just
	// sent to, if any, and PendingTo the number of anchors that note covers.
	Pending   peer.ID
	PendingTo int
}

// Lookup matches a request's anchors against the memo, the board (for the
// candidates only) and the pending notes.
func (ix *Index) Lookup(modelName string, anchors []uint64, candidates []peer.ID) Match {
	var res Match
	m := ix.model(modelName, false)
	if m == nil || len(anchors) == 0 {
		return res
	}
	now := ix.now().UnixNano()
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := len(anchors) - 1; k >= 0; k-- {
		if e, ok := m.memo[anchors[k]]; ok && (e.exp > now || (len(e.blocks) > 0 && confirmedAnywhere(m.board[e.blocks[len(e.blocks)-1]]))) {
			res.Known, res.Blocks = k+1, e.blocks
			break
		}
	}
	if len(res.Blocks) > 0 {
		res.Depth = make(map[peer.ID]int, len(candidates))
		alive := make(map[peer.ID]bool, len(candidates))
		for _, c := range candidates {
			alive[c] = true
		}
		for d, b := range res.Blocks {
			hs := m.board[b]
			for c := range alive {
				if !holds(hs, c, now) {
					res.Depth[c] = d
					delete(alive, c)
				}
			}
			if len(alive) == 0 {
				break
			}
		}
		for c := range alive {
			res.Depth[c] = len(res.Blocks)
		}
	}
	for j := len(anchors) - 1; j >= res.Known; j-- {
		if n, ok := m.pending[anchors[j]]; ok && n.exp > now {
			res.Pending, res.PendingTo = n.agent, j+1
			break
		}
	}
	return res
}

func holds(hs []holder, agent peer.ID, now int64) bool {
	for _, h := range hs {
		if h.agent == agent && (h.exp == 0 || h.exp > now) {
			return true
		}
	}
	return false
}

// Commit records a routing decision: the matched blocks are predicted on agent
// for blockTTL, and every anchor past the memo's knowledge gets a pending note
// for pendingTTL.
func (ix *Index) Commit(modelName string, agent peer.ID, anchors []uint64, match Match, blockTTL, pendingTTL time.Duration) {
	if len(anchors) == 0 {
		return
	}
	m := ix.model(modelName, true)
	now := ix.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(match.Blocks) > 0 {
		ix.predict(m, agent, match.Blocks, now.Add(ix.predictionTTL(agent, blockTTL)).UnixNano(), now)
	}
	exp := now.Add(pendingTTL).UnixNano()
	for _, a := range anchors[match.Known:] {
		m.pending[a] = note{agent: agent, exp: exp}
	}
	if len(m.pending) > ix.cfg.PendingCap {
		for a, n := range m.pending {
			if n.exp <= now.UnixNano() || len(m.pending) > ix.cfg.PendingCap {
				delete(m.pending, a)
			}
			if len(m.pending) <= ix.cfg.PendingCap*9/10 {
				break
			}
		}
	}
}

// RemoveAgent forgets everything an agent holds, in every model.
func (ix *Index) RemoveAgent(agent peer.ID) {
	ix.mu.Lock()
	delete(ix.eventAgents, agent)
	delete(ix.trust, agent)
	delete(ix.agentCap, agent)
	ms := make([]*model, 0, len(ix.models))
	for _, m := range ix.models {
		ms = append(ms, m)
	}
	ix.mu.Unlock()
	for _, m := range ms {
		m.mu.Lock()
		m.dropAgent(agent)
		for a, n := range m.pending {
			if n.agent == agent {
				delete(m.pending, a)
			}
		}
		m.mu.Unlock()
	}
}

// Sweep drops expired entries; the router runs it periodically.
func (ix *Index) Sweep() {
	now := ix.now()
	nanos := now.UnixNano()
	ix.mu.RLock()
	ms := make([]*model, 0, len(ix.models))
	for _, m := range ix.models {
		ms = append(ms, m)
	}
	ix.mu.RUnlock()
	for _, m := range ms {
		m.mu.Lock()
		for a, e := range m.memo {
			if e.exp > nanos {
				continue
			}
			// An entry whose last block an engine still confirms stays: the
			// memo forgets with the cache, the time limit is for the rest.
			if len(e.blocks) > 0 && confirmedAnywhere(m.board[e.blocks[len(e.blocks)-1]]) {
				e.exp = now.Add(ix.cfg.MemoTTL).UnixNano()
				m.memo[a] = e
				continue
			}
			delete(m.memo, a)
		}
		for a, n := range m.pending {
			if n.exp <= nanos {
				delete(m.pending, a)
			}
		}
		for b, hs := range m.board {
			kept := hs[:0]
			for _, h := range hs {
				if h.exp == 0 || h.exp > nanos {
					kept = append(kept, h)
				} else {
					m.agentBlocks[h.agent]--
				}
			}
			if len(kept) == 0 {
				delete(m.board, b)
				m.forgetEndingAt(b)
			} else {
				m.board[b] = kept
			}
		}
		for b, as := range m.memoByEnd {
			kept := as[:0]
			for _, a := range as {
				if e, ok := m.memo[a]; ok && len(e.blocks) > 0 && e.blocks[len(e.blocks)-1] == b {
					kept = append(kept, a)
				}
			}
			if len(kept) == 0 {
				delete(m.memoByEnd, b)
			} else {
				m.memoByEnd[b] = kept
			}
		}
		m.mu.Unlock()
	}
}

// Stats counts the index's entries, per kind, over all models.
type Stats struct{ Memo, Blocks, Pending int }

// Stats returns the current sizes.
func (ix *Index) Stats() Stats {
	var s Stats
	// Never hold ix.mu while taking a model's lock: Learn and Commit take
	// ix.mu (predictionTTL, capOf) while holding one.
	ix.mu.RLock()
	ms := make([]*model, 0, len(ix.models))
	for _, m := range ix.models {
		ms = append(ms, m)
	}
	ix.mu.RUnlock()
	for _, m := range ms {
		m.mu.Lock()
		s.Memo += len(m.memo)
		s.Blocks += len(m.board)
		s.Pending += len(m.pending)
		m.mu.Unlock()
	}
	return s
}

// capMemo evicts expired anchors, then arbitrary ones (Go map order), down to
// 90 % of the cap. Caller holds m.mu.
func (ix *Index) capMemo(m *model, now time.Time) {
	if len(m.memo) <= ix.cfg.MemoCap {
		return
	}
	nanos := now.UnixNano()
	for a, e := range m.memo {
		if e.exp <= nanos {
			delete(m.memo, a)
		}
	}
	for a := range m.memo {
		if len(m.memo) <= ix.cfg.MemoCap*9/10 {
			break
		}
		delete(m.memo, a)
	}
}

// trimAgent brings agent's board entries down to 90 % of its cap: expired
// predictions first, then live predictions, then confirmed holdings (an
// agent never holds more than its cap, whatever it reports). Caller holds m.mu.
func (ix *Index) trimAgent(m *model, agent peer.ID, now time.Time) {
	nanos := now.UnixNano()
	target := ix.capOf(agent) * 9 / 10
	drop := []func(h holder) bool{
		func(h holder) bool { return h.exp != 0 && h.exp <= nanos },
		func(h holder) bool { return h.exp != 0 },
		func(h holder) bool { return true },
	}
	for _, match := range drop {
		for b, hs := range m.board {
			if m.agentBlocks[agent] <= target {
				return
			}
			for _, h := range hs {
				if h.agent == agent && match(h) {
					m.unhold(b, agent)
					break
				}
			}
		}
	}
}

// pruneHolders drops the expired holders of one block. Caller holds m.mu.
func (m *model) pruneHolders(hs []holder, now int64) []holder {
	kept := hs[:0]
	for _, h := range hs {
		if h.exp == 0 || h.exp > now {
			kept = append(kept, h)
		} else {
			m.agentBlocks[h.agent]--
		}
	}
	return kept
}

func confirmedAnywhere(hs []holder) bool {
	for _, h := range hs {
		if h.exp == 0 {
			return true
		}
	}
	return false
}
