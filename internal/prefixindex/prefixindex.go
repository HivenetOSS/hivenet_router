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
	maxHoldersPerBlockSoft = 64
)

// Index is safe for concurrent use.
type Index struct {
	mu     sync.RWMutex
	models map[string]*model
	cfg    Config
	now    func() time.Time
}

// Config bounds the index. Zero fields take the defaults.
type Config struct {
	MemoTTL        time.Duration
	MemoCap        int
	BlocksPerAgent int
	PendingCap     int
}

type model struct {
	mu          sync.Mutex
	memo        map[uint64]memoEntry
	board       map[uint64][]holder
	agentBlocks map[peer.ID]int
	pending     map[uint64]note
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
	return &Index{models: map[string]*model{}, cfg: cfg, now: time.Now}
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
		m = &model{memo: map[uint64]memoEntry{}, board: map[uint64][]holder{}, agentBlocks: map[peer.ID]int{}, pending: map[uint64]note{}}
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
	}
	ix.capMemo(m, now)
	ix.predict(m, agent, blocks, now.Add(blockTTL).UnixNano(), now)
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
	if m.agentBlocks[agent] > ix.cfg.BlocksPerAgent {
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
		if e, ok := m.memo[anchors[k]]; ok && e.exp > now {
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
		ix.predict(m, agent, match.Blocks, now.Add(blockTTL).UnixNano(), now)
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
	ix.mu.RLock()
	ms := make([]*model, 0, len(ix.models))
	for _, m := range ix.models {
		ms = append(ms, m)
	}
	ix.mu.RUnlock()
	for _, m := range ms {
		m.mu.Lock()
		if m.agentBlocks[agent] > 0 {
			for b, hs := range m.board {
				m.board[b] = dropHolder(hs, agent)
				if len(m.board[b]) == 0 {
					delete(m.board, b)
				}
			}
		}
		delete(m.agentBlocks, agent)
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
			if e.exp <= nanos {
				delete(m.memo, a)
			}
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
			} else {
				m.board[b] = kept
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
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for _, m := range ix.models {
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

// trimAgent drops expired predictions of agent, then arbitrary ones, down to
// 90 % of the per-agent cap. Caller holds m.mu.
func (ix *Index) trimAgent(m *model, agent peer.ID, now time.Time) {
	nanos := now.UnixNano()
	target := ix.cfg.BlocksPerAgent * 9 / 10
	for _, expiredOnly := range []bool{true, false} {
		for b, hs := range m.board {
			if m.agentBlocks[agent] <= target {
				return
			}
			for i, h := range hs {
				if h.agent != agent || h.exp == 0 || (expiredOnly && h.exp > nanos) {
					continue
				}
				hs = append(hs[:i], hs[i+1:]...)
				m.agentBlocks[agent]--
				break
			}
			if len(hs) == 0 {
				delete(m.board, b)
			} else {
				m.board[b] = hs
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

func dropHolder(hs []holder, agent peer.ID) []holder {
	kept := hs[:0]
	for _, h := range hs {
		if h.agent != agent {
			kept = append(kept, h)
		}
	}
	return kept
}
