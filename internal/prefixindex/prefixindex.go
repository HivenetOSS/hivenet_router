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
	"runtime"
	"slices"
	"sync"
	"time"

	"hivenet_router/internal/prefixhash"

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
	// the global BlocksPerAgent; 0: its capacity is known and the global cap
	// applies. An agent without an entry has no known capacity yet.
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
	// memoByEnd is the set of anchors whose memo entry ends at a block, so
	// the memo forgets them when no agent holds that block any more. Kept in
	// step with memo by setMemo and dropMemo.
	memoByEnd map[uint64]map[uint64]struct{}
}

type memoEntry struct {
	blocks []uint64 // a prefix of one served prompt's blocks; shared between that prompt's anchors
	exp    int64
}

type holder struct {
	agent peer.ID
	exp   int64 // unix nanos; 0 = confirmed by the engine (never expires on its own)
	// guess marks a prediction this router made from its own routing decision
	// (Commit), as opposed to what the agent reported (Learn, events). Every
	// router receives the same reports, but each makes its own guesses.
	guess bool
	// since is when the engine last confirmed the block (unix nanos): a block
	// evicted and stored again has a later since, so a check can tell it was
	// not held all along.
	since int64
}

type note struct {
	agent peer.ID
	exp   int64
	// reported marks a text entry the agent reported (LearnText), as opposed
	// to this router's own pending note (Commit).
	reported bool
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
		m = &model{memo: map[uint64]memoEntry{}, board: map[uint64][]holder{}, agentBlocks: map[peer.ID]int{}, pending: map[uint64]note{}, memoByEnd: map[uint64]map[uint64]struct{}{}}
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
		m.setMemo(a, memoEntry{blocks: memoBlocks(blocks, e), exp: memoExp})
	}
	ix.capMemo(m, now)
	ix.predict(m, agent, blocks, now.Add(ix.predictionTTL(agent, blockTTL)).UnixNano(), false, now)
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
		m.pending[a] = note{agent: agent, exp: exp, reported: true}
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

// Results of CheckClaim.
const (
	CheckHeld    = "held"
	CheckMissed  = "missed"
	CheckEvicted = "evicted"
)

// CheckClaim checks what agent's engine confirmed holding at a routing
// decision (the leading blocks, confirmed at time at) against the cached
// tokens it reported for that request. Blocks its engine evicted since (by a
// cache event; a block stored again later counts as evicted) are not held
// against it: under KV-cache pressure an engine evicts confirmed blocks
// before the request reaches it. It returns CheckHeld or CheckMissed when the
// agent was judged, CheckEvicted when evictions left too little to judge,
// and "" when the claim was too small to judge.
func (ix *Index) CheckClaim(modelName string, agent peer.ID, blocks []uint64, at int64, actual int) string {
	if len(blocks)*prefixhash.BlockTokens < trustMinPredicted {
		return ""
	}
	kept := 0
	if m := ix.model(modelName, false); m != nil {
		m.mu.Lock()
		for _, b := range blocks {
			if !holdsIf(m.board[b], agent, func(h holder) bool { return h.exp == 0 && h.since <= at }) {
				break
			}
			kept++
		}
		m.mu.Unlock()
	}
	expected := kept * prefixhash.BlockTokens
	if expected < trustMinPredicted {
		return CheckEvicted
	}
	if ix.Observe(agent, expected, actual) {
		return CheckHeld
	}
	return CheckMissed
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
		return
	}
	ix.agentCap[agent] = 0 // known: the global cap applies
}

// AgentCapKnown reports whether SetAgentCap was called for agent since it
// was added, whether its cap is its own or the global one.
func (ix *Index) AgentCapKnown(agent peer.ID) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	_, ok := ix.agentCap[agent]
	return ok
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
				if hs[i].exp != 0 {
					hs[i].since = now.UnixNano()
				}
				hs[i].exp, hs[i].guess = 0, false
				found = true
				break
			}
		}
		if !found {
			m.board[b] = append(hs, holder{agent: agent, since: now.UnixNano()})
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
	for a := range m.memoByEnd[b] {
		delete(m.memo, a)
	}
	delete(m.memoByEnd, b)
}

// setMemo sets anchor a's memo entry and indexes it by its last block.
// Caller holds m.mu.
func (m *model) setMemo(a uint64, e memoEntry) {
	m.dropMemo(a)
	m.memo[a] = e
	if n := len(e.blocks); n > 0 {
		end := e.blocks[n-1]
		set := m.memoByEnd[end]
		if set == nil {
			set = map[uint64]struct{}{}
			m.memoByEnd[end] = set
		}
		set[a] = struct{}{}
	}
}

// dropMemo removes anchor a's memo entry and its index. Caller holds m.mu.
func (m *model) dropMemo(a uint64) {
	e, ok := m.memo[a]
	if !ok {
		return
	}
	delete(m.memo, a)
	if n := len(e.blocks); n > 0 {
		end := e.blocks[n-1]
		if set := m.memoByEnd[end]; set != nil {
			delete(set, a)
			if len(set) == 0 {
				delete(m.memoByEnd, end)
			}
		}
	}
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
// confirmed them. guess marks this router's own prediction (Commit); a report
// (Learn) turns a guess into a reported holding. Caller holds m.mu.
func (ix *Index) predict(m *model, agent peer.ID, blocks []uint64, exp int64, guess bool, now time.Time) {
	for _, b := range blocks {
		hs := m.board[b]
		found := false
		for i := range hs {
			if hs[i].agent == agent {
				if hs[i].exp != 0 && hs[i].exp < exp {
					hs[i].exp = exp
				}
				if !guess {
					hs[i].guess = false
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
		m.board[b] = append(hs, holder{agent: agent, exp: exp, guess: guess})
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
	// Depth is, per candidate, how many leading Blocks it holds, counting
	// this router's own predictions.
	Depth map[peer.ID]int
	// Reported counts only what the agents reported (engine events and served
	// prompts), the same on every router; Confirmed only what the engine's
	// cache events confirmed.
	Reported  map[peer.ID]int
	Confirmed map[peer.ID]int
	// At is when the lookup ran (unix nanos), for CheckClaim.
	At int64
	// Pending is the agent a request whose text went past the memo was just
	// sent to, if any, and PendingTo the number of anchors that note covers.
	// PendingReported is set when the note is the agent's own text entry
	// (LearnText) rather than this router's routing note.
	Pending         peer.ID
	PendingTo       int
	PendingReported bool
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
	res.At = now
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := len(anchors) - 1; k >= 0; k-- {
		if e, ok := m.memo[anchors[k]]; ok && (e.exp > now || (len(e.blocks) > 0 && confirmedAnywhere(m.board[e.blocks[len(e.blocks)-1]]))) {
			res.Known, res.Blocks = k+1, e.blocks
			break
		}
	}
	if len(res.Blocks) > 0 {
		res.Depth = leadingDepth(m.board, res.Blocks, candidates, func(h holder) bool { return h.exp == 0 || h.exp > now })
		res.Reported = leadingDepth(m.board, res.Blocks, candidates, func(h holder) bool { return !h.guess && (h.exp == 0 || h.exp > now) })
		res.Confirmed = leadingDepth(m.board, res.Blocks, candidates, func(h holder) bool { return h.exp == 0 })
	}
	for j := len(anchors) - 1; j >= res.Known; j-- {
		if n, ok := m.pending[anchors[j]]; ok && n.exp > now {
			res.Pending, res.PendingTo, res.PendingReported = n.agent, j+1, n.reported
			break
		}
	}
	return res
}

// leadingDepth returns, per candidate, how many leading blocks it holds by
// a holding that counts. Caller holds m.mu.
func leadingDepth(board map[uint64][]holder, blocks []uint64, candidates []peer.ID, counts func(holder) bool) map[peer.ID]int {
	depth := make(map[peer.ID]int, len(candidates))
	alive := make(map[peer.ID]bool, len(candidates))
	for _, c := range candidates {
		alive[c] = true
	}
	for d, b := range blocks {
		hs := board[b]
		for c := range alive {
			if !holdsIf(hs, c, counts) {
				depth[c] = d
				delete(alive, c)
			}
		}
		if len(alive) == 0 {
			break
		}
	}
	for c := range alive {
		depth[c] = len(blocks)
	}
	return depth
}

func holdsIf(hs []holder, agent peer.ID, counts func(holder) bool) bool {
	for _, h := range hs {
		if h.agent == agent && counts(h) {
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
		ix.predict(m, agent, match.Blocks, now.Add(ix.predictionTTL(agent, blockTTL)).UnixNano(), true, now)
	}
	exp := now.Add(pendingTTL).UnixNano()
	for _, a := range anchors[match.Known:] {
		// The agent's own text entry outlives a routing note: keep it.
		if n, ok := m.pending[a]; ok && n.reported && n.agent == agent && n.exp > exp {
			continue
		}
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
		n := 0
		for a := range m.memo {
			m.pause(&n)
			e, ok := m.memo[a] // read after the pause: it may have changed
			if !ok || e.exp > nanos {
				continue
			}
			// An entry whose last block an engine still confirms stays: the
			// memo forgets with the cache, the time limit is for the rest.
			if len(e.blocks) > 0 && confirmedAnywhere(m.board[e.blocks[len(e.blocks)-1]]) {
				e.exp = now.Add(ix.cfg.MemoTTL).UnixNano()
				m.memo[a] = e // same blocks: the index is unchanged
				continue
			}
			m.dropMemo(a)
		}
		for a, p := range m.pending {
			if p.exp <= nanos {
				delete(m.pending, a)
			}
			m.pause(&n)
		}
		for b := range m.board {
			m.pause(&n)
			hs := m.board[b] // read after the pause: it may have changed
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
		m.mu.Unlock()
	}
}

// memoBlocks is the first e blocks of a prompt for a memo entry. The anchors
// of one prompt share its array; an entry much shorter than the prompt gets
// its own copy, so it does not keep the whole prompt's blocks alive once the
// longer entries are gone.
func memoBlocks(blocks []uint64, e int) []uint64 {
	if 2*e < len(blocks) {
		return slices.Clone(blocks[:e])
	}
	return blocks[:e:e]
}

// sweepChunk is how many entries Sweep visits before letting the model's
// other users (Lookup, Learn, Commit) in.
const sweepChunk = 8192

// pause releases m.mu every sweepChunk calls. A map range continues
// correctly across it: the map changes only under m.mu, and the range then
// skips deleted entries and may or may not visit new ones.
func (m *model) pause(n *int) {
	if *n++; *n%sweepChunk == 0 {
		m.mu.Unlock()
		runtime.Gosched()
		m.mu.Lock()
	}
}

// Stats counts the index's entries, per kind, over all models.
type Stats struct {
	Memo     int
	MemoEnds int // memo entries indexed by their last block (follows Memo)
	Blocks   int
	Pending  int
}

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
		for _, set := range m.memoByEnd {
			s.MemoEnds += len(set)
		}
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
			m.dropMemo(a)
		}
	}
	for a := range m.memo {
		if len(m.memo) <= ix.cfg.MemoCap*9/10 {
			break
		}
		m.dropMemo(a)
	}
}

// trimAgent brings agent's board entries down to 90 % of its cap: expired
// predictions first, then this router's guesses, then live predictions, then
// confirmed holdings, in no particular order within each (an agent never
// holds more than its cap, whatever it reports). Caller holds m.mu.
func (ix *Index) trimAgent(m *model, agent peer.ID, now time.Time) {
	nanos := now.UnixNano()
	target := ix.capOf(agent) * 9 / 10
	drop := []func(h holder) bool{
		func(h holder) bool { return h.exp != 0 && h.exp <= nanos },
		func(h holder) bool { return h.guess },
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
