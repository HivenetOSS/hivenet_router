// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Credits shares an occupancy budget (B2 or B4) across the routers of a site.
//
// Valkey holds, for each model, a pool: how much of the budget every router
// holds right now. A router admits against the credits in its own wallet, in
// memory, and calls Valkey only to refill the wallet or to write back what it
// holds. A refill is granted only from the part of the budget no router holds,
// so the wallets together never exceed the budget.
//
// Each tick a router keeps what it uses plus a small headroom and hands back
// the rest; when another router is starved it keeps no headroom. It also
// rewrites what it holds, which heals a write lost in a Valkey failover.
//
// A router that stops heartbeating keeps its credits while it may still be
// admitting within them. They go back to the pool once a newer run of the same
// router is live, or once every agent it published with its last heartbeat
// reports to this router that it has no connection to it: it cannot be
// running anything then, since the agents stop the requests of a router they
// lose. A silent router whose agents this router cannot ask keeps its credits.
// The silent router, for its part, admits nothing once an agent joins it
// after the list it last published (Store.Fenced): that agent may have
// reported it gone in between.
//
// While Valkey is unreachable a router keeps admitting within what it holds,
// capped at its share of the budget, and asks for no more. After an outage or
// an eviction Valkey may have lost what the other routers hold: until every
// router seen before is back or gone, this router holds at most the budget
// minus what those routers held when it last saw them.
type Credits struct {
	s    *Store
	ns   string
	gone func(peerID string, agents []string) bool

	mu      sync.Mutex
	wallets map[string]*wallet
	evicted uint64

	refills  atomic.Int64
	returned atomic.Int64
	reclaims atomic.Int64
	closed   atomic.Bool // ReleaseAll ran: admit nothing more

	idleAfter time.Duration
}

// errStaleEpoch is a grant refused because the store lost this run (an
// eviction, or lost data) and this router has not noticed yet.
var errStaleEpoch = errors.New("shared occupancy: this router's epoch is stale")

type wallet struct {
	key string

	mu         sync.Mutex
	grantW     int64 // credits held: token weight
	grantC     int64 // credits held: requests
	usedW      int64
	usedC      int64
	budget     int64
	maxIn      int64
	starved    bool      // a request found no credit since the last tick
	failedAt   time.Time // last refill that came back short
	lastActive time.Time
	// What the other runs held at the last tick that read the pool; kept for
	// pending runs the pool no longer knows. firstRead is when a tick first
	// read it: what it says about a pending run counts only if read before
	// this router started waiting for that run.
	peerHeld  map[string][2]int64
	firstRead time.Time

	refill sync.Mutex // one refill (or rewrite) in flight per wallet
}

// chunkDiv sizes a refill: budget / (chunkDiv × routers), so a router asks
// for a small slice at a time and the rest stays available to the others.
const chunkDiv = 8

// refillBackoff spaces out refills that came back short, so parked requests
// never turn into a stream of Valkey calls.
const refillBackoff = 50 * time.Millisecond

// idleAfter is how long a wallet with nothing in flight keeps its credits;
// then it hands them all back and is dropped, here and in Valkey.
const idleAfter = time.Minute

// NewCredits shares a budget under namespace ns ("occ" for B2, "kocc" for B4).
// gone, if set, reports whether every agent in agents told this router that
// it has no connection to the router with libp2p peer ID peerID; without it a
// silent router's credits come back only when a newer run replaces it.
func (s *Store) NewCredits(ns string, gone func(peerID string, agents []string) bool) *Credits {
	return &Credits{s: s, ns: ns, gone: gone, wallets: map[string]*wallet{}, idleAfter: idleAfter}
}

// SetIdleAfter changes how long an unused wallet keeps its credits (default
// one minute). Call before Start.
func (c *Credits) SetIdleAfter(d time.Duration) { c.idleAfter = d }

// Wallets returns how many wallets this router holds (tests, metrics).
func (c *Credits) Wallets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.wallets)
}

func (c *Credits) poolKey(key string) string { return c.s.key("pool", c.ns, key) }

func (c *Credits) wallet(key string) *wallet {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.wallets[key]
	if w == nil {
		w = &wallet{key: key, lastActive: time.Now()}
		c.wallets[key] = w
	}
	return w
}

func (c *Credits) degraded() bool { return c.s.DegradedEpoch() != 0 }

// limits returns the most this wallet may hold. Caller holds wl.mu.
//   - Store unreachable, or grants not replicated: this router's share of the
//     budget, so the site stays within it even if every router holds its share.
//   - Back after an outage or an eviction: the budget minus what the routers
//     not back yet held when this router last saw them (the pool may have lost
//     it); nothing more than it holds for a wallet whose pool it did not read
//     before the loss.
func (c *Credits) limits(wl *wallet) (capW, capC int64) {
	capW, capC = -1, -1 // no limit
	if c.degraded() || c.s.Unreplicated() {
		n := int64(c.s.Size())
		if wl.budget > 0 {
			capW = wl.budget / n
		}
		if wl.maxIn > 0 {
			capC = max(wl.maxIn/n, 1)
		}
		return
	}
	if c.s.PendingCount() == 0 {
		return
	}
	pending := c.s.Pending()
	var heldW, heldC int64
	for run, p := range pending {
		if wl.firstRead.IsZero() || !wl.firstRead.Before(p.Since) {
			return wl.grantW, wl.grantC
		}
		heldW += wl.peerHeld[run][0]
		heldC += wl.peerHeld[run][1]
	}
	if wl.budget > 0 {
		capW = max(wl.budget-heldW, 0)
	}
	if wl.maxIn > 0 {
		capC = max(wl.maxIn-heldC, 0)
	}
	return
}

// fits reports whether w more weight and one more request fit the wallet.
// Caller holds wl.mu.
func (c *Credits) fits(wl *wallet, w int64) bool {
	gW, gC := wl.grantW, wl.grantC
	capW, capC := c.limits(wl)
	if capW >= 0 {
		gW = min(gW, capW)
	}
	if capC >= 0 {
		gC = min(gC, capC)
	}
	if wl.budget > 0 && wl.usedW+w > gW {
		return false
	}
	if wl.maxIn > 0 && wl.usedC+1 > gC {
		return false
	}
	return true
}

func (wl *wallet) take(w int64) {
	wl.usedW += w
	wl.usedC++
	wl.lastActive = time.Now()
}

// Take implements admission.Pool.
func (c *Credits) Take(ctx context.Context, key string, w, budget int64, maxInflight int) bool {
	if c.closed.Load() || c.s.Fenced() {
		return false
	}
	c.checkEvicted()
	wl := c.wallet(key)
	wl.mu.Lock()
	wl.budget, wl.maxIn, wl.lastActive = budget, int64(maxInflight), time.Now()
	if c.fits(wl, w) {
		wl.take(w)
		wl.mu.Unlock()
		return true
	}
	if c.degraded() || time.Since(wl.failedAt) < refillBackoff {
		wl.starved = true
		wl.mu.Unlock()
		return false
	}
	wl.mu.Unlock()

	if !wl.refill.TryLock() {
		// Another request is refilling; the caller parks and retries.
		wl.mu.Lock()
		wl.starved = true
		wl.mu.Unlock()
		return false
	}
	defer wl.refill.Unlock()

	wl.mu.Lock()
	if c.fits(wl, w) {
		wl.take(w)
		wl.mu.Unlock()
		return true
	}
	n := int64(max(c.s.Size(), 1))
	var needW, wantW, needC, wantC int64
	if budget > 0 {
		needW = max(wl.usedW+w-wl.grantW, 0)
		wantW = max(needW, budget/(chunkDiv*n))
	}
	if maxInflight > 0 {
		needC = max(wl.usedC+1-wl.grantC, 0)
		wantC = max(needC, int64(maxInflight)/(chunkDiv*n), 1)
	}
	// Never ask past what this wallet may hold.
	capW, capC := c.limits(wl)
	if capW >= 0 {
		wantW = min(wantW, max(capW-wl.grantW, 0))
	}
	if capC >= 0 {
		wantC = min(wantC, max(capC-wl.grantC, 0))
	}
	if wantW < needW || wantC < needC {
		wl.starved = true
		wl.mu.Unlock()
		return false
	}
	wl.mu.Unlock()

	gotW, gotC, err := c.grant(ctx, key, needW, wantW, needC, wantC, budget, int64(maxInflight))

	wl.mu.Lock()
	defer wl.mu.Unlock()
	if err == nil {
		c.refills.Add(1)
		wl.grantW += gotW
		wl.grantC += gotC
		if c.fits(wl, w) {
			wl.take(w)
			return true
		}
	}
	wl.failedAt = time.Now()
	wl.starved = true
	return false
}

// Adjust implements admission.Pool: a release, streamed growth or a true-up.
// Credits stay in the wallet; the tick hands back what is not needed.
func (c *Credits) Adjust(key string, dw int64, dc int) {
	wl := c.wallet(key)
	wl.mu.Lock()
	wl.usedW = max(wl.usedW+dw, 0)
	wl.usedC = max(wl.usedC+int64(dc), 0)
	wl.lastActive = time.Now()
	wl.mu.Unlock()
}

// grant asks the pool for credits: at least need (else nothing), up to want.
// It is called on a request's path, so it never connects: it fails at once
// while the store does not answer.
//
// The grant presents this run's epoch: when the store lost this run (an
// eviction, or lost data) and this router has not noticed yet, it is refused.
//
// With Sentinel a replica must acknowledge the grant (WAIT), so a failover
// cannot forget it and hand the same capacity to another router. When no
// replica acknowledges in time the grant is still used, but the router stays
// at its share of the budget until a replica acknowledges again (Unreplicated):
// refusing it would stop admission on the whole site while replicas lag.
func (c *Credits) grant(ctx context.Context, key string, needW, wantW, needC, wantC, budget, maxIn int64) (int64, int64, error) {
	cl, err := c.s.connected()
	if err != nil {
		return 0, 0, err
	}
	epoch := c.s.currentEpoch()
	if epoch == "" {
		return 0, 0, ErrNotConnected
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	args := []string{c.s.inc, itoa(needW), itoa(wantW), itoa(needC), itoa(wantC), itoa(budget), itoa(maxIn), epoch}
	cmds := valkey.Commands{cl.B().Eval().Script(grantScript).Numkeys(3).
		Key(c.poolKey(key), c.s.key("routers"), c.s.key("epochs")).Arg(args...).Build()}
	replicated := c.s.cfg.SentinelMaster != ""
	if replicated {
		cmds = append(cmds, cl.B().Wait().Numreplicas(1).Timeout(20).Build())
	}
	res := cl.DoMulti(ctx, cmds...)
	c.s.m.credit.WithLabelValues("refill").Observe(time.Since(start).Seconds())
	v, err := res[0].AsIntSlice()
	if err != nil || len(v) != 2 {
		c.s.m.creditErrors.Inc()
		if err == nil {
			err = errShortReply
		}
		return 0, 0, err
	}
	if v[0] < 0 {
		return 0, 0, errStaleEpoch
	}
	if replicated && (v[0] > 0 || v[1] > 0) {
		if acks, err := res[1].AsInt64(); err != nil || acks < 1 {
			c.s.m.creditErrors.Inc()
			c.s.setUnreplicated(true)
		}
	}
	return v[0], v[1], nil
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// Start runs the tick until ctx is done.
func (c *Credits) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(c.s.cfg.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.tick(ctx)
			}
		}
	}()
}

// checkEvicted drops every grant this router holds once the other routers
// have evicted it: they took its credits back, so it may only use them again
// once they are granted afresh. What is in use stays recorded as held.
func (c *Credits) checkEvicted() {
	ev := c.s.Evictions()
	c.mu.Lock()
	if ev == c.evicted {
		c.mu.Unlock()
		return
	}
	c.evicted = ev
	ws := make([]*wallet, 0, len(c.wallets))
	for _, wl := range c.wallets {
		ws = append(ws, wl)
	}
	c.mu.Unlock()
	for _, wl := range ws {
		wl.mu.Lock()
		wl.grantW, wl.grantC = wl.usedW, wl.usedC
		wl.mu.Unlock()
	}
}

// tick hands back unneeded credits, flags starved wallets, rewrites what this
// router holds, and takes back the credits of routers that are gone.
func (c *Credits) tick(ctx context.Context) {
	if c.degraded() {
		return
	}
	c.checkEvicted()
	c.resolvePending()
	cl, err := c.s.conn()
	if err != nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	c.reclaimUnreachable(cctx, cl)

	// A wallet idle past idleAfter handed its credits back on an earlier
	// tick (keep is 0 below); drop it here and in Valkey, so keys and models
	// no longer used cost nothing.
	c.mu.Lock()
	ws := make([]*wallet, 0, len(c.wallets))
	var drops valkey.Commands
	for k, wl := range c.wallets {
		wl.mu.Lock()
		idle := wl.usedW == 0 && wl.usedC == 0 && wl.grantW == 0 && wl.grantC == 0 && time.Since(wl.lastActive) > c.idleAfter
		wl.mu.Unlock()
		if idle {
			delete(c.wallets, k)
			drops = append(drops, cl.B().Hdel().Key(c.poolKey(k)).Field("g|"+c.s.inc+"|w", "g|"+c.s.inc+"|c", "want|"+c.s.inc).Build())
			continue
		}
		ws = append(ws, wl)
	}
	c.mu.Unlock()
	if len(drops) > 0 {
		cl.DoMulti(cctx, drops...)
	}
	if len(ws) == 0 {
		return
	}

	pending := map[string]bool{}
	for run := range c.s.Pending() {
		pending[run] = true
	}

	// Who else is starved, and what the others hold, per pool.
	reads := make(valkey.Commands, len(ws))
	for i, wl := range ws {
		reads[i] = cl.B().Hgetall().Key(c.poolKey(wl.key)).Build()
	}
	res := cl.DoMulti(cctx, reads...)
	now := time.Now().UnixMilli()
	window := 2*c.s.cfg.Interval.Milliseconds() + 500

	var writes valkey.Commands
	var locked []*wallet
	for i, wl := range ws {
		// Hold the refill lock until the rewrite has landed, so a grant can
		// never be overwritten by an older holding.
		if !wl.refill.TryLock() {
			continue
		}
		locked = append(locked, wl)
		othersWant := false
		fields, ferr := res[i].AsStrMap()
		held := map[string][2]int64{}
		for f, v := range fields {
			if strings.HasPrefix(f, "want|") && f != "want|"+c.s.inc {
				if ts, _ := strconv.ParseInt(v, 10, 64); now-ts < window {
					othersWant = true
				}
			}
			if run, kind, ok := grantField(f); ok && run != c.s.inc {
				n, _ := strconv.ParseInt(v, 10, 64)
				h := held[run]
				if kind == 'w' {
					h[0] = n
				} else {
					h[1] = n
				}
				held[run] = h
			}
		}
		wl.mu.Lock()
		if ferr == nil {
			// Keep what pending runs held: the pool may have lost it.
			for run, h := range wl.peerHeld {
				if _, ok := held[run]; !ok && pending[run] {
					held[run] = h
				}
			}
			wl.peerHeld = held
			if wl.firstRead.IsZero() {
				wl.firstRead = time.Now()
			}
		}
		n := int64(max(c.s.Size(), 1))
		var headW, headC int64
		if !othersWant {
			if wl.budget > 0 {
				headW = wl.budget / (chunkDiv * n)
			}
			if wl.maxIn > 0 {
				headC = max(wl.maxIn/(chunkDiv*n), 1)
			}
		}
		keepW, keepC := wl.usedW+headW, wl.usedC+headC
		if wl.usedW == 0 && wl.usedC == 0 && time.Since(wl.lastActive) > c.idleAfter {
			keepW, keepC = 0, 0 // idle: hand everything back
		}
		if wl.budget <= 0 {
			keepW = 0
		}
		if wl.maxIn <= 0 {
			keepC = 0
		}
		if wl.grantW > keepW || wl.grantC > keepC {
			c.returned.Add(1)
		}
		wl.grantW, wl.grantC = min(wl.grantW, keepW), min(wl.grantC, keepC)
		// Back after an outage: hand back what is past this wallet's limit.
		if capW, capC := c.limits(wl); capW >= 0 || capC >= 0 {
			if capW >= 0 {
				wl.grantW = min(wl.grantW, capW)
			}
			if capC >= 0 {
				wl.grantC = min(wl.grantC, capC)
			}
		}
		// Output streamed past the credits is in use: record it as held.
		if wl.budget > 0 {
			wl.grantW = max(wl.grantW, wl.usedW)
		}
		if wl.maxIn > 0 {
			wl.grantC = max(wl.grantC, wl.usedC)
		}
		gW, gC := wl.grantW, wl.grantC
		uW, uC := wl.usedW, wl.usedC
		starved := "0"
		if wl.starved {
			starved = "1"
		}
		wl.starved = false
		wl.mu.Unlock()

		writes = append(writes, cl.B().Eval().Script(rewriteScript).Numkeys(2).
			Key(c.poolKey(wl.key), c.s.key("epochs")).
			Arg(c.s.inc, c.s.currentEpoch(), itoa(gW), itoa(gC), itoa(uW), itoa(uC), starved, itoa(now)).Build())
	}
	if len(writes) > 0 {
		for _, r := range cl.DoMulti(cctx, writes...) {
			if r.Error() != nil {
				c.s.m.creditErrors.Inc()
				break
			}
		}
	}
	for _, wl := range locked {
		wl.refill.Unlock()
	}
	// Grants were not acknowledged: check whether a replica follows again.
	if c.s.Unreplicated() {
		if acks, err := cl.Do(cctx, cl.B().Wait().Numreplicas(1).Timeout(100).Build()).AsInt64(); err == nil && acks >= 1 {
			c.s.setUnreplicated(false)
		}
	}
}

// grantField parses a pool field "g|<run>|w" or "g|<run>|c".
func grantField(f string) (run string, kind byte, ok bool) {
	if len(f) < 5 || !strings.HasPrefix(f, "g|") || f[len(f)-2] != '|' {
		return "", 0, false
	}
	k := f[len(f)-1]
	if k != 'w' && k != 'c' {
		return "", 0, false
	}
	return f[2 : len(f)-2], k, true
}

// goneWhy reports why a silent run has certainly stopped serving, or "" when
// it may still be: a newer run of the same router is live, or every agent it
// published reports no connection to it. agents is nil when this router does
// not know which agents the run served.
func (c *Credits) goneWhy(run, peerID string, agents []string) string {
	if c.s.Superseded(run) {
		return "a newer run of the same router is live"
	}
	if agents != nil && c.gone != nil && peerID != "" && c.gone(peerID, agents) {
		if len(agents) == 0 {
			return "it served no agent"
		}
		return "none of its agents is connected to it"
	}
	return ""
}

// resolvePending stops waiting for the routers seen before an outage that
// are gone; see Store.Pending.
func (c *Credits) resolvePending() {
	for run, p := range c.s.Pending() {
		agents, ok := c.s.AgentsOf(run)
		if !ok {
			agents = nil
		}
		if why := c.goneWhy(run, p.Peer, agents); why != "" {
			c.s.Forget(run)
			log.Infof("shared occupancy: no longer waiting for router %s: %s", run, why)
		}
	}
}

// reclaimUnreachable evicts the silent routers that are gone, so their
// credits go back to the pool. A silent router that may still be admitting
// keeps its credits.
func (c *Credits) reclaimUnreachable(ctx context.Context, cl valkey.Client) {
	stale, err := staleRouters.Exec(ctx, cl, []string{c.s.key("routers"), c.s.key("peers"), c.s.key("agents")},
		[]string{itoa(c.s.cfg.DownAfter.Milliseconds())}).AsStrSlice()
	if err != nil {
		return
	}
	for i := 0; i+3 < len(stale); i += 4 {
		run, peerID, score := stale[i], stale[i+1], stale[i+2]
		if run == c.s.inc {
			continue
		}
		var agents []string
		if _, list, ok := strings.Cut(stale[i+3], "|"); ok {
			agents = splitList(list)
		}
		why := c.goneWhy(run, peerID, agents)
		if why == "" {
			continue
		}
		// Evicted only if it is still silent: one atomic check and removal.
		n, err := evictRouter.Exec(ctx, cl, []string{c.s.key("routers"), c.s.key("epochs")}, []string{run, score}).AsInt64()
		if err == nil && n == 1 {
			c.reclaims.Add(1)
			c.s.m.creditReclaims.Inc()
			log.Infof("shared occupancy: router %s stopped (%s); its credits are back in the pool", run, why)
		}
	}
}

// ReleaseAll hands back every credit this router holds, at shutdown once it
// can no longer serve. A request still in flight then holds nothing: call it
// after the router has stopped forwarding.
func (c *Credits) ReleaseAll(ctx context.Context) {
	c.closed.Store(true)
	cl, err := c.s.conn()
	if err != nil {
		return
	}
	c.mu.Lock()
	var cmds valkey.Commands
	for k := range c.wallets {
		cmds = append(cmds, cl.B().Hdel().Key(c.poolKey(k)).Field("g|"+c.s.inc+"|w", "g|"+c.s.inc+"|c", "want|"+c.s.inc).Build())
	}
	c.wallets = map[string]*wallet{}
	c.mu.Unlock()
	if len(cmds) > 0 {
		cl.DoMulti(ctx, cmds...)
	}
}

// Stats returns refills, hand-backs and early reclaims so far (tests, logs).
func (c *Credits) Stats() (refills, returned, reclaims int64) {
	return c.refills.Load(), c.returned.Load(), c.reclaims.Load()
}

// grantScript grants credits from a model's pool.
// KEYS: pool hash, live-router set, epoch hash.
// ARGV: run, needW, wantW, needC, wantC, budget, maxIn, epoch.
// A run whose epoch does not match gets {-1, -1}: the store lost it since it
// last looked. What routers no longer in the live set held (evicted) is
// dropped first. Grants min(want, free) when free covers need, else nothing.
// Returns {weight, requests} granted.
const grantScript = `
local me = ARGV[1]
if redis.call('HGET', KEYS[3], me) ~= ARGV[8] then return {-1, -1} end
local live = {}
for _, r in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do live[r] = true end
live[me] = true
local h = redis.call('HGETALL', KEYS[1])
local sw, sc = 0, 0
for i = 1, #h, 2 do
  local inst, kind = string.match(h[i], '^g|(.+)|([wc])$')
  if inst then
    if live[inst] then
      if kind == 'w' then sw = sw + tonumber(h[i+1]) else sc = sc + tonumber(h[i+1]) end
    else
      redis.call('HDEL', KEYS[1], h[i])
    end
  elseif string.sub(h[i], 1, 5) == 'want|' and not live[string.sub(h[i], 6)] then
    redis.call('HDEL', KEYS[1], h[i])
  end
end
local needW, wantW, needC, wantC = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local budget, maxin = tonumber(ARGV[6]), tonumber(ARGV[7])
local gw, gc = 0, 0
if budget > 0 then
  local free = budget - sw
  if free < needW then return {0, 0} end
  gw = math.min(wantW, free)
end
if maxin > 0 then
  local free = maxin - sc
  if free < needC then return {0, 0} end
  gc = math.min(wantC, free)
end
if gw > 0 then redis.call('HINCRBY', KEYS[1], 'g|' .. me .. '|w', gw) end
if gc > 0 then redis.call('HINCRBY', KEYS[1], 'g|' .. me .. '|c', gc) end
redis.call('EXPIRE', KEYS[1], 3600)
return {gw, gc}`

// rewriteScript writes what this run holds in a pool. With a stale epoch
// (the store lost this run and it has not noticed yet) it writes only what is
// in use: credits it was granted before may have gone to another router.
// KEYS: pool hash, epoch hash.
// ARGV: run, epoch, grantW, grantC, usedW, usedC, starved ("1"/"0"), now ms.
const rewriteScript = `
local me = ARGV[1]
local w, c = ARGV[3], ARGV[4]
if redis.call('HGET', KEYS[2], me) ~= ARGV[2] then w, c = ARGV[5], ARGV[6] end
redis.call('HSET', KEYS[1], 'g|' .. me .. '|w', w, 'g|' .. me .. '|c', c)
if ARGV[7] == '1' then
  redis.call('HSET', KEYS[1], 'want|' .. me, ARGV[8])
else
  redis.call('HDEL', KEYS[1], 'want|' .. me)
end
redis.call('EXPIRE', KEYS[1], 3600)
return 1`

// staleRouters lists routers silent for longer than ARGV[1] ms: run, peer ID,
// score (last heartbeat, ms), agent list ("version|id,id,…"), and so on.
var staleRouters = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local out = {}
local rs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. (now - tonumber(ARGV[1])), 'WITHSCORES')
for i = 1, #rs, 2 do
  out[#out + 1] = rs[i]
  out[#out + 1] = redis.call('HGET', KEYS[2], rs[i]) or ''
  out[#out + 1] = rs[i + 1]
  out[#out + 1] = redis.call('HGET', KEYS[3], rs[i]) or ''
end
return out`)

// evictRouter removes a run from the live set and drops its epoch, only if
// its last heartbeat is still ARGV[2]: a run that came back is not evicted.
// KEYS: live set, epoch hash. ARGV: run, score seen.
var evictRouter = valkey.NewLuaScript(`
local sc = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not sc or tonumber(sc) ~= tonumber(ARGV[2]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1`)
