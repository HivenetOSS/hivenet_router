// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared

import (
	"context"
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
// A router that stops heartbeating keeps its credits until reclaimAfter (the
// request timeout, so requests it was still running have ended) or until the
// agents show that it has no connection to any of them: then it cannot be
// running anything, and the other routers take its credits back at once.
//
// While Valkey is unreachable a router keeps admitting within what it holds,
// capped at its share of the budget, and asks for no more.
type Credits struct {
	s            *Store
	ns           string
	reclaimAfter time.Duration
	reachable    func(peerID string) (known, reachable bool)

	mu      sync.Mutex
	wallets map[string]*wallet
	evicted uint64

	refills  atomic.Int64
	returned atomic.Int64
	reclaims atomic.Int64
}

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

	refill sync.Mutex // one refill (or rewrite) in flight per wallet
}

// chunkDiv sizes a refill: budget / (chunkDiv × routers), so a router asks
// for a small slice at a time and the rest stays available to the others.
const chunkDiv = 8

// refillBackoff spaces out refills that came back short, so parked requests
// never turn into a stream of Valkey calls.
const refillBackoff = 10 * time.Millisecond

// NewCredits shares a budget under namespace ns ("occ" for B2, "kocc" for B4).
// reclaimAfter is how long a silent router keeps its credits. reachable, if
// set, reports whether any agent has a connection to the router with that
// libp2p peer ID (known is false when no agent has reported recently).
func (s *Store) NewCredits(ns string, reclaimAfter time.Duration, reachable func(peerID string) (known, reachable bool)) *Credits {
	if reclaimAfter < 3*s.cfg.DownAfter {
		reclaimAfter = 3 * s.cfg.DownAfter
	}
	return &Credits{s: s, ns: ns, reclaimAfter: reclaimAfter, reachable: reachable, wallets: map[string]*wallet{}}
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

// fits reports whether w more weight and one more request fit the wallet.
// Caller holds wl.mu.
func (c *Credits) fits(wl *wallet, w int64) bool {
	gW, gC := wl.grantW, wl.grantC
	if c.degraded() {
		// No refills while the store is down: spend at most this router's
		// share, so the site stays within the budget even if every router
		// holds its share.
		n := int64(c.s.Size())
		if wl.budget > 0 {
			gW = min(gW, wl.budget/max(n, 1))
		}
		if wl.maxIn > 0 {
			gC = min(gC, max(wl.maxIn/max(n, 1), 1))
		}
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
		return false // another request is refilling; the caller parks and retries
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
// A replica acknowledges the grant before it is used (WAIT), so a failover
// cannot forget it and hand the same capacity to another router.
func (c *Credits) grant(ctx context.Context, key string, needW, wantW, needC, wantC, budget, maxIn int64) (int64, int64, error) {
	cl, err := c.s.conn()
	if err != nil {
		return 0, 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	args := []string{c.s.inc, itoa(needW), itoa(wantW), itoa(needC), itoa(wantC), itoa(budget), itoa(maxIn),
		itoa(c.reclaimAfter.Milliseconds())}
	res := cl.DoMulti(ctx,
		cl.B().Eval().Script(grantScript).Numkeys(2).Key(c.poolKey(key), c.s.key("routers")).Arg(args...).Build(),
		cl.B().Wait().Numreplicas(1).Timeout(20).Build())
	c.s.m.credit.WithLabelValues("refill").Observe(time.Since(start).Seconds())
	v, err := res[0].AsIntSlice()
	if err != nil || len(v) != 2 {
		c.s.m.creditErrors.Inc()
		if err == nil {
			err = errShortReply
		}
		return 0, 0, err
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
	cl, err := c.s.conn()
	if err != nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	c.reclaimUnreachable(cctx, cl)

	c.mu.Lock()
	ws := make([]*wallet, 0, len(c.wallets))
	for k, wl := range c.wallets {
		wl.mu.Lock()
		idle := wl.usedW == 0 && wl.usedC == 0 && wl.grantW == 0 && wl.grantC == 0 && time.Since(wl.lastActive) > time.Minute
		wl.mu.Unlock()
		if idle {
			delete(c.wallets, k)
			continue
		}
		ws = append(ws, wl)
	}
	c.mu.Unlock()
	if len(ws) == 0 {
		return
	}

	// Who else is starved, per pool.
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
		if fields, err := res[i].AsStrMap(); err == nil {
			for f, v := range fields {
				if strings.HasPrefix(f, "want|") && f != "want|"+c.s.inc {
					if ts, _ := strconv.ParseInt(v, 10, 64); now-ts < window {
						othersWant = true
					}
				}
			}
		}
		wl.mu.Lock()
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
		// Output streamed past the credits is in use: record it as held.
		if wl.budget > 0 {
			wl.grantW = max(wl.grantW, wl.usedW)
		}
		if wl.maxIn > 0 {
			wl.grantC = max(wl.grantC, wl.usedC)
		}
		gW, gC := wl.grantW, wl.grantC
		starved := wl.starved
		wl.starved = false
		wl.mu.Unlock()

		pk := c.poolKey(wl.key)
		writes = append(writes, cl.B().Hset().Key(pk).FieldValue().
			FieldValue("g|"+c.s.inc+"|w", itoa(gW)).FieldValue("g|"+c.s.inc+"|c", itoa(gC)).Build())
		if starved {
			writes = append(writes, cl.B().Hset().Key(pk).FieldValue().FieldValue("want|"+c.s.inc, itoa(now)).Build())
		} else {
			writes = append(writes, cl.B().Hdel().Key(pk).Field("want|"+c.s.inc).Build())
		}
		writes = append(writes, cl.B().Expire().Key(pk).Seconds(3600).Build())
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
}

// reclaimUnreachable evicts routers that stopped heartbeating and that no
// agent is connected to: they cannot be running anything, so their credits go
// back to the pool now instead of after reclaimAfter.
func (c *Credits) reclaimUnreachable(ctx context.Context, cl valkey.Client) {
	if c.reachable == nil {
		return
	}
	stale, err := staleRouters.Exec(ctx, cl, []string{c.s.key("routers"), c.s.key("peers")},
		[]string{itoa(c.s.cfg.DownAfter.Milliseconds()), itoa(c.reclaimAfter.Milliseconds())}).AsStrSlice()
	if err != nil {
		return
	}
	for i := 0; i+1 < len(stale); i += 2 {
		inst, peerID := stale[i], stale[i+1]
		if inst == c.s.inc || peerID == "" {
			continue
		}
		if known, reachable := c.reachable(peerID); known && !reachable {
			if cl.Do(ctx, cl.B().Zrem().Key(c.s.key("routers")).Member(inst).Build()).Error() == nil {
				c.reclaims.Add(1)
				c.s.m.creditReclaims.Inc()
				log.Infof("shared occupancy: router %s is gone and no agent is connected to it; its credits are back in the pool", inst)
			}
		}
	}
}

// ReleaseAll hands back every credit this router holds, at shutdown once it
// can no longer serve. A request still in flight then holds nothing: call it
// after the router has stopped forwarding.
func (c *Credits) ReleaseAll(ctx context.Context) {
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
// KEYS: pool hash, live-router set.
// ARGV: run, needW, wantW, needC, wantC, budget, maxIn, reclaim window (ms).
// What routers silent for longer than the reclaim window held is dropped
// first. Grants min(want, free) when free covers need, else nothing.
// Returns {weight, requests} granted.
const grantScript = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local live = {}
for _, r in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], now - tonumber(ARGV[8]), '+inf')) do live[r] = true end
local me = ARGV[1]
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

// staleRouters lists routers silent for longer than ARGV[1] ms but not yet
// past the reclaim window ARGV[2], with their peer IDs: run, peer, run, peer…
var staleRouters = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local out = {}
for _, r in ipairs(redis.call('ZRANGEBYSCORE', KEYS[1], now - tonumber(ARGV[2]), '(' .. (now - tonumber(ARGV[1])))) do
  out[#out + 1] = r
  out[#out + 1] = redis.call('HGET', KEYS[2], r) or ''
end
return out`)
