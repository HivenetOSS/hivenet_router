// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package shared lets the routers of one policloud enforce each key's rate and
// daily limits together, through one Valkey (normally behind Sentinel).
//
// Every router keeps deciding in memory, so a request never waits on Valkey.
// Once per interval each router publishes the usage it admitted as one stream
// entry, reads the entries the other routers published, and hands them to the
// limiters, which subtract them from their own buckets. The same round trip
// records a heartbeat, so every router knows how many routers are live.
//
// Today's token totals are kept per router run: each run writes its own
// running total (HSET, not an increment), so a write lost in a failover is
// healed by the next one and a retry never counts twice. A bucket's total is
// the sum over runs.
//
// When Valkey is unreachable each router keeps serving with its share of every
// limit (limit ÷ the routers it last saw, never fewer than MinRouters), so the
// routers together still stay within the limit; it catches up when Valkey
// answers again.
package shared

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/valkey-io/valkey-go"
)

var log = logging.Logger("shared")

var errShortReply = errors.New("shared store: unexpected reply")

// Usage kinds published on the usage stream. They match the limiters' kinds in
// package auth.
const (
	KindRPM   byte = 'r' // requests against a requests-per-minute bucket
	KindDaily byte = 'd' // tokens against a daily bucket
	KindITPM  byte = 'i' // input tokens against a tokens-per-minute bucket
	KindOTPM  byte = 'o' // output tokens against a tokens-per-minute bucket
)

// Applier receives the usage other routers admitted. limit is the limit the
// publishing router applied, so a bucket this router has not seen yet can be
// created with the right size. age is how long ago the usage was admitted. day
// is the UTC day a daily usage belongs to (0 for the other kinds), and id the
// stream entry that carried it (see DailyTotal).
type Applier interface {
	ApplyPeerUsage(kind byte, bucket string, limit, n int, age time.Duration, day int, id string)
}

// Config configures the connection and the sync loop.
type Config struct {
	Addrs          []string      // Valkey addresses, or Sentinel addresses with SentinelMaster
	SentinelMaster string        // Sentinel master set name; empty for a direct connection
	Password       string        // Valkey (and Sentinel) password; empty for none
	TLS            bool          // connect with TLS (system roots)
	Prefix         string        // key prefix, one per policloud (the site ID)
	RouterID       string        // stable router identity, e.g. the pod name
	PeerID         string        // this router's libp2p peer ID, so other routers can ask the agents about it
	Interval       time.Duration // sync interval
	DownAfter      time.Duration // unreachable this long → each router enforces its share
	MinRouters     int           // routers assumed while the live count is unknown or Valkey is down
}

// ErrNotConnected is returned by the calls made on a request's path when the
// store has no working connection: they never connect or wait for one.
var ErrNotConnected = errors.New("shared store not connected")

// Store is the Valkey connection plus the sync loop.
type Store struct {
	cfg      Config
	inc      string // this run: RouterID@start-ms, so a restart never counts as a live peer
	appliers []Applier

	connMu sync.Mutex
	client valkey.Client
	closed bool

	stop context.CancelFunc
	done chan struct{}

	mu       sync.Mutex
	buf      map[usageKey]*usageVal // usage admitted since the last successful tick
	own      map[dayBucket]int64    // this run's daily totals
	dirty    map[dayBucket]bool     // daily totals to write on the next tick
	fullNext bool                   // write every daily total on the next tick

	cursor    string    // last stream entry read
	lastOK    time.Time // last successful tick
	lastFull  time.Time // last time every daily total was written
	lastReset time.Time // last time the client was rebuilt

	okAt     atomic.Int64  // unix ms of the last successful tick
	fleet    atomic.Int32  // live routers, this one included (last known while down)
	evicted  atomic.Uint64 // times the other routers removed this run from the live set
	seenLive bool          // this run has been in the live set
	seq      atomic.Uint64 // successful ticks
	degraded atomic.Uint64 // 0 while Valkey answers, else the outage number
	outages  uint64

	// Runs seen live before this router lost Valkey or was evicted, and not
	// seen back since. Valkey may have lost what they hold (a restart without
	// data, or the eviction itself), so occupancy stays at this router's share
	// until each one is back or known to be gone.
	liveMu   sync.Mutex
	others   []string          // other live runs at the last successful tick
	peerOf   map[string]string // run → libp2p peer ID, as last seen
	pending  map[string]*PendingRun
	pendingN atomic.Int32

	m *storeMetrics
}

type usageKey struct {
	kind   byte
	bucket string
	day    int
}

type usageVal struct {
	limit int
	n     int
	first time.Time // oldest admission in n
}

type dayBucket struct {
	day    int
	bucket string
}

// entry is one bucket's usage inside a stream message.
type entry struct {
	K string `json:"k"`
	B string `json:"b"`
	L int    `json:"l"`
	N int    `json:"n"`
	D int    `json:"d,omitempty"` // UTC day (daily usage)
	A int64  `json:"a,omitempty"` // ms between the oldest admission and the publish
}

// fullEvery is how often every daily total of today is written again, so a
// write lost in a failover the router did not notice is healed.
const fullEvery = 5 * time.Minute

// reconnectAfter is how many intervals Valkey may stay unreachable before the
// client is rebuilt. When Valkey and its Sentinels are replaced at new
// addresses, the client can keep waiting on connections to hosts that are
// gone; a new client asks the Sentinels again from the configured addresses.
const reconnectAfter = 10

// New validates cfg. It does not connect: the first tick does, and a router
// whose Valkey is down starts with each limit at its share.
func New(cfg Config) (*Store, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("shared counters need at least one Valkey address")
	}
	if cfg.RouterID == "" {
		return nil, errors.New("shared counters need a router ID")
	}
	if strings.ContainsAny(cfg.Prefix, "{}") {
		return nil, fmt.Errorf("shared counters: site ID %q must not contain braces", cfg.Prefix)
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.DownAfter <= 0 {
		cfg.DownAfter = 3 * cfg.Interval
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "default"
	}
	if cfg.MinRouters < 1 {
		cfg.MinRouters = 1
	}
	s := &Store{
		cfg:   cfg,
		inc:   cfg.RouterID + "@" + strconv.FormatInt(time.Now().UnixMilli(), 10),
		buf:   make(map[usageKey]*usageVal),
		own:   make(map[dayBucket]int64),
		dirty: make(map[dayBucket]bool),
		m:     newStoreMetrics(),
	}
	s.fleet.Store(int32(cfg.MinRouters))
	// Until Valkey first answers, every limit is at this router's share.
	s.outages = 1
	s.degraded.Store(1)
	return s, nil
}

// SetPeerID records this router's libp2p peer ID, published with its
// heartbeat so the other routers can ask the agents about it. Call before
// Start.
func (s *Store) SetPeerID(id string) { s.cfg.PeerID = id }

// AddApplier registers a receiver for the other routers' usage. Call before Start.
func (s *Store) AddApplier(a Applier) { s.appliers = append(s.appliers, a) }

// Size returns the live routers in the policloud, this one included. While
// Valkey is unreachable, or before it first answered, it is the last count
// seen, never fewer than MinRouters.
func (s *Store) Size() int {
	n := int(s.fleet.Load())
	if s.degraded.Load() != 0 || s.seq.Load() == 0 {
		n = max(n, s.cfg.MinRouters)
	}
	return max(n, 1)
}

// SyncSeq changes after every tick that applied the other routers' usage.
func (s *Store) SyncSeq() uint64 { return s.seq.Load() }

// DegradedEpoch is 0 while Valkey answers, else a number that changes with each
// outage, so a limiter can snapshot its state once per outage. Before Valkey
// first answers the router counts as being in an outage.
func (s *Store) DegradedEpoch() uint64 { return s.degraded.Load() }

// Record adds usage this router admitted; it is published on the next tick.
func (s *Store) Record(kind byte, bucket string, limit, n int) {
	if n <= 0 {
		return
	}
	now := time.Now()
	k := usageKey{kind: kind, bucket: bucket}
	s.mu.Lock()
	if kind == KindDaily {
		k.day = dayIndex(now)
		db := dayBucket{k.day, bucket}
		s.own[db] += int64(n)
		s.dirty[db] = true
	}
	v := s.buf[k]
	if v == nil {
		v = &usageVal{first: now}
		s.buf[k] = v
	}
	v.limit = limit
	v.n += n
	s.mu.Unlock()
}

// key builds a key. The site prefix is a hash tag, so every key of a site
// lives on one shard and multi-key scripts work on a cluster too.
func (s *Store) key(parts ...string) string {
	return "hivenet:{" + s.cfg.Prefix + "}:" + strings.Join(parts, ":")
}

func (s *Store) dailyKey(day int, bucket string) string {
	return s.key("tpd", strconv.Itoa(day), bucket)
}

func dayIndex(t time.Time) int { return int(t.UTC().Unix() / 86400) }

// conn returns the client, connecting on first use. Only the sync loop calls
// it: a request never waits for a connection.
func (s *Store) conn() (valkey.Client, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closed {
		return nil, ErrNotConnected
	}
	if s.client != nil {
		return s.client, nil
	}
	opt := valkey.ClientOption{
		InitAddress:      s.cfg.Addrs,
		Password:         s.cfg.Password,
		DisableCache:     true,
		AlwaysRESP2:      true,
		ConnWriteTimeout: s.cfg.Interval,
		Dialer:           net.Dialer{Timeout: s.cfg.Interval},
	}
	if s.cfg.TLS {
		opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if s.cfg.SentinelMaster != "" {
		opt.Sentinel = valkey.SentinelOption{MasterSet: s.cfg.SentinelMaster, Password: s.cfg.Password}
		if s.cfg.TLS {
			opt.Sentinel.TLSConfig = opt.TLSConfig
		}
	}
	c, err := valkey.NewClient(opt)
	if err != nil {
		return nil, err
	}
	s.client = c
	return c, nil
}

// connected returns the client only while Valkey answers. Calls on a
// request's path use it, so they fail at once instead of connecting.
func (s *Store) connected() (valkey.Client, error) {
	if s.degraded.Load() != 0 || s.seq.Load() == 0 {
		return nil, ErrNotConnected
	}
	// A round already failed: do not make a request wait on a store that
	// may be gone, even before the outage is declared.
	if time.Since(time.UnixMilli(s.okAt.Load())) > 2*s.cfg.Interval {
		return nil, ErrNotConnected
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closed || s.client == nil {
		return nil, ErrNotConnected
	}
	return s.client, nil
}

// resetConn drops the client; the next tick connects from scratch.
func (s *Store) resetConn() {
	s.connMu.Lock()
	c := s.client
	s.client = nil
	s.connMu.Unlock()
	if c != nil {
		go c.Close() // may wait on connections to hosts that are gone
	}
}

// DailyTotal returns the policloud's tokens used today in a daily bucket, and
// the last stream entry the total already includes: usage carried by that
// entry or an earlier one must not be added again. It fails at once, without
// waiting, while Valkey is not answering.
func (s *Store) DailyTotal(ctx context.Context, bucket string) (int, string, error) {
	c, err := s.connected()
	if err != nil {
		return 0, "", err
	}
	// The stream position is read first. Each router writes its totals
	// before it adds the stream entry, so every entry up to that position is
	// in the totals read next.
	res := c.DoMulti(ctx,
		c.B().Xrevrange().Key(s.key("usage")).End("+").Start("-").Count(1).Build(),
		c.B().Hvals().Key(s.dailyKey(dayIndex(time.Now()), bucket)).Build())
	mark := "0-0"
	msgs, err := res[0].AsXRange()
	if err != nil {
		return 0, "", err
	}
	if len(msgs) > 0 {
		mark = msgs[0].ID
	}
	vals, err := res[1].AsStrSlice()
	if err != nil {
		return 0, "", err
	}
	total := 0
	for _, v := range vals {
		n, _ := strconv.Atoi(v)
		total += n
	}
	return total, mark, nil
}

// Start runs the sync loop until ctx is done or Close is called. The first
// tick replays the last minute of the usage stream, so a restarted router
// knows its rate buckets.
func (s *Store) Start(ctx context.Context) {
	ctx, s.stop = context.WithCancel(ctx)
	s.done = make(chan struct{})
	s.cursor = strconv.FormatInt(time.Now().Add(-time.Minute).UnixMilli(), 10) + "-0"
	s.lastOK = time.Now()
	go func() {
		defer close(s.done)
		t := time.NewTicker(s.cfg.Interval)
		defer t.Stop()
		for {
			s.tick(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// Close stops the sync loop, publishes the usage admitted since the last tick
// so the other routers count it, then closes the connection.
func (s *Store) Close() {
	if s.stop != nil {
		s.stop()
		<-s.done
	}
	s.mu.Lock()
	pending := len(s.buf) > 0 || len(s.dirty) > 0
	s.mu.Unlock()
	if pending {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Interval)
		s.tick(ctx)
		cancel()
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.closed = true
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

// tick publishes this router's usage, refreshes its heartbeat, learns the live
// routers and reads the others' usage: one script call and one pipeline.
func (s *Store) tick(ctx context.Context) {
	start := time.Now()
	defer func() { s.m.tick.Observe(time.Since(start).Seconds()) }()

	today := dayIndex(start)
	s.mu.Lock()
	buf := s.buf
	s.buf = make(map[usageKey]*usageVal)
	full := s.fullNext || time.Since(s.lastFull) >= fullEvery
	totals := map[dayBucket]int64{}
	for db, n := range s.own {
		switch {
		case db.day < today-1:
			delete(s.own, db) // past yesterday: its key has expired or will
		case full || s.dirty[db]:
			totals[db] = n
		}
	}
	s.dirty = make(map[dayBucket]bool)
	s.fullNext = false
	s.mu.Unlock()

	c, err := s.conn()
	if err != nil {
		s.failTick(buf, err)
		return
	}
	cctx, cancel := context.WithTimeout(ctx, s.cfg.Interval)
	defer cancel()

	hb, err := heartbeat.Exec(cctx, c, []string{s.key("routers"), s.key("peers")},
		[]string{s.inc, strconv.FormatInt(s.cfg.DownAfter.Milliseconds(), 10), s.cfg.PeerID}).AsStrSlice()
	if err != nil || len(hb) == 0 {
		if err == nil {
			err = errShortReply
		}
		s.failTick(buf, err)
		return
	}
	// hb[0] says whether this run was still in the live set. Once it has
	// been, missing means the other routers evicted it (they saw it as gone
	// and took back what it held), so whatever it holds must be re-granted.
	evictedNow := false
	if hb[0] == "1" {
		s.seenLive = true
	} else if s.seenLive {
		evictedNow = true
		s.evicted.Add(1)
		log.Warnf("shared counters: this router was evicted from the live set; re-requesting its shares")
	}
	var live, livePeers []string
	for i := 1; i+1 < len(hb); i += 2 {
		live = append(live, hb[i])
		livePeers = append(livePeers, hb[i+1])
	}

	// Daily totals first, then the stream entry: see DailyTotal.
	var cmds valkey.Commands
	for db, n := range totals {
		k := s.dailyKey(db.day, db.bucket)
		cmds = append(cmds,
			c.B().Hset().Key(k).FieldValue().FieldValue(s.inc, strconv.FormatInt(n, 10)).Build(),
			c.B().Expire().Key(k).Seconds(int64((48 * time.Hour).Seconds())).Build())
	}
	entries := make([]entry, 0, len(buf))
	for k, v := range buf {
		entries = append(entries, entry{K: string(k.kind), B: k.bucket, L: v.limit, N: v.n, D: k.day,
			A: time.Since(v.first).Milliseconds()})
	}
	payload, _ := json.Marshal(entries)
	stream := s.key("usage")
	minID := strconv.FormatInt(time.Now().Add(-2*time.Minute).UnixMilli(), 10)
	cmds = append(cmds,
		c.B().Xadd().Key(stream).Minid().Almost().Threshold(minID).Id("*").
			FieldValue().FieldValue("r", s.inc).FieldValue("u", string(payload)).Build(),
		c.B().Xrange().Key(stream).Start("("+s.cursor).End("+").Count(100000).Build())
	res := c.DoMulti(cctx, cmds...)
	for _, r := range res[:len(res)-1] {
		if err := r.Error(); err != nil {
			s.failTick(buf, err)
			return
		}
	}
	if full {
		s.lastFull = time.Now()
	}
	msgs, err := res[len(res)-1].AsXRange()
	if err != nil {
		s.failTick(nil, err) // usage is published; only the read failed
		return
	}

	s.trackLive(live, livePeers, evictedNow || (s.degraded.Load() != 0 && s.seq.Load() > 0))
	switch {
	case s.seq.Load() == 0:
		log.Infof("shared counters: connected to Valkey; enforcing policloud-wide limits")
	case s.degraded.Load() != 0:
		log.Infof("shared counters: Valkey reachable again; enforcing policloud-wide limits")
	}
	s.degraded.Store(0)
	s.lastOK = time.Now()
	s.okAt.Store(s.lastOK.UnixMilli())
	s.m.up.Set(1)

	n := len(live)
	self := false
	for _, id := range live {
		if id == s.inc {
			self = true
		}
	}
	if !self {
		n++
	}
	s.fleet.Store(int32(n))
	s.m.peers.Set(float64(n - 1))

	now := time.Now()
	for _, msg := range msgs {
		s.cursor = msg.ID
		if msg.FieldValues["r"] == s.inc {
			continue // our own usage, counted when admitted
		}
		var es []entry
		if json.Unmarshal([]byte(msg.FieldValues["u"]), &es) != nil {
			continue
		}
		lag := now.Sub(time.UnixMilli(streamMillis(msg.ID)))
		s.m.lag.Set(lag.Seconds())
		for _, e := range es {
			if len(e.K) != 1 || e.N <= 0 {
				continue
			}
			// Usage held back during an outage is as old as its admission,
			// not its publish: a rate bucket has refilled it since.
			age := lag + time.Duration(e.A)*time.Millisecond
			for _, a := range s.appliers {
				a.ApplyPeerUsage(e.K[0], e.B, e.L, e.N, age, e.D, msg.ID)
			}
		}
	}
	s.seq.Add(1)
}

// failTick puts unpublished usage back for the next tick and, once Valkey has
// been unreachable for DownAfter, switches every limit to this router's share.
// Every daily total is written again on the next successful tick: a failed
// tick may mean a failover that lost recent writes.
func (s *Store) failTick(buf map[usageKey]*usageVal, err error) {
	s.mu.Lock()
	for k, v := range buf {
		if cur := s.buf[k]; cur != nil {
			cur.n += v.n
			if v.first.Before(cur.first) {
				cur.first = v.first
			}
		} else {
			s.buf[k] = v
		}
	}
	s.fullNext = true
	s.mu.Unlock()
	s.m.errors.Inc()
	if time.Since(s.lastOK) >= s.cfg.DownAfter && s.degraded.Load() == 0 {
		s.outages++
		s.degraded.Store(s.outages)
		s.m.up.Set(0)
		log.Warnf("shared counters: Valkey unreachable for %v (%v); each router now enforces 1/%d of every limit",
			time.Since(s.lastOK).Round(time.Millisecond), err, s.Size())
	}
	every := reconnectAfter * s.cfg.Interval
	if time.Since(s.lastOK) >= every && time.Since(s.lastReset) >= every {
		s.lastReset = time.Now()
		s.resetConn()
		log.Infof("shared counters: Valkey unreachable for %v; reconnecting from the configured addresses",
			time.Since(s.lastOK).Round(time.Second))
	}
}

func streamMillis(id string) int64 {
	if i := strings.IndexByte(id, '-'); i > 0 {
		id = id[:i]
	}
	ms, _ := strconv.ParseInt(id, 10, 64)
	return ms
}

// heartbeat records this run as live (scored with Valkey's own clock, so the
// routers' clocks do not matter) and its libp2p peer ID, drops runs silent for
// an hour, and returns whether this run was in the live set ("1"/"0")
// followed by each run seen within ARGV[2] ms and its peer ID.
// KEYS: live set, peer-ID hash. ARGV: run, live window (ms), peer ID.
var heartbeat = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local present = redis.call('ZSCORE', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 3600000)
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
  redis.call('EXPIRE', KEYS[2], 3600)
end
local out = {present and '1' or '0'}
for _, r in ipairs(redis.call('ZRANGEBYSCORE', KEYS[1], now - tonumber(ARGV[2]), '+inf')) do
  out[#out + 1] = r
  out[#out + 1] = redis.call('HGET', KEYS[2], r) or ''
end
return out`)

// PendingRun is a run seen live before an outage or eviction and not back yet.
type PendingRun struct {
	Peer  string    // its libp2p peer ID, if known
	Since time.Time // when this router started waiting for it
	back  time.Time // first tick it was seen live again
}

// Pending returns the runs this router waits for after an outage or eviction.
func (s *Store) Pending() map[string]PendingRun {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	out := make(map[string]PendingRun, len(s.pending))
	for r, p := range s.pending {
		out[r] = *p
	}
	return out
}

// PendingCount is len(Pending()), without copying.
func (s *Store) PendingCount() int { return int(s.pendingN.Load()) }

// Forget stops waiting for a run known to be gone.
func (s *Store) Forget(run string) {
	s.liveMu.Lock()
	delete(s.pending, run)
	s.pendingN.Store(int32(len(s.pending)))
	s.liveMu.Unlock()
}

// Superseded reports whether a newer run of the same router is live, which
// means run has ended: one router ID runs one process at a time.
func (s *Store) Superseded(run string) bool {
	id, start, ok := splitRun(run)
	if !ok {
		return false
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	for _, r := range append([]string{s.inc}, s.others...) {
		if rid, rstart, ok := splitRun(r); ok && rid == id && rstart > start {
			return true
		}
	}
	return false
}

func splitRun(run string) (id string, start int64, ok bool) {
	i := strings.LastIndexByte(run, '@')
	if i < 0 {
		return "", 0, false
	}
	start, err := strconv.ParseInt(run[i+1:], 10, 64)
	return run[:i], start, err == nil
}

// trackLive records the live runs of a successful tick. After an outage or
// an eviction (lost), the runs live before become pending until they have
// been back for two ticks, so their own credit rewrite has landed.
func (s *Store) trackLive(live, peers []string, lost bool) {
	now := time.Now()
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.peerOf == nil {
		s.peerOf = map[string]string{}
		s.pending = map[string]*PendingRun{}
	}
	if lost {
		for _, r := range s.others {
			if s.pending[r] == nil {
				s.pending[r] = &PendingRun{Peer: s.peerOf[r], Since: now}
			}
		}
	}
	isLive := map[string]bool{}
	s.others = s.others[:0]
	for i, r := range live {
		if r == s.inc {
			continue
		}
		isLive[r] = true
		s.others = append(s.others, r)
		if peers[i] != "" {
			s.peerOf[r] = peers[i]
		}
	}
	for r, p := range s.pending {
		switch {
		case !isLive[r]:
			p.back = time.Time{}
		case p.back.IsZero():
			p.back = now
		case now.Sub(p.back) >= 2*s.cfg.Interval:
			delete(s.pending, r)
		}
	}
	for r := range s.peerOf {
		if !isLive[r] && s.pending[r] == nil {
			delete(s.peerOf, r)
		}
	}
	s.pendingN.Store(int32(len(s.pending)))
}

// Evictions counts the times the other routers removed this run from the live
// set; a change means every share this router held was taken back.
func (s *Store) Evictions() uint64 { return s.evicted.Load() }

// FirstUse records id until the given time and reports whether this is its
// first use across every router of the site (SET NX with an expiry). It fails
// at once, without connecting, while the store does not answer.
func (s *Store) FirstUse(ctx context.Context, id string, until time.Time) (bool, error) {
	c, err := s.connected()
	if err != nil {
		return false, err
	}
	ttl := time.Until(until)
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = c.Do(ctx, c.B().Set().Key(s.key("rt", id)).Value("1").Nx().PxMilliseconds(ttl.Milliseconds()).Build()).Error()
	if valkey.IsValkeyNil(err) {
		return false, nil // already set: a replay
	}
	return err == nil, err
}

// String describes the configuration for the startup banner (no secrets).
func (c Config) String() string {
	mode := "direct"
	if c.SentinelMaster != "" {
		mode = "sentinel master " + c.SentinelMaster
	}
	prefix := c.Prefix
	if prefix == "" {
		prefix = "default"
	}
	return fmt.Sprintf("Valkey %s (%s), router %s, prefix %s, every %v, at least %d routers while unknown",
		strings.Join(c.Addrs, ","), mode, c.RouterID, prefix, c.Interval, max(c.MinRouters, 1))
}
