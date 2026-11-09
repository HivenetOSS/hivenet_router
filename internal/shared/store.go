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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
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
	TLS            bool          // connect with TLS
	TLSCAFile      string        // PEM CA bundle to verify Valkey and Sentinel; empty for the system roots
	Prefix         string        // key prefix, one per policloud (the site ID)
	RouterID       string        // stable router identity, e.g. the pod name
	Interval       time.Duration // sync interval
	DownAfter      time.Duration // unreachable this long → each router enforces its share
	MinRouters     int           // routers assumed while the live count is unknown or Valkey is down

	tlsRoots *x509.CertPool
}

// validSiteID keeps the site ID usable inside a key's hash tag.
var validSiteID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

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

	fleet    atomic.Int32  // live routers, this one included (last known while down)
	seq      atomic.Uint64 // successful ticks
	degraded atomic.Uint64 // 0 while Valkey answers, else the outage number
	outages  uint64

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
	if cfg.Prefix != "" && !validSiteID.MatchString(cfg.Prefix) {
		return nil, fmt.Errorf("shared counters: site ID %q must be 1-64 letters, digits, '.', '_' or '-'", cfg.Prefix)
	}
	if cfg.TLSCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("shared counters: TLS CA file: %w", err)
		}
		cfg.tlsRoots = x509.NewCertPool()
		if !cfg.tlsRoots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("shared counters: TLS CA file %q holds no PEM certificate", cfg.TLSCAFile)
		}
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
		opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: s.cfg.tlsRoots}
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
	// One script, so the stream position and the totals are read at the
	// same instant: each router writes its totals before it adds the stream
	// entry, so every entry up to that position is in the totals.
	v, err := dailyTotal.Exec(ctx, c, []string{s.key("usage"), s.dailyKey(dayIndex(time.Now()), bucket)}, nil).ToArray()
	if err != nil || len(v) != 2 {
		if err == nil {
			err = errShortReply
		}
		return 0, "", err
	}
	mark, err := v[0].ToString()
	if err != nil {
		return 0, "", err
	}
	total, err := v[1].AsInt64()
	if err != nil {
		return 0, "", err
	}
	return int(total), mark, nil
}

// dailyTotal returns the last stream entry and a daily bucket's total.
// KEYS: usage stream, daily key.
var dailyTotal = valkey.NewLuaScript(`
local m = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local mark = '0-0'
if #m > 0 then mark = m[1][1] end
local total = 0
for _, v in ipairs(redis.call('HVALS', KEYS[2])) do total = total + tonumber(v) end
return {mark, total}`)

// Start runs the sync loop until ctx is done or Close is called. The first
// tick replays the last minute of the usage stream, so a restarted router
// knows its rate buckets.
func (s *Store) Start(ctx context.Context) {
	ctx, s.stop = context.WithCancel(ctx)
	s.done = make(chan struct{})
	s.cursor = strconv.FormatInt(time.Now().Add(-time.Minute).UnixMilli(), 10) + "-0"
	s.lastOK = time.Now()
	// The first tick runs before the router serves (at most one interval),
	// so daily-limited keys are not refused while a healthy Valkey has not
	// been read yet.
	s.tick(ctx)
	go func() {
		defer close(s.done)
		t := time.NewTicker(s.cfg.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			s.tick(ctx)
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
		case s.dirty[db] || (full && db.day == today):
			// Yesterday is rewritten only if it changed, so its key keeps
			// the expiry set on its last write.
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

	live, err := heartbeat.Exec(cctx, c, []string{s.key("routers")},
		[]string{s.inc, strconv.FormatInt(s.cfg.DownAfter.Milliseconds(), 10)}).AsStrSlice()
	if err != nil {
		s.failTick(buf, err)
		return
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

	switch {
	case s.seq.Load() == 0:
		log.Infof("shared counters: connected to Valkey; enforcing policloud-wide limits")
	case s.degraded.Load() != 0:
		log.Infof("shared counters: Valkey reachable again; enforcing policloud-wide limits")
	}
	s.degraded.Store(0)
	s.lastOK = time.Now()
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
// routers' clocks do not matter), drops runs silent for an hour, and returns
// the runs seen within ARGV[2] ms.
var heartbeat = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 3600000)
return redis.call('ZRANGEBYSCORE', KEYS[1], now - tonumber(ARGV[2]), '+inf')`)

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
