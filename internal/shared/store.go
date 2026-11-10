// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package shared lets the routers of one site enforce each key's rate and
// daily limits together, through one Valkey (normally behind Sentinel).
//
// Every router keeps deciding in memory: a request waits on Valkey only to
// read a daily total, once per key and day (see DailyTotal).
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
	"sort"
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
	Prefix         string        // key prefix, one per site (the site ID)
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

	cursor    string            // last stream entry read
	pubSeq    uint64            // number of the last usage batch published
	unacked   []batch           // batches whose publish has no reply: sent again, same number
	seen      map[string]uint64 // per run, the last batch number applied
	lastOK    time.Time         // last successful tick
	lastFull  time.Time         // last time every daily total was written
	lastReset time.Time         // last time the client was rebuilt

	fleet    atomic.Int32  // live routers, this one included (last known while down)
	silent   atomic.Int32  // routers in the live set that stopped heartbeating, not replaced
	silentEp atomic.Uint64 // changes whenever the set of silent routers changes
	silentAt string        // the silent routers at the last tick, sorted
	seq      atomic.Uint64 // successful ticks
	degraded atomic.Uint64 // 0 while Valkey answers, else the outage number
	resync   atomic.Uint64 // changes when usage entries may have been missed
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

// batch is one tick's usage, numbered so that a batch sent again after a
// publish with no reply is applied only once.
type batch struct {
	seq     uint64
	entries []entry
}

// maxUnacked bounds the batches kept for sending again; past it they are
// merged into one, which may count the oldest twice but never loses usage.
const maxUnacked = 60

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
		seen:  make(map[string]uint64),
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

// Size returns the live routers in the site, this one included. While
// Valkey is unreachable, or before it first answered, it is the last count
// seen, never fewer than MinRouters.
func (s *Store) Size() int {
	n := int(s.fleet.Load())
	if s.degraded.Load() != 0 || s.seq.Load() == 0 {
		n = max(n, s.cfg.MinRouters)
	}
	return max(n, 1)
}

// Silent returns how many routers stopped heartbeating (for longer than
// DownAfter, less than an hour) without a newer run of the same router
// taking their place. Such a router may only have lost the store: it keeps
// enforcing its share of every limit on its own, so the others keep that
// share free for it. A router that shuts down cleanly leaves the live set
// and is not silent. 0 while the store is down.
func (s *Store) Silent() int { return int(s.silent.Load()) }

// SilentEpoch changes whenever the set of silent routers changes.
func (s *Store) SilentEpoch() uint64 { return s.silentEp.Load() }

// SyncSeq changes after every tick that applied the other routers' usage.
func (s *Store) SyncSeq() uint64 { return s.seq.Load() }

// ResyncEpoch changes when this router may have missed other routers' usage
// entries (it was cut off long enough for them to be trimmed, or a failover
// lost them). A daily total read before the change must be read again.
func (s *Store) ResyncEpoch() uint64 { return s.resync.Load() }

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
	if s.cfg.SentinelMaster == "" {
		// One Valkey without Sentinel: never guessed as a cluster.
		opt.ForceSingleClient = true
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

// DailyTotal returns the site's tokens used today in a daily bucket, and
// the last stream entry the total already includes: usage carried by that
// entry or an earlier one must not be added again. It fails at once, without
// waiting, while Valkey is not answering.
func (s *Store) DailyTotal(ctx context.Context, bucket string) (int, string, error) {
	c, err := s.connected()
	if err != nil {
		return 0, "", err
	}
	// One script, so the stream position and the totals are read at the
	// same instant: each router writes its totals and its stream entry in
	// one transaction, so the totals hold exactly the entries up to that
	// position.
	day := dayIndex(time.Now())
	v, err := dailyTotal.Exec(ctx, c, []string{s.key("usage"), s.dailyKey(day, bucket)}, []string{s.inc}).ToArray()
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
	s.mu.Lock()
	total += s.own[dayBucket{day, bucket}]
	s.mu.Unlock()
	return int(total), mark, nil
}

// dailyTotal returns the last stream entry and a daily bucket's total,
// without this run's own field (ARGV[1]): the run adds its own usage from
// memory, which also holds what it has not published yet.
// KEYS: usage stream, daily key.
var dailyTotal = valkey.NewLuaScript(`
local m = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local mark = '0-0'
if #m > 0 then mark = m[1][1] end
local total = 0
local kv = redis.call('HGETALL', KEYS[2])
for i = 1, #kv, 2 do
  if kv[i] ~= ARGV[1] then total = total + tonumber(kv[i+1]) end
end
return {mark, total}`)

// Start runs the sync loop until ctx is done or Close is called. The first
// tick replays the last minute of the usage stream, so a restarted router
// knows its rate buckets.
func (s *Store) Start(ctx context.Context) {
	ctx, s.stop = context.WithCancel(ctx)
	s.done = make(chan struct{})
	s.cursor = "" // the first tick starts one minute back, by Valkey's clock
	s.lastOK = time.Now()
	// The router waits for the first tick, at most two intervals, so a
	// healthy Valkey is read before daily-limited keys are admitted; an
	// unreachable one does not hold startup longer than that.
	first := make(chan struct{})
	go func() {
		defer close(s.done)
		s.tick(ctx)
		close(first)
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
	select {
	case <-first:
	case <-time.After(2 * s.cfg.Interval):
	}
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
	// Leave the live set: a router that shut down is not silent, and the
	// others need not keep a share free for it.
	if c, err := s.connected(); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Interval)
		c.Do(ctx, c.B().Zrem().Key(s.key("routers")).Member(s.inc).Build())
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

	// Stream IDs carry Valkey's clock: ages, trimming and the first read
	// position are taken from it, never from this router's clock.
	vt, err := c.Do(cctx, c.B().Time().Build()).AsStrSlice()
	if err != nil || len(vt) != 2 {
		if err == nil {
			err = errShortReply
		}
		s.failTick(buf, err)
		return
	}
	sec, _ := strconv.ParseInt(vt[0], 10, 64)
	usec, _ := strconv.ParseInt(vt[1], 10, 64)
	vNow, vAt := sec*1000+usec/1000, time.Now()
	if s.cursor == "" {
		s.cursor = strconv.FormatInt(vNow-time.Minute.Milliseconds(), 10) + "-0"
	}

	live, err := heartbeat.Exec(cctx, c, []string{s.key("routers")},
		[]string{s.inc, strconv.FormatInt(s.cfg.DownAfter.Milliseconds(), 10)}).AsStrSlice()
	if err != nil {
		s.failTick(buf, err)
		return
	}

	// The newest entry first: after a failover the new primary may not have
	// the entries this router read last, and then the read position moves
	// back to what it has. Daily totals next, then the stream entry: see
	// DailyTotal.
	cmds := valkey.Commands{c.B().Xrevrange().Key(s.key("usage")).End("+").Start("-").Count(1).Build(),
		// Runs that stopped heartbeating in the last hour: see Silent.
		c.B().Zrangebyscore().Key(s.key("routers")).
			Min(strconv.FormatInt(vNow-time.Hour.Milliseconds(), 10)).
			Max("(" + strconv.FormatInt(vNow-s.cfg.DownAfter.Milliseconds(), 10)).Build()}
	// The daily totals and the stream entries are written in one
	// transaction: a total read together with the stream position (see
	// DailyTotal) then holds exactly the entries up to that position.
	writes := valkey.Commands{}
	for db, n := range totals {
		k := s.dailyKey(db.day, db.bucket)
		writes = append(writes,
			c.B().Hset().Key(k).FieldValue().FieldValue(s.inc, strconv.FormatInt(n, 10)).Build(),
			c.B().Expire().Key(k).Seconds(int64((48 * time.Hour).Seconds())).Build())
	}
	pending := s.unacked
	if len(buf) > 0 {
		entries := make([]entry, 0, len(buf))
		for k, v := range buf {
			entries = append(entries, entry{K: string(k.kind), B: k.bucket, L: v.limit, N: v.n, D: k.day,
				A: time.Since(v.first).Milliseconds()})
		}
		s.pubSeq++
		pending = append(pending, batch{seq: s.pubSeq, entries: entries})
		if len(pending) > maxUnacked {
			merged := batch{seq: pending[len(pending)-1].seq}
			for _, b := range pending {
				merged.entries = append(merged.entries, b.entries...)
			}
			pending = []batch{merged}
		}
	}
	stream := s.key("usage")
	minID := strconv.FormatInt(vNow-(2*time.Minute).Milliseconds(), 10)
	for _, b := range pending { // nothing to publish on an idle tick
		payload, _ := json.Marshal(b.entries)
		writes = append(writes, c.B().Xadd().Key(stream).Minid().Almost().Threshold(minID).Id("*").
			FieldValue().FieldValue("r", s.inc).FieldValue("s", strconv.FormatUint(b.seq, 10)).
			FieldValue("u", string(payload)).Build())
	}
	if len(writes) > 0 {
		cmds = append(cmds, c.B().Multi().Build())
		cmds = append(cmds, writes...)
		cmds = append(cmds, c.B().Exec().Build())
	}
	cmds = append(cmds, c.B().Xrange().Key(stream).Start("("+s.cursor).End("+").Count(100000).Build())
	res := c.DoMulti(cctx, cmds...)
	for _, r := range res[:len(res)-1] {
		if err := r.Error(); err != nil {
			// The batches may have been written even though the reply was
			// lost: they are sent again with the same numbers, which the
			// other routers apply only once.
			s.unacked = pending
			s.failTick(nil, err)
			return
		}
	}
	s.unacked = nil
	if full {
		s.lastFull = time.Now()
	}
	msgs, err := res[len(res)-1].AsXRange()
	if err != nil {
		s.failTick(nil, err) // usage is published; only the read failed
		return
	}
	if newest, err := res[0].AsXRange(); err == nil && len(newest) > 0 && StreamBefore(newest[0].ID, s.cursor) {
		// Entries past the newest one were lost (a failover); new ones
		// will follow it.
		log.Infof("shared counters: the usage stream is behind this router's read position (failover?); reading from %s", newest[0].ID)
		s.cursor = newest[0].ID
		s.resync.Add(1)
	}

	switch {
	case s.seq.Load() == 0:
		log.Infof("shared counters: connected to Valkey; enforcing site-wide limits")
	case s.degraded.Load() != 0:
		log.Infof("shared counters: Valkey reachable again; enforcing site-wide limits")
		// Entries older than two minutes may have been trimmed while this
		// router was cut off: daily totals are read again.
		s.resync.Add(1)
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
	if stale, err := res[1].AsStrSlice(); err == nil {
		s.trackSilent(stale, live)
		// Forget the batch numbers of runs gone for more than an hour.
		known := map[string]bool{}
		for _, r := range append(stale, live...) {
			known[r] = true
		}
		for r := range s.seen {
			if !known[r] {
				delete(s.seen, r)
			}
		}
	}
	s.m.peers.Set(float64(n - 1))

	now := vNow + time.Since(vAt).Milliseconds() // Valkey's clock, now
	for _, msg := range msgs {
		s.cursor = msg.ID
		run := msg.FieldValues["r"]
		if run == s.inc {
			continue // our own usage, counted when admitted
		}
		if n, err := strconv.ParseUint(msg.FieldValues["s"], 10, 64); err == nil {
			if n <= s.seen[run] {
				continue // a batch sent again: already applied
			}
			s.seen[run] = n
		}
		var es []entry
		if json.Unmarshal([]byte(msg.FieldValues["u"]), &es) != nil {
			continue
		}
		lag := time.Duration(now-streamMillis(msg.ID)) * time.Millisecond
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

// trackSilent records the runs that stopped heartbeating (stale) and were
// not replaced by a newer run of the same router (live).
func (s *Store) trackSilent(stale, live []string) {
	newest := map[string]int64{}
	for _, r := range live {
		if id, start, ok := splitRun(r); ok && start > newest[id] {
			newest[id] = start
		}
	}
	var silent []string
	for _, r := range stale {
		if r == s.inc {
			continue
		}
		if id, start, ok := splitRun(r); ok && newest[id] > start {
			continue // restarted: this run has ended
		}
		silent = append(silent, r)
	}
	sort.Strings(silent)
	if key := strings.Join(silent, ","); key != s.silentAt {
		if len(silent) > 0 {
			log.Warnf("shared counters: %d router(s) stopped heartbeating (%s); the others keep their share of every limit free for them", len(silent), key)
		}
		s.silentAt = key
		s.silentEp.Add(1)
	}
	s.silent.Store(int32(len(silent)))
}

func splitRun(run string) (id string, start int64, ok bool) {
	i := strings.LastIndexByte(run, '@')
	if i < 0 {
		return "", 0, false
	}
	start, err := strconv.ParseInt(run[i+1:], 10, 64)
	return run[:i], start, err == nil
}

// StreamBefore reports whether stream ID a ("ms-seq") is before b; an empty
// ID is before every entry.
func StreamBefore(a, b string) bool {
	am, bm := streamMillis(a), streamMillis(b)
	if am != bm {
		return am < bm
	}
	return streamSeq(a) < streamSeq(b)
}

func streamSeq(id string) int64 {
	if i := strings.IndexByte(id, '-'); i > 0 {
		n, _ := strconv.ParseInt(id[i+1:], 10, 64)
		return n
	}
	return 0
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
