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
// When Valkey is unreachable each router keeps serving with its share of every
// limit (limit ÷ the routers it last saw), so the routers together still stay
// within the limit; it catches up when Valkey answers again.
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
// created with the right size. age is how long ago the usage was published.
type Applier interface {
	ApplyPeerUsage(kind byte, bucket string, limit, n int, age time.Duration)
}

// Config configures the connection and the sync loop.
type Config struct {
	Addrs          []string      // Valkey addresses, or Sentinel addresses with SentinelMaster
	SentinelMaster string        // Sentinel master set name; empty for a direct connection
	Password       string        // Valkey (and Sentinel) password; empty for none
	TLS            bool          // connect with TLS (system roots)
	Prefix         string        // key prefix, one per policloud (the site ID)
	RouterID       string        // stable router identity, e.g. the pod name
	Interval       time.Duration // sync interval
	DownAfter      time.Duration // unreachable this long → each router enforces its share
}

// Store is the Valkey connection plus the sync loop.
type Store struct {
	cfg      Config
	inc      string // this run: RouterID@start-ms, so a restart never counts as a live peer
	appliers []Applier

	connMu sync.Mutex
	client valkey.Client

	mu  sync.Mutex
	buf map[usageKey]*usageVal // usage admitted since the last successful tick

	cursor string    // last stream entry read
	lastOK time.Time // last successful tick

	fleet    atomic.Int32  // live routers, this one included (last known while down)
	seq      atomic.Uint64 // successful ticks
	degraded atomic.Uint64 // 0 while Valkey answers, else the outage number
	outages  uint64

	m *storeMetrics
}

type usageKey struct {
	kind   byte
	bucket string
}

type usageVal struct {
	limit int
	n     int
}

// entry is one bucket's usage inside a stream message.
type entry struct {
	K string `json:"k"`
	B string `json:"b"`
	L int    `json:"l"`
	N int    `json:"n"`
}

// New validates cfg. It does not connect: the first tick does, and a router
// whose Valkey is down starts with each limit at its share.
func New(cfg Config) (*Store, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("shared counters need at least one Valkey address")
	}
	if cfg.RouterID == "" {
		return nil, errors.New("shared counters need a router ID")
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
	s := &Store{
		cfg: cfg,
		inc: cfg.RouterID + "@" + strconv.FormatInt(time.Now().UnixMilli(), 10),
		buf: make(map[usageKey]*usageVal),
		m:   newStoreMetrics(),
	}
	s.fleet.Store(1)
	return s, nil
}

// AddApplier registers a receiver for the other routers' usage. Call before Start.
func (s *Store) AddApplier(a Applier) { s.appliers = append(s.appliers, a) }

// Size returns the live routers in the policloud, this one included. While
// Valkey is unreachable it is the last count seen.
func (s *Store) Size() int {
	if n := int(s.fleet.Load()); n > 0 {
		return n
	}
	return 1
}

// SyncSeq changes after every tick that applied the other routers' usage.
func (s *Store) SyncSeq() uint64 { return s.seq.Load() }

// DegradedEpoch is 0 while Valkey answers, else a number that changes with each
// outage, so a limiter can snapshot its state once per outage.
func (s *Store) DegradedEpoch() uint64 { return s.degraded.Load() }

// Record adds usage this router admitted; it is published on the next tick.
func (s *Store) Record(kind byte, bucket string, limit, n int) {
	if n <= 0 {
		return
	}
	k := usageKey{kind, bucket}
	s.mu.Lock()
	v := s.buf[k]
	if v == nil {
		v = &usageVal{}
		s.buf[k] = v
	}
	v.limit = limit
	v.n += n
	s.mu.Unlock()
}

func (s *Store) key(parts ...string) string {
	return "hivenet:" + s.cfg.Prefix + ":" + strings.Join(parts, ":")
}

func dayIndex(t time.Time) int { return int(t.UTC().Unix() / 86400) }

// conn returns the client, connecting on first use. A failed connect is
// retried on the next call.
func (s *Store) conn() (valkey.Client, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
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

// DailyTotal returns the policloud's tokens used today in a daily bucket.
func (s *Store) DailyTotal(ctx context.Context, bucket string) (int, error) {
	c, err := s.conn()
	if err != nil {
		return 0, err
	}
	v, err := c.Do(ctx, c.B().Hget().Key(s.key("tpd", strconv.Itoa(dayIndex(time.Now())))).Field(bucket).Build()).AsInt64()
	if valkey.IsValkeyNil(err) {
		return 0, nil
	}
	return int(v), err
}

// Start runs the sync loop until ctx is done. The first tick replays the last
// minute of the usage stream, so a restarted router knows its rate buckets.
func (s *Store) Start(ctx context.Context) {
	s.cursor = strconv.FormatInt(time.Now().Add(-time.Minute).UnixMilli(), 10) + "-0"
	s.lastOK = time.Now()
	go func() {
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

// Close publishes the usage admitted since the last tick, so the other
// routers count it, then closes the connection.
func (s *Store) Close() {
	s.mu.Lock()
	pending := len(s.buf) > 0
	s.mu.Unlock()
	if pending {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Interval)
		s.tick(ctx)
		cancel()
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
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

	s.mu.Lock()
	buf := s.buf
	s.buf = make(map[usageKey]*usageVal)
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

	entries := make([]entry, 0, len(buf))
	daily := map[string]int64{}
	for k, v := range buf {
		entries = append(entries, entry{K: string(k.kind), B: k.bucket, L: v.limit, N: v.n})
		if k.kind == KindDaily {
			daily[k.bucket] += int64(v.n)
		}
	}
	payload, _ := json.Marshal(entries)
	stream := s.key("usage")
	minID := strconv.FormatInt(time.Now().Add(-2*time.Minute).UnixMilli(), 10)
	cmds := valkey.Commands{
		c.B().Xadd().Key(stream).Minid().Almost().Threshold(minID).Id("*").
			FieldValue().FieldValue("r", s.inc).FieldValue("u", string(payload)).Build(),
	}
	if len(daily) > 0 {
		dk := s.key("tpd", strconv.Itoa(dayIndex(time.Now())))
		for b, n := range daily {
			cmds = append(cmds, c.B().Hincrby().Key(dk).Field(b).Increment(n).Build())
		}
		cmds = append(cmds, c.B().Expire().Key(dk).Seconds(int64((48 * time.Hour).Seconds())).Build())
	}
	cmds = append(cmds, c.B().Xrange().Key(stream).Start("("+s.cursor).End("+").Count(100000).Build())
	res := c.DoMulti(cctx, cmds...)
	for _, r := range res[:len(res)-1] {
		if err := r.Error(); err != nil {
			s.failTick(buf, err)
			return
		}
	}
	msgs, err := res[len(res)-1].AsXRange()
	if err != nil {
		s.failTick(nil, err) // usage is published; only the read failed
		return
	}

	if s.degraded.Load() != 0 {
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
		age := now.Sub(time.UnixMilli(streamMillis(msg.ID)))
		s.m.lag.Set(age.Seconds())
		for _, e := range es {
			if len(e.K) != 1 || e.N <= 0 {
				continue
			}
			for _, a := range s.appliers {
				a.ApplyPeerUsage(e.K[0], e.B, e.L, e.N, age)
			}
		}
	}
	s.seq.Add(1)
}

// failTick puts unpublished usage back for the next tick and, once Valkey has
// been unreachable for DownAfter, switches every limit to this router's share.
func (s *Store) failTick(buf map[usageKey]*usageVal, err error) {
	if len(buf) > 0 {
		s.mu.Lock()
		for k, v := range buf {
			if cur := s.buf[k]; cur != nil {
				cur.n += v.n
			} else {
				s.buf[k] = v
			}
		}
		s.mu.Unlock()
	}
	s.m.errors.Inc()
	if time.Since(s.lastOK) >= s.cfg.DownAfter && s.degraded.Load() == 0 {
		s.outages++
		s.degraded.Store(s.outages)
		s.m.up.Set(0)
		log.Warnf("shared counters: Valkey unreachable for %v (%v); each router now enforces 1/%d of every limit",
			time.Since(s.lastOK).Round(time.Millisecond), err, s.Size())
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
// routers' clocks do not matter), drops runs silent for 10 minutes, and returns
// the runs seen within ARGV[2] ms.
var heartbeat = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 600000)
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
	return fmt.Sprintf("Valkey %s (%s), router %s, prefix %s, every %v", strings.Join(c.Addrs, ","), mode, c.RouterID, prefix, c.Interval)
}
