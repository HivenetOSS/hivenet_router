// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

// TaskIDHeader lets a client name its task explicitly; it takes precedence over
// the fingerprint.
const TaskIDHeader = "X-Hivenet-Task-ID"

// MaxTaskIDLen bounds the TaskIDHeader value. Longer values are rejected by the
// API layer; the header is hashed either way, so a pin key is always 34 bytes.
const MaxTaskIDLen = 128

// TaskKey returns the pinning key for a request to alias. It is a hash of the
// alias and the caller's key id plus either the client's task header (when
// present) or a fingerprint of (end user, system prompt, first human turn).
// Agent harnesses keep the fingerprint inputs stable for a whole task, while two
// tasks from the same key differ in their first user message. The alias is part
// of the key so a pin never leaks from one alias to another; the end user
// (OpenAI "user", Anthropic metadata.user_id) separates users that share one API
// key and one system prompt. Raw client values never reach the pin store.
func TaskKey(alias, headerValue, keyID string, v *RequestView) string {
	h := sha256.New()
	field := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	field(alias)
	field(keyID)
	prefix := "f:"
	if headerValue != "" {
		prefix = "h:"
		field(headerValue)
	} else {
		field(v.EndUser)
		field(v.System)
		field(v.FirstUserText)
	}
	return prefix + hex.EncodeToString(h.Sum(nil)[:16])
}

// Pin store defaults, used when the constructor gets a non-positive value.
const (
	DefaultPinMax       = 100_000
	DefaultPinMaxPerKey = 10_000

	pinShards = 32
	// shardThreshold: below this capacity the store uses one shard so the cap is
	// exact (small stores are tests or tiny deployments; contention is moot).
	shardThreshold = pinShards * 256
)

// pinEntry is one cached task→model decision. It sits in two lists of its
// shard: the shard-wide LRU (capacity eviction) and its owner's LRU (per-key cap).
type pinEntry struct {
	key, owner   string
	model, route string
	ttl          time.Duration
	expires      time.Time // sliding: extended by ttl on every hit
	deadline     time.Time // absolute: created + max age (zero = none)
	lru, own     *list.Element
}

func (e *pinEntry) expired(now time.Time) bool {
	return now.After(e.expires) || (!e.deadline.IsZero() && now.After(e.deadline))
}

type pinShard struct {
	mu     sync.Mutex
	m      map[string]*pinEntry
	lru    *list.List            // front = most recently used
	owners map[string]*list.List // owner → that owner's entries, front = most recent
}

// PinStore caches task→model decisions in memory. A pin has a sliding TTL (each
// hit extends it, so a long task stays pinned while it keeps calling) and an
// optional absolute max age (so a pin shared by colliding fingerprints cannot
// live forever). Capacity is bounded globally and per owner (API key); at either
// cap the least recently used pin is evicted, in O(1). The store is sharded by
// key hash to keep lock hold times short under concurrent alias traffic. Pins
// are per router process and are lost on restart, which only costs one
// re-decision.
type PinStore struct {
	shards      []pinShard
	seed        maphash.Seed
	perShard    int
	perOwnerCap int
	now         atomic.Pointer[func() time.Time]
}

// NewPinStore creates a store holding about max pins in total and at most
// maxPerKey per owner (non-positive values select DefaultPinMax /
// DefaultPinMaxPerKey). Above shardThreshold the cap is split over 32 shards,
// so the total may exceed max by less than the shard count.
func NewPinStore(max, maxPerKey int) *PinStore {
	if max <= 0 {
		max = DefaultPinMax
	}
	if maxPerKey <= 0 {
		maxPerKey = DefaultPinMaxPerKey
	}
	n := 1
	if max >= shardThreshold {
		n = pinShards
	}
	s := &PinStore{
		shards:      make([]pinShard, n),
		seed:        maphash.MakeSeed(),
		perShard:    ceilDiv(max, n),
		perOwnerCap: ceilDiv(maxPerKey, n),
	}
	for i := range s.shards {
		s.shards[i] = pinShard{m: make(map[string]*pinEntry), lru: list.New(), owners: make(map[string]*list.List)}
	}
	now := time.Now
	s.now.Store(&now)
	return s
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }

// SetClock replaces the time source (tests). Safe for concurrent use.
func (s *PinStore) SetClock(now func() time.Time) { s.now.Store(&now) }

func (s *PinStore) clock() time.Time { return (*s.now.Load())() }

func (s *PinStore) shard(key string) *pinShard {
	if len(s.shards) == 1 {
		return &s.shards[0]
	}
	return &s.shards[maphash.String(s.seed, key)%uint64(len(s.shards))]
}

// Get returns the pinned model and route for key, extending the sliding TTL on
// a hit. Expired pins are removed and reported as a miss.
func (s *PinStore) Get(key string) (model, route string, ok bool) {
	sh := s.shard(key)
	now := s.clock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, found := sh.m[key]
	if !found {
		return "", "", false
	}
	if e.expired(now) {
		sh.remove(e)
		return "", "", false
	}
	e.expires = now.Add(e.ttl)
	sh.lru.MoveToFront(e.lru)
	sh.owners[e.owner].MoveToFront(e.own)
	return e.model, e.route, true
}

// Put pins key (owned by owner, the caller's API key identity) to model for a
// sliding ttl, with an absolute maxAge (0 = none). Re-putting an existing key
// replaces its decision and restarts its max age.
func (s *PinStore) Put(key, owner, model, route string, ttl, maxAge time.Duration) {
	if ttl <= 0 {
		return
	}
	sh := s.shard(key)
	now := s.clock()
	var deadline time.Time
	if maxAge > 0 {
		deadline = now.Add(maxAge)
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := sh.m[key]; ok {
		sh.remove(e) // re-insert below so owner changes and list order stay consistent
	}
	sh.evictExpiredTail(now)
	for len(sh.m) >= s.perShard {
		sh.remove(sh.lru.Back().Value.(*pinEntry))
	}
	ol := sh.owners[owner]
	if ol == nil {
		ol = list.New()
		sh.owners[owner] = ol
	}
	for ol.Len() >= s.perOwnerCap {
		sh.remove(ol.Back().Value.(*pinEntry))
	}
	e := &pinEntry{key: key, owner: owner, model: model, route: route, ttl: ttl, expires: now.Add(ttl), deadline: deadline}
	e.lru = sh.lru.PushFront(e)
	e.own = ol.PushFront(e)
	sh.m[key] = e
}

// Delete removes a pin (used when the pinned model is no longer eligible).
func (s *PinStore) Delete(key string) {
	sh := s.shard(key)
	sh.mu.Lock()
	if e, ok := sh.m[key]; ok {
		sh.remove(e)
	}
	sh.mu.Unlock()
}

// Len returns the number of pins held, expired or not (tests / diagnostics).
func (s *PinStore) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

// remove unlinks e from every index. Caller holds sh.mu.
func (sh *pinShard) remove(e *pinEntry) {
	delete(sh.m, e.key)
	sh.lru.Remove(e.lru)
	if ol := sh.owners[e.owner]; ol != nil {
		ol.Remove(e.own)
		if ol.Len() == 0 {
			delete(sh.owners, e.owner)
		}
	}
}

// evictExpiredTail drops expired pins from the least recently used end, so
// abandoned tasks free space before a live pin has to be evicted. Stops at the
// first live pin: amortised O(1) per Put. Caller holds sh.mu.
func (sh *pinShard) evictExpiredTail(now time.Time) {
	for el := sh.lru.Back(); el != nil; el = sh.lru.Back() {
		e := el.Value.(*pinEntry)
		if !e.expired(now) {
			return
		}
		sh.remove(e)
	}
}
