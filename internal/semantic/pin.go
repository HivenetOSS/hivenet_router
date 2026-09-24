// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// TaskIDHeader lets a client name its task explicitly; it takes precedence over
// the fingerprint.
const TaskIDHeader = "X-Hivenet-Task-ID"

// TaskKey returns the pinning key for a request: the client's task header when
// present, else a fingerprint of (API key id, system prompt, first human turn).
// Agent harnesses keep both stable for a whole task, while two tasks from the
// same key differ in their first user message.
func TaskKey(headerValue, keyID string, v *RequestView) string {
	if headerValue != "" {
		return "h:" + keyID + ":" + headerValue
	}
	h := sha256.New()
	h.Write([]byte(keyID))
	h.Write([]byte{0})
	h.Write([]byte(v.System))
	h.Write([]byte{0})
	h.Write([]byte(v.FirstUserText))
	return "f:" + hex.EncodeToString(h.Sum(nil)[:16])
}

// pin is one cached task→model decision.
type pin struct {
	model   string
	route   string
	ttl     time.Duration
	expires time.Time
}

// PinStore caches task→model decisions in memory with a sliding TTL (each hit
// extends the pin, so a long task stays pinned while it keeps calling) and a
// size cap. Pins are per router process and are lost on restart, which only
// costs one re-decision.
type PinStore struct {
	mu  sync.Mutex
	m   map[string]pin
	max int
	now func() time.Time
}

// NewPinStore creates a store holding at most max pins (<=0 means 100000).
func NewPinStore(max int) *PinStore {
	if max <= 0 {
		max = 100000
	}
	return &PinStore{m: make(map[string]pin), max: max, now: time.Now}
}

// SetClock replaces the time source (tests).
func (s *PinStore) SetClock(now func() time.Time) { s.now = now }

// Get returns the pinned model and route for key, extending the pin on a hit.
func (s *PinStore) Get(key string) (model, route string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, found := s.m[key]
	if !found {
		return "", "", false
	}
	now := s.now()
	if now.After(p.expires) {
		delete(s.m, key)
		return "", "", false
	}
	p.expires = now.Add(p.ttl)
	s.m[key] = p
	return p.model, p.route, true
}

// Put pins key to model for ttl. When full it first drops expired pins, then
// an arbitrary one, so memory stays bounded under a flood of distinct tasks.
func (s *PinStore) Put(key, model, route string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if _, exists := s.m[key]; !exists && len(s.m) >= s.max {
		for k, p := range s.m {
			if now.After(p.expires) {
				delete(s.m, k)
			}
		}
		if len(s.m) >= s.max {
			for k := range s.m {
				delete(s.m, k)
				break
			}
		}
	}
	s.m[key] = pin{model: model, route: route, ttl: ttl, expires: now.Add(ttl)}
}

// Delete removes a pin (used when the pinned model is no longer eligible).
func (s *PinStore) Delete(key string) {
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

// Len returns the number of pins held (tests / diagnostics).
func (s *PinStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
