// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package pin_test covers the shared pin store and pin key: capacity and
// per-owner eviction, sliding TTL vs absolute max age, concurrency, and the
// bounded/scoped shape of TaskKey. The store backs both semantic model pins
// and the executor's agent affinity pins.
package pin_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/pin"
)

// fp builds a fingerprint-keyed TaskKeyInput (no header).
func fp(alias, keyID, endUser, system, first string) pin.TaskKeyInput {
	return pin.TaskKeyInput{Alias: alias, KeyID: keyID, EndUser: endUser, System: system, FirstUserText: first}
}

// TestTaskKey_BoundedAndScoped: a pin key is a fixed-size hash whatever the
// client sends, and differs per alias, per key and per end user.
func TestTaskKey_BoundedAndScoped(t *testing.T) {
	huge := strings.Repeat("x", 64<<10)
	tests := []struct {
		name   string
		a, b   string
		differ bool
	}{
		// A 64 KB header must not become a 64 KB map key.
		{"header is hashed", pin.TaskKey(hdr(fp("auto", "k", "", "", ""), huge)), "", false},
		{"alias scopes header keys", pin.TaskKey(hdr(fp("auto", "k", "", "", ""), "t1")), pin.TaskKey(hdr(fp("auto-fast", "k", "", "", ""), "t1")), true},
		{"alias scopes fingerprints", pin.TaskKey(fp("auto", "k", "e", "s", "u")), pin.TaskKey(fp("auto-fast", "k", "e", "s", "u")), true},
		{"key id scopes headers", pin.TaskKey(hdr(fp("auto", "k1", "", "", ""), "t1")), pin.TaskKey(hdr(fp("auto", "k2", "", "", ""), "t1")), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.a) != 34 {
				t.Errorf("key length %d, want 34", len(tc.a))
			}
			if tc.b != "" && (tc.a != tc.b) != tc.differ {
				t.Errorf("keys %q and %q: differ=%v, want %v", tc.a, tc.b, tc.a != tc.b, tc.differ)
			}
		})
	}
}

// hdr returns in with HeaderValue set (test helper: TaskKeyInput is a plain
// struct, so tests build it literally).
func hdr(in pin.TaskKeyInput, h string) pin.TaskKeyInput {
	in.HeaderValue = h
	return in
}

// TestTaskKey_HeaderBeatsFingerprint: a non-empty header replaces the
// fingerprint entirely, so a conversation stays keyed when its prompt churns.
func TestTaskKey_HeaderBeatsFingerprint(t *testing.T) {
	a := pin.TaskKey(fp("auto", "k", "e", "system v1", "hello"))
	b := pin.TaskKey(fp("auto", "k", "e", "system v2 (stamp)", "hello"))
	if a == b {
		t.Error("fingerprint keys must differ when the system prompt changes")
	}
	ha := pin.TaskKey(hdr(fp("auto", "k", "e", "system v1", "hello"), "task-42"))
	hb := pin.TaskKey(hdr(fp("auto", "k", "e", "system v2 (stamp)", "more text"), "task-42"))
	if ha != hb {
		t.Error("header key must be stable across prompt churn")
	}
	if !strings.HasPrefix(ha, "h:") || !strings.HasPrefix(a, "f:") {
		t.Errorf("prefixes: header=%q fingerprint=%q", ha, a)
	}
}

// TestTaskKey_EndUserSeparatesSharedKeys: one API key serving many users with
// the same system prompt and opener must not share a fingerprint when the
// request names its end user (OpenAI "user", Anthropic metadata.user_id).
func TestTaskKey_EndUserSeparatesSharedKeys(t *testing.T) {
	if pin.TaskKey(fp("auto", "k", "alice", "bot", "hi")) == pin.TaskKey(fp("auto", "k", "bob", "bot", "hi")) {
		t.Error("different end users must not share a fingerprint")
	}
}

// TestPinStore_EvictsLeastRecentlyUsed: at capacity the least recently used pin
// goes, so a task that keeps calling is never evicted by newer ones.
func TestPinStore_EvictsLeastRecentlyUsed(t *testing.T) {
	s := pin.NewPinStore(3, 0)
	for _, k := range []string{"a", "b", "c"} {
		s.Put(k, "owner", "m-"+k, "r", time.Minute, 0)
	}
	if _, _, ok := s.Get("a"); !ok { // touch a: b is now the oldest
		t.Fatal("a missing")
	}
	s.Put("d", "owner", "m-d", "r", time.Minute, 0)
	if _, _, ok := s.Get("b"); ok {
		t.Error("b (least recently used) should have been evicted")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, _, ok := s.Get(k); !ok {
			t.Errorf("%s should still be pinned", k)
		}
	}
	if s.Len() != 3 {
		t.Errorf("len = %d, want 3", s.Len())
	}
}

// TestPinStore_PerKeyCap: one caller flooding new tasks evicts only its own
// pins, never another key's.
func TestPinStore_PerKeyCap(t *testing.T) {
	s := pin.NewPinStore(100, 2)
	s.Put("victim-task", "victim", "m", "r", time.Minute, 0)
	for i := range 50 {
		s.Put(fmt.Sprintf("flood-%d", i), "flooder", "m", "r", time.Minute, 0)
	}
	if _, _, ok := s.Get("victim-task"); !ok {
		t.Error("another key's flood evicted the victim's pin")
	}
	if s.Len() != 3 {
		t.Errorf("len = %d, want 3 (victim + flooder's cap of 2)", s.Len())
	}
	if _, _, ok := s.Get("flood-49"); !ok {
		t.Error("the flooder's newest pin should be kept")
	}
}

// TestPinStore_MaxAge: hits slide the TTL, but never past the absolute max age.
func TestPinStore_MaxAge(t *testing.T) {
	s := pin.NewPinStore(10, 0)
	now := time.Unix(1_700_000_000, 0)
	s.SetClock(func() time.Time { return now })
	s.Put("task", "k", "m", "r", 30*time.Minute, time.Hour)
	for range 3 { // a hit every 20 min keeps the sliding TTL alive...
		now = now.Add(20 * time.Minute)
		if _, _, ok := s.Get("task"); !ok {
			t.Fatalf("pin expired early at %v", now)
		}
	}
	now = now.Add(time.Minute) // ...but 61 min after the Put the max age ends it
	if _, _, ok := s.Get("task"); ok {
		t.Error("pin outlived pin_max_age")
	}
}

// TestPinStore_Bounded: the store never exceeds its cap.
func TestPinStore_Bounded(t *testing.T) {
	s := pin.NewPinStore(3, 0)
	for i := range 10 {
		s.Put(string(rune('a'+i)), "owner", "m", "r", time.Minute, 0)
	}
	if s.Len() > 3 {
		t.Errorf("len = %d, want <= 3", s.Len())
	}
}

// TestPinStore_Concurrent exercises every operation from many goroutines; run
// with -race.
func TestPinStore_Concurrent(t *testing.T) {
	s := pin.NewPinStore(64, 8)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				k := fmt.Sprintf("t%d", (g*7+i)%97)
				s.Put(k, fmt.Sprintf("o%d", g%4), "m", "r", time.Minute, time.Hour)
				s.Get(k)
				if i%10 == 0 {
					s.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
	if n := s.Len(); n > 64 {
		t.Errorf("len = %d exceeds the cap", n)
	}
}

// BenchmarkPinStorePutAtCapacity: a Put into a full store is O(1); the old
// implementation scanned every pin under one lock.
func BenchmarkPinStorePutAtCapacity(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			s := pin.NewPinStore(size, size)
			for i := range size {
				s.Put(fmt.Sprint("fill-", i), "k", "m", "r", time.Hour, 0)
			}
			keys := make([]string, b.N)
			for i := range keys {
				keys[i] = fmt.Sprint("new-", i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Put(keys[i], "k", "m", "r", time.Hour, 0)
			}
		})
	}
}
