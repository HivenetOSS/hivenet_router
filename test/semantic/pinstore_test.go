// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/semantic"
)

// TestTaskKey_BoundedAndScoped: a pin key is a fixed-size hash whatever the
// client sends, and differs per alias, per key and per end user.
func TestTaskKey_BoundedAndScoped(t *testing.T) {
	v := view(t, userMsg("hi"), semantic.DialectOpenAI)
	huge := strings.Repeat("x", 64<<10)
	tests := []struct {
		name   string
		a, b   string
		differ bool
	}{
		// A 64 KB header must not become a 64 KB map key.
		{"header is hashed", semantic.TaskKey("auto", huge, "k", v), "", false},
		{"alias scopes header keys", semantic.TaskKey("auto", "t1", "k", v), semantic.TaskKey("auto-fast", "t1", "k", v), true},
		{"alias scopes fingerprints", semantic.TaskKey("auto", "", "k", v), semantic.TaskKey("auto-fast", "", "k", v), true},
		{"key id scopes headers", semantic.TaskKey("auto", "t1", "k1", v), semantic.TaskKey("auto", "t1", "k2", v), true},
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

// TestTaskKey_EndUserSeparatesSharedKeys: one API key serving many users with
// the same system prompt and opener must not share a fingerprint when the
// request names its end user (OpenAI "user", Anthropic metadata.user_id).
func TestTaskKey_EndUserSeparatesSharedKeys(t *testing.T) {
	openai := func(user string) *semantic.RequestView {
		return view(t, `{"user":"`+user+`","messages":[{"role":"system","content":"bot"},{"role":"user","content":"hi"}]}`, semantic.DialectOpenAI)
	}
	anthropic := func(user string) *semantic.RequestView {
		return view(t, `{"metadata":{"user_id":"`+user+`"},"system":"bot","messages":[{"role":"user","content":"hi"}]}`, semantic.DialectAnthropic)
	}
	for name, mk := range map[string]func(string) *semantic.RequestView{"openai user": openai, "anthropic metadata.user_id": anthropic} {
		t.Run(name, func(t *testing.T) {
			if mk("alice").EndUser != "alice" {
				t.Fatalf("EndUser = %q", mk("alice").EndUser)
			}
			if semantic.TaskKey("auto", "", "k", mk("alice")) == semantic.TaskKey("auto", "", "k", mk("bob")) {
				t.Error("different end users must not share a fingerprint")
			}
		})
	}
	// A malformed user field degrades to "no end user", never to a 400.
	if _, err := semantic.ParseView([]byte(`{"user":42,"metadata":"x","messages":[]}`), semantic.DialectOpenAI, nil); err != nil {
		t.Errorf("odd user/metadata shapes must parse: %v", err)
	}
}

// TestPinStore_EvictsLeastRecentlyUsed: at capacity the least recently used pin
// goes, so a task that keeps calling is never evicted by newer ones.
func TestPinStore_EvictsLeastRecentlyUsed(t *testing.T) {
	s := semantic.NewPinStore(3, 0)
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
	s := semantic.NewPinStore(100, 2)
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
	s := semantic.NewPinStore(10, 0)
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

// TestPinStore_Concurrent exercises every operation from many goroutines; run
// with -race.
func TestPinStore_Concurrent(t *testing.T) {
	s := semantic.NewPinStore(64, 8)
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
			s := semantic.NewPinStore(size, size)
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
