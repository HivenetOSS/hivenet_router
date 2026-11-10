// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"hivenet_router/internal/shared"
)

// cutProxy forwards TCP to a Valkey address until cut, so a test can cut one
// router off while the others keep the store.
type cutProxy struct {
	ln    net.Listener
	mu    sync.Mutex
	cut   bool
	conns []net.Conn
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &cutProxy{ln: ln}
	t.Cleanup(func() { ln.Close(); p.setCut(true) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.cut {
				p.mu.Unlock()
				c.Close()
				continue
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				p.mu.Unlock()
				c.Close()
				continue
			}
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); up.Close() }()
			go func() { _, _ = io.Copy(c, up); c.Close() }()
		}
	}()
	return p
}

func (p *cutProxy) setCut(cut bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = cut
	if cut {
		for _, c := range p.conns {
			c.Close()
		}
		p.conns = nil
	}
}

func usageStream(t *testing.T, mr *miniredis.Miniredis) string {
	t.Helper()
	for _, k := range mr.Keys() {
		if strings.HasSuffix(k, ":usage") {
			return k
		}
	}
	return ""
}

// A router cut off long enough for the others' usage entries to be trimmed
// reads today's total again when it reconnects, instead of missing that
// usage for the rest of the day.
func TestDailyReadAgainAfterTrimmedOutage(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newRouter(t, mr.Addr(), "a")
	p := newCutProxy(t, mr.Addr())
	b := newRouter(t, p.ln.Addr().String(), "b")
	waitFor(t, 5*time.Second, "routers to see each other", func() bool {
		return a.store.Size() == 2 && b.store.Size() == 2
	})
	const tpd = 1000
	if ok, _, _ := b.rate.AllowInputTokens("t", "", tpd, 1); !ok {
		t.Fatal("b refused")
	}
	p.setCut(true)
	waitFor(t, 5*time.Second, "b to notice the outage", func() bool { return b.store.DegradedEpoch() != 0 })
	if ok, _, _ := a.rate.AllowInputTokens("t", "", tpd, 500); !ok {
		t.Fatal("a refused")
	}
	time.Sleep(3 * interval)   // a publishes its usage and total
	mr.Del(usageStream(t, mr)) // trimmed before b could read it
	p.setCut(false)
	waitFor(t, 5*time.Second, "b to reconnect", func() bool { return b.store.DegradedEpoch() == 0 })
	// A failed read is tried again on the next request (the first one after
	// a reconnect may hit a connection the cut closed).
	waitFor(t, 2*time.Second, "b to count a's 500 tokens", func() bool {
		rem, _ := b.rate.RemainingTokens("t", "", tpd)
		return rem <= 499
	})
}

// An idle router adds nothing to the usage stream: the heartbeat already
// shows it is alive.
func TestIdleTicksPublishNothing(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 2)
	rs[0].rate.AllowRequest("t", "", 60)
	time.Sleep(3 * interval)
	stream := usageStream(t, mr)
	before, _ := mr.Stream(stream)
	time.Sleep(5 * interval)
	after, _ := mr.Stream(stream)
	if len(after) != len(before) {
		t.Fatalf("the stream grew from %d to %d entries while idle", len(before), len(after))
	}
}

// A batch sent again after a publish whose reply was lost carries the same
// number: the other routers apply it once.
func TestRepublishedBatchCountedOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	rs := newFleet(t, mr, 2)
	const tpd = 1000
	if rem, _ := rs[1].rate.RemainingTokens("t", "", tpd); rem != tpd {
		t.Fatalf("r1 sees %d left, want %d", rem, tpd)
	}
	if ok, _, _ := rs[0].rate.AllowInputTokens("t", "", tpd, 100); !ok {
		t.Fatal("r0 refused")
	}
	waitFor(t, 2*time.Second, "r1 to count r0's usage", func() bool {
		rem, _ := rs[1].rate.RemainingTokens("t", "", tpd)
		return rem == 900
	})
	stream := usageStream(t, mr)
	entries, _ := mr.Stream(stream)
	last := entries[len(entries)-1]
	if _, err := mr.XAdd(stream, "*", last.Values); err != nil { // the same batch again
		t.Fatal(err)
	}
	time.Sleep(3 * interval)
	if rem, _ := rs[1].rate.RemainingTokens("t", "", tpd); rem != 900 {
		t.Fatalf("r1 sees %d left after the batch came twice, want 900", rem)
	}
}

// A Sentinel that never answers (packets dropped, as in a network cut) must
// not hold the sync loop: every connect has a timeout, so the router notices
// a healed cut within a round instead of after the kernel's SYN retries.
// 192.0.2.1 is reserved for documentation and never answers.
func TestUnansweredSentinelDoesNotHoldLoop(t *testing.T) {
	st, err := shared.New(shared.Config{Addrs: []string{"192.0.2.1:26379"}, SentinelMaster: "m",
		Prefix: "pc", RouterID: "a", Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	st.Start(context.Background())
	time.Sleep(3 * interval)
	done := make(chan struct{})
	go func() { st.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sync loop is stuck connecting to a Sentinel that never answers")
	}
}
