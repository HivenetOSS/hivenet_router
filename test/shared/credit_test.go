// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package shared_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/valkey-io/valkey-go"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/shared"
)

// valkeyBackend is the store the credit tests run against: miniredis, or a
// real Valkey when HIVENET_TEST_VALKEY_ADDR is set (each test then uses its
// own site prefix).
type valkeyBackend struct {
	addr   string
	prefix string
	mr     *miniredis.Miniredis
}

func newBackend(t *testing.T) *valkeyBackend {
	t.Helper()
	if addr := os.Getenv("HIVENET_TEST_VALKEY_ADDR"); addr != "" {
		return &valkeyBackend{addr: addr, prefix: fmt.Sprintf("t%d", time.Now().UnixNano())}
	}
	mr := miniredis.RunT(t)
	return &valkeyBackend{addr: mr.Addr(), prefix: "pc", mr: mr}
}

// flush drops every key of the site, as a Valkey restart without persistence
// or a FLUSHALL does.
func (b *valkeyBackend) flush(t *testing.T) {
	t.Helper()
	if b.mr != nil {
		b.mr.FlushAll()
		return
	}
	cl, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{b.addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	keys, err := cl.Do(ctx, cl.B().Keys().Pattern("hivenet:{"+b.prefix+"}:*").Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		cl.Do(ctx, cl.B().Del().Key(k).Build())
	}
}

func (b *valkeyBackend) zrem(t *testing.T, member string) {
	t.Helper()
	cl, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{b.addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cl.Do(context.Background(), cl.B().Zrem().Key("hivenet:{"+b.prefix+"}:routers").Member(member).Build())
}

func (b *valkeyBackend) members(t *testing.T) []string {
	t.Helper()
	cl, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{b.addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ms, _ := cl.Do(context.Background(), cl.B().Zrange().Key("hivenet:{"+b.prefix+"}:routers").Min("0").Max("-1").Build()).AsStrSlice()
	return ms
}

// proxy forwards one router's connections to the store; cutting it drops
// them and refuses new ones, as a network partition between that router and
// Valkey does.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
	cut   atomic.Bool
}

func newProxy(t *testing.T, to string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if p.cut.Load() {
				c.Close()
				continue
			}
			u, err := net.Dial("tcp", to)
			if err != nil {
				c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, u)
			p.mu.Unlock()
			go func() { io.Copy(u, c); u.Close() }() //nolint:errcheck
			go func() { io.Copy(c, u); c.Close() }() //nolint:errcheck
		}
	}()
	t.Cleanup(func() { ln.Close(); p.setCut(true) })
	return p
}

func (p *proxy) setCut(v bool) {
	p.cut.Store(v)
	if v {
		p.mu.Lock()
		for _, c := range p.conns {
			c.Close()
		}
		p.conns = nil
		p.mu.Unlock()
	}
}

// agents models which agents are connected to which router (by libp2p peer
// ID), and which agents' reports each router receives: an agent reports only
// to the routers it is connected to, unless a test cuts it off from one.
type agents struct {
	mu   sync.Mutex
	conn map[string]map[string]bool // agent → router peer → connected
	deaf map[string]map[string]bool // router peer → agents it hears nothing from
}

func newAgents() *agents {
	return &agents{conn: map[string]map[string]bool{}, deaf: map[string]map[string]bool{}}
}

func (a *agents) set(agent, router string, connected bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conn[agent] == nil {
		a.conn[agent] = map[string]bool{}
	}
	a.conn[agent][router] = connected
}

func (a *agents) deafen(router, agent string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.deaf[router] == nil {
		a.deaf[router] = map[string]bool{}
	}
	a.deaf[router][agent] = true
}

// servedBy lists the agents connected to router.
func (a *agents) servedBy(router string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for ag, rs := range a.conn {
		if rs[router] {
			out = append(out, ag)
		}
	}
	return out
}

// reportedTo lists the routers the agents that report to me are connected to.
func (a *agents) reportedTo(me string) func() []string {
	return func() []string {
		a.mu.Lock()
		defer a.mu.Unlock()
		seen := map[string]bool{}
		var out []string
		for ag, rs := range a.conn {
			if a.deaf[me][ag] || !rs[me] {
				continue
			}
			for r, ok := range rs {
				if ok && !seen[r] {
					seen[r] = true
					out = append(out, r)
				}
			}
		}
		return out
	}
}

// leftRouter is the router's view: every listed agent reported to me, and
// none is connected to target.
func (a *agents) leftRouter(me string) func(target string, list []string) bool {
	return func(target string, list []string) bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, ag := range list {
			if a.deaf[me][ag] || !a.conn[ag][me] || a.conn[ag][target] {
				return false
			}
		}
		return true
	}
}

type occRouter struct {
	peer    string
	store   *shared.Store
	credits *shared.Credits
	occ     *admission.Controller
	cancel  context.CancelFunc
	proxy   *proxy
}

// newOccFleet starts n routers sharing one occupancy pool, each through its
// own proxy to the store. Agent "a<i>" is connected to every router.
func newOccFleet(t *testing.T, b *valkeyBackend, ag *agents, n int) []*occRouter {
	t.Helper()
	var rs []*occRouter
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			ag.set(fmt.Sprintf("a%d", i), fmt.Sprintf("peer-%d", j), true)
		}
	}
	for i := 0; i < n; i++ {
		rs = append(rs, startOccRouter(t, b, ag, fmt.Sprintf("r%d", i), fmt.Sprintf("peer-%d", i)))
	}
	waitFor(t, 5*time.Second, "routers to see each other", func() bool {
		for _, r := range rs {
			if r.store.Size() != n || r.store.DegradedEpoch() != 0 || r.store.UnknownRouter() != "" {
				return false
			}
		}
		return true
	})
	return rs
}

func startOccRouter(t *testing.T, b *valkeyBackend, ag *agents, id, peer string) *occRouter {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	px := newProxy(t, b.addr)
	st, err := shared.New(shared.Config{Addrs: []string{px.ln.Addr().String()}, Prefix: b.prefix, RouterID: id, Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	st.SetPeerID(peer)
	st.SetAgents(func() []string { return ag.servedBy(peer) })
	st.SetRouterReports(ag.reportedTo(peer))
	cr := st.NewCredits("occ", ag.leftRouter(peer))
	c := admission.NewController(1, 300*time.Millisecond)
	c.SetPool(cr)
	st.Start(ctx)
	cr.Start(ctx)
	t.Cleanup(func() { cancel(); st.Close() })
	return &occRouter{peer: peer, store: st, credits: cr, occ: c, cancel: cancel, proxy: px}
}

// fill admits requests of weight w on r until one is refused, up to max.
func fill(r *occRouter, w, budget, max int) []*admission.Reservation {
	var out []*admission.Reservation
	for i := 0; i < max; i++ {
		res := r.occ.Admit(context.Background(), "m", w, false, budget, 0)
		if res == nil {
			break
		}
		out = append(out, res)
	}
	return out
}

// Three routers admitting at once never hold more than the budget, and the
// store is called far less than once per request.
func TestCreditsNeverOversubscribe(t *testing.T) {
	rs := newOccFleet(t, newBackend(t), newAgents(), 3)
	const budget, w = 2000, 100 // 20 requests site-wide
	var inflight, peak, admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 600; i++ {
		wg.Add(1)
		go func(r *occRouter) {
			defer wg.Done()
			res := r.occ.Admit(context.Background(), "m", w, false, budget, 0)
			if res == nil {
				return
			}
			admitted.Add(1)
			n := inflight.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			time.Sleep(20 * time.Millisecond)
			inflight.Add(-1)
			res.Release()
		}(rs[i%3])
		if i%50 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	wg.Wait()
	if peak.Load() > budget/w {
		t.Fatalf("peak in flight %d exceeds the budget of %d requests", peak.Load(), budget/w)
	}
	var refills int64
	for _, r := range rs {
		f, _, _ := r.credits.Stats()
		refills += f
	}
	if admitted.Load() < 100 {
		t.Fatalf("admitted only %d", admitted.Load())
	}
	if refills >= admitted.Load() {
		t.Fatalf("%d refills for %d admissions: credits save no store calls", refills, admitted.Load())
	}
}

// Undeclared output grows a reservation past the credits it took; the tick
// records the growth as held, so the other routers cannot be granted it.
func TestCreditsCountStreamedGrowth(t *testing.T) {
	rs := newOccFleet(t, newBackend(t), newAgents(), 2)
	const budget = 1000
	res := rs[0].occ.Admit(context.Background(), "m", 100, true, budget, 0)
	if res == nil {
		t.Fatal("first admission refused")
	}
	res.Grow(800) // the response streams 800 tokens: r0 now uses 900
	time.Sleep(3 * interval)
	if r := rs[1].occ.Admit(context.Background(), "m", 200, false, budget, 0); r != nil {
		t.Fatal("r1 admitted 200 although r0's streamed output leaves only 100")
	}
	if r := rs[1].occ.Admit(context.Background(), "m", 90, false, budget, 0); r == nil {
		t.Fatal("r1 refused 90 although 100 are free")
	}
}

// A starved router gets the others' idle credits within a few ticks.
func TestCreditsRebalanceToStarvedRouter(t *testing.T) {
	rs := newOccFleet(t, newBackend(t), newAgents(), 3)
	const budget = 1000
	for _, r := range rs[:2] {
		if res := r.occ.Admit(context.Background(), "m", 10, false, budget, 0); res != nil {
			defer res.Release()
		}
	}
	waitFor(t, 3*time.Second, "the starved router to get the idle credits", func() bool {
		if res := rs[2].occ.Admit(context.Background(), "m", 950, false, budget, 0); res != nil {
			res.Release()
			return true
		}
		return false
	})
}

// A crashed router's agents drop their connection to it and report so: its
// credits come back as soon as it is seen as down.
func TestCreditsReclaimedWhenItsAgentsLeftIt(t *testing.T) {
	ag := newAgents()
	rs := newOccFleet(t, newBackend(t), ag, 2)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].cancel() // crash
	ag.set("a0", "peer-0", false)
	ag.set("a1", "peer-0", false)
	start := time.Now()
	waitFor(t, 5*time.Second, "reclaim", func() bool {
		return rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil
	})
	if _, _, reclaims := rs[1].credits.Stats(); reclaims == 0 {
		t.Fatal("credits came back without a reclaim")
	}
	t.Logf("credits back after %v", time.Since(start).Round(100*time.Millisecond))
}

// A silent router keeps its credits while one of its agents is still
// connected to it (it lost Valkey, not the GPUs), however long that lasts.
func TestCreditsKeptWhileAnAgentReachesRouter(t *testing.T) {
	ag := newAgents()
	rs := newOccFleet(t, newBackend(t), ag, 2)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].proxy.setCut(true)
	ag.set("a1", "peer-0", false) // one agent left it, a0 still serves it
	time.Sleep(2 * time.Second)
	if rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil {
		t.Fatal("credits taken from a router an agent still reaches")
	}
	ag.set("a0", "peer-0", false)
	waitFor(t, 3*time.Second, "credits back once no agent reaches it", func() bool {
		return rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil
	})
}

// The agents of a silent router that this router hears nothing from (a zone
// partition) may still serve it: its credits are kept.
func TestCreditsKeptWhenItsAgentsCannotBeAsked(t *testing.T) {
	ag := newAgents()
	rs := newOccFleet(t, newBackend(t), ag, 2)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].proxy.setCut(true)
	ag.set("a1", "peer-0", false)
	ag.deafen("peer-1", "a0") // a0 is on r0's side of the partition
	time.Sleep(2 * time.Second)
	if rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil {
		t.Fatal("credits taken although r1 cannot ask one of r0's agents")
	}
}

// A router that restarts runs again under the same ID and libp2p peer ID, so
// the agents still reach that peer; the old run has ended all the same, and
// its credits come back as soon as the new run is live.
func TestCreditsOfRestartedRouterReclaimed(t *testing.T) {
	b, ag := newBackend(t), newAgents()
	rs := newOccFleet(t, b, ag, 2)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].cancel()
	time.Sleep(10 * time.Millisecond) // the new run starts later
	startOccRouter(t, b, ag, "r0", "peer-0")
	waitFor(t, 3*time.Second, "the old run's credits to come back", func() bool {
		return rs[1].occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil
	})
}

// Valkey loses its data while both routers are cut off from it, and r0 holds
// far more than its share. r1, back first, holds at most what r0 left free
// when r1 last saw the pool, until r0 is back or gone.
func TestCreditsAfterStoreRestartWithoutData(t *testing.T) {
	b, ag := newBackend(t), newAgents()
	rs := newOccFleet(t, b, ag, 2)
	const budget, w = 1000, 100
	for _, r := range fill(rs[1], w, budget, 1) { // r1 has seen this pool
		r.Release()
	}
	a := fill(rs[0], w, budget, 9)
	time.Sleep(3 * interval) // r1 reads the pool
	rs[0].proxy.setCut(true)
	rs[1].proxy.setCut(true)
	b.flush(t)
	time.Sleep(time.Second)
	rs[1].proxy.setCut(false)
	waitFor(t, 3*time.Second, "r1 back", func() bool { return rs[1].store.DegradedEpoch() == 0 })
	var got []*admission.Reservation
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		got = append(got, fill(rs[1], w, budget, 10-len(got))...)
		time.Sleep(5 * time.Millisecond)
	}
	if site := (len(a) + len(got)) * w; site > budget {
		t.Fatalf("site holds %d > budget %d (r0 %d, r1 %d)", site, budget, len(a)*w, len(got)*w)
	}
	// r0 comes back and rewrites what it holds; then r1 knows the pool again.
	rs[0].proxy.setCut(false)
	for _, r := range a {
		r.Release()
	}
	waitFor(t, 5*time.Second, "r1 to use the budget r0 freed", func() bool {
		got = append(got, fill(rs[1], w, budget, 10-len(got))...)
		return len(got) >= 9
	})
}

// The store loses its data without an outage (FLUSHALL): grants made before
// a router notices are refused, so no router is granted what another holds.
func TestCreditsAfterFlushWithoutOutage(t *testing.T) {
	b, ag := newBackend(t), newAgents()
	rs := newOccFleet(t, b, ag, 2)
	const budget, w = 1000, 100
	a := fill(rs[0], w, budget, 9)
	time.Sleep(3 * interval)
	b.flush(t)
	var got []*admission.Reservation
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		got = append(got, fill(rs[1], w, budget, 10-len(got))...)
		if site := (len(a) + len(got)) * w; site > budget {
			t.Fatalf("site holds %d > budget %d", site, budget)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// r0 is cut off from Valkey and its agents drop it for a moment: r1 takes its
// credits back. When the agents connect to r0 again it admits nothing from
// the credits it held, until Valkey tells it it was evicted.
func TestPartitionedRouterFencedWhenAgentsReturn(t *testing.T) {
	b, ag := newBackend(t), newAgents()
	rs := newOccFleet(t, b, ag, 2)
	const budget, w = 1000, 100
	a := fill(rs[0], w, budget, 4)
	rs[0].proxy.setCut(true)
	time.Sleep(4 * interval)
	// The agents lose r0 and stop its requests.
	ag.set("a0", "peer-0", false)
	ag.set("a1", "peer-0", false)
	for _, r := range a {
		r.Release()
	}
	waitFor(t, 3*time.Second, "r1 to reclaim r0", func() bool {
		_, _, n := rs[1].credits.Stats()
		return n > 0
	})
	b1 := fill(rs[1], w, budget, 10)
	// The agents connect to r0 again; it is still cut off from Valkey.
	ag.set("a0", "peer-0", true)
	ag.set("a1", "peer-0", true)
	rs[0].store.AgentJoined()
	a2 := fill(rs[0], w, budget, 10)
	if site := (len(b1) + len(a2)) * w; site > budget {
		t.Fatalf("site holds %d > budget %d (r0 %d, r1 %d)", site, budget, len(a2)*w, len(b1)*w)
	}
	// Valkey back: r0 learns it was evicted and asks the pool again.
	rs[0].proxy.setCut(false)
	waitFor(t, 3*time.Second, "r0 back", func() bool { return rs[0].store.Evictions() > 0 && !rs[0].store.Fenced() })
	a3 := fill(rs[0], w, budget, 10)
	if site := (len(b1) + len(a2) + len(a3)) * w; site > budget {
		t.Fatalf("after rejoin site holds %d > budget %d", site, budget)
	}
}

// A router evicted while cut off from the store, whose first heartbeat back
// re-added it but lost its reply (the tick failed after it), still learns it
// was evicted: its epoch changed. It drops the credits it held.
func TestEvictionSeenAfterLostHeartbeatReply(t *testing.T) {
	b := newBackend(t)
	rs := newOccFleet(t, b, newAgents(), 2)
	const budget, w = 1000, 100
	a := fill(rs[0], w, budget, 5)
	for _, r := range a {
		r.Release() // r0 keeps holding these credits, unused
	}
	rs[0].proxy.setCut(true)
	time.Sleep(4 * interval)
	// r1 evicts r0; r0's first heartbeat back re-adds it with a new epoch,
	// and r0 never sees that reply.
	cl, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{b.addr}, DisableCache: true, ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	for _, m := range b.members(t) {
		if strings.HasPrefix(m, "r0@") {
			cl.Do(ctx, cl.B().Hset().Key("hivenet:{"+b.prefix+"}:epochs").FieldValue().FieldValue(m, "1").Build())
		}
	}
	b1 := fill(rs[1], w, budget, 10)
	rs[0].proxy.setCut(false)
	waitFor(t, 3*time.Second, "r0 to notice the eviction", func() bool { return rs[0].store.Evictions() > 0 })
	a2 := fill(rs[0], w, budget, 10)
	if site := (len(b1) + len(a2)) * w; site > budget {
		t.Fatalf("site holds %d > budget %d (r0 %d from old credits)", site, budget, len(a2)*w)
	}
}

// Valkey loses its data while r0, cut off from it, still runs 900. A router
// that starts then never saw r0, and the store does not list it; but the
// agents report a connection to it, so no router takes new credits until
// r0 is back and has rewritten what it holds.
func TestFreshRouterAfterDataLossTakesNothing(t *testing.T) {
	b, ag := newBackend(t), newAgents()
	rs := newOccFleet(t, b, ag, 2)
	const budget, w = 1000, 100
	a := fill(rs[0], w, budget, 9)
	time.Sleep(3 * interval)
	rs[0].proxy.setCut(true)
	b.flush(t)
	ag.set("a0", "peer-2", true)
	c := startOccRouter(t, b, ag, "r2", "peer-2")
	waitFor(t, 3*time.Second, "r2 up", func() bool { return c.store.DegradedEpoch() == 0 })
	var got []*admission.Reservation
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		got = append(got, fill(c, w, budget, 10-len(got))...)
		got = append(got, fill(rs[1], w, budget, 10-len(got))...)
		if site := (len(a) + len(got)) * w; site > budget {
			t.Fatalf("site holds %d > budget %d", site, budget)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// r0 is back and rewrites what it holds: the others may use the rest.
	rs[0].proxy.setCut(false)
	waitFor(t, 5*time.Second, "admission to resume", func() bool {
		got = append(got, fill(c, w, budget, 1)...)
		return len(got) > 0
	})
	if site := (len(a) + len(got)) * w; site > budget {
		t.Fatalf("after r0 is back the site holds %d > budget %d", site, budget)
	}
}

// A router that just started has no agent reports yet: it never takes the
// credits of a silent router that may still be serving.
func TestNewRouterDoesNotReclaimLiveRouter(t *testing.T) {
	b, ag := newBackend(t), newAgents()
	rs := newOccFleet(t, b, ag, 1)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].proxy.setCut(true)
	ag.deafen("peer-9", "a0") // the new router hears nothing from r0's agent yet
	r9 := startOccRouter(t, b, ag, "r9", "peer-9")
	time.Sleep(2 * time.Second)
	if r9.occ.Admit(context.Background(), "m", 500, false, budget, 0) != nil {
		t.Fatal("a new router took the credits of a router its agents still reach")
	}
}

// A router evicted by the others drops the credits it believed it held and
// asks for them again, so the pool never counts them twice.
func TestEvictedRouterDropsItsCredits(t *testing.T) {
	b := newBackend(t)
	rs := newOccFleet(t, b, newAgents(), 2)
	const budget = 1000
	res := rs[0].occ.Admit(context.Background(), "m", 100, false, budget, 0)
	if res == nil {
		t.Fatal("first admission refused")
	}
	res.Release()
	for _, m := range b.members(t) {
		if strings.HasPrefix(m, "r0@") {
			b.zrem(t, m) // as a reclaim would
		}
	}
	waitFor(t, 3*time.Second, "r0 to notice the eviction", func() bool { return rs[0].store.Evictions() > 0 })
	waitFor(t, 3*time.Second, "r1 to get the whole budget", func() bool {
		r := rs[1].occ.Admit(context.Background(), "m", 950, false, budget, 0)
		if r != nil {
			r.Release()
		}
		return r != nil
	})
}

// A router shutting down hands its credits back at once.
func TestReleaseAllHandsCreditsBack(t *testing.T) {
	rs := newOccFleet(t, newBackend(t), newAgents(), 2)
	const budget = 1000
	if rs[0].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("first admission refused")
	}
	rs[0].cancel()
	rs[0].credits.ReleaseAll(context.Background())
	if rs[1].occ.Admit(context.Background(), "m", 900, false, budget, 0) == nil {
		t.Fatal("credits not back after ReleaseAll")
	}
}

// A redirect token is accepted once across the site, not once per router.
func TestFirstUseAcrossRouters(t *testing.T) {
	rs := newOccFleet(t, newBackend(t), newAgents(), 2)
	until := time.Now().Add(time.Minute)
	if ok, err := rs[0].store.FirstUse(context.Background(), "jti-1", until); err != nil || !ok {
		t.Fatalf("first use refused: %v %v", ok, err)
	}
	if ok, err := rs[1].store.FirstUse(context.Background(), "jti-1", until); err != nil || ok {
		t.Fatalf("replay on another router accepted: %v %v", ok, err)
	}
}

// A wallet no request uses any more hands its credits back and is dropped,
// here and in Valkey: per-key budgets (B4) seen once cost nothing after.
func TestIdleWalletDropped(t *testing.T) {
	b := newBackend(t)
	if b.mr == nil {
		t.Skip("reads the pool through miniredis")
	}
	rs := newOccFleet(t, b, newAgents(), 1)
	rs[0].credits.SetIdleAfter(300 * time.Millisecond)
	res := rs[0].occ.Admit(context.Background(), "k1\x00m", 100, false, 1000, 0)
	if res == nil {
		t.Fatal("admission refused")
	}
	res.Release()
	waitFor(t, 5*time.Second, "the idle wallet to be dropped", func() bool { return rs[0].credits.Wallets() == 0 })
	waitFor(t, time.Second, "the pool to be emptied", func() bool {
		for _, k := range b.mr.Keys() {
			if strings.Contains(k, ":pool:") {
				if fields, _ := b.mr.HKeys(k); len(fields) > 0 {
					return false
				}
			}
		}
		return true
	})
}

// After ReleaseAll (shutdown) the router admits nothing more: its credits are
// back in the pool, so a request it still took would hold none.
func TestNothingAdmittedAfterReleaseAll(t *testing.T) {
	rs := newOccFleet(t, newBackend(t), newAgents(), 1)
	rs[0].credits.ReleaseAll(context.Background())
	if rs[0].occ.Admit(context.Background(), "m", 10, false, 1000, 0) != nil {
		t.Fatal("admitted after ReleaseAll")
	}
}

// With Sentinel, a grant waits for a replica to acknowledge it. Without any
// replica the routers still admit, each within its share; with one, a router
// may use the whole budget. Runs only against a real Sentinel
// (HIVENET_TEST_SENTINEL_ADDR, master set HIVENET_TEST_SENTINEL_MASTER).
func TestCreditsWithSentinel(t *testing.T) {
	addr, master := os.Getenv("HIVENET_TEST_SENTINEL_ADDR"), os.Getenv("HIVENET_TEST_SENTINEL_MASTER")
	if addr == "" || master == "" {
		t.Skip("needs HIVENET_TEST_SENTINEL_ADDR and HIVENET_TEST_SENTINEL_MASTER")
	}
	prefix := fmt.Sprintf("s%d", time.Now().UnixNano())
	var rs []*occRouter
	for _, id := range []string{"r0", "r1"} {
		ctx, cancel := context.WithCancel(context.Background())
		st, err := shared.New(shared.Config{Addrs: []string{addr}, SentinelMaster: master, Prefix: prefix, RouterID: id, Interval: interval})
		if err != nil {
			t.Fatal(err)
		}
		st.SetPeerID("peer-" + id)
		cr := st.NewCredits("occ", nil)
		c := admission.NewController(1, 300*time.Millisecond)
		c.SetPool(cr)
		st.Start(ctx)
		cr.Start(ctx)
		t.Cleanup(func() { cancel(); st.Close() })
		rs = append(rs, &occRouter{store: st, credits: cr, occ: c})
	}
	waitFor(t, 5*time.Second, "routers to see each other", func() bool {
		return rs[0].store.Size() == 2 && rs[1].store.Size() == 2
	})
	const budget, w = 1000, 100
	a := fill(rs[0], w, budget, 10)
	b := fill(rs[1], w, budget, 10)
	if len(a) == 0 {
		t.Fatal("admission stalled")
	}
	if site := (len(a) + len(b)) * w; site > budget {
		t.Fatalf("site holds %d > budget %d", site, budget)
	}
	if share := budget / 2 / w; rs[0].store.Unreplicated() && (len(a) > share || len(b) > share) {
		t.Fatalf("unreplicated grants past the share: r0 %d, r1 %d", len(a), len(b))
	}
	t.Logf("r0 %d, r1 %d, unreplicated %v", len(a), len(b), rs[0].store.Unreplicated())
}
