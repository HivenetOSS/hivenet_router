// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for agent-level prefix affinity (step.agent_affinity):
// pin honoured within the load margin, margin re-pinning, ttl/max-age expiry,
// slot-race fall-through, transient misses that keep the pin, no mid-request
// re-pin after a forward failure, owner-cap eviction, hot reload scoping, key
// precedence (task/session header vs content fingerprint) and the guards that
// keep embeddings/count_tokens/no-flag traffic pin-free.
//
// Reuses the stubs from exhaustion_log_test.go (makeAgent, newExec, stub*).
package policy_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/pin"
	"hivenet_router/internal/policy"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func iptr(v int) *int { return &v }

// affinityPolicy builds a one-step least-loaded policy with an agent_affinity
// block (nil = feature off).
func affinityPolicy(a *policy.AgentAffinity) *policy.Policy {
	return &policy.Policy{RoutingPolicy: policy.PolicyStep{
		Match:         policy.MatchFilter{Engine: "vllm"},
		Strategy:      "least-loaded",
		MaxTries:      3,
		AgentAffinity: a,
	}}
}

// pool builds two healthy vLLM llm agents for model with capacity 10 each.
func pool(model string) (*stubAgentLister, *domain.Agent, *domain.Agent) {
	a1 := makeAgent(model+"-1", domain.AgentMetadata{Model: model, Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	a2 := makeAgent(model+"-2", domain.AgentMetadata{Model: model, Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	return &stubAgentLister{agents: map[string][]*domain.Agent{model: {a1, a2}}}, a1, a2
}

// hdrCtx keys affinity by an explicit header (task id / litellm session id).
func hdrCtx(keyID, task string) *policy.AffinityContext {
	return &policy.AffinityContext{KeyID: keyID, TaskHeader: task}
}

// fixedCtx keys affinity by a stable content fingerprint.
func fixedCtx(keyID, endUser, system, first string) *policy.AffinityContext {
	return &policy.AffinityContext{
		KeyID: keyID,
		Fingerprint: func() (string, string, string) {
			return endUser, system, first
		},
	}
}

func headerKey(model, keyID, task string) string {
	return pin.TaskKey(pin.TaskKeyInput{Alias: model, KeyID: keyID, HeaderValue: task})
}

func contentKey(model, keyID, endUser, system, first string) string {
	return pin.TaskKey(pin.TaskKeyInput{Alias: model, KeyID: keyID, EndUser: endUser, System: system, FirstUserText: first})
}

// selectOnce creates a session and selects, returning the agent and the
// affinity decision recorded on it.
func selectOnce(t *testing.T, exec *policy.Executor, model string, ctx *policy.AffinityContext) (*domain.Agent, policy.AffinityResult) {
	t.Helper()
	s := exec.NewSession(model, domain.CapabilityLLM, ctx)
	a, err := s.Select(context.Background())
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	return a, s.AffinityResult()
}

// pinRoute reads the agent a pin points at ("" , false when absent / no store).
func pinRoute(exec *policy.Executor, key string) (string, bool) {
	st := exec.AffinityPins()
	if st == nil {
		return "", false
	}
	_, route, ok := st.Get(key)
	return route, ok
}

// ── 1. pin honoured within the margin ─────────────────────────────────────────

// TestAffinity_HitWithinMargin: first turn pins the served agent; on the next
// turn the pinned agent is NOT the least-loaded one but is within load_margin,
// so it wins anyway (that is the whole point of the flag: APC over balance).
func TestAffinity_HitWithinMargin(t *testing.T) {
	const model = "aff-hit"
	lister, a1, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m", LoadMargin: f64(0.25)}))
	key := headerKey(model, "k1", "task-1")

	// Turn 1: no pin → miss, served by the strategy's best (a1, first in pool).
	got1, res1 := selectOnce(t, exec, model, hdrCtx("k1", "task-1"))
	if res1 != policy.AffinityMiss {
		t.Errorf("turn 1 result = %s, want miss", res1)
	}
	if got1.ID != a1.ID {
		t.Errorf("turn 1 served %s, want %s", got1.ID, a1.ID)
	}
	if route, ok := pinRoute(exec, key); !ok || route != a1.ID.String() {
		t.Errorf("turn 1 pin = %q ok=%v, want %q", route, ok, a1.ID)
	}

	// Turn 2: a1 now holds a slot (ratio 0.1), a2 is idle (ratio 0.0): the
	// strategy would pick a2, but a1 is within margin → hit.
	got2, res2 := selectOnce(t, exec, model, hdrCtx("k1", "task-1"))
	if res2 != policy.AffinityHit {
		t.Errorf("turn 2 result = %s, want hit", res2)
	}
	if got2.ID != a1.ID {
		t.Errorf("turn 2 served %s, want pinned %s", got2.ID, a1.ID)
	}
}

// ── 2. margin exceeded → serve best and re-pin ────────────────────────────────

// TestAffinity_MarginExceededRepins: when the pinned agent is more than
// load_margin above the best candidate it loses this turn — and the agent that
// actually serves becomes the new pin (verified with a third session).
func TestAffinity_MarginExceededRepins(t *testing.T) {
	const model = "aff-margin"
	lister, a1, a2 := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m", LoadMargin: f64(0.25)}))
	key := headerKey(model, "k1", "task-m")

	got1, res1 := selectOnce(t, exec, model, hdrCtx("k1", "task-m"))
	if res1 != policy.AffinityMiss || got1.ID != a1.ID {
		t.Fatalf("turn 1: got %s/%s, want miss/%s", res1, got1.ID, a1.ID)
	}
	// Load a1 to ratio 0.3 (0.1 from turn 1 + two slots) > 0 + 0.25.
	a1.TryAcquireSlot()
	a1.TryAcquireSlot()

	got2, res2 := selectOnce(t, exec, model, hdrCtx("k1", "task-m"))
	if res2 != policy.AffinityMargin {
		t.Errorf("turn 2 result = %s, want margin", res2)
	}
	if got2.ID != a2.ID {
		t.Errorf("turn 2 served %s, want best %s", got2.ID, a2.ID)
	}
	if route, ok := pinRoute(exec, key); !ok || route != a2.ID.String() {
		t.Errorf("pin after margin = %q ok=%v, want re-pinned %q", route, ok, a2.ID)
	}

	// Turn 3: the new pin (a2) is honoured.
	got3, res3 := selectOnce(t, exec, model, hdrCtx("k1", "task-m"))
	if res3 != policy.AffinityHit || got3.ID != a2.ID {
		t.Errorf("turn 3: got %s/%s, want hit/%s", res3, got3.ID, a2.ID)
	}
}

// ── 3. expiry: sliding ttl and absolute max age ───────────────────────────────

// fakeClock is a manual time source for PinStore.SetClock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestAffinity_TTLExpiry: a hit refreshes the sliding ttl; once the conversation
// goes idle past ttl the pin expires and the next turn is a fresh miss.
func TestAffinity_TTLExpiry(t *testing.T) {
	const model = "aff-ttl"
	lister, _, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "1m"}))

	if _, res := selectOnce(t, exec, model, hdrCtx("k", "ttl")); res != policy.AffinityMiss {
		t.Fatalf("turn 1 result = %s, want miss", res)
	}
	clk := &fakeClock{now: time.Now()}
	exec.AffinityPins().SetClock(clk.Now)

	clk.Advance(50 * time.Second) // within the 1m ttl → hit, ttl refreshed to +1m10s... (+50s+60s)
	if _, res := selectOnce(t, exec, model, hdrCtx("k", "ttl")); res != policy.AffinityHit {
		t.Errorf("turn 2 (at +50s) result = %s, want hit", res)
	}
	clk.Advance(61 * time.Second) // last hit was at +50s, so it expires at +110s
	if _, res := selectOnce(t, exec, model, hdrCtx("k", "ttl")); res != policy.AffinityMiss {
		t.Errorf("turn 3 (at +111s, past the refreshed ttl) result = %s, want miss", res)
	}
}

// TestAffinity_MaxAgeExpiry: hits keep the sliding ttl alive, but the absolute
// max age (restarted only on re-pin) still bounds the pin's life.
func TestAffinity_MaxAgeExpiry(t *testing.T) {
	const model = "aff-maxage"
	lister, _, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{
		TTL: "30m", MaxAge: "1h", LoadMargin: f64(1),
	}))

	if _, res := selectOnce(t, exec, model, hdrCtx("k", "age")); res != policy.AffinityMiss {
		t.Fatalf("turn 1 result = %s, want miss", res)
	}
	clk := &fakeClock{now: time.Now()}
	exec.AffinityPins().SetClock(clk.Now)

	clk.Advance(20 * time.Minute) // within ttl → hit
	if _, res := selectOnce(t, exec, model, hdrCtx("k", "age")); res != policy.AffinityHit {
		t.Errorf("turn 2 (at +20m) result = %s, want hit", res)
	}
	clk.Advance(20 * time.Minute) // at +40m: ttl refreshed twice, max age not reached
	if _, res := selectOnce(t, exec, model, hdrCtx("k", "age")); res != policy.AffinityHit {
		t.Errorf("turn 3 (at +40m) result = %s, want hit", res)
	}
	clk.Advance(25 * time.Minute) // at +65m > max age (1h, fixed at turn 1's Put)
	if _, res := selectOnce(t, exec, model, hdrCtx("k", "age")); res != policy.AffinityMiss {
		t.Errorf("turn 4 (at +65m, past max_age) result = %s, want miss (max age must not be extended by hits)", res)
	}
}

// ── 4. slot race: fall through without losing the pin ─────────────────────────

// TestAffinity_SlotRaceFallThrough hammers one shared pin from concurrent
// sessions against a tiny pool: winners must never over-acquire a slot (no
// double-acquire), losers must fail cleanly or be served, and the pin must
// survive the contention. The follow-up single-threaded turn proves the
// conversation is still pinned after the race window.
func TestAffinity_SlotRaceFallThrough(t *testing.T) {
	const model = "aff-race"
	a1 := makeAgent("race-1", domain.AgentMetadata{Model: model, Capacity: 1, Engine: "vllm", Capability: domain.CapabilityLLM})
	a2 := makeAgent("race-2", domain.AgentMetadata{Model: model, Capacity: 1, Engine: "vllm", Capability: domain.CapabilityLLM})
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{model: {a1, a2}}}
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "5m"}))
	key := headerKey(model, "k", "race")

	const workers, iterations = 6, 30
	var wg sync.WaitGroup
	var mu sync.Mutex
	var overAcquire, unexpectedErr int

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				s := exec.NewSession(model, domain.CapabilityLLM, hdrCtx("k", "race"))
				a, err := s.Select(context.Background())
				if err != nil {
					// All slots full → the documented ErrNoCapacity, nothing else.
					if !errors.Is(err, policy.ErrNoCapacity) {
						mu.Lock()
						unexpectedErr++
						mu.Unlock()
					}
					continue
				}
				// The slot this Select acquired must be within capacity even
				// while other goroutines are acquiring concurrently.
				if a.GetLoad() > a.Metadata.Capacity {
					mu.Lock()
					overAcquire++
					mu.Unlock()
				}
				a.DecrementLoad() // forward would release the slot when done
				exec.SignalCapacity(model)
			}
		}()
	}
	wg.Wait()

	if overAcquire != 0 {
		t.Errorf("%d selections observed load above capacity (double-acquire)", overAcquire)
	}
	if unexpectedErr != 0 {
		t.Errorf("%d selections failed with an unexpected error", unexpectedErr)
	}
	if route, ok := pinRoute(exec, key); !ok || route == "" {
		t.Fatalf("pin did not survive the slot races (ok=%v)", ok)
	}

	// Quiet turn after the contention: the pinned agent is healthy and idle, so
	// the pin must be honoured — the race window never invalidated it.
	if _, res := selectOnce(t, exec, model, hdrCtx("k", "race")); res != policy.AffinityHit {
		t.Errorf("post-race result = %s, want hit (pin must survive)", res)
	}
}

// ── 5. transient miss keeps the pin ───────────────────────────────────────────

// TestAffinity_TransientMissKeepsPin: when the pinned agent cannot serve
// (unhealthy, backend down, or at capacity) the request falls back AND the pin
// is kept — mirroring semantic's keepsEntry — so the conversation snaps back
// the moment the agent returns.
func TestAffinity_TransientMissKeepsPin(t *testing.T) {
	const model = "aff-transient"
	lister, a1, a2 := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))
	key := headerKey(model, "k", "tr")

	if _, res := selectOnce(t, exec, model, hdrCtx("k", "tr")); res != policy.AffinityMiss {
		t.Fatalf("turn 1 result = %s, want miss", res)
	}

	// (a) pinned agent marked unhealthy → fallback serves, pin kept.
	a1.SetHealthy(false)
	got, res := selectOnce(t, exec, model, hdrCtx("k", "tr"))
	if res != policy.AffinityIneligible {
		t.Errorf("unhealthy pinned agent: result = %s, want ineligible", res)
	}
	if got.ID != a2.ID {
		t.Errorf("unhealthy pinned agent: served %s, want fallback %s", got.ID, a2.ID)
	}
	if route, ok := pinRoute(exec, key); !ok || route != a1.ID.String() {
		t.Errorf("pin was not kept after unhealthy fallback: %q ok=%v", route, ok)
	}

	// (b) pinned agent at capacity (excluded by the capacity gate) → same.
	a1.SetHealthy(true)
	for i := 0; i < a1.Metadata.Capacity; i++ {
		a1.TryAcquireSlot()
	}
	got, res = selectOnce(t, exec, model, hdrCtx("k", "tr"))
	if res != policy.AffinityIneligible {
		t.Errorf("pinned agent at capacity: result = %s, want ineligible", res)
	}
	if got.ID != a2.ID {
		t.Errorf("pinned agent at capacity: served %s, want fallback %s", got.ID, a2.ID)
	}
	if route, ok := pinRoute(exec, key); !ok || route != a1.ID.String() {
		t.Errorf("pin was not kept after capacity fallback: %q ok=%v", route, ok)
	}
}

// ── 6. forward-failure retry: one decision, no mid-request re-pin ─────────────

// TestAffinity_ForwardFailureDoesNotRepin: the affinity attempt is one-shot per
// request. After a failed forward the retry serves another agent, but neither
// the recorded result nor the stored pin move — the pin is re-evaluated by the
// NEXT request instead.
func TestAffinity_ForwardFailureDoesNotRepin(t *testing.T) {
	const model = "aff-retry"
	lister, a1, a2 := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))
	key := headerKey(model, "k", "retry")

	s := exec.NewSession(model, domain.CapabilityLLM, hdrCtx("k", "retry"))
	first, err := s.Select(context.Background())
	if err != nil {
		t.Fatalf("turn 1 Select: %v", err)
	}
	if res := s.AffinityResult(); res != policy.AffinityMiss {
		t.Fatalf("turn 1 result = %s, want miss", res)
	}

	// Forward failure on the pinned agent → retry inside the same session.
	s.RecordFailure(first.ID)
	second, err := s.Select(context.Background())
	if err != nil {
		t.Fatalf("retry Select: %v", err)
	}
	if second.ID == first.ID {
		t.Fatalf("retry served the failed agent %s", second.ID)
	}
	if res := s.AffinityResult(); res != policy.AffinityMiss {
		t.Errorf("retry result = %s: affinity must not be re-decided mid-request", res)
	}
	// The pin still points at the first (pinned) agent — not the retry target.
	want := a1.ID.String()
	if first.ID == a2.ID { // pool order decides who turn 1 pins; record it
		want = a2.ID.String()
	}
	if route, ok := pinRoute(exec, key); !ok || route != want {
		t.Errorf("pin after retry = %q ok=%v, want %q (no mid-request re-pin)", route, ok, want)
	}
}

// ── 7. owner-cap eviction = one re-decision ───────────────────────────────────

// TestAffinity_OwnerCapEviction: one API key (owner) capped at two pins loses
// its least recently used pin when it opens a third conversation — the evicted
// conversation simply pays one extra decision next turn.
func TestAffinity_OwnerCapEviction(t *testing.T) {
	const model = "aff-owner"
	lister, _, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{
		TTL: "30m", MaxPins: iptr(4), MaxPinsPerKey: iptr(2),
	}))

	k1 := headerKey(model, "owner", "conv-1")
	k2 := headerKey(model, "owner", "conv-2")
	k3 := headerKey(model, "owner", "conv-3")

	for _, task := range []string{"conv-1", "conv-2", "conv-3"} {
		if _, res := selectOnce(t, exec, model, hdrCtx("owner", task)); res != policy.AffinityMiss {
			t.Fatalf("conversation %s: result = %s, want miss", task, res)
		}
	}
	if n := exec.AffinityPins().Len(); n != 2 {
		t.Errorf("store holds %d pins, want 2 (per-key cap)", n)
	}
	if _, ok := pinRoute(exec, k1); ok {
		t.Error("oldest pin (conv-1) should have been evicted by the owner cap")
	}
	if _, ok := pinRoute(exec, k2); !ok {
		t.Error("conv-2 pin should still be alive")
	}
	if _, ok := pinRoute(exec, k3); !ok {
		t.Error("conv-3 pin should still be alive")
	}

	// The evicted conversation re-decides exactly once and pins again (evicting
	// the next-oldest owner pin).
	if _, res := selectOnce(t, exec, model, hdrCtx("owner", "conv-1")); res != policy.AffinityMiss {
		t.Errorf("evicted conversation result = %s, want miss (one re-decision)", res)
	}
	if _, ok := pinRoute(exec, k1); !ok {
		t.Error("evicted conversation should be pinned again")
	}
	if _, ok := pinRoute(exec, k2); ok {
		t.Error("conv-2 should now be the LRU victim of the owner cap")
	}
}

// ── 8. hot reload: in-flight sessions keep their snapshot ─────────────────────

// TestAffinity_HotReloadNewSessionsOnly: a session created before SetPolicy
// still applies its agent_affinity step; sessions created after the reload of a
// policy without the flag do no affinity at all (COW: reload reaches new
// sessions only, and old pins simply go unused).
func TestAffinity_HotReloadNewSessionsOnly(t *testing.T) {
	const model = "aff-reload"
	lister, _, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))

	inflight := exec.NewSession(model, domain.CapabilityLLM, hdrCtx("k", "old")) // created pre-reload

	// Reload to a policy without agent_affinity (rollback = delete the block).
	exec.SetPolicy(&policy.Policy{RoutingPolicy: policy.PolicyStep{
		Match: policy.MatchFilter{Engine: "vllm"}, Strategy: "least-loaded", MaxTries: 3,
	}})

	// The in-flight session still pins (its step snapshot still carries the block).
	if _, err := inflight.Select(context.Background()); err != nil {
		t.Fatalf("in-flight Select: %v", err)
	}
	if res := inflight.AffinityResult(); res != policy.AffinityMiss {
		t.Errorf("in-flight result = %s, want miss (snapshot must keep affinity)", res)
	}
	nAfterOld := exec.AffinityPins().Len()
	if nAfterOld != 1 {
		t.Fatalf("expected 1 pin from the in-flight session, got %d", nAfterOld)
	}

	// A new session sees the reloaded policy: no affinity, no new pins.
	fresh := exec.NewSession(model, domain.CapabilityLLM, hdrCtx("k", "new"))
	if _, err := fresh.Select(context.Background()); err != nil {
		t.Fatalf("fresh Select: %v", err)
	}
	if res := fresh.AffinityResult(); res != policy.AffinityNone {
		t.Errorf("fresh session result = %s, want none after rollback", res)
	}
	if n := exec.AffinityPins().Len(); n != nAfterOld {
		t.Errorf("fresh session stored %d pins, want %d (no affinity after reload)", n-nAfterOld, 0)
	}
}

// ── 9. header key vs content-hash churn (pi stamp simulation) ────────────────

// TestAffinity_HeaderKeySurvivesFingerprintChurn: with a session header the
// fingerprint is never even computed, so a system prompt that changes every
// turn (pi's "Snapshot session_start at …" stamp) cannot break the pin.
func TestAffinity_HeaderKeySurvivesFingerprintChurn(t *testing.T) {
	const model = "aff-churn"
	lister, a1, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))

	turns := 0
	churnCtx := func(task string) *policy.AffinityContext {
		return &policy.AffinityContext{
			KeyID:      "k",
			TaskHeader: task,
			Fingerprint: func() (string, string, string) {
				turns++
				return "user", "Snapshot session_start at turn-1" + string(rune('0'+turns%10)), "hello"
			},
		}
	}

	// Every turn releases its slot when the (simulated) forward completes, so
	// load stays flat and only the key decides the outcome.
	if a, res := selectOnce(t, exec, model, churnCtx("sess-1")); res != policy.AffinityMiss {
		t.Fatalf("turn 1 result = %s, want miss", res)
	} else {
		a.DecrementLoad()
	}
	for turn := 2; turn <= 4; turn++ {
		got, res := selectOnce(t, exec, model, churnCtx("sess-1"))
		if res != policy.AffinityHit || got.ID != a1.ID {
			t.Errorf("turn %d: result=%s agent=%s, want hit/%s despite system churn", turn, res, got.ID, a1.ID)
		}
		got.DecrementLoad()
	}
	if turns != 0 {
		t.Errorf("fingerprint was computed %d times although a header was present", turns)
	}
}

// TestAffinity_ContentKeyChurnesEachTurn: without any header the key comes from
// the fingerprint — stable content pins, churning content (the pi stamp) re-decides
// every turn with a different key, which is the documented pi caveat.
func TestAffinity_ContentKeyChurnesEachTurn(t *testing.T) {
	const model = "aff-content"
	lister, _, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))

	// Stable fingerprint → hit on turn 2 (the content path works when stable).
	if _, res := selectOnce(t, exec, model, fixedCtx("k", "u", "sys v1", "hello")); res != policy.AffinityMiss {
		t.Fatalf("stable turn 1 result = %s, want miss", res)
	}
	if _, res := selectOnce(t, exec, model, fixedCtx("k", "u", "sys v1", "hello")); res != policy.AffinityHit {
		t.Errorf("stable turn 2 result = %s, want hit", res)
	}

	// Churning system prompt → a fresh key each turn → miss every time.
	if _, res := selectOnce(t, exec, model, fixedCtx("k", "u", "Snapshot session_start at 10:00:01", "hello")); res != policy.AffinityMiss {
		t.Errorf("churned turn 1 result = %s, want miss", res)
	}
	if _, res := selectOnce(t, exec, model, fixedCtx("k", "u", "Snapshot session_start at 10:00:02", "hello")); res != policy.AffinityMiss {
		t.Errorf("churned turn 2 result = %s, want miss (new key every turn)", res)
	}
	kA := contentKey(model, "k", "u", "Snapshot session_start at 10:00:01", "hello")
	kB := contentKey(model, "k", "u", "Snapshot session_start at 10:00:02", "hello")
	if kA == kB {
		t.Errorf("churned fingerprints produced the same key %q", kA)
	}
}

// ── 10. header precedence (X-Hivenet-Task-ID > x-litellm-session-id) ──────────

// TestAffinityContext_HeaderPrecedence locks the D4 key order on the plain path:
// explicit task id wins, an over-long task id is IGNORED (falls through to the
// litellm session header), and the body fingerprint is only a last resort.
func TestAffinityContext_HeaderPrecedence(t *testing.T) {
	pending := func(headers http.Header) *domain.PendingRequest {
		req := &domain.ChatRequest{
			Model:       "m",
			HttpHeaders: headers,
			RawBytes:    []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"user":"u"}`),
		}
		p := domain.NewPendingRequest("req-1", req, time.Minute)
		p.Capability = domain.CapabilityLLM
		p.Path = "/v1/chat/completions"
		return p
	}
	h := func(kvs ...string) http.Header {
		hd := make(http.Header)
		for i := 0; i+1 < len(kvs); i += 2 {
			hd.Set(kvs[i], kvs[i+1])
		}
		return hd
	}

	cases := []struct {
		name    string
		headers http.Header
		want    string
	}{
		{
			"task id beats session id",
			h("X-Hivenet-Task-ID", "task-42", "x-litellm-session-id", "sess-99"),
			"task-42",
		},
		{
			"session id when no task id",
			h("x-litellm-session-id", "sess-99"),
			"sess-99",
		},
		{
			"task id at the 128-byte limit is kept",
			h("X-Hivenet-Task-ID", strings.Repeat("t", 128)),
			strings.Repeat("t", 128),
		},
		{
			"over-long task id ignored, session id used",
			h("X-Hivenet-Task-ID", strings.Repeat("t", 129), "x-litellm-session-id", "sess-99"),
			"sess-99",
		},
		{
			"over-long task id and no session id → fingerprint path",
			h("X-Hivenet-Task-ID", strings.Repeat("t", 129)),
			"",
		},
		{"no headers → fingerprint path", h(), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := policy.NewAffinityContext(pending(tc.headers))
			if ctx == nil {
				t.Fatal("NewAffinityContext returned nil for an LLM chat request")
			}
			if ctx.TaskHeader != tc.want {
				t.Errorf("TaskHeader = %q, want %q", ctx.TaskHeader, tc.want)
			}
			if ctx.KeyID != "" {
				t.Errorf("KeyID = %q, want empty (never set by the test pending)", ctx.KeyID)
			}
			if ctx.Fingerprint == nil {
				t.Fatal("Fingerprint must always be wired (lazy body parse)")
			}
			eu, sys, first := ctx.Fingerprint()
			if eu != "u" || sys != "" || first != "hi" {
				t.Errorf("Fingerprint() = (%q,%q,%q), want (u,\"\",hi)", eu, sys, first)
			}
		})
	}
}

// ── 11. alias/model scoping ───────────────────────────────────────────────────

// TestAffinity_ModelScopedPins: on the alias path the session carries the
// RESOLVED model, so one conversation is pinned per resolved model + key: the
// same header on another model (or another key) starts its own pin, and pins
// never leak across models.
func TestAffinity_ModelScopedPins(t *testing.T) {
	const headerVal = "task-shared"
	fast := makeAgent("fast-1", domain.AgentMetadata{Model: "qwen-fast", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	slow := makeAgent("slow-1", domain.AgentMetadata{Model: "qwen-large", Capacity: 10, Engine: "vllm", Capability: domain.CapabilityLLM})
	lister := &stubAgentLister{agents: map[string][]*domain.Agent{
		"qwen-fast":  {fast},
		"qwen-large": {slow},
	}}
	exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))

	if _, res := selectOnce(t, exec, "qwen-fast", hdrCtx("k1", headerVal)); res != policy.AffinityMiss {
		t.Errorf("qwen-fast turn 1 = %s, want miss", res)
	}
	if _, res := selectOnce(t, exec, "qwen-large", hdrCtx("k1", headerVal)); res != policy.AffinityMiss {
		t.Errorf("qwen-large turn 1 = %s, want miss (different model ⇒ different pin)", res)
	}
	if _, res := selectOnce(t, exec, "qwen-fast", hdrCtx("k2", headerVal)); res != policy.AffinityMiss {
		t.Errorf("other key = %s, want miss (different key ⇒ different pin)", res)
	}

	// Both model-scoped pins are alive, on their own agent.
	if route, ok := pinRoute(exec, headerKey("qwen-fast", "k1", headerVal)); !ok || route != fast.ID.String() {
		t.Errorf("qwen-fast pin = %q ok=%v, want %q", route, ok, fast.ID)
	}
	if route, ok := pinRoute(exec, headerKey("qwen-large", "k1", headerVal)); !ok || route != slow.ID.String() {
		t.Errorf("qwen-large pin = %q ok=%v, want %q", route, ok, slow.ID)
	}
	// Same conversation, same model → honoured.
	got, res := selectOnce(t, exec, "qwen-fast", hdrCtx("k1", headerVal))
	if res != policy.AffinityHit || got.ID != fast.ID {
		t.Errorf("qwen-fast turn 2 = %s/%s, want hit/%s", res, got.ID, fast.ID)
	}
}

// ── 12. guards: what must NOT be pinned ───────────────────────────────────────

// TestAffinityContext_Guards: non-LLM capabilities and count_tokens get no
// affinity context at all, so their traffic never touches the pin store.
func TestAffinityContext_Guards(t *testing.T) {
	mk := func(capability, path string) *domain.PendingRequest {
		req := &domain.ChatRequest{Model: "m", RawBytes: []byte(`{"model":"m"}`)}
		p := domain.NewPendingRequest("req-g", req, time.Minute)
		p.Capability = capability
		p.Path = path
		return p
	}
	cases := []struct {
		name       string
		capability string
		path       string
		wantNil    bool
	}{
		{"embeddings never pin", domain.CapabilityEmbedding, "/v1/embeddings", true},
		{"rerank never pin", domain.CapabilityReranker, "/v1/rerank", true},
		{"count_tokens is stateless", domain.CapabilityLLM, "/v1/messages/count_tokens", true},
		{"chat completions pin", domain.CapabilityLLM, "/v1/chat/completions", false},
		{"anthropic messages pin", "", "/v1/messages", false}, // "" defaults to llm
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := policy.NewAffinityContext(mk(tc.capability, tc.path))
			if (ctx == nil) != tc.wantNil {
				t.Fatalf("NewAffinityContext(%q, %q) nil=%v, want %v", tc.capability, tc.path, ctx == nil, tc.wantNil)
			}
		})
	}
	if ctx := policy.NewAffinityContext(&domain.PendingRequest{}); ctx != nil {
		t.Error("a pending request without a parsed body must yield nil")
	}
}

// TestAffinity_NoBlockNoStore: a policy without agent_affinity never creates the
// pin store, even for requests that carry a full affinity context — zero cost
// for everyone not opted in.
func TestAffinity_NoBlockNoStore(t *testing.T) {
	const model = "aff-off"
	lister, _, _ := pool(model)
	exec := newExec(lister, &stubStorage{}, affinityPolicy(nil)) // feature off

	for i := 0; i < 3; i++ {
		s := exec.NewSession(model, domain.CapabilityLLM, hdrCtx("k", "task"))
		if _, err := s.Select(context.Background()); err != nil {
			t.Fatalf("Select: %v", err)
		}
		if res := s.AffinityResult(); res != policy.AffinityNone {
			t.Fatalf("result = %s, want none", res)
		}
	}
	if exec.AffinityPins() != nil {
		t.Error("pin store was created although no step declares agent_affinity")
	}
}

// TestAffinityResult_String pins the wire/metric spellings used by the response
// header and hivenet_router_affinity_total.
func TestAffinityResult_String(t *testing.T) {
	cases := map[policy.AffinityResult]string{
		policy.AffinityNone:       "none",
		policy.AffinityHit:        "hit",
		policy.AffinityMiss:       "miss",
		policy.AffinityMargin:     "margin",
		policy.AffinityIneligible: "ineligible",
	}
	for res, want := range cases {
		if got := res.String(); got != want {
			t.Errorf("AffinityResult(%d).String() = %q, want %q", res, got, want)
		}
	}
}

// ── 13. loaded policies get their documented defaults ─────────────────────────

// TestAffinity_LoaderDefaults loads a real agent_affinity block from YAML and
// proves the unset fields take their documented defaults behaviourally:
// ttl = 30m (alive at exactly 30m, dead just past it) and load_margin = 0.25
// (a pinned agent at ratio 0.2 wins, at ratio 0.3 it does not).
func TestAffinity_LoaderDefaults(t *testing.T) {
	const yaml = `
routing_policy:
  strategy: least-loaded
  match:
    engine: vllm
  agent_affinity:
    max_pins: 4000
`
	pol, err := policy.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if pol.RoutingPolicy.AgentAffinity == nil {
		t.Fatal("agent_affinity block was dropped by the loader")
	}

	t.Run("ttl defaults to 30m", func(t *testing.T) {
		lister, _, _ := pool("aff-default-ttl")
		exec := newExec(lister, &stubStorage{}, pol)
		// Capture the fake clock base BEFORE the first Put so the pin's expiry
		// (real Put time + 30m) is never earlier than base + 30m: the boundary
		// check below then measures the ttl, not the test's own latency.
		clk := &fakeClock{now: time.Now()}
		if _, res := selectOnce(t, exec, "aff-default-ttl", hdrCtx("k", "a")); res != policy.AffinityMiss {
			t.Fatalf("turn 1 result = %s, want miss", res)
		}
		exec.AffinityPins().SetClock(clk.Now)

		clk.Advance(30 * time.Minute) // alive at exactly the default ttl
		if _, res := selectOnce(t, exec, "aff-default-ttl", hdrCtx("k", "a")); res != policy.AffinityHit {
			t.Errorf("at +30m: result = %s, want hit (default ttl >= 30m)", res)
		}
		// A pin created now dies just past 30m even though it is never touched.
		if _, res := selectOnce(t, exec, "aff-default-ttl", hdrCtx("k", "b")); res != policy.AffinityMiss {
			t.Errorf("fresh conversation result = %s, want miss", res)
		}
		clk.Advance(30*time.Minute + time.Second)
		if _, res := selectOnce(t, exec, "aff-default-ttl", hdrCtx("k", "b")); res != policy.AffinityMiss {
			t.Errorf("at +30m01s after pinning: result = %s, want miss (default ttl <= 30m)", res)
		}
	})

	t.Run("load_margin defaults to 0.25", func(t *testing.T) {
		// 0.2 <= 0 + default margin → the pin still wins.
		lister, a1, _ := pool("aff-default-margin")
		exec := newExec(lister, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))
		if a, res := selectOnce(t, exec, "aff-default-margin", hdrCtx("k", "m1")); res != policy.AffinityMiss {
			t.Fatalf("turn 1 result = %s, want miss", res)
		} else if a.ID != a1.ID {
			t.Fatalf("turn 1 served %s, want %s", a.ID, a1.ID)
		}
		a1.TryAcquireSlot() // ratio 0.2 vs best 0.0
		if a, res := selectOnce(t, exec, "aff-default-margin", hdrCtx("k", "m1")); res != policy.AffinityHit || a.ID != a1.ID {
			t.Errorf("pinned at 0.2: result = %s, want hit (default margin >= 0.2)", res)
		}

		// 0.3 > 0 + default margin → margin exceeded on a fresh pin.
		lister2, b1, _ := pool("aff-default-margin-2")
		exec2 := newExec(lister2, &stubStorage{}, affinityPolicy(&policy.AgentAffinity{TTL: "30m"}))
		if a, res := selectOnce(t, exec2, "aff-default-margin-2", hdrCtx("k", "m2")); res != policy.AffinityMiss {
			t.Fatalf("turn 1 result = %s, want miss", res)
		} else if a.ID != b1.ID {
			t.Fatalf("turn 1 served %s, want %s", a.ID, b1.ID)
		}
		b1.TryAcquireSlot()
		b1.TryAcquireSlot() // ratio 0.3 vs best 0.0
		if _, res := selectOnce(t, exec2, "aff-default-margin-2", hdrCtx("k", "m2")); res != policy.AffinityMargin {
			t.Errorf("pinned at 0.3: result = %s, want margin (default margin <= 0.3)", res)
		}
	})
}
