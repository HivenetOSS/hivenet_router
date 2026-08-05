// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package metrics_test — RFC 6298 SRTT/RTTVAR estimation, diskDB warm-start,
// counter accumulation over baselines, and periodic flush.
package metrics_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/test/testutil"

	"github.com/libp2p/go-libp2p/core/peer"
)

// approx compares floats with a tolerance so exact RFC 6298 arithmetic
// (which involves 1/2, 1/4, 1/8 weightings) is not tripped up by FP rounding.
func approx(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func testAgent(id peer.ID) *domain.Agent {
	return domain.NewAgent(id, domain.AgentMetadata{Model: "m", Engine: "vllm", Capacity: 10}, "")
}

// warmStorage returns a fixed history on GetUniversalHistory (warm-start seed).
type warmStorage struct {
	testutil.NoopStorage
	hist *domain.AgentUniversalHistory
}

func (w warmStorage) GetUniversalHistory(peer.ID) (*domain.AgentUniversalHistory, error) {
	return w.hist, nil
}

// captureStorage records the last flushed history per peer.
type captureStorage struct {
	testutil.NoopStorage
	mu      sync.Mutex
	flushed map[peer.ID]*domain.AgentUniversalHistory
}

func (c *captureStorage) SetUniversalHistory(id peer.ID, h *domain.AgentUniversalHistory) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flushed == nil {
		c.flushed = map[peer.ID]*domain.AgentUniversalHistory{}
	}
	c.flushed[id] = h
	return nil
}

// TestRFC6298_FirstMeasurementAndAsymmetricEWMA locks in the SRTT/RTTVAR update:
// RFC 6298 §2.2 init on the first sample, then asymmetric α (0.5 down / 0.125 up).
func TestRFC6298_FirstMeasurementAndAsymmetricEWMA(t *testing.T) {
	store := metrics.NewUniversalCounterStore(testutil.NoopStorage{}, metrics.NewRouterMetrics())
	id := peer.ID("rtt-agent")
	store.Bootstrap(id, "m", "vllm", "org", "mach")
	agent := testAgent(id)

	// RecordSuccess(agent, inputTokens, outputTokens, rttMs). R=100ms is the
	// first RTT sample, so RFC 6298 §2.2 initialises srtt=R, rttvar=R/2 (not EWMA).
	// First measurement: srtt = R, rttvar = R/2.
	store.RecordSuccess(agent, 1, 1, 100)
	st, _ := store.GetAgentStats(id)
	if !approx(st.SRTTMs, 100) || !approx(st.RTTVARMs, 50) {
		t.Fatalf("after first sample: srtt=%v rttvar=%v, want 100/50", st.SRTTMs, st.RTTVARMs)
	}

	// Better sample (60 < 100): downward, so this impl uses α=0.5 for fast
	// convergence (β=0.25 always). error |SRTT-R'|=|100-60|=40.
	// rttvar = (1-β)*rttvar + β*|err| = 0.75*50 + 0.25*40 = 47.5.
	// srtt   = (1-α)*srtt + α*R'    = 0.5*100 + 0.5*60   = 80.
	// Better sample (60 < 100): α=0.5. rttvar=0.75*50+0.25*40=47.5; srtt=0.5*100+0.5*60=80.
	store.RecordSuccess(agent, 1, 1, 60)
	st, _ = store.GetAgentStats(id)
	if !approx(st.SRTTMs, 80) || !approx(st.RTTVARMs, 47.5) {
		t.Fatalf("after better sample: srtt=%v rttvar=%v, want 80/47.5", st.SRTTMs, st.RTTVARMs)
	}

	// Worse sample (120 > 80): upward, so α=0.125 (RFC 6298 default) to resist
	// latency spikes. error |SRTT-R'|=|80-120|=40.
	// rttvar = 0.75*47.5 + 0.25*40  = 45.625.
	// srtt   = 0.875*80  + 0.125*120 = 85.
	// Worse sample (120 > 80): α=0.125. rttvar=0.75*47.5+0.25*40=45.625; srtt=0.875*80+0.125*120=85.
	store.RecordSuccess(agent, 1, 1, 120)
	st, _ = store.GetAgentStats(id)
	if !approx(st.SRTTMs, 85) || !approx(st.RTTVARMs, 45.625) {
		t.Fatalf("after worse sample: srtt=%v rttvar=%v, want 85/45.625", st.SRTTMs, st.RTTVARMs)
	}
}

// TestWarmStartFromDisk verifies Bootstrap seeds SRTT and counter baselines from
// persisted history, and that the first post-restart sample converges from the
// warm value rather than re-initialising.
func TestWarmStartFromDisk(t *testing.T) {
	stor := warmStorage{hist: &domain.AgentUniversalHistory{
		SuccessfulRequests: 1000, FailedRequests: 100, SRTT: 42, RTTVAR: 5,
	}}
	store := metrics.NewUniversalCounterStore(stor, metrics.NewRouterMetrics())
	id := peer.ID("warm-agent")
	store.Bootstrap(id, "m", "vllm", "org", "mach")

	st, ok := store.GetAgentStats(id)
	if !ok {
		t.Fatal("no stats after bootstrap")
	}
	if !st.SRTTInited || !approx(st.SRTTMs, 42) {
		t.Errorf("warm-start SRTT not restored: inited=%v srtt=%v", st.SRTTInited, st.SRTTMs)
	}
	if st.SuccessfulRequests != 1000 || st.FailedRequests != 100 {
		t.Errorf("baselines not restored: %d/%d", st.SuccessfulRequests, st.FailedRequests)
	}
	// Better sample 10 < 42 → α=0.5 → srtt = 0.5*42 + 0.5*10 = 26 (NOT re-init to 10).
	store.RecordSuccess(testAgent(id), 1, 1, 10)
	st, _ = store.GetAgentStats(id)
	if !approx(st.SRTTMs, 26) {
		t.Errorf("post-warm sample srtt=%v, want 26 (converge, not re-init)", st.SRTTMs)
	}
	// Session success is added on top of the 1000 baseline.
	if st.SuccessfulRequests != 1001 {
		t.Errorf("SuccessfulRequests=%d, want 1001 (baseline+session)", st.SuccessfulRequests)
	}
}

// TestSuccessRateAndFailure verifies counter accumulation: success/failure counts,
// derived success rate, and token totals summed across requests.
func TestSuccessRateAndFailure(t *testing.T) {
	store := metrics.NewUniversalCounterStore(testutil.NoopStorage{}, metrics.NewRouterMetrics())
	id := peer.ID("rate-agent")
	store.Bootstrap(id, "m", "vllm", "o", "x")
	agent := testAgent(id)

	// 3 successes of (5 in, 7 out) each → 15 input / 21 output tokens total.
	store.RecordSuccess(agent, 5, 7, 20)
	store.RecordSuccess(agent, 5, 7, 20)
	store.RecordSuccess(agent, 5, 7, 20)
	store.RecordFailure(agent, 0) // rttMs=0 → RTT not updated

	st, _ := store.GetAgentStats(id)
	if st.SuccessfulRequests != 3 || st.FailedRequests != 1 {
		t.Fatalf("counts = %d/%d, want 3/1", st.SuccessfulRequests, st.FailedRequests)
	}
	// 3 successes out of 4 total requests → 0.75.
	if !approx(st.SuccessRate, 0.75) {
		t.Errorf("SuccessRate = %v, want 0.75", st.SuccessRate)
	}
	if st.InputTokens != 15 || st.OutputTokens != 21 {
		t.Errorf("tokens = %d/%d, want 15/21", st.InputTokens, st.OutputTokens)
	}
}

// TestFlushAllPersists verifies periodic flush writes accumulated history to disk.
func TestFlushAllPersists(t *testing.T) {
	stor := &captureStorage{}
	store := metrics.NewUniversalCounterStore(stor, metrics.NewRouterMetrics())
	id := peer.ID("flush-agent")
	store.Bootstrap(id, "m", "vllm", "o", "x")
	agent := testAgent(id)
	store.RecordSuccess(agent, 1, 2, 30)
	store.RecordSuccess(agent, 1, 2, 30)

	store.FlushAll()

	stor.mu.Lock()
	h := stor.flushed[id]
	stor.mu.Unlock()
	if h == nil {
		t.Fatal("FlushAll did not persist history")
	}
	if h.SuccessfulRequests != 2 || h.InputTokens != 2 || h.OutputTokens != 4 {
		t.Errorf("flushed history = %+v, want success=2 in=2 out=4", h)
	}
	if !approx(h.SRTT, 30) {
		t.Errorf("flushed SRTT = %v, want 30", h.SRTT)
	}
}

// TestLiveSnapshot_NoHistory returns ok=false when the agent has no request
// history yet (so exclude_if success-rate gates are skipped for it).
func TestLiveSnapshot_NoHistory(t *testing.T) {
	store := metrics.NewUniversalCounterStore(testutil.NoopStorage{}, metrics.NewRouterMetrics())
	id := peer.ID("fresh-agent")
	store.Bootstrap(id, "m", "vllm", "o", "x")
	_, _, _, ok := store.LiveSnapshot(id)
	if ok {
		t.Error("LiveSnapshot ok must be false with no request history")
	}
	// Unknown agent → ok=false too.
	if _, _, _, ok := store.LiveSnapshot(peer.ID("nope")); ok {
		t.Error("LiveSnapshot ok must be false for unknown agent")
	}
}

// TestStartPeriodicFlush exercises the flush goroutine lifecycle.
func TestStartPeriodicFlush(t *testing.T) {
	stor := &captureStorage{}
	store := metrics.NewUniversalCounterStore(stor, metrics.NewRouterMetrics())
	id := peer.ID("periodic")
	store.Bootstrap(id, "m", "vllm", "o", "x")
	store.RecordSuccess(testAgent(id), 1, 1, 10)

	ctx, cancel := context.WithCancel(context.Background())
	store.StartPeriodicFlush(ctx, 5*time.Millisecond)
	// Give the ticker a couple of cycles.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stor.mu.Lock()
		n := len(stor.flushed)
		stor.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	store.WaitFlush() // must return promptly after cancel

	stor.mu.Lock()
	defer stor.mu.Unlock()
	if stor.flushed[id] == nil {
		t.Error("periodic flush did not persist history")
	}
}
