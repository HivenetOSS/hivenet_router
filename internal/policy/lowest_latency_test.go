// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import "testing"

func f(v float64) *float64 { return &v }

// The candidate expected to answer first wins: here the one with fewer
// requests running, though both have the same load as the router counts it.
func TestLowestLatency_PicksTheFastestEstimate(t *testing.T) {
	a, b := llm("a"), llm("b")
	got := lowestLatencyStrategy{}.Select([]ScoredCandidate{
		{Agent: a, Snapshot: AgentSnapshot{RunningRequests: f(40), AvgITLSeconds: f(0.02)}},
		{Agent: b, Snapshot: AgentSnapshot{RunningRequests: f(2), AvgITLSeconds: f(0.02)}},
	}, SelectRequest{Model: "m"})
	if got != b {
		t.Fatalf("chose %s, want b (fewer running requests)", got.ID)
	}
}

// Each term counts: traffic around the routers, waiting requests and
// prefill queued on the agent.
func TestLowestLatency_Terms(t *testing.T) {
	cases := []struct {
		name       string
		slow, fast AgentSnapshot
	}{
		{"traffic around the routers", AgentSnapshot{RunningRequests: f(30)}, AgentSnapshot{RunningRequests: f(1)}},
		{"requests waiting", AgentSnapshot{RunningRequests: f(4), WaitingRequests: f(6)}, AgentSnapshot{RunningRequests: f(4)}},
		{"prefill queued", AgentSnapshot{}, AgentSnapshot{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slow, fast := llm("slow"), llm("fast")
			if tc.name == "prefill queued" {
				slow.SetReportedLoad(0)
				slow.SetReportedPrefill(5000)
				fast.SetReportedLoad(0)
			}
			got := lowestLatencyStrategy{}.Select([]ScoredCandidate{{Agent: slow, Snapshot: tc.slow}, {Agent: fast, Snapshot: tc.fast}}, SelectRequest{})
			if got != fast {
				t.Errorf("chose %s, want fast", got.ID)
			}
		})
	}
}

// Without metrics, or with signals that only change when a candidate is
// used, it is least-loaded.
func TestLowestLatency_TiesGoToLeastLoaded(t *testing.T) {
	busy, idle := llm("busy"), llm("idle")
	busy.TryAcquireSlot()
	busy.TryAcquireSlot()
	for name, snaps := range map[string][2]AgentSnapshot{
		"no metrics": {},
		// Lifetime averages say nothing about now: they are ignored.
		"old averages and SRTT": {{AvgTTFTSeconds: f(0.9), AvgITLSeconds: f(0.05), SRTT: f(900)}, {AvgTTFTSeconds: f(0.02), AvgITLSeconds: f(0.005), SRTT: f(20)}},
	} {
		got := lowestLatencyStrategy{}.Select([]ScoredCandidate{{Agent: busy, Snapshot: snaps[0]}, {Agent: idle, Snapshot: snaps[1]}}, SelectRequest{})
		if got != idle {
			t.Errorf("%s: chose %s, want idle (least-loaded among ties)", name, got.ID)
		}
	}
}

// lowest-latency is a valid policy strategy and a valid key strategy.
func TestLowestLatency_Known(t *testing.T) {
	if err := Validate(&Policy{RoutingPolicy: PolicyStep{Strategy: "lowest-latency"}}); err != nil {
		t.Errorf("policy: %v", err)
	}
	if err := ValidateKeyStrategies([]string{"lowest-latency", "least-loaded"}); err != nil {
		t.Errorf("key: %v", err)
	}
	if err := Validate(&Policy{RoutingPolicy: PolicyStep{Strategy: "lowest-latency", PrefixAware: &PrefixAwareParams{}}}); err == nil {
		t.Error("prefix_aware on a lowest-latency step must be rejected")
	}
}

// The engine reports every few seconds; the requests in flight the routers
// count move at once, so a burst spreads instead of piling onto the
// candidate whose last report looked best.
func TestLowestLatency_BurstSpreads(t *testing.T) {
	a, b := llm("a"), llm("b")
	for range 3 {
		a.TryAcquireSlot()
	}
	snap := AgentSnapshot{RunningRequests: f(0)}
	if got := (lowestLatencyStrategy{}).Select([]ScoredCandidate{{Agent: a, Snapshot: snap}, {Agent: b, Snapshot: snap}}, SelectRequest{}); got != b {
		t.Fatalf("chose %s, want b (a has 3 requests in flight its engine has not reported yet)", got.ID)
	}
}

// Requests the routers sent and the engine already counts are not counted
// twice: an engine running exactly what the agent reports adds nothing.
func TestLowestLatency_RoutedLoadCountedOnce(t *testing.T) {
	a, b := llm("a"), llm("b")
	a.SetReportedLoad(6) // the agent serves 6 router requests, its engine runs 6
	b.SetReportedLoad(0)
	b.TryAcquireSlot()
	ca := ScoredCandidate{Agent: a, Snapshot: AgentSnapshot{RunningRequests: f(6), AvgITLSeconds: f(0.01)}}
	cb := ScoredCandidate{Agent: b, Snapshot: AgentSnapshot{RunningRequests: f(6), AvgITLSeconds: f(0.01)}}
	if got, want := latencyCost(ca), 0.12; got < want-1e-9 || got > want+1e-9 {
		t.Errorf("a: cost %g, want %g (6 in flight, no excess)", got, want)
	}
	// b's engine runs 6 requests its agent did not get from a router.
	if got, want := latencyCost(cb), 0.14; got < want-1e-9 || got > want+1e-9 {
		t.Errorf("b: cost %g, want %g (1 in flight + 6 around the routers)", got, want)
	}
}
