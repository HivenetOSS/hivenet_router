// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"math"

	"hivenet_router/internal/domain"
)

// lowestLatencyStrategy sends a request to the candidate expected to give its
// first token soonest, from what the candidate is doing now:
//
//	prompt tokens queued on it (the agent's report) at the prefill rate
//	+ busy requests × 20 ms (one decode step each), where busy is the requests in
//	  flight on it as the routers count them (it moves with each request)
//	  plus the requests its engine runs or queues that did not come through
//	  the routers (engine running + waiting − the agent's own in-flight
//	  count, both from its last report)
//
// It is the prefix-aware cost without the cache term: it does not look at
// what candidates hold, so it needs no prompt anchors. Signals that only
// change when a candidate is used are left out, or a candidate that was slow
// once would be avoided long after it recovered: the engine's average time
// to first token and inter-token latency (averages since the engine
// started) and SRTT (measured on the requests a router sent it). A metric a
// candidate does not report adds nothing. Candidates
// within 5% (or 5 ms) of the best are a tie, broken by least-loaded, so with
// no metrics at all it behaves like least-loaded.
type lowestLatencyStrategy struct{}

// latencyTieEpsilon and latencyTieFloor make near-equal estimates a tie.
const (
	latencyTieEpsilon = 0.05
	latencyTieFloor   = 0.005 // seconds
)

func (lowestLatencyStrategy) Select(candidates []ScoredCandidate, req SelectRequest) *domain.Agent {
	if len(candidates) == 0 {
		panic("policy: Select called with empty candidates slice — this is a bug in the executor")
	}
	costs := make([]float64, len(candidates))
	best := math.Inf(1)
	for i, c := range candidates {
		costs[i] = latencyCost(c)
		best = min(best, costs[i])
	}
	eps := max(latencyTieEpsilon*best, latencyTieFloor)
	ties := make([]ScoredCandidate, 0, len(candidates))
	for i, c := range candidates {
		if costs[i] <= best+eps {
			ties = append(ties, c)
		}
	}
	return leastLoadedStrategy{}.Select(ties, req)
}

// latencyCost estimates the seconds before a new request on c gets its first
// token (see lowestLatencyStrategy).
func latencyCost(c ScoredCandidate) float64 {
	s := c.Snapshot
	busy := float64(c.Agent.InFlight())
	if s.RunningRequests != nil || s.WaitingRequests != nil {
		engine := 0.0
		if s.RunningRequests != nil {
			engine = *s.RunningRequests
		}
		if s.WaitingRequests != nil {
			engine += *s.WaitingRequests
		}
		// Requests the engine serves that no router sent: traffic around the
		// routers, or a shared engine. The engine's counts are seconds old,
		// so only the excess over the agent's own count is added to the
		// routers' live count.
		routed, _ := c.Agent.ReportedInFlight()
		busy += max(0, engine-float64(routed))
	}
	return float64(c.Agent.ReportedPrefill())/DefaultPrefixPrefillRate + busy*defaultITLSeconds
}
