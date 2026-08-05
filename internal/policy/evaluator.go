// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"fmt"
	"sort"
	"strings"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Evaluator assembles per-agent snapshots from live metric stores and
// provides the stateless match/exclude_if predicate functions.
type Evaluator struct {
	storage  storage.RoutingStorage
	counters *metrics.UniversalCounterStore
}

// NewEvaluator creates an Evaluator.
// counters is used to read live in-memory success rate and SRTT — values that are
// only flushed to storage every UniversalFlushInterval (default 30s) and would be
// dangerously stale for routing decisions if read from diskDB.
func NewEvaluator(s storage.RoutingStorage, counters *metrics.UniversalCounterStore) *Evaluator {
	return &Evaluator{storage: s, counters: counters}
}

// Snapshot builds an AgentSnapshot for peerID by reading from live metric stores.
// Metrics that are absent or cannot be read are left as nil — a nil value silently
// passes all exclude_if gates (gate is skipped for that agent).
//
// Source per field:
//   - CapacityUtilization: live from agent.GetLoad()/Capacity (computed here, no I/O)
//   - SuccessRate, SRTT:   live from UniversalCounterStore in-memory state (no I/O)
//   - KVCache, Waiting:    memDB via GetEnginePunctual   (max 500ms stale)
//   - GPU, Memory, CPU:    memDB via GetHardwareSnapshot (max 500ms stale)
func (e *Evaluator) Snapshot(peerID peer.ID, agent *domain.Agent) AgentSnapshot {
	var snap AgentSnapshot

	// CapacityUtilization: computed live from the agent's current load counter.
	// More accurate than the stored value in universalPunctual which is written
	// only after each request completes and may lag during concurrent bursts.
	if agent.Metadata.Capacity > 0 {
		v := float64(agent.GetLoad()) / float64(agent.Metadata.Capacity)
		snap.CapacityUtilization = &v
	}

	// SuccessRate, SRTT, ConsecutiveFailures: read from the in-memory agentCounterState.
	// Using LiveSnapshot avoids the 30s flush delay of diskDB universalHistory.
	// All three are left nil when ok=false (agent has no request history yet) so that
	// exclude_if gates are silently skipped — consistent with the nil-passes contract.
	if sr, srtt, cf, ok := e.counters.LiveSnapshot(peerID); ok {
		snap.SuccessRate = &sr
		if srtt > 0 {
			snap.SRTT = &srtt
		}
		v := float64(cf)
		snap.ConsecutiveFailures = &v
	}

	if bm, err := e.storage.GetEnginePunctual(peerID); err == nil && bm != nil {
		snap.KVCacheUtilization = bm.KVCacheUtilization
		snap.RunningRequests = bm.RunningRequests
		snap.WaitingRequests = bm.WaitingRequests
		snap.AvgTTFTSeconds = bm.AvgTTFTSeconds
		snap.P90TTFTSeconds = bm.P90TTFTSeconds
		snap.AvgITLSeconds = bm.AvgITLSeconds
		snap.P90ITLSeconds = bm.P90ITLSeconds
	}

	if hw, err := e.storage.GetHardwareSnapshot(peerID); err == nil && hw != nil {
		// Normalise 0–100 percent values to 0.0–1.0 fractions so that all
		// AgentSnapshot fields share a single unit convention. Prometheus gauges
		// are unaffected — they are written from the raw domain values before this
		// normalisation takes place.
		v := hw.Memory.UsedPercent / 100.0
		snap.MemoryUsedPercent = &v
		c := hw.CPU.UsagePercent / 100.0
		snap.CPUUsagePercent = &c
		if len(hw.GPUs) > 0 {
			maxTemp := hw.GPUs[0].TemperatureC
			maxUtil := hw.GPUs[0].UtilPercent / 100.0
			maxVRAM := gpuVRAMUsedPercent(hw.GPUs[0])
			for _, gpu := range hw.GPUs[1:] {
				if gpu.TemperatureC > maxTemp {
					maxTemp = gpu.TemperatureC
				}
				if u := gpu.UtilPercent / 100.0; u > maxUtil {
					maxUtil = u
				}
				if p := gpuVRAMUsedPercent(gpu); p > maxVRAM {
					maxVRAM = p
				}
			}
			snap.GPUTemperatureC = &maxTemp
			snap.GPUUtilPercent = &maxUtil
			snap.GPUVRAMUsedPercent = &maxVRAM
		}
	}

	return snap
}

// gpuVRAMUsedPercent returns VRAMUsedBytes/VRAMTotalBytes for one GPU.
// Returns 0 when VRAMTotalBytes is zero (avoids division by zero).
func gpuVRAMUsedPercent(g domain.GPUMetric) float64 {
	if g.VRAMTotalBytes == 0 {
		return 0
	}
	return float64(g.VRAMUsedBytes) / float64(g.VRAMTotalBytes)
}

// getValue maps an exclude_if field name to the corresponding AgentSnapshot value.
// Returns nil for unrecognised field names — unrecognised gates are silently skipped.
func (snap AgentSnapshot) getValue(field string) *float64 {
	switch field {
	// Universal
	case "capacity_utilization":
		return snap.CapacityUtilization
	case "success_rate":
		return snap.SuccessRate
	case "srtt":
		return snap.SRTT
	case "consecutive_failures":
		return snap.ConsecutiveFailures
	// Engine (vLLM)
	case "kv_cache_utilization":
		return snap.KVCacheUtilization
	case "running_requests":
		return snap.RunningRequests
	case "waiting_requests":
		return snap.WaitingRequests
	case "avg_ttft_seconds":
		return snap.AvgTTFTSeconds
	case "p90_ttft_seconds":
		return snap.P90TTFTSeconds
	case "avg_itl_seconds":
		return snap.AvgITLSeconds
	case "p90_itl_seconds":
		return snap.P90ITLSeconds
	// Hardware
	case "gpu_temperature_c":
		return snap.GPUTemperatureC
	case "gpu_util_percent":
		return snap.GPUUtilPercent
	case "gpu_vram_used_percent":
		return snap.GPUVRAMUsedPercent
	case "memory_used_percent":
		return snap.MemoryUsedPercent
	case "cpu_usage_percent":
		return snap.CPUUsagePercent
	}
	return nil
}

// MatchesFilter returns true when the agent's metadata satisfies every non-empty
// field in f. An empty MatchFilter always returns true.
// All comparisons are case-sensitive.
func MatchesFilter(agent *domain.Agent, f MatchFilter) bool {
	if f.Region != "" && agent.Metadata.Region != f.Region {
		return false
	}
	if f.Engine != "" && agent.Metadata.Engine != f.Engine {
		return false
	}
	if f.Organization != "" && agent.Metadata.Organization != f.Organization {
		return false
	}
	if f.Machine != "" && agent.Metadata.Machine != f.Machine {
		return false
	}
	for _, required := range f.Tags {
		found := false
		for _, tag := range agent.Metadata.Tags {
			if tag == required {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.GPUModel != "" && agent.Metadata.GPUModel != f.GPUModel {
		return false
	}
	return true
}

// filterMissReason returns the first filter attribute that caused the given agent
// to fail MatchesFilter. The returned string includes the agent's actual value for
// scalar fields so it can be read directly in exhaustion logs without consulting
// the agent registry. For the tag path the full agent tag list is included.
// Returns "unknown" only as an unreachable safety net — callers invoke this
// function only after MatchesFilter has already returned false.
func filterMissReason(agent *domain.Agent, f MatchFilter) string {
	if f.Region != "" && agent.Metadata.Region != f.Region {
		return fmt.Sprintf("region(got=%q,want=%q)", agent.Metadata.Region, f.Region)
	}
	if f.Engine != "" && agent.Metadata.Engine != f.Engine {
		return fmt.Sprintf("engine(got=%q,want=%q)", agent.Metadata.Engine, f.Engine)
	}
	if f.Organization != "" && agent.Metadata.Organization != f.Organization {
		return fmt.Sprintf("organization(got=%q,want=%q)", agent.Metadata.Organization, f.Organization)
	}
	if f.Machine != "" && agent.Metadata.Machine != f.Machine {
		return fmt.Sprintf("machine(got=%q,want=%q)", agent.Metadata.Machine, f.Machine)
	}
	for _, required := range f.Tags {
		found := false
		for _, tag := range agent.Metadata.Tags {
			if tag == required {
				found = true
				break
			}
		}
		if !found {
			return fmt.Sprintf("tag(missing=%q,got=%v)", required, agent.Metadata.Tags)
		}
	}
	if f.GPUModel != "" && agent.Metadata.GPUModel != f.GPUModel {
		return fmt.Sprintf("gpu_model(got=%q,want=%q)", agent.Metadata.GPUModel, f.GPUModel)
	}
	return "unknown"
}

// gateFailReason returns all exclude_if gates that the snapshot violates, joined
// as a comma-separated string in sorted order. Sorted iteration over the gates map
// ensures the log output is deterministic regardless of Go's map randomness.
// Returns "unknown" only when no gate violation can be attributed (should not happen
// if called only after PassesGates returns false).
func gateFailReason(snap AgentSnapshot, gates map[string]ThresholdRule) string {
	keys := make([]string, 0, len(gates))
	for field := range gates {
		keys = append(keys, field)
	}
	sort.Strings(keys)

	var violated []string
	for _, field := range keys {
		v := snap.getValue(field)
		if v == nil {
			continue
		}
		if violates(*v, gates[field]) {
			violated = append(violated, field)
		}
	}
	if len(violated) == 0 {
		return "unknown"
	}
	return strings.Join(violated, ", ")
}

// PassesGates returns true when the snapshot does not violate any exclude_if rule.
// A nil metric value always passes — the gate is skipped for that agent.
func PassesGates(snap AgentSnapshot, gates map[string]ThresholdRule) bool {
	for field, rule := range gates {
		v := snap.getValue(field)
		if v == nil {
			continue // metric unavailable → skip gate
		}
		if violates(*v, rule) {
			return false
		}
	}
	return true
}

// violates returns true when value breaches the threshold defined by rule,
// i.e. the agent should be excluded from the eligible pool.
func violates(value float64, rule ThresholdRule) bool {
	if rule.GT != nil && value > *rule.GT {
		return true
	}
	if rule.LT != nil && value < *rule.LT {
		return true
	}
	if rule.GTE != nil && value >= *rule.GTE {
		return true
	}
	if rule.LTE != nil && value <= *rule.LTE {
		return true
	}
	return false
}
