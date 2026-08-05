// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
)

// knownExcludeIfFields is the set of field names recognised by AgentSnapshot.getValue().
// A typo in an exclude_if key would silently return nil and disable the gate, so we
// reject unknown names at load time.
//
// Keep this in sync with the switch statement in evaluator.go:getValue().
var knownExcludeIfFields = map[string]struct{}{
	// Universal
	"capacity_utilization": {},
	"success_rate":         {},
	"srtt":                 {},
	"consecutive_failures": {},
	// Engine (vLLM / SGLang)
	"kv_cache_utilization": {},
	"running_requests":     {},
	"waiting_requests":     {},
	"avg_ttft_seconds":     {},
	"p90_ttft_seconds":     {},
	"avg_itl_seconds":      {},
	"p90_itl_seconds":      {},
	// Hardware
	"gpu_temperature_c":     {},
	"gpu_util_percent":      {},
	"gpu_vram_used_percent": {},
	"memory_used_percent":   {},
	"cpu_usage_percent":     {},
}

// knownStrategies is the set of strategy names that are currently implemented.
// Add entries here as new strategies are written (see strategy.go).
var knownStrategies = map[string]struct{}{
	"least-loaded": {},
	// "lowest-srtt":  {},  // add when implemented
	// "round-robin":  {},
	// "prefix-aware": {},
}

// engineSpecificStrategies require engine: vllm in the match block.
// They may be recognised in the future but are not yet implemented.
var engineSpecificStrategies = map[string]struct{}{
	"lowest-kv-cache": {},
	"lowest-queue":    {},
	"best-ttft":       {},
	"best-itl":        {},
}

// Default returns the built-in policy used when no policy file is configured.
// It is behaviourally identical to the previous hard-coded selector:
// no match filter, no exclude_if gates, least-loaded strategy, global max_tries.
func Default() *Policy {
	return &Policy{
		RoutingPolicy: PolicyStep{
			Strategy: "least-loaded",
			// MaxTries 0 → executor substitutes globalMaxTries
		},
	}
}

// Load reads and validates a YAML policy file from path.
func Load(path string) (*Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("policy: read %q: %w", path, err)
	}
	return LoadBytes(data)
}

// LoadBytes parses and validates a YAML policy from a byte slice.
// Used by PUT /admin/policy to accept an inline YAML body.
func LoadBytes(data []byte) (*Policy, error) {
	var p Policy
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("policy: parse YAML: %w", err)
	}
	if err := Validate(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// Validate checks all policy steps for correctness and returns the first error found.
func Validate(p *Policy) error {
	if err := validateStep("routing_policy", p.RoutingPolicy); err != nil {
		return err
	}
	for i, fb := range p.FallbackChain {
		name := fb.Name
		if name == "" {
			name = fmt.Sprintf("fallback_chain[%d]", i)
		}
		if err := validateStep(name, fb.PolicyStep); err != nil {
			return err
		}
	}
	if fp := p.FallbackProvider; fp != nil {
		if fp.Engine == "" {
			return fmt.Errorf("policy: fallback_provider.engine is required")
		}
		if fp.Model == "" {
			return fmt.Errorf("policy: fallback_provider.model is required")
		}
	}
	return nil
}

func validateStep(name string, step PolicyStep) error {
	if step.Strategy == "" {
		return fmt.Errorf("policy: step %q: strategy is required", name)
	}
	if _, ok := knownStrategies[step.Strategy]; !ok {
		if _, isEngine := engineSpecificStrategies[step.Strategy]; isEngine {
			return fmt.Errorf("policy: step %q: strategy %q is not yet implemented — supported strategies: least-loaded",
				name, step.Strategy)
		}
		return fmt.Errorf("policy: step %q: unknown strategy %q — supported strategies: least-loaded",
			name, step.Strategy)
	}
	// Engine-specific strategies (future) require engine: vllm in the match block.
	if _, isEngine := engineSpecificStrategies[step.Strategy]; isEngine {
		if step.Match.Engine != "vllm" {
			return fmt.Errorf("policy: step %q: strategy %q requires 'engine: vllm' in the match block",
				name, step.Strategy)
		}
	}
	for field, rule := range step.ExcludeIf {
		if err := validateThreshold(name, field, rule); err != nil {
			return err
		}
	}
	return nil
}

// DirSnapshot is the result of loading a policy model directory.
// Global holds the policy from _default.yaml (nil if absent).
// Named maps policy document names (filename stem, e.g. "llama3-large" for
// "llama3-large.yaml") to their policies. The models each policy applies to
// are declared inside the file via the "models:" field; the executor expands
// this into a model-keyed routing map.
// Conflicted maps the stem of each file that was skipped due to a model
// ownership conflict to a human-readable reason string. A file in Conflicted
// is valid YAML — it simply lost the conflict. This is distinct from a parse
// error, where the file is absent from both Named and Conflicted.
// Hashes maps each filename to its sha256 digest — used by the SIGHUP handler
// to detect which files actually changed and skip unchanged ones.
type DirSnapshot struct {
	Global     *Policy
	Named      map[string]*Policy
	Conflicted map[string]string // stem → conflict reason
	Hashes     map[string][32]byte
}

// LoadDirSnapshot reads every .yaml/.yml file in dir and returns a DirSnapshot.
//
// Special file "_default.yaml" (or "_default.yml") is loaded as the global
// policy override. Its "models:" field is ignored. A parse error in _default.yaml
// is a hard failure.
//
// All other files must declare at least one model via the top-level "models:"
// field. Files are processed in modification-time ascending order (oldest first),
// so the first file that claims a model owns it. Any later file that tries to
// claim an already-owned model is skipped entirely with an error log.
// Parse errors in per-model files are logged and skipped — they never block
// the loading of other files (partial failure).
func LoadDirSnapshot(dir string) (*DirSnapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("policy: read dir %q: %w", dir, err)
	}

	snap := &DirSnapshot{
		Named:      make(map[string]*Policy),
		Conflicted: make(map[string]string),
		Hashes:     make(map[string][32]byte),
	}

	// Collect entries with their modification times.
	type fileEntry struct {
		name  string
		mtime time.Time
	}
	fileEntries := make([]fileEntry, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			log.Warnf("policy dir: skipping %q: stat error: %v", e.Name(), err)
			continue
		}
		fileEntries = append(fileEntries, fileEntry{name: e.Name(), mtime: info.ModTime()})
	}

	// Sort by mtime ascending (oldest file first = first in time wins on conflict).
	// Use filename as a tiebreak so results are deterministic when mtimes are equal.
	sort.Slice(fileEntries, func(i, j int) bool {
		if fileEntries[i].mtime.Equal(fileEntries[j].mtime) {
			return fileEntries[i].name < fileEntries[j].name
		}
		return fileEntries[i].mtime.Before(fileEntries[j].mtime)
	})

	// claimedBy tracks which stem first claimed each model, for conflict detection.
	claimedBy := make(map[string]string) // model → stem

	for _, fe := range fileEntries {
		name := fe.name
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			if name == "_default.yaml" || name == "_default.yml" {
				return nil, fmt.Errorf("policy: read %q: %w", name, err)
			}
			log.Warnf("policy dir: skipping %q: read error: %v", name, err)
			continue
		}

		snap.Hashes[name] = sha256.Sum256(data)

		if name == "_default.yaml" || name == "_default.yml" {
			p, err := LoadBytes(data)
			if err != nil {
				return nil, fmt.Errorf("policy: %s: %w", name, err)
			}
			snap.Global = p
			continue
		}

		p, err := LoadBytes(data)
		if err != nil {
			log.Warnf("policy dir: skipping %q: %v", name, err)
			continue
		}
		if len(p.Models) == 0 {
			log.Warnf("policy dir: %q has no 'models:' field — skipping (not applied to any model)", name)
			continue
		}

		stem := strings.TrimSuffix(strings.TrimSuffix(name, ".yaml"), ".yml")

		// Reject the entire file if any of its models are already claimed.
		conflict := ""
		for _, model := range p.Models {
			if owner, taken := claimedBy[model]; taken {
				conflict = fmt.Sprintf("model %q already claimed by %q", model, owner+".yaml")
				break
			}
		}
		if conflict != "" {
			log.Errorf("policy dir: skipping %q — %s", name, conflict)
			snap.Conflicted[stem] = conflict
			continue
		}

		for _, model := range p.Models {
			claimedBy[model] = stem
		}
		snap.Named[stem] = p
	}

	return snap, nil
}

func validateThreshold(stepName, field string, rule ThresholdRule) error {
	if _, ok := knownExcludeIfFields[field]; !ok {
		return fmt.Errorf("policy: step %q: exclude_if.%s: unknown field — see ROUTING_POLICY.md for supported fields",
			stepName, field)
	}
	count := 0
	if rule.GT != nil {
		count++
	}
	if rule.LT != nil {
		count++
	}
	if rule.GTE != nil {
		count++
	}
	if rule.LTE != nil {
		count++
	}
	if count == 0 {
		return fmt.Errorf("policy: step %q: exclude_if.%s: must specify exactly one operator (gt, lt, gte, lte)",
			stepName, field)
	}
	if count > 1 {
		return fmt.Errorf("policy: step %q: exclude_if.%s: only one operator allowed, got %d",
			stepName, field, count)
	}
	return nil
}
