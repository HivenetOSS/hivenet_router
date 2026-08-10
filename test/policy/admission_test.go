// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for the admission-control schema fields on Policy: mode (with
// its reserved default), the per-request/occupancy caps, and the shed_if block.
package policy_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/policy"

	"github.com/goccy/go-yaml"
)

// reservedPolicyYAML is a reserved-mode policy with every admission field set
// (plus the routing_policy block every policy requires).
const reservedPolicyYAML = `
models: [gemma-4-31b]
routing_policy:
  strategy: least-loaded
max_input_tokens: 262144
images_max: 10
admit_budget_tokens: 408987
max_inflight: 37
shed_if:
  kv_cache_utilization: { gt: 0.90 }
  waiting_requests:     { gt: 20 }
`

// serverlessPolicyYAML is the same policy in serverless mode.
const serverlessPolicyYAML = `
models: [gemma-4-31b]
mode: serverless
routing_policy:
  strategy: least-loaded
max_input_tokens: 262144
images_max: 10
admit_budget_tokens: 408987
max_inflight: 37
shed_if:
  kv_cache_utilization: { gt: 0.90 }
  waiting_requests:     { gt: 20 }
`

// assertRoundTrip loads a policy, marshals it, reloads it, and asserts the
// re-marshaled bytes are byte-identical — a stable equality check that avoids
// reflect.DeepEqual's nil-map / pointer pitfalls on ThresholdRule.
func assertRoundTrip(t *testing.T, p *policy.Policy) {
	t.Helper()
	out1, err := yaml.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	p2, err := policy.LoadBytes(out1)
	if err != nil {
		t.Fatalf("LoadBytes(round-trip): %v", err)
	}
	out2, err := yaml.Marshal(p2)
	if err != nil {
		t.Fatalf("Marshal(round-trip): %v", err)
	}
	if string(out1) != string(out2) {
		t.Errorf("round-trip mismatch:\n first:\n%s\nsecond:\n%s", out1, out2)
	}
}

func TestPolicy_ReservedRoundTrip(t *testing.T) {
	p, err := policy.LoadBytes([]byte(reservedPolicyYAML))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if p.EffectiveMode() != policy.ModeReserved {
		t.Errorf("mode = %q, want reserved (default)", p.EffectiveMode())
	}
	if p.IsServerless() {
		t.Error("IsServerless() = true, want false for reserved policy")
	}
	if p.MaxInputTokens != 262144 || p.ImagesMax != 10 ||
		p.AdmitBudgetTokens != 408987 || p.MaxInflight != 37 {
		t.Errorf("caps not loaded: %+v", p)
	}
	rule, ok := p.ShedIf["kv_cache_utilization"]
	if !ok || rule.GT == nil || *rule.GT != 0.90 {
		t.Errorf("shed_if.kv_cache_utilization not loaded: %+v", p.ShedIf)
	}
	assertRoundTrip(t, p)
}

func TestPolicy_ServerlessRoundTrip(t *testing.T) {
	p, err := policy.LoadBytes([]byte(serverlessPolicyYAML))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if !p.IsServerless() {
		t.Errorf("IsServerless() = false, want true; mode=%q", p.Mode)
	}
	assertRoundTrip(t, p)
}

func TestPolicy_ModeDefaults(t *testing.T) {
	// A routing-only policy (no admission fields) must still load and default to
	// reserved — adding the schema changes no existing behaviour.
	p, err := policy.LoadBytes([]byte("routing_policy:\n  strategy: least-loaded\n"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if p.Mode != policy.ModeReserved {
		t.Errorf("Mode = %q, want normalised to reserved", p.Mode)
	}
	if p.EffectiveMode() != policy.ModeReserved {
		t.Errorf("EffectiveMode() = %q, want reserved", p.EffectiveMode())
	}
}

func TestPolicy_RejectUnknownMode(t *testing.T) {
	_, err := policy.LoadBytes([]byte("mode: reserverd\nrouting_policy:\n  strategy: least-loaded\n"))
	if err == nil {
		t.Fatal("expected error for unknown mode, got nil")
	}
	if !strings.Contains(err.Error(), "unknown mode") {
		t.Errorf("error = %q, want it to mention 'unknown mode'", err)
	}
}

func TestPolicy_RejectNegativeCap(t *testing.T) {
	_, err := policy.LoadBytes([]byte("max_input_tokens: -1\nrouting_policy:\n  strategy: least-loaded\n"))
	if err == nil {
		t.Fatal("expected error for negative max_input_tokens, got nil")
	}
	if !strings.Contains(err.Error(), "max_input_tokens") {
		t.Errorf("error = %q, want it to name the offending field", err)
	}
}

func TestPolicy_RejectUnknownShedIfField(t *testing.T) {
	// gpu_temperature_c is a valid exclude_if field but NOT a valid shed_if field
	// — shedding at the front door only makes sense on kv/queue signals.
	y := "routing_policy:\n  strategy: least-loaded\nshed_if:\n  gpu_temperature_c: { gt: 80 }\n"
	_, err := policy.LoadBytes([]byte(y))
	if err == nil {
		t.Fatal("expected error for unknown shed_if field, got nil")
	}
	if !strings.Contains(err.Error(), "shed_if") {
		t.Errorf("error = %q, want it to mention shed_if", err)
	}
}

func TestPolicy_RejectShedIfNoOperator(t *testing.T) {
	y := "routing_policy:\n  strategy: least-loaded\nshed_if:\n  kv_cache_utilization: {}\n"
	_, err := policy.LoadBytes([]byte(y))
	if err == nil {
		t.Fatal("expected error for shed_if rule with no operator, got nil")
	}
	if !strings.Contains(err.Error(), "exactly one operator") {
		t.Errorf("error = %q, want it to mention the operator requirement", err)
	}
}
