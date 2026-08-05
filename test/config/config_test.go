// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package config_test verifies default construction and environment-variable
// overriding, including the validation guards that reject invalid values and
// fall back to the default rather than corrupting the config.
package config_test

import (
	"testing"
	"time"

	"hivenet_router/internal/config"
)

// TestDefaultConfig pins the documented out-of-the-box defaults so accidental
// changes to DefaultConfig are caught.
func TestDefaultConfig(t *testing.T) {
	c := config.DefaultConfig()
	// The three listen ports: HTTP API, gRPC auth, Prometheus metrics.
	if c.HTTPPort != ":8080" || c.GRPCPort != ":50051" || c.MetricsPort != ":2112" {
		t.Errorf("unexpected default ports: %+v", c)
	}
	if c.P2PMaxConnsPerIP != 32 {
		t.Errorf("P2PMaxConnsPerIP = %d, want 32", c.P2PMaxConnsPerIP)
	}
	// Health timers: heartbeat 5s, unhealthy 3x, remove 6x.
	if c.HeartbeatInterval != 5*time.Second || c.UnhealthyAfter != 15*time.Second || c.RemoveAfter != 30*time.Second {
		t.Errorf("health timers wrong: hb=%v unhealthy=%v remove=%v", c.HeartbeatInterval, c.UnhealthyAfter, c.RemoveAfter)
	}
	if c.MaxTriesPerStep != 3 || c.QueueDepth != 30 || c.DiskDBTTLDays != 30 {
		t.Errorf("unexpected defaults: tries=%d depth=%d ttl=%d", c.MaxTriesPerStep, c.QueueDepth, c.DiskDBTTLDays)
	}
	// libp2p protocol identifier is version-pinned.
	if c.ProtocolID != "/hivenet_router/1.0.0" {
		t.Errorf("ProtocolID = %q", c.ProtocolID)
	}
}

func TestLoadFromEnv_NoEnv_MatchesDefaults(t *testing.T) {
	// With no relevant env set, LoadFromEnv should equal DefaultConfig for the
	// overridable scalar fields. (t.Setenv isn't needed; we just don't set any.)
	got := config.LoadFromEnv()
	def := config.DefaultConfig()
	if got.HTTPPort != def.HTTPPort || got.QueueDepth != def.QueueDepth || got.MaxTriesPerStep != def.MaxTriesPerStep {
		t.Errorf("LoadFromEnv diverged from defaults without env: %+v", got)
	}
}

// TestLoadFromEnv_Overrides sets every overridable env var and asserts each is
// parsed into the matching config field with the right type.
func TestLoadFromEnv_Overrides(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_HTTP_PORT", ":9999")
	t.Setenv("HIVENET_ROUTER_GRPC_PORT", ":9998")
	t.Setenv("HIVENET_ROUTER_METRICS_PORT", ":9997")
	t.Setenv("HIVENET_ROUTER_P2P_PORT", "9001")
	t.Setenv("HIVENET_ROUTER_P2P_MAX_CONNS_PER_IP", "64")
	t.Setenv("HIVENET_ROUTER_QUEUE_SIZE", "250")
	t.Setenv("HIVENET_ROUTER_DISK_DB_PATH", "/data/db")
	t.Setenv("HIVENET_ROUTER_REQUEST_TIMEOUT", "90s")
	t.Setenv("HIVENET_ROUTER_POLICY_FILE", "/etc/policy.yaml")
	t.Setenv("HIVENET_ROUTER_POLICY_MODEL_DIR", "/etc/policies")
	t.Setenv("HIVENET_ROUTER_MAX_TRIES_PER_STEP", "5")
	t.Setenv("HIVENET_ROUTER_QUEUE_DEPTH", "0")
	t.Setenv("HIVENET_ROUTER_SESSION_TTL", "2h")
	t.Setenv("HIVENET_ROUTER_JWT_SECRET", "s3cr3t")
	t.Setenv("HIVENET_ROUTER_AUTH_CONFIG", "/etc/auth.yaml")
	t.Setenv("HIVENET_ROUTER_AUTH_MODE", "api-key")
	t.Setenv("HIVENET_ROUTER_QUOTA_BACKEND", "badger")
	t.Setenv("HIVENET_ROUTER_MAX_REQUEST_BYTES", "1048576")

	c := config.LoadFromEnv()
	// Table of field-under-test vs expected parsed value; note the type
	// conversions (string port stays string, numbers become int, durations
	// become time.Duration).
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"HTTPPort", c.HTTPPort, ":9999"},
		{"GRPCPort", c.GRPCPort, ":9998"},
		{"MetricsPort", c.MetricsPort, ":9997"},
		{"P2PPort", c.P2PPort, "9001"},
		{"P2PMaxConnsPerIP", c.P2PMaxConnsPerIP, 64},
		{"QueueSize", c.QueueSize, 250},
		{"DiskDBPath", c.DiskDBPath, "/data/db"},
		{"RequestTimeout", c.RequestTimeout, 90 * time.Second},
		{"PolicyFile", c.PolicyFile, "/etc/policy.yaml"},
		{"PolicyModelDir", c.PolicyModelDir, "/etc/policies"},
		{"MaxTriesPerStep", c.MaxTriesPerStep, 5},
		{"QueueDepth", c.QueueDepth, 0},
		{"SessionTTL", c.SessionTTL, 2 * time.Hour},
		{"JWTSecret", c.JWTSecret, "s3cr3t"},
		{"AuthConfigFile", c.AuthConfigFile, "/etc/auth.yaml"},
		{"AuthMode", c.AuthMode, "api-key"},
		{"QuotaBackend", c.QuotaBackend, "badger"},
		{"MaxRequestBytes", c.MaxRequestBytes, int64(1048576)},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
}

// TestLoadFromEnv_MaxRequestBytesZeroDisables locks in that 0 is a valid,
// distinct value meaning "no limit" (not treated as unset/invalid), while a
// negative value is rejected and leaves the default intact.
func TestLoadFromEnv_MaxRequestBytesZeroDisables(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_MAX_REQUEST_BYTES", "0")
	if got := config.LoadFromEnv().MaxRequestBytes; got != 0 {
		t.Errorf("MaxRequestBytes = %d, want 0 (disabled)", got)
	}

	def := config.DefaultConfig()
	t.Setenv("HIVENET_ROUTER_MAX_REQUEST_BYTES", "-1")
	if got := config.LoadFromEnv().MaxRequestBytes; got != def.MaxRequestBytes {
		t.Errorf("MaxRequestBytes = %d, want default %d (negative ignored)", got, def.MaxRequestBytes)
	}
}

// TestLoadFromEnv_InvalidValuesIgnored locks in the guard behaviour: malformed or
// out-of-range values must be ignored, leaving the default intact — never a
// zero/garbage value.
func TestLoadFromEnv_InvalidValuesIgnored(t *testing.T) {
	def := config.DefaultConfig()
	t.Setenv("HIVENET_ROUTER_P2P_MAX_CONNS_PER_IP", "0") // must be > 0
	t.Setenv("HIVENET_ROUTER_QUEUE_SIZE", "notanumber")  // unparseable
	t.Setenv("HIVENET_ROUTER_REQUEST_TIMEOUT", "xyz")    // unparseable duration
	t.Setenv("HIVENET_ROUTER_MAX_TRIES_PER_STEP", "-1")  // must be > 0
	t.Setenv("HIVENET_ROUTER_QUEUE_DEPTH", "-5")         // must be >= 0
	t.Setenv("HIVENET_ROUTER_SESSION_TTL", "-1h")        // must be > 0

	c := config.LoadFromEnv()
	// Each field must equal the default (bad env silently discarded).
	if c.P2PMaxConnsPerIP != def.P2PMaxConnsPerIP {
		t.Errorf("P2PMaxConnsPerIP: bad value not ignored, got %d", c.P2PMaxConnsPerIP)
	}
	if c.QueueSize != def.QueueSize {
		t.Errorf("QueueSize: bad value not ignored, got %d", c.QueueSize)
	}
	if c.RequestTimeout != def.RequestTimeout {
		t.Errorf("RequestTimeout: bad value not ignored, got %v", c.RequestTimeout)
	}
	if c.MaxTriesPerStep != def.MaxTriesPerStep {
		t.Errorf("MaxTriesPerStep: negative not ignored, got %d", c.MaxTriesPerStep)
	}
	if c.QueueDepth != def.QueueDepth {
		t.Errorf("QueueDepth: negative not ignored, got %d", c.QueueDepth)
	}
	if c.SessionTTL != def.SessionTTL {
		t.Errorf("SessionTTL: non-positive not ignored, got %v", c.SessionTTL)
	}
}

// QueueDepth=0 is valid (disables the wait queue) and must be honoured, distinct
// from the "invalid, keep default" path above.
func TestLoadFromEnv_QueueDepthZeroHonoured(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_QUEUE_DEPTH", "0")
	if c := config.LoadFromEnv(); c.QueueDepth != 0 {
		t.Errorf("QueueDepth = %d, want 0 (wait queue disabled)", c.QueueDepth)
	}
}

// TestLoadFromEnv_ProviderKeys: each provider API-key env var must produce one
// entry in the Providers slice, keyed by provider name.
func TestLoadFromEnv_ProviderKeys(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_OPENAI_API_KEY", "sk-openai")
	t.Setenv("HIVENET_ROUTER_ANTHROPIC_API_KEY", "sk-anthropic")
	c := config.LoadFromEnv()
	if len(c.Providers) != 2 {
		t.Fatalf("expected 2 providers, got %d: %+v", len(c.Providers), c.Providers)
	}
	// Order is unspecified, so index by name before asserting keys.
	seen := map[string]string{}
	for _, p := range c.Providers {
		seen[p.Name] = p.APIKey
	}
	if seen["openai"] != "sk-openai" || seen["anthropic"] != "sk-anthropic" {
		t.Errorf("provider keys wrong: %+v", seen)
	}
}

// TestDefaultAgentConfig_JWTFromEnv: the agent config reads its JWT secret from
// the environment while keeping the other agent defaults.
func TestDefaultAgentConfig_JWTFromEnv(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_JWT_SECRET", "agent-secret")
	a := config.DefaultAgentConfig()
	if a.JWTSecret != "agent-secret" {
		t.Errorf("agent JWTSecret = %q, want agent-secret", a.JWTSecret)
	}
	// Non-secret agent defaults: vLLM engine, llm capability, capacity 10.
	if a.Engine != "vllm" || a.Capability != "llm" || a.Capacity != 10 {
		t.Errorf("unexpected agent defaults: %+v", a)
	}
}
