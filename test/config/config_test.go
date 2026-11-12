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

// TestDefaultAgentConfig_BackendAPIKeyTrimmed: the backend key from the
// environment is trimmed like the key file is, so a trailing newline (common
// with "export KEY=$(cat file)" or secret mounts) does not break auth.
func TestDefaultAgentConfig_BackendAPIKeyTrimmed(t *testing.T) {
	tests := []struct{ name, env, want string }{
		{"trailing newline", "sk-backend\n", "sk-backend"},
		{"surrounding spaces", "  sk-backend\t", "sk-backend"},
		{"whitespace only means no key", " \n", ""},
		{"unset", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HIVENET_ROUTER_BACKEND_API_KEY", tc.env)
			if got := config.DefaultAgentConfig().BackendAPIKey; got != tc.want {
				t.Errorf("BackendAPIKey = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLoadFromEnv_SemanticPinKnobs: the semantic pin store limits default
// sensibly, take positive env overrides, and ignore invalid values.
func TestLoadFromEnv_SemanticPinKnobs(t *testing.T) {
	cfg := config.LoadFromEnv()
	if cfg.SemanticPinMax != 100_000 || cfg.SemanticPinMaxPerKey != 10_000 {
		t.Errorf("defaults = %d/%d", cfg.SemanticPinMax, cfg.SemanticPinMaxPerKey)
	}
	t.Setenv("HIVENET_ROUTER_SEMANTIC_PIN_MAX", "500")
	t.Setenv("HIVENET_ROUTER_SEMANTIC_PIN_MAX_PER_KEY", "-1") // invalid → default kept
	cfg = config.LoadFromEnv()
	if cfg.SemanticPinMax != 500 || cfg.SemanticPinMaxPerKey != 10_000 {
		t.Errorf("overrides = %d/%d", cfg.SemanticPinMax, cfg.SemanticPinMaxPerKey)
	}
}

// TestLoadFromEnv_SemanticDecisionLog: the decision log path and buffer come
// from the environment; an invalid buffer keeps the default.
func TestLoadFromEnv_SemanticDecisionLog(t *testing.T) {
	cfg := config.LoadFromEnv()
	if cfg.SemanticDecisionLog != "" || cfg.SemanticDecisionLogBuffer != 4096 {
		t.Errorf("defaults = %q/%d", cfg.SemanticDecisionLog, cfg.SemanticDecisionLogBuffer)
	}
	t.Setenv("HIVENET_ROUTER_SEMANTIC_DECISION_LOG", "/var/log/decisions.jsonl")
	t.Setenv("HIVENET_ROUTER_SEMANTIC_DECISION_LOG_BUFFER", "0") // invalid → default kept
	cfg = config.LoadFromEnv()
	if cfg.SemanticDecisionLog != "/var/log/decisions.jsonl" || cfg.SemanticDecisionLogBuffer != 4096 {
		t.Errorf("overrides = %q/%d", cfg.SemanticDecisionLog, cfg.SemanticDecisionLogBuffer)
	}
}

// TestLoadFromEnv_Redirect: the cross-site redirect settings. Values are
// trimmed, the secrets list is comma-separated in env (one id:secret per line
// in the file form), and a bad TTL refuses startup via EnvErrors.
func TestLoadFromEnv_Redirect(t *testing.T) {
	if c := config.LoadFromEnv(); c.SiteID != "" || c.RedirectSecrets != "" || c.RedirectTokenTTL != 60*time.Second || c.RedirectAllowedDomains != "" {
		t.Fatalf("defaults: site %q secrets %q ttl %v domains %q, want disabled and 60s",
			c.SiteID, c.RedirectSecrets, c.RedirectTokenTTL, c.RedirectAllowedDomains)
	}

	t.Setenv("HIVENET_ROUTER_SITE_ID", " site-fr-1 ")
	t.Setenv("HIVENET_ROUTER_REDIRECT_SECRETS", " k2:new-secret,k1:old-secret ")
	t.Setenv("HIVENET_ROUTER_REDIRECT_TOKEN_TTL", "90s")
	t.Setenv("HIVENET_ROUTER_REDIRECT_ALLOWED_DOMAINS", " .inference.hivecompute.ai ")
	c := config.LoadFromEnv()
	if c.SiteID != "site-fr-1" || c.RedirectSecrets != "k2:new-secret\nk1:old-secret" || c.RedirectTokenTTL != 90*time.Second ||
		c.RedirectAllowedDomains != ".inference.hivecompute.ai" || len(c.EnvErrors) != 0 {
		t.Fatalf("site %q secrets %q ttl %v domains %q errors %v",
			c.SiteID, c.RedirectSecrets, c.RedirectTokenTTL, c.RedirectAllowedDomains, c.EnvErrors)
	}

	for _, bad := range []string{"soon", "0s", "-1m"} {
		t.Setenv("HIVENET_ROUTER_REDIRECT_TOKEN_TTL", bad)
		if c := config.LoadFromEnv(); len(c.EnvErrors) != 1 || c.RedirectTokenTTL != 60*time.Second {
			t.Errorf("TTL %q: errors %v ttl %v, want one EnvError and the 60s default", bad, c.EnvErrors, c.RedirectTokenTTL)
		}
	}
}

// Shared-counter settings come from the environment; a malformed interval or
// TLS flag is collected as an error instead of being silently ignored.
func TestSyncSettingsFromEnv(t *testing.T) {
	t.Setenv("HIVENET_ROUTER_SYNC_STORE", "valkey")
	t.Setenv("HIVENET_ROUTER_SYNC_VALKEY_ADDRS", "s0:26379,s1:26379")
	t.Setenv("HIVENET_ROUTER_SYNC_VALKEY_SENTINEL_MASTER", "hivenet")
	t.Setenv("HIVENET_ROUTER_SYNC_VALKEY_TLS", "true")
	t.Setenv("HIVENET_ROUTER_SYNC_INTERVAL", "500ms")
	t.Setenv("HIVENET_ROUTER_SYNC_MIN_ROUTERS", "3")
	t.Setenv("HIVENET_ROUTER_SYNC_SITE", "site-a")
	t.Setenv("HIVENET_ROUTER_ROUTER_ID", "router-0")
	cfg := config.LoadFromEnv()
	if cfg.SyncStore != "valkey" || cfg.SyncValkeyAddrs != "s0:26379,s1:26379" || cfg.SyncSentinelMaster != "hivenet" ||
		!cfg.SyncValkeyTLS || cfg.SyncInterval != 500*time.Millisecond || cfg.SyncMinRouters != 3 || cfg.SyncSite != "site-a" || cfg.RouterID != "router-0" {
		t.Fatalf("sync settings not read from the environment: %+v", cfg)
	}
	if len(cfg.EnvErrors) != 0 {
		t.Fatalf("unexpected env errors: %v", cfg.EnvErrors)
	}

	t.Setenv("HIVENET_ROUTER_SYNC_INTERVAL", "1sec")
	t.Setenv("HIVENET_ROUTER_SYNC_VALKEY_TLS", "maybe")
	t.Setenv("HIVENET_ROUTER_SYNC_MIN_ROUTERS", "0")
	if cfg := config.LoadFromEnv(); len(cfg.EnvErrors) != 3 {
		t.Fatalf("want 3 env errors for a bad interval, TLS flag and router count, got %v", cfg.EnvErrors)
	}
}

func TestSyncIntervalDefault(t *testing.T) {
	if cfg := config.LoadFromEnv(); cfg.SyncInterval != time.Second || cfg.SyncStore != "" {
		t.Fatalf("defaults: interval %v store %q", cfg.SyncInterval, cfg.SyncStore)
	}
}

// Admission defaults: the occupancy budget follows the engine's KV size, and
// models nothing bounds get 8 requests in flight per replica. Both can be
// changed from the environment.
func TestAdmissionDefaultsFromEnv(t *testing.T) {
	cfg := config.LoadFromEnv()
	if !cfg.AdmitFromEngineKV || cfg.DefaultMaxInflightPerReplica != 8 {
		t.Fatalf("defaults: from engine KV %v, max in flight %d", cfg.AdmitFromEngineKV, cfg.DefaultMaxInflightPerReplica)
	}
	t.Setenv("HIVENET_ROUTER_ADMIT_FROM_ENGINE_KV", "false")
	t.Setenv("HIVENET_ROUTER_DEFAULT_MAX_INFLIGHT_PER_REPLICA", "0")
	cfg = config.LoadFromEnv()
	if cfg.AdmitFromEngineKV || cfg.DefaultMaxInflightPerReplica != 0 || len(cfg.EnvErrors) != 0 {
		t.Fatalf("not read from the environment: %v %d %v", cfg.AdmitFromEngineKV, cfg.DefaultMaxInflightPerReplica, cfg.EnvErrors)
	}
	t.Setenv("HIVENET_ROUTER_DEFAULT_MAX_INFLIGHT_PER_REPLICA", "-1")
	if cfg := config.LoadFromEnv(); len(cfg.EnvErrors) != 1 {
		t.Fatalf("want 1 env error for a negative cap, got %v", cfg.EnvErrors)
	}
}

// TestLoadFromEnv_WakeHold: the hold-at-zero settings. A bad value refuses
// startup via EnvErrors and keeps the default.
func TestLoadFromEnv_WakeHold(t *testing.T) {
	if c := config.LoadFromEnv(); c.WakeHoldBudget != 5*time.Minute || c.WakeHoldMax != 100 || c.WakeKeepAlive != 15*time.Second {
		t.Fatalf("defaults: budget %v max %d keepalive %v, want 5m, 100, 15s", c.WakeHoldBudget, c.WakeHoldMax, c.WakeKeepAlive)
	}

	t.Setenv("HIVENET_ROUTER_WAKE_HOLD_BUDGET", "4m")
	t.Setenv("HIVENET_ROUTER_WAKE_HOLD_MAX", "20")
	t.Setenv("HIVENET_ROUTER_WAKE_KEEPALIVE", "0s") // 0 disables keepalives
	if c := config.LoadFromEnv(); c.WakeHoldBudget != 4*time.Minute || c.WakeHoldMax != 20 || c.WakeKeepAlive != 0 || len(c.EnvErrors) != 0 {
		t.Fatalf("overrides: budget %v max %d keepalive %v errors %v", c.WakeHoldBudget, c.WakeHoldMax, c.WakeKeepAlive, c.EnvErrors)
	}

	for env, bad := range map[string][]string{
		"HIVENET_ROUTER_WAKE_HOLD_BUDGET": {"soon", "0s", "-1m"},
		"HIVENET_ROUTER_WAKE_HOLD_MAX":    {"many", "0", "-3"},
		"HIVENET_ROUTER_WAKE_KEEPALIVE":   {"often", "-5s"},
	} {
		for _, v := range bad {
			t.Run(env+"="+v, func(t *testing.T) {
				t.Setenv(env, v)
				if c := config.LoadFromEnv(); len(c.EnvErrors) != 1 {
					t.Errorf("errors %v, want exactly one", c.EnvErrors)
				}
			})
		}
	}
}

// The occupancy queue follows --queue-depth unless set on its own.
func TestLoadFromEnv_AdmissionQueueDepth(t *testing.T) {
	if got := config.DefaultConfig().AdmissionQueueDepth; got != -1 {
		t.Fatalf("default AdmissionQueueDepth = %d, want -1 (follow QueueDepth)", got)
	}
	t.Setenv("HIVENET_ROUTER_ADMISSION_QUEUE_DEPTH", "100")
	if got := config.LoadFromEnv().AdmissionQueueDepth; got != 100 {
		t.Fatalf("AdmissionQueueDepth = %d, want 100", got)
	}
}

// TestLoadFromEnv_IsolationSwitches: the tenant-isolation switches default
// off, read booleans from the environment, and an unparsable value stops
// startup (EnvErrors) instead of silently leaving isolation off.
func TestLoadFromEnv_IsolationSwitches(t *testing.T) {
	if c := config.LoadFromEnv(); c.RequireModelAllowlist {
		t.Error("RequireModelAllowlist defaults on, want off")
	}
	t.Setenv("HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST", "true")
	if c := config.LoadFromEnv(); !c.RequireModelAllowlist || len(c.EnvErrors) != 0 {
		t.Errorf("RequireModelAllowlist=%v errors=%v, want true and none", c.RequireModelAllowlist, c.EnvErrors)
	}
	t.Setenv("HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST", "yes please")
	if c := config.LoadFromEnv(); c.RequireModelAllowlist || len(c.EnvErrors) != 1 {
		t.Errorf("RequireModelAllowlist=%v errors=%v, want false and one error", c.RequireModelAllowlist, c.EnvErrors)
	}
}
