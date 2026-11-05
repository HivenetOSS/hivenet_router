// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the Hivenet Router
type Config struct {
	// Server ports
	HTTPPort      string
	GRPCPort      string
	MetricsPort   string
	P2PPort       string
	P2PListenAddr string
	// P2PAnnounceAddr overrides the address advertised to agents via the gRPC
	// auth response. Required when the router runs behind NAT or inside Docker,
	// where the local listen address is not reachable from outside.
	// Example: /dns4/my-host.example.com/tcp/8903
	P2PAnnounceAddr string

	// P2PMaxConnsPerIP caps the number of concurrent libp2p connections the
	// router accepts from a single source IP (a /32 IPv4 or /56 IPv6 prefix).
	// go-libp2p's resource manager defaults this to 8, which is too low when
	// many agents reach the router through one shared NAT/egress IP (e.g. a
	// Kubernetes SNAT gateway): once 8 agents are connected, the next agent's
	// connection is accepted at TCP but reset during the Noise handshake.
	// Keep this comfortably above the number of agents behind a single egress
	// IP — rule of thumb >= 2x the fleet size — to absorb reconnect overlap
	// during rollouts, where a restarting agent briefly holds both its old
	// (not yet timed out) and its new connection.
	P2PMaxConnsPerIP int

	// Timeouts
	RequestTimeout      time.Duration
	HealthCheckInterval time.Duration // how often the health monitor goroutine runs (check frequency)
	UnhealthyAfter      time.Duration // mark agent unhealthy after this long without a heartbeat
	RemoveAfter         time.Duration // remove agent after this long without a heartbeat
	HeartbeatInterval   time.Duration

	// Queue settings
	QueueSize             int
	MaxConcurrentForwards int // max goroutines forwarding requests to agents simultaneously

	// Storage
	DiskDBPath             string        // path for the persistent BadgerDB instance (engineHistory)
	ResetDiskDB            bool          // if true, wipe DiskDBPath on startup (testing / schema migrations)
	FlushPeriod            time.Duration // interval for BadgerDB value-log GC on diskDB
	DiskDBTTLDays          int           // TTL for diskDB entries in days; 0 means no expiry
	UniversalFlushInterval time.Duration // how often universalHistory counters are flushed to diskDB

	// Session
	SessionTTL time.Duration

	// Security
	// JWTSecret is the shared HMAC-SHA256 key used to sign and verify agent JWTs.
	// Must match between router and all agents. Required — startup fails if empty.
	JWTSecret string

	// Cross-site redirect (internal/redirect, contract 3 v3). SiteID is this
	// router's site ID in the sibling map; empty (the default) disables
	// redirects entirely. RedirectSecrets holds the token signing secrets as
	// "id:secret" lines (parsed by redirect.ParseSecrets: the first signs,
	// every line verifies; required when SiteID is set). RedirectTokenTTL is
	// how long a token stays valid (60s: an overloaded client took up to ~46s
	// between the 307 and its follow-up). RedirectAllowedDomains lists the
	// host suffixes a sibling-map URL must end with (parsed by
	// redirect.ParseDomains; required when SiteID is set). Invalid values stop
	// startup.
	// Env: HIVENET_ROUTER_SITE_ID, HIVENET_ROUTER_REDIRECT_SECRETS
	// (comma-separated id:secret), HIVENET_ROUTER_REDIRECT_TOKEN_TTL,
	// HIVENET_ROUTER_REDIRECT_ALLOWED_DOMAINS (comma-separated).
	// Flag: --site-id, --redirect-secret-file (one id:secret per line),
	// --redirect-token-ttl, --redirect-allowed-domains.
	SiteID                 string
	RedirectSecrets        string
	RedirectTokenTTL       time.Duration
	RedirectAllowedDomains string

	// Hold-at-zero (internal/wake): a request for a model the sibling map
	// places on this site with scale_from_zero, while no healthy agent
	// serves it, is held until the model wakes. Needs SiteID. WakeHoldBudget
	// is the longest a request is held when its placement has no
	// hold_budget (5m: a large model can take several minutes to wake).
	// WakeHoldMax caps the requests held per model, WakeHoldMaxPerKey per
	// API key and model (so one key cannot take a model's whole share), and
	// WakeHoldMaxTotal across all models (which bounds the memory held
	// requests use); past any of them a request gets 503 model_waking at
	// once. WakeKeepAlive is the delay before, and interval between, the
	// keepalive bytes a held request receives (0 disables them). Invalid
	// values stop startup.
	// Env: HIVENET_ROUTER_WAKE_HOLD_BUDGET, HIVENET_ROUTER_WAKE_HOLD_MAX,
	// HIVENET_ROUTER_WAKE_HOLD_MAX_PER_KEY, HIVENET_ROUTER_WAKE_HOLD_MAX_TOTAL,
	// HIVENET_ROUTER_WAKE_KEEPALIVE.
	// Flag: --wake-hold-budget, --wake-hold-max, --wake-hold-max-per-key,
	// --wake-hold-max-total, --wake-keepalive.
	WakeHoldBudget    time.Duration
	WakeHoldMax       int
	WakeHoldMaxPerKey int
	WakeHoldMaxTotal  int
	WakeKeepAlive     time.Duration

	// MaxRequestBytes caps request body size on the /v1/* inference endpoints,
	// protecting the router from memory exhaustion by an oversized payload
	// (bodies are buffered before parsing). Default 10 MB.
	// Env: HIVENET_ROUTER_MAX_REQUEST_BYTES (value in bytes).
	MaxRequestBytes int64

	// Protocol
	ProtocolID string

	// Routing policy
	PolicyFile      string // path to routing policy YAML; empty = built-in default (least-loaded)
	PolicyModelDir  string // path to directory of per-model policy YAML files; empty = disabled
	MaxTriesPerStep int    // global default for steps that don't set max_tries
	// SemanticPinMax caps the semantic task pins held in memory;
	// SemanticPinMaxPerKey caps one API key's share so a single caller cannot
	// evict everyone else's pins. At either cap the least recently used pin is
	// evicted.
	// Env: HIVENET_ROUTER_SEMANTIC_PIN_MAX / HIVENET_ROUTER_SEMANTIC_PIN_MAX_PER_KEY
	// Flag: --semantic-pin-max / --semantic-pin-max-per-key
	SemanticPinMax       int
	SemanticPinMaxPerKey int
	// SemanticDecisionLog is the path of the JSONL log that records every semantic
	// alias decision. Empty = disabled.
	// Env: HIVENET_ROUTER_SEMANTIC_DECISION_LOG  Flag: --semantic-decision-log
	SemanticDecisionLog string
	// SemanticDecisionLogBuffer is how many decision records may queue before
	// new ones are dropped (counted in hivenet_semantic_decision_log_dropped_total).
	// Env: HIVENET_ROUTER_SEMANTIC_DECISION_LOG_BUFFER  Flag: --semantic-decision-log-buffer
	SemanticDecisionLogBuffer int

	// Per-model wait queue: max requests that park waiting for a slot before being rejected.
	// 0 disables the wait queue (any ErrNoCapacity immediately escalates to the fallback chain).
	QueueDepth int

	// AdmissionQueueDepth is the per-model queue of requests waiting for room
	// in the occupancy budget (B2). Negative (the default) uses QueueDepth.
	// Env: HIVENET_ROUTER_ADMISSION_QUEUE_DEPTH  Flag: --admission-queue-depth
	AdmissionQueueDepth int

	// AdmitFraction scales a model's KV token capacity into the occupancy admit
	// budget: a request is admitted only while the weighted in-flight token sum
	// stays within AdmitFraction × admit_budget_tokens. Below 1.0 it leaves head-
	// room for token-estimate error. The default is 0.90, safe now that the
	// learned per-model estimator (with true-up on backend usage) replaced the
	// len/4 heuristic that under-counted coding inputs — before that the fraction
	// was held at 0.85 to absorb the error. Range (0,1]; values outside clamp to 1.
	// Env: HIVENET_ROUTER_ADMIT_FRACTION
	AdmitFraction float64

	// AdmitFromEngineKV takes a model's occupancy budget (B2) from the KV-cache
	// size its engines report when the policy sets no admit_budget_tokens.
	// false leaves B2 off unless the policy sets it.
	// Env: HIVENET_ROUTER_ADMIT_FROM_ENGINE_KV (default true)
	AdmitFromEngineKV bool

	// DefaultMaxInflightPerReplica bounds the requests in flight per healthy
	// replica of a model that has no occupancy budget: its policy sets neither
	// admit_budget_tokens nor max_inflight and its engines report no KV-cache
	// size (Ollama, llama.cpp, custom). 0 leaves such models unbounded.
	// Env: HIVENET_ROUTER_DEFAULT_MAX_INFLIGHT_PER_REPLICA (default 8)
	DefaultMaxInflightPerReplica int

	// AdmitParkTimeout bounds how long an over-budget request waits for occupancy
	// to free before it is rejected with 429. 0 rejects immediately.
	// Env: HIVENET_ROUTER_ADMIT_PARK_TIMEOUT (a Go duration, e.g. "250ms").
	AdmitParkTimeout time.Duration

	// RPMBurstSeconds sizes the per-tenant RPM token bucket's burst as that many
	// seconds of the rate, instead of a full minute. 0 (default) keeps the legacy
	// full-minute burst; a short certified window (e.g. 10) stops a tenant from
	// spending its whole minute's quota in one instant — the anti-flood burst
	// control. Range [0,60); values outside keep the legacy full-minute burst.
	// Env: HIVENET_ROUTER_RPM_BURST_SECONDS
	RPMBurstSeconds int

	// Auth configuration file path.
	// Path to auth.yaml; if empty both /v1/* and /admin/* default to mode: none.
	// Env: HIVENET_ROUTER_AUTH_CONFIG
	AuthConfigFile string

	// AuthMode overrides the auth mode when no auth.yaml is provided.
	// Values: "none" (default), "api-key", "dynamic".
	// Env: HIVENET_ROUTER_AUTH_MODE
	AuthMode string

	// QuotaBackend selects the storage backend for per-tenant quota enforcement.
	//   "memory" (default) — in-process counters only; state is lost on restart.
	//   "badger"           — in-process counters flushed to BadgerDB diskDB;
	//                        token usage survives router restarts.
	// Env: HIVENET_ROUTER_QUOTA_BACKEND
	QuotaBackend string

	// Shared counters (router high availability). With SyncStore "valkey" the
	// routers of one site enforce each key's rate and daily limits
	// together through one Valkey; "" or "none" keeps every counter local to
	// this router. Excludes QuotaBackend "badger".
	//   SyncValkeyAddrs     comma-separated host:port list — Sentinel
	//                       addresses when SyncSentinelMaster is set, else
	//                       Valkey addresses
	//   SyncSentinelMaster  Sentinel master set name (empty: direct connection)
	//   SyncValkeyPassword  password for Valkey and Sentinel (flag: file)
	//   SyncValkeyTLS       connect with TLS
	//   SyncInterval        how often usage is published and read (default 1s)
	//   SyncMinRouters      routers assumed while the store is unreachable or
	//                       before it first answers (default 1; set it to the
	//                       number of routers of the site)
	//   RouterID            stable identity of this router (default: hostname)
	// Env: HIVENET_ROUTER_SYNC_STORE, HIVENET_ROUTER_SYNC_VALKEY_ADDRS,
	// HIVENET_ROUTER_SYNC_VALKEY_SENTINEL_MASTER, HIVENET_ROUTER_SYNC_VALKEY_PASSWORD,
	// HIVENET_ROUTER_SYNC_VALKEY_TLS, HIVENET_ROUTER_SYNC_INTERVAL, HIVENET_ROUTER_SYNC_MIN_ROUTERS,
	// HIVENET_ROUTER_ROUTER_ID
	SyncStore          string
	SyncValkeyAddrs    string
	SyncSentinelMaster string
	SyncValkeyPassword string
	SyncValkeyTLS      bool
	SyncValkeyTLSCA    string // PEM CA bundle for Valkey/Sentinel TLS; empty = system roots
	SyncInterval       time.Duration
	SyncMinRouters     int
	SyncSite           string // key prefix in the shared store; empty = SiteID, else "default"
	RouterID           string

	// KeyStoreBackend selects the persistence backend for the dynamic API key
	// registry (dynamic auth mode only; static keys live in auth.yaml).
	//   "memory" (default) — the registry is RAM-only; state is lost on
	//                        restart and rebuilt by the upstream key-sync
	//                        service (the router is a cache, never the source
	//                        of truth for keys).
	//   "badger"           — the registry is persisted to a dedicated BadgerDB
	//                        at KeyStorePath on every mutation and used as a
	//                        warm-start cache on startup (bounded by
	//                        KeyStoreMaxAge), so a restarted router serves keys
	//                        before the upstream key-sync service re-pushes.
	// Flag: --key-store   Env: HIVENET_ROUTER_KEY_STORE
	KeyStoreBackend string

	// KeyStorePath is the directory of the key-store BadgerDB. It is separate
	// from DiskDBPath so --reset-disk-db and --reset-key-store each wipe only
	// their own data. Created with mode 0700.
	// Flag: --key-store-path   Env: HIVENET_ROUTER_KEY_STORE_PATH
	KeyStorePath string

	// KeyStoreMaxAge bounds the staleness of a restored snapshot: one whose
	// last write or last key-sync confirmation (whichever is older) is older
	// than this, or that was never confirmed, is discarded at startup (the router starts
	// empty, as in memory mode). 0 disables the bound — only for deployments
	// with no upstream key-sync service, where the snapshot is the only copy.
	// Flag: --key-store-max-age   Env: HIVENET_ROUTER_KEY_STORE_MAX_AGE
	KeyStoreMaxAge time.Duration

	// KeyStoreSecret is the router-only secret the key-store record MAC key is
	// derived from (HKDF). Required when KeyStoreBackend is "badger", at least
	// 32 bytes, and must differ from JWTSecret (which every agent also holds).
	// Changing it discards the persisted registry (one cold start).
	// Flag: --key-store-secret-file   Env: HIVENET_ROUTER_KEY_STORE_SECRET
	KeyStoreSecret string

	// ResetKeyStore wipes the key-store directory on startup (use to recover
	// from an unreadable snapshot; only meaningful when KeyStoreBackend is
	// "badger"). One-shot: remove it after use.
	// Flag: --reset-key-store   Env: HIVENET_ROUTER_RESET_KEY_STORE
	ResetKeyStore bool

	// MultiTenant hardens the router for serving several customers: it turns
	// on RequireModelAllowlist, CacheSalt and HideAgentIdentity, and startup
	// refuses API or admin auth mode none and an unauthenticated /metrics on
	// every interface (see router.ApplyMultiTenant, ValidateIsolationConfig).
	// Flag: --multi-tenant   Env: HIVENET_ROUTER_MULTI_TENANT
	MultiTenant bool

	// RequireModelAllowlist makes a key with no model allowlist (no `models`
	// / `allowed_models` and no `quota.per_model`) unable to call or list any
	// model, instead of every model. For shared multi-tenant routers, where an
	// unrestricted key would reach models served for other customers.
	// Flag: --require-model-allowlist   Env: HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST
	RequireModelAllowlist bool

	// MetricsBearerToken, when set, makes /metrics require
	// "Authorization: Bearer <token>". The series name every tenant and key.
	// Flag: --metrics-bearer-token-file   Env: HIVENET_ROUTER_METRICS_BEARER_TOKEN
	MetricsBearerToken string

	// HideAgentIdentity leaves the agent's peer ID, organization, machine,
	// version and last-seen time out of the client-facing GET /v1/models/:model
	// (the admin view keeps them).
	// Flag: --hide-agent-identity   Env: HIVENET_ROUTER_HIDE_AGENT_IDENTITY
	HideAgentIdentity bool

	// CacheSalt sets a per-tenant cache_salt on every request forwarded to an
	// engine that supports it (vLLM) and strips any client-sent cache_salt, so
	// tenants never share prefix-cache blocks (prefix-cache timing side
	// channel). Requires CacheSaltSecret.
	// Flag: --cache-salt   Env: HIVENET_ROUTER_CACHE_SALT
	CacheSalt bool
	// CacheSaltSet records that CacheSalt was set explicitly (flag or env), so
	// --multi-tenant turns salting on by default but honours an explicit off.
	CacheSaltSet bool

	// CacheSaltSecret keys the per-tenant salt HMAC. At least 32 bytes, the same
	// on every router in front of the same agents, and never given to agents.
	// Flag: --cache-salt-secret-file   Env: HIVENET_ROUTER_CACHE_SALT_SECRET
	CacheSaltSecret string

	// EnvErrors collects invalid environment values that LoadFromEnv could
	// not apply and that must stop startup (see the key-store settings).
	EnvErrors []error

	// Closed-source provider configuration.
	// Credentials for providers that may be used as fallbacks when local routing steps are exhausted.
	// The routing policy (e.g., fallback_provider.engine) selects which provider to use; the order
	// of this slice does not control fallback behavior. API keys are supplied via env variables
	// (HIVENET_ROUTER_OPENAI_API_KEY, HIVENET_ROUTER_ANTHROPIC_API_KEY).
	Providers []ProviderConfig
}

// ProviderConfig holds credentials for one closed-source model provider.
type ProviderConfig struct {
	// Name is the provider identifier: "openai" or "anthropic".
	Name string
	// APIKey is the API key for the provider.
	APIKey string
}

// DefaultConfig returns sensible defaults
func DefaultConfig() *Config {
	return &Config{
		HTTPPort:         ":8080",
		GRPCPort:         ":50051",
		MetricsPort:      ":2112",
		P2PPort:          "9000",
		P2PListenAddr:    "127.0.0.1",
		P2PMaxConnsPerIP: 32, // > libp2p's default of 8; see field doc for the >= 2x fleet-size rule

		RequestTimeout:               60 * time.Second,
		HealthCheckInterval:          5 * time.Second,
		UnhealthyAfter:               15 * time.Second, // 3× HeartbeatInterval — tolerates one missed heartbeat
		RemoveAfter:                  30 * time.Second, // 6× HeartbeatInterval — removes after ~5 missed heartbeats
		HeartbeatInterval:            5 * time.Second,
		QueueSize:                    100,
		MaxConcurrentForwards:        50,
		DiskDBPath:                   "./badger_disk",
		KeyStoreBackend:              "memory",
		KeyStorePath:                 "./badger_keystore",
		KeyStoreMaxAge:               15 * time.Minute,
		FlushPeriod:                  5 * time.Second,
		DiskDBTTLDays:                30,
		UniversalFlushInterval:       30 * time.Second,
		SessionTTL:                   1 * time.Hour,
		ProtocolID:                   "/hivenet_router/1.0.0",
		MaxTriesPerStep:              3,
		SemanticPinMax:               100_000,
		SemanticPinMaxPerKey:         10_000,
		QueueDepth:                   30,
		AdmissionQueueDepth:          -1,
		MaxRequestBytes:              10 << 20, // 10 MB
		AdmitFraction:                0.90,     // learned estimator + true-up now cover the token-estimate error
		AdmitParkTimeout:             250 * time.Millisecond,
		AdmitFromEngineKV:            true,
		DefaultMaxInflightPerReplica: 8,
		SyncInterval:                 time.Second,
		RedirectTokenTTL:             60 * time.Second,
		WakeHoldBudget:               5 * time.Minute,
		WakeHoldMax:                  100,
		WakeHoldMaxPerKey:            20,
		WakeHoldMaxTotal:             1000,
		WakeKeepAlive:                15 * time.Second,

		// Semantic decision log queue (the log itself is off unless a path is set).
		SemanticDecisionLogBuffer: 4096,
	}
}

// LoadFromEnv overrides the default configuration with values from environment variables.
// This allows runtime customization without code changes.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if v := os.Getenv("HIVENET_ROUTER_HTTP_PORT"); v != "" {
		cfg.HTTPPort = v
	}
	if v := os.Getenv("HIVENET_ROUTER_GRPC_PORT"); v != "" {
		cfg.GRPCPort = v
	}
	if v := os.Getenv("HIVENET_ROUTER_METRICS_PORT"); v != "" {
		cfg.MetricsPort = v
	}
	if v := os.Getenv("HIVENET_ROUTER_P2P_PORT"); v != "" {
		cfg.P2PPort = v
	}
	if v := os.Getenv("HIVENET_ROUTER_P2P_MAX_CONNS_PER_IP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.P2PMaxConnsPerIP = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_QUEUE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.QueueSize = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_DISK_DB_PATH"); v != "" {
		cfg.DiskDBPath = v
	}
	if v := os.Getenv("HIVENET_ROUTER_REQUEST_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.RequestTimeout = d
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_POLICY_FILE"); v != "" {
		cfg.PolicyFile = v
	}
	if v := os.Getenv("HIVENET_ROUTER_POLICY_MODEL_DIR"); v != "" {
		cfg.PolicyModelDir = v
	}
	if v := os.Getenv("HIVENET_ROUTER_SEMANTIC_DECISION_LOG"); v != "" {
		cfg.SemanticDecisionLog = v
	}
	for env, dst := range map[string]*int{
		"HIVENET_ROUTER_SEMANTIC_PIN_MAX":             &cfg.SemanticPinMax,
		"HIVENET_ROUTER_SEMANTIC_PIN_MAX_PER_KEY":     &cfg.SemanticPinMaxPerKey,
		"HIVENET_ROUTER_SEMANTIC_DECISION_LOG_BUFFER": &cfg.SemanticDecisionLogBuffer,
	} {
		if v := os.Getenv(env); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				*dst = n
			}
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_MAX_TRIES_PER_STEP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxTriesPerStep = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_QUEUE_DEPTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.QueueDepth = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_ADMISSION_QUEUE_DEPTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.AdmissionQueueDepth = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_ADMIT_FRACTION"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 1 {
			cfg.AdmitFraction = f
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_ADMIT_FROM_ENGINE_KV"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_ADMIT_FROM_ENGINE_KV=%q: %w", v, err))
		} else {
			cfg.AdmitFromEngineKV = b
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_DEFAULT_MAX_INFLIGHT_PER_REPLICA"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_DEFAULT_MAX_INFLIGHT_PER_REPLICA=%q: must be 0 or more", v))
		} else {
			cfg.DefaultMaxInflightPerReplica = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_ADMIT_PARK_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.AdmitParkTimeout = d
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_RPM_BURST_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n < 60 {
			cfg.RPMBurstSeconds = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_SESSION_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SessionTTL = d
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_JWT_SECRET"); v != "" {
		cfg.JWTSecret = v
	}
	// Cross-site redirect values are validated strictly (like the key store):
	// a typo must stop startup, not silently disable or weaken redirects.
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_SITE_ID")); v != "" {
		cfg.SiteID = v
	}
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_REDIRECT_SECRETS")); v != "" {
		// One id:secret per comma here; redirect.ParseSecrets reads lines.
		cfg.RedirectSecrets = strings.ReplaceAll(v, ",", "\n")
	}
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_REDIRECT_TOKEN_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_REDIRECT_TOKEN_TTL=%q: %w", v, err))
		case d <= 0:
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_REDIRECT_TOKEN_TTL=%q: must be > 0", v))
		default:
			cfg.RedirectTokenTTL = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_REDIRECT_ALLOWED_DOMAINS")); v != "" {
		cfg.RedirectAllowedDomains = v
	}
	// Hold-at-zero values are validated strictly too.
	for env, dst := range map[string]struct {
		d         *time.Duration
		allowZero bool
	}{
		"HIVENET_ROUTER_WAKE_HOLD_BUDGET": {&cfg.WakeHoldBudget, false},
		"HIVENET_ROUTER_WAKE_KEEPALIVE":   {&cfg.WakeKeepAlive, true},
	} {
		v := strings.TrimSpace(os.Getenv(env))
		if v == "" {
			continue
		}
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("%s=%q: %w", env, v, err))
		case d < 0 || (d == 0 && !dst.allowZero):
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("%s=%q: must be > 0", env, v))
		default:
			*dst.d = d
		}
	}
	for env, dst := range map[string]*int{
		"HIVENET_ROUTER_WAKE_HOLD_MAX":         &cfg.WakeHoldMax,
		"HIVENET_ROUTER_WAKE_HOLD_MAX_PER_KEY": &cfg.WakeHoldMaxPerKey,
		"HIVENET_ROUTER_WAKE_HOLD_MAX_TOTAL":   &cfg.WakeHoldMaxTotal,
	} {
		v := strings.TrimSpace(os.Getenv(env))
		if v == "" {
			continue
		}
		if n, err := strconv.Atoi(v); err != nil || n <= 0 {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("%s=%q: must be an integer > 0", env, v))
		} else {
			*dst = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_MAX_REQUEST_BYTES"); v != "" {
		// n == 0 is a valid value meaning "disabled"; only reject negatives.
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cfg.MaxRequestBytes = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_AUTH_CONFIG"); v != "" {
		cfg.AuthConfigFile = v
	}
	if v := os.Getenv("HIVENET_ROUTER_AUTH_MODE"); v != "" {
		cfg.AuthMode = v
	}
	if v := os.Getenv("HIVENET_ROUTER_QUOTA_BACKEND"); v != "" {
		cfg.QuotaBackend = v
	}
	cfg.SyncStore = os.Getenv("HIVENET_ROUTER_SYNC_STORE")
	cfg.SyncValkeyAddrs = os.Getenv("HIVENET_ROUTER_SYNC_VALKEY_ADDRS")
	cfg.SyncSentinelMaster = os.Getenv("HIVENET_ROUTER_SYNC_VALKEY_SENTINEL_MASTER")
	cfg.SyncValkeyPassword = os.Getenv("HIVENET_ROUTER_SYNC_VALKEY_PASSWORD")
	cfg.RouterID = os.Getenv("HIVENET_ROUTER_ROUTER_ID")
	cfg.SyncValkeyTLSCA = os.Getenv("HIVENET_ROUTER_SYNC_VALKEY_TLS_CA_FILE")
	if v := os.Getenv("HIVENET_ROUTER_SYNC_VALKEY_TLS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_SYNC_VALKEY_TLS=%q: %w", v, err))
		}
		cfg.SyncValkeyTLS = b
	}
	if v := os.Getenv("HIVENET_ROUTER_SYNC_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_SYNC_INTERVAL=%q: %w", v, err))
		} else {
			cfg.SyncInterval = d
		}
	}
	cfg.SyncSite = os.Getenv("HIVENET_ROUTER_SYNC_SITE")
	if v := os.Getenv("HIVENET_ROUTER_SYNC_MIN_ROUTERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_SYNC_MIN_ROUTERS=%q: must be a positive integer", v))
		} else {
			cfg.SyncMinRouters = n
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_KEY_STORE"); v != "" {
		cfg.KeyStoreBackend = v
	}
	if v := os.Getenv("HIVENET_ROUTER_KEY_STORE_PATH"); v != "" {
		cfg.KeyStorePath = v
	}
	// Key-store values are validated strictly: the max age is a security
	// bound, so a typo ("15min") must stop startup rather than silently fall
	// back to the default. Errors are collected in EnvErrors.
	if v := os.Getenv("HIVENET_ROUTER_KEY_STORE_MAX_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_KEY_STORE_MAX_AGE=%q: %w", v, err))
		case d < 0:
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_KEY_STORE_MAX_AGE=%q: must be >= 0", v))
		default:
			cfg.KeyStoreMaxAge = d
		}
	}
	if v := os.Getenv("HIVENET_ROUTER_RESET_KEY_STORE"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("HIVENET_ROUTER_RESET_KEY_STORE=%q: %w", v, err))
		} else {
			cfg.ResetKeyStore = b
		}
	}
	// Trimmed like --key-store-secret-file, so the same secret yields the same
	// MAC key whichever source provides it.
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_KEY_STORE_SECRET")); v != "" {
		cfg.KeyStoreSecret = v
	}
	envBool(cfg, "HIVENET_ROUTER_MULTI_TENANT", &cfg.MultiTenant)
	envBool(cfg, "HIVENET_ROUTER_REQUIRE_MODEL_ALLOWLIST", &cfg.RequireModelAllowlist)
	// Only a value that parses counts as set: an empty variable must not opt
	// a multi-tenant router out of prefix-cache isolation.
	cfg.CacheSaltSet = envBool(cfg, "HIVENET_ROUTER_CACHE_SALT", &cfg.CacheSalt)
	envBool(cfg, "HIVENET_ROUTER_HIDE_AGENT_IDENTITY", &cfg.HideAgentIdentity)
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_METRICS_BEARER_TOKEN")); v != "" {
		cfg.MetricsBearerToken = v
	}
	// Trimmed like --cache-salt-secret-file, so either source gives the same salts.
	if v := strings.TrimSpace(os.Getenv("HIVENET_ROUTER_CACHE_SALT_SECRET")); v != "" {
		cfg.CacheSaltSecret = v
	}
	if v := os.Getenv("HIVENET_ROUTER_OPENAI_API_KEY"); v != "" {
		cfg.Providers = append(cfg.Providers, ProviderConfig{Name: "openai", APIKey: v})
	}
	if v := os.Getenv("HIVENET_ROUTER_ANTHROPIC_API_KEY"); v != "" {
		cfg.Providers = append(cfg.Providers, ProviderConfig{Name: "anthropic", APIKey: v})
	}

	return cfg
}

// RouterList returns the routers the agent connects to: Routers, or the
// single RouterGRPCAddr when Routers is empty. Duplicates are dropped.
func (c *AgentConfig) RouterList() []string {
	list := c.Routers
	if len(list) == 0 && c.RouterGRPCAddr != "" {
		list = []string{c.RouterGRPCAddr}
	}
	seen := map[string]bool{}
	var out []string
	for _, r := range list {
		if r = strings.TrimSpace(r); r != "" && !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	return out
}

// envBool sets *dst from the boolean environment variable name when it is
// set and not empty, collecting an unparsable value in cfg.EnvErrors (which
// stops startup). It reports whether it assigned a value.
func envBool(cfg *Config, name string, dst *bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		cfg.EnvErrors = append(cfg.EnvErrors, fmt.Errorf("%s=%q: %w", name, v, err))
		return false
	}
	*dst = b
	return true
}

// AgentConfig holds configuration specific to agents
type AgentConfig struct {
	Model          string // Model name override; auto-detected if empty
	HealthURL      string // Custom health endpoint (required for engine=custom)
	Capacity       int
	Version        string
	Region         string
	Tags           []string
	Organization   string // Cloud provider / compute provider (e.g. AWS, OVH, Hivenet Compute)
	Machine        string // Machine name within the organization
	RouterGRPCAddr string
	RouterP2PAddr  string
	// Routers lists the gRPC addresses of every router of the site; the agent
	// keeps one independent session with each. Empty means [RouterGRPCAddr].
	Routers    []string
	BackendURL string
	// BackendAPIKey, when non-empty, is sent on every request the agent makes
	// to its backend (health checks, model discovery, metrics, inference) as
	// Authorization: Bearer <key>, replacing the client's credentials; any
	// x-api-key header is dropped. Use it when the backend requires its own key,
	// e.g. vLLM, SGLang or llama.cpp started with --api-key. The key is sent
	// only to the BackendURL and HealthURL hosts; a request to any other host
	// (such as a redirect) goes out with no Authorization header.
	// Empty (default) forwards the client's headers unchanged. Surrounding
	// whitespace is trimmed from both the env value and the file contents.
	// Env: HIVENET_ROUTER_BACKEND_API_KEY  Flag: --backend-api-key-file
	BackendAPIKey string
	Engine        string
	HTTPTimeout   time.Duration
	// StreamWriteIdleTimeout bounds how long a single streaming-response chunk may
	// block while being written back to the router. Applied as a rolling per-chunk
	// deadline so a reader that stops reading cannot leave the agent's write blocked
	// forever (which leaks libp2p streams and eventually exhausts the resource manager).
	// 0 disables it.
	StreamWriteIdleTimeout time.Duration
	IdentityPath           string // Path to the persistent libp2p private key file (Ed25519)

	// P2PListenPort is the local port of the agent's libp2p node, bound to
	// 127.0.0.1 only (0 = random). The agent needs no inbound port: it dials the
	// router, and the router sends inference back over that connection.
	P2PListenPort int

	// HardwareSampleInterval controls how often the background sampler collects
	// GPU/CPU/memory readings. The latest snapshot is included in every heartbeat.
	HardwareSampleInterval time.Duration

	// EngineSampleInterval controls how often the fast poller scrapes the engine's
	// metrics endpoint (e.g. vLLM /metrics).
	EngineSampleInterval time.Duration

	// JWTSecret is the shared HMAC-SHA256 key used to sign agent JWTs presented
	// to the router at authentication time. Must match the router's JWTSecret.
	JWTSecret string

	// RoutingSignalInterval controls how often the agent pushes fresh engine and
	// hardware metrics to the router's /routing-signals endpoint, independently
	// of the heartbeat. This is the primary freshness knob for routing decisions.
	//
	// The push always carries the latest cached snapshot. If RoutingSignalInterval
	// is shorter than a sample interval, the same snapshot is pushed multiple times
	// until a new one is collected — this is harmless. Set this to EngineSampleInterval
	// (500ms) so engine metrics reach the router without additional delay.
	RoutingSignalInterval time.Duration

	// GPUDevicesFile is an optional path to a file containing the GPU UUIDs the
	// engine was assigned (one UUID per line or comma-separated). When set, the
	// hardware collector restricts NVML metrics to only those GPUs, ignoring any
	// other GPUs visible on the node. Leave empty to report all visible GPUs
	// (default bare-metal behaviour).
	GPUDevicesFile string

	// GPUModel is an operator-supplied hardware identifier used as routing metadata
	// (e.g. "RTX4090", "RTX5090"). Sent to the router at authentication time and
	// available as a match filter in routing policy YAML.
	// Env: HIVENET_ROUTER_GPU_MODEL  Flag: --gpu-model
	GPUModel string

	// DeploymentID identifies the logical deployment this agent serves. Supplied
	// by whatever schedules the agent and included in the /register payload so
	// the router can stamp deployment_id on routed metrics.
	// Empty when the agent is run directly (local dev, bare-metal).
	// Env: HIVENET_ROUTER_DEPLOYMENT_ID  Flag: --deployment-id
	DeploymentID string

	// ReplicaID identifies this specific replica of DeploymentID. Sent in the
	// /register payload so the router surfaces it on /admin/routing-table and
	// emits it on the registration change feed. Together the two form a stable
	// join key an external scheduler can use to map a router row back to the
	// workload it started. Empty when the agent is run directly.
	// Env: HIVENET_ROUTER_REPLICA_ID  Flag: --replica-id
	ReplicaID string

	// HideLLM controls whether this agent is excluded from the /v1/models listing.
	// When true, the agent still accepts requests but does not appear to clients
	// browsing the model catalogue.
	HideLLM bool

	// LLMPrettyName is a human-readable display name for the model served by this
	// agent (e.g. "xLAM 8B"). Shown as-is in /v1/models responses.
	LLMPrettyName string

	// LLMInfo is a short free-text description of the model's capabilities or
	// intended use (e.g. "A small model useful for FC"). Shown in /v1/models.
	LLMInfo string

	// Capability declares what type of inference this agent serves.
	// Valid values: "llm" (default), "embedding", "reranker".
	// Controls which libp2p endpoint the agent registers (/v1/chat/completions,
	// /v1/embeddings, or /v1/rerank) and how the router routes requests to it.
	Capability string
}

// BuildVersion is the agent version, injected at build time via
//
//	-ldflags="-X hivenet_router/internal/config.BuildVersion=<sha>"
//
// Falls back to "dev" for local builds.
var BuildVersion = "dev"

// DefaultAgentConfig returns sensible agent defaults
func DefaultAgentConfig() *AgentConfig {
	return &AgentConfig{
		JWTSecret:              os.Getenv("HIVENET_ROUTER_JWT_SECRET"),
		BackendAPIKey:          strings.TrimSpace(os.Getenv("HIVENET_ROUTER_BACKEND_API_KEY")),
		Model:                  "", // Auto-detected from backend if empty
		Capacity:               10,
		Version:                BuildVersion,
		Region:                 "UE-France",
		Tags:                   []string{"production"},
		Organization:           "Unknown",
		Machine:                "Unknown",
		RouterGRPCAddr:         "localhost:50051",
		RouterP2PAddr:          "/ip4/127.0.0.1/tcp/9000",
		BackendURL:             "http://localhost:8888",
		Engine:                 "vllm",
		Capability:             "llm",
		HTTPTimeout:            120 * time.Second,
		StreamWriteIdleTimeout: 60 * time.Second,
		IdentityPath:           "./agent_identity.key",
		HardwareSampleInterval: 2 * time.Second,
		EngineSampleInterval:   500 * time.Millisecond,
		RoutingSignalInterval:  500 * time.Millisecond,
	}
}
