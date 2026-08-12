// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package config

import (
	"os"
	"strconv"
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

	// Per-model wait queue: max requests that park waiting for a slot before being rejected.
	// 0 disables the wait queue (any ErrNoCapacity immediately escalates to the fallback chain).
	QueueDepth int

	// AdmitFraction scales a model's KV token capacity into the occupancy admit
	// budget: a request is admitted only while the weighted in-flight token sum
	// stays within AdmitFraction × admit_budget_tokens. Below 1.0 it leaves head-
	// room for token-estimate error. The default is 0.90, safe now that the
	// learned per-model estimator (with true-up on backend usage) replaced the
	// len/4 heuristic that under-counted coding inputs — before that the fraction
	// was held at 0.85 to absorb the error. Range (0,1]; values outside clamp to 1.
	// Env: HIVENET_ROUTER_ADMIT_FRACTION
	AdmitFraction float64

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

		RequestTimeout:         60 * time.Second,
		HealthCheckInterval:    5 * time.Second,
		UnhealthyAfter:         15 * time.Second, // 3× HeartbeatInterval — tolerates one missed heartbeat
		RemoveAfter:            30 * time.Second, // 6× HeartbeatInterval — removes after ~5 missed heartbeats
		HeartbeatInterval:      5 * time.Second,
		QueueSize:              100,
		MaxConcurrentForwards:  50,
		DiskDBPath:             "./badger_disk",
		FlushPeriod:            5 * time.Second,
		DiskDBTTLDays:          30,
		UniversalFlushInterval: 30 * time.Second,
		SessionTTL:             1 * time.Hour,
		ProtocolID:             "/hivenet_router/1.0.0",
		MaxTriesPerStep:        3,
		QueueDepth:             30,
		MaxRequestBytes:        10 << 20, // 10 MB
		AdmitFraction:          0.90,     // learned estimator + true-up now cover the token-estimate error
		AdmitParkTimeout:       250 * time.Millisecond,
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
	if v := os.Getenv("HIVENET_ROUTER_ADMIT_FRACTION"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 1 {
			cfg.AdmitFraction = f
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
	if v := os.Getenv("HIVENET_ROUTER_OPENAI_API_KEY"); v != "" {
		cfg.Providers = append(cfg.Providers, ProviderConfig{Name: "openai", APIKey: v})
	}
	if v := os.Getenv("HIVENET_ROUTER_ANTHROPIC_API_KEY"); v != "" {
		cfg.Providers = append(cfg.Providers, ProviderConfig{Name: "anthropic", APIKey: v})
	}

	return cfg
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
	BackendURL     string
	Engine         string
	HTTPTimeout    time.Duration
	// StreamWriteIdleTimeout bounds how long a single streaming-response chunk may
	// block while being written back to the router. Applied as a rolling per-chunk
	// deadline so a reader that stops reading cannot leave the agent's write blocked
	// forever (which leaks libp2p streams and eventually exhausts the resource manager).
	// 0 disables it.
	StreamWriteIdleTimeout time.Duration
	IdentityPath           string // Path to the persistent libp2p private key file (Ed25519)

	// P2PListenPort is the TCP port the agent's libp2p node listens on.
	// Default 0 lets the OS pick a random port (fine for bare-metal).
	// Set to a fixed value when running in Docker so the port can be mapped.
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
