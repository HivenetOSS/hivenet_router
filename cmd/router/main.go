// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package main is the entry point for the Hivenet Router.
//
// The router is the central orchestration hub of the Hivenet Router.
// It exposes three network interfaces:
//   - HTTP API (default :8080) — OpenAI-compatible client endpoint
//   - gRPC (default :50051) — Agent authentication and session bootstrapping
//   - libp2p (default :9000) — Persistent P2P data plane for agent communication
//
// Usage:
//
//	./hivenet-router [flags]
//	./hivenet-router --http-port :8080 --grpc-port :50051 --p2p-port 9000
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"hivenet_router/internal/api"
	"hivenet_router/internal/config"
	"hivenet_router/internal/logger"
	"hivenet_router/internal/router"
	"hivenet_router/internal/tracing"
)

var log = logging.Logger("router")

func main() {
	// Handle "keygen" subcommand before any other setup.
	if len(os.Args) > 1 && os.Args[1] == "keygen" {
		runKeygen(os.Args[2:])
		return
	}

	logger.Setup(nil)

	// Load configuration: env vars first (via LoadFromEnv), then CLI flags override below.
	// This establishes the precedence chain: CLI flag > env var > built-in default.
	cfg := config.LoadFromEnv()

	// Define CLI flags. Each flag falls back to the default from config if not provided.
	// This allows zero-config startup for local development while remaining configurable for production.
	httpPort := flag.String("http-port", cfg.HTTPPort, "HTTP API port")
	grpcPort := flag.String("grpc-port", cfg.GRPCPort, "gRPC auth port")
	p2pPort := flag.String("p2p-port", cfg.P2PPort, "libp2p port")
	p2pListenAddr := flag.String("p2p-listen-addr", cfg.P2PListenAddr, "libp2p listen address (e.g., 127.0.0.1 or 0.0.0.0)")
	p2pAnnounceAddr := flag.String("p2p-announce-addr", cfg.P2PAnnounceAddr, "libp2p address advertised to agents (required behind NAT/Docker, e.g. /dns4/my-host.example.com/tcp/8903)")
	p2pMaxConnsPerIP := flag.Int("p2p-max-conns-per-ip", cfg.P2PMaxConnsPerIP, "Max concurrent libp2p connections accepted from a single source IP. Raise above libp2p's default of 8 when many agents share one NAT/egress IP; keep >= 2x the fleet size")
	queueSize := flag.Int("queue-size", cfg.QueueSize, "Request queue size")
	requestTimeout := flag.Duration("request-timeout", cfg.RequestTimeout, "Request timeout")
	healthCheckInterval := flag.Duration("health-check-interval", cfg.HealthCheckInterval, "How often the health monitor runs (check frequency)")
	unhealthyAfter := flag.Duration("unhealthy-after", cfg.UnhealthyAfter, "Mark agent unhealthy after this long without a heartbeat (must be > heartbeat-interval)")
	removeAfter := flag.Duration("remove-after", cfg.RemoveAfter, "Remove agent after this long without a heartbeat (must be > unhealthy-after)")
	heartbeatInterval := flag.Duration("heartbeat-interval", cfg.HeartbeatInterval, "Heartbeat interval")
	diskDBPath := flag.String("disk-db-path", cfg.DiskDBPath, "Path for persistent BadgerDB (engineHistory)")
	resetDiskDB := flag.Bool("reset-disk-db", false, "Wipe the persistent disk BadgerDB on startup (use for testing or schema migrations)")
	diskDBTTLDays := flag.Int("disk-db-ttl", cfg.DiskDBTTLDays, "TTL for diskDB entries in days (0 = no expiry)")
	universalFlushInterval := flag.Duration("universal-flush-interval", cfg.UniversalFlushInterval, "How often universal history counters are flushed to diskDB")
	metricsPort := flag.String("metrics-port", cfg.MetricsPort, "Prometheus metrics port")
	maxConcurrentForwards := flag.Int("max-concurrent", cfg.MaxConcurrentForwards, "Max concurrent in-flight request forwards to agents")
	policyFile := flag.String("policy-file", cfg.PolicyFile, "Path to routing policy YAML file (optional; omit to use built-in default)")
	policyModelDir := flag.String("policy-model-dir", cfg.PolicyModelDir, "Path to directory of per-model policy YAML files (optional; env: HIVENET_ROUTER_POLICY_MODEL_DIR)")
	maxTriesPerStep := flag.Int("max-tries-per-step", cfg.MaxTriesPerStep, "Default maximum forward attempts per policy step (used when a step does not set max_tries)")
	defaultQueueDepth := flag.Int("queue-depth", cfg.QueueDepth, "Max concurrent waiters per model in the capacity wait queue; 0 disables the queue")
	sessionTTL := flag.Duration("session-ttl", cfg.SessionTTL, "Session token TTL (must be > 5m; env: HIVENET_ROUTER_SESSION_TTL)")
	jwtSecretFile := flag.String("jwt-secret-file", "", "Path to file containing the HMAC-SHA256 secret (env: HIVENET_ROUTER_JWT_SECRET)")
	authConfigFile := flag.String("auth-config-file", cfg.AuthConfigFile, "Path to auth.yaml (env: HIVENET_ROUTER_AUTH_CONFIG)")

	flag.Parse()

	// Apply parsed flag values back into the config struct.
	// The config struct is the single source of truth passed to all router subsystems.
	cfg.HTTPPort = *httpPort
	cfg.GRPCPort = *grpcPort
	cfg.P2PPort = *p2pPort
	cfg.P2PListenAddr = *p2pListenAddr
	cfg.P2PAnnounceAddr = *p2pAnnounceAddr
	cfg.P2PMaxConnsPerIP = *p2pMaxConnsPerIP
	cfg.QueueSize = *queueSize
	cfg.RequestTimeout = *requestTimeout
	cfg.HealthCheckInterval = *healthCheckInterval
	cfg.UnhealthyAfter = *unhealthyAfter
	cfg.RemoveAfter = *removeAfter
	cfg.HeartbeatInterval = *heartbeatInterval
	cfg.DiskDBPath = *diskDBPath
	cfg.ResetDiskDB = *resetDiskDB
	cfg.DiskDBTTLDays = *diskDBTTLDays
	cfg.UniversalFlushInterval = *universalFlushInterval
	cfg.MetricsPort = *metricsPort
	cfg.MaxConcurrentForwards = *maxConcurrentForwards
	cfg.PolicyFile = *policyFile
	cfg.PolicyModelDir = *policyModelDir
	if *maxTriesPerStep < 1 {
		log.Fatalf("--max-tries-per-step must be >= 1, got %d", *maxTriesPerStep)
	}
	cfg.MaxTriesPerStep = *maxTriesPerStep
	if *defaultQueueDepth < 0 {
		log.Fatalf("--queue-depth must be >= 0, got %d", *defaultQueueDepth)
	}
	cfg.QueueDepth = *defaultQueueDepth
	if *sessionTTL <= 5*time.Minute {
		log.Fatalf("--session-ttl must be greater than 5m (re-auth safety margin), got %v", *sessionTTL)
	}
	cfg.SessionTTL = *sessionTTL
	if *jwtSecretFile != "" {
		data, err := os.ReadFile(*jwtSecretFile)
		if err != nil {
			log.Fatalf("cannot read --jwt-secret-file %q: %v", *jwtSecretFile, err)
		}
		cfg.JWTSecret = strings.TrimSpace(string(data))
	}
	if cfg.JWTSecret == "" {
		log.Fatal("JWT secret is required — set HIVENET_ROUTER_JWT_SECRET or use --jwt-secret-file")
	}
	cfg.AuthConfigFile = *authConfigFile

	// Log startup banner with resolved configuration for operator visibility.
	log.Info("Hivenet Router")
	log.Info("=================")
	log.Info("Network:")
	log.Infof("  HTTP API:            %s", cfg.HTTPPort)
	log.Infof("  gRPC Auth:           %s", cfg.GRPCPort)
	log.Infof("  libp2p port:         %s", cfg.P2PPort)
	log.Infof("  libp2p listen addr:  %s", cfg.P2PListenAddr)
	if cfg.P2PAnnounceAddr != "" {
		log.Infof("  libp2p announce:     %s", cfg.P2PAnnounceAddr)
	}
	log.Infof("  libp2p max conns/IP: %d", cfg.P2PMaxConnsPerIP)
	log.Infof("  Metrics (Prometheus):%s", cfg.MetricsPort)
	log.Info("Queue & Timeouts:")
	log.Infof("  Queue size:          %d", cfg.QueueSize)
	log.Infof("  Max concurrent fwd:  %d", cfg.MaxConcurrentForwards)
	log.Infof("  Request timeout:     %v", cfg.RequestTimeout)
	log.Info("Health & Heartbeat:")
	log.Infof("  Health check freq:   %v", cfg.HealthCheckInterval)
	log.Infof("  Unhealthy after:     %v", cfg.UnhealthyAfter)
	log.Infof("  Remove after:        %v", cfg.RemoveAfter)
	log.Infof("  Heartbeat interval:  %v", cfg.HeartbeatInterval)
	log.Info("Storage:")
	log.Infof("  Disk DB path:        %s", cfg.DiskDBPath)
	if cfg.DiskDBTTLDays == 0 {
		log.Info("  Disk DB TTL:         disabled")
	} else {
		log.Infof("  Disk DB TTL:         %d days", cfg.DiskDBTTLDays)
	}
	log.Infof("  GC flush period:     %v", cfg.FlushPeriod)
	log.Infof("  Universal flush:     %v", cfg.UniversalFlushInterval)
	log.Infof("  Reset disk DB:       %v", cfg.ResetDiskDB)
	log.Info("Security:")
	log.Info("  JWT secret:          configured")
	log.Info("Session:")
	log.Infof("  Session TTL:         %v", cfg.SessionTTL)
	log.Info("Routing Policy:")
	if cfg.PolicyFile != "" {
		log.Infof("  Policy file:         %s", cfg.PolicyFile)
	} else {
		log.Info("  Policy file:         (built-in default: least-loaded, no filter, no fallback)")
	}
	if cfg.PolicyModelDir != "" {
		log.Infof("  Policy model dir:    %s", cfg.PolicyModelDir)
	} else {
		log.Info("  Policy model dir:    (not configured)")
	}
	log.Infof("  Max tries per step:  %d", cfg.MaxTriesPerStep)
	if cfg.QueueDepth == 0 {
		log.Info("  Queue depth/model:   disabled")
	} else {
		log.Infof("  Queue depth/model:   %d", cfg.QueueDepth)
	}
	log.Info("Auth:")
	if cfg.AuthConfigFile != "" {
		log.Infof("  Auth config file:    %s", cfg.AuthConfigFile)
	} else {
		log.Info("  Auth config file:    (not configured — both /v1/* and /admin/* default to mode: none)")
	}
	log.Info("  Admin API keys:      set via HIVENET_ROUTER_ADMIN_API_KEYS env var")
	log.Info("=================")

	// Initialize OpenTelemetry tracing.
	// Reads OTEL_EXPORTER_OTLP_ENDPOINT env var; noop when unset.
	shutdownTracer, err := tracing.Init(context.Background(), "hivenet-router", "1.0.0")
	if err != nil {
		log.Fatalf("Failed to initialize tracing: %v", err)
	}
	defer func() {
		if err := shutdownTracer(context.Background()); err != nil {
			log.Errorf("Error shutting down tracer: %v", err)
		}
	}()
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Infof("Tracing enabled — exporting to %s", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	} else {
		log.Info("Tracing disabled — set OTEL_EXPORTER_OTLP_ENDPOINT to enable")
	}

	// Initialize the router with all subsystems: registry, selector, processor, session manager, storage.
	// This does NOT start listening yet — that happens in r.Start().
	r, err := router.New(cfg)
	if err != nil {
		log.Errorf("Failed to create router: %v", err)
		os.Exit(1)
	}

	// Set up graceful shutdown handler.
	// On SIGINT (Ctrl+C) or SIGTERM (kill), close the router cleanly:
	// this shuts down the libp2p host (disconnecting all agents) and releases storage resources.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info("Received shutdown signal...")
		r.Close()
		api.SyncAuditLog()
		os.Exit(0)
	}()

	// Start the router. This is a blocking call that:
	// 1. Creates the libp2p host and sets the stream handler
	// 2. Starts the gRPC auth server in a goroutine
	// 3. Starts the health monitor in a goroutine
	// 4. Starts the request processor in a goroutine
	// 5. Starts the HTTP API server in a goroutine
	// 6. Blocks forever with select{}
	if err := r.Start(); err != nil {
		log.Errorf("Router error: %v", err)
		os.Exit(1)
	}
}
