// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/api"
	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/provider"
	"hivenet_router/internal/storage"
	"hivenet_router/internal/tokenizer"
	"hivenet_router/internal/transport/grpc"
	"hivenet_router/internal/transport/p2p"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	p2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"github.com/multiformats/go-multiaddr"
)

// routerProtocolID is the libp2p HTTP protocol ID for all router management
// endpoints (agent registration and heartbeats). Agents discover its URL
// prefix via the /.well-known/libp2p resource and use NamespacedClient to
// call it, so there is no need for separate protocol IDs per endpoint.
const routerProtocolID = "/hivenet_router/router/1.0.0"

// inferenceProtocolID is the protocol ID exposed by agents for inference
// requests. The router uses NamespacedClient to forward chat completions.
const inferenceProtocolID = "/hivenet_router/inference/1.0.0"

// connProtectTag marks registered agents' connections as protected in the
// libp2p connection manager. The router never dials agents — every inference
// stream rides the connection the agent opened — so losing that connection to
// a connmgr trim would make the agent unreachable until it re-registers.
const connProtectTag = "registered-agent"

// agentRegistrationPayload is the JSON body sent by an agent on registration.
// The router never dials agents (inference streams reuse the agent-initiated
// connection), so no dial addresses are exchanged.
type agentRegistrationPayload struct {
	SessionToken string `json:"session_token"`
	PeerID       string `json:"peer_id"`
	DeploymentID string `json:"deployment_id,omitempty"` // logical deployment id; empty when the agent was not given one
	ReplicaID    string `json:"replica_id,omitempty"`    // replica id; empty when the agent was not given one
}

// agentRoutingSignalPayload is the JSON body sent by the agent every RoutingSignalInterval
// to /routing-signals. It carries only instantaneous routing-critical snapshots.
// It does NOT update LastSeen or health state — the heartbeat owns those concerns.
type agentRoutingSignalPayload struct {
	SessionToken   string                   `json:"session_token"`
	BackendMetrics *domain.BackendMetrics   `json:"backend_metrics,omitempty"`
	Hardware       *domain.HardwareSnapshot `json:"hardware,omitempty"`
}

// agentHeartbeatPayload is the JSON body sent by an agent on each heartbeat.
// Hardware is absent only before the agent's first sample completes (startup
// window up to one HardwareSampleInterval) or when collection keeps failing.
// CPU-only agents (NullCollector) still send hardware — it carries CPU and
// memory metrics even when the GPU slice is empty.
// BackendMetrics is absent when the engine does not implement MetricsProvider
// (e.g. CustomEngine) or before the first successful scrape completes.
// BackendHealthy reflects the agent's most recent periodic GET /health check
// on its inference backend; false means the model server is down.
type agentHeartbeatPayload struct {
	SessionToken   string                   `json:"session_token"`
	Hardware       *domain.HardwareSnapshot `json:"hardware,omitempty"`
	BackendMetrics *domain.BackendMetrics   `json:"backend_metrics,omitempty"`
	BackendHealthy bool                     `json:"backend_healthy"`
}

var log = logging.Logger("router")

// Router is the main Hivenet Router
type Router struct {
	cfg *config.Config

	// P2P networking
	p2pHost  host.Host
	httpHost *p2phttp.Host

	// Core components
	storage        storage.RoutingStorage
	sessionManager *auth.SessionManager
	jwtValidator   *auth.JWTValidator
	grpcTLSCert    tls.Certificate
	agents         *AgentRegistry
	executor       *policy.Executor
	processor      *RequestProcessor
	metrics        *metrics.RouterMetrics
	counters       *metrics.UniversalCounterStore
	hardware       *metrics.HardwareStore
	enginePunctual *metrics.EnginePunctualStore
	// registrationNotifier broadcasts settled agent-registration deltas to
	// every live SSE subscriber on /admin/registration-stream.
	// Fired from handleAgentRegister, the libp2p DisconnectedF notifier, and
	// the heartbeat-timeout reaper.
	registrationNotifier *RegistrationNotifier

	// Request queue
	requestQueue chan *domain.PendingRequest

	// providers maps engine name to the configured closed-source provider.
	// Built once at startup from cfg.Providers; read-only after that.
	providers map[string]provider.Provider

	// processorCancel stops the request processor goroutine on shutdown.
	processorCancel context.CancelFunc

	// policyDirHashes holds the sha256 of each file last loaded from cfg.PolicyModelDir.
	// Written once at startup and then only inside the single watchPolicyReload goroutine,
	// so no mutex is needed.
	policyDirHashes map[string][32]byte

	// HTTP auth providers, wrapped in AtomicProvider for live hot-swap on SIGHUP.
	// apiAuth guards /v1/* endpoints; adminAuth guards /admin/* endpoints.
	apiAuth   *auth.AtomicProvider
	adminAuth *auth.AtomicProvider

	// rateLimiter enforces per-tenant RPM and daily token limits on /v1/* endpoints.
	rateLimiter auth.RateLimiter

	// minuteLimiter enforces the serverless per-key tokens-per-minute caps
	// (ITPM/OTPM). Reset alongside rateLimiter on auth reload so updated caps
	// take effect immediately.
	minuteLimiter *auth.MinuteRateLimiter

	// keyRegistry is the mutable in-memory API key registry (dynamic mode).
	// Nil when running in static-key or no-auth mode — guards against SIGHUP reload.
	keyRegistry *auth.DynamicKeyProvider
}

// New creates a new Router instance with initialized components.
func New(cfg *config.Config) (*Router, error) {
	if cfg.JWTSecret == "" {
		return nil, fmt.Errorf("JWT secret is not configured — set --jwt-secret or HIVENET_ROUTER_JWT_SECRET")
	}
	if len(cfg.JWTSecret) < 32 {
		return nil, fmt.Errorf("JWT secret too short (%d bytes): minimum 32 bytes required for HS256 (RFC 7518 §3.2)", len(cfg.JWTSecret))
	}

	if cfg.ResetDiskDB {
		log.Warnf("--reset-disk-db: wiping disk database at %s", cfg.DiskDBPath)
		if err := os.RemoveAll(cfg.DiskDBPath); err != nil {
			return nil, fmt.Errorf("reset disk DB: %w", err)
		}
		log.Infof("reset disk DB: successfully wiped disk database at %s", cfg.DiskDBPath)
	}

	diskTTL := time.Duration(cfg.DiskDBTTLDays) * 24 * time.Hour
	store, err := storage.NewBadgerStorage(cfg.DiskDBPath, diskTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	tlsCert, _, err := auth.DeriveGRPCCredentials([]byte(cfg.JWTSecret))
	if err != nil {
		store.Close() //nolint:errcheck
		return nil, fmt.Errorf("derive gRPC TLS credentials: %w", err)
	}

	sessionManager := auth.NewSessionManager(cfg.SessionTTL)
	jwtValidator := auth.NewJWTValidator([]byte(cfg.JWTSecret))
	agents := NewAgentRegistry()
	registrationNotifier := NewRegistrationNotifier()
	requestQueue := make(chan *domain.PendingRequest, cfg.QueueSize)
	routerMetrics := metrics.NewRouterMetrics()
	counters := metrics.NewUniversalCounterStore(store, routerMetrics)
	hardwareStore := metrics.NewHardwareStore(store, routerMetrics)
	enginePunctualStore := metrics.NewEnginePunctualStore(store, routerMetrics)

	// Load routing policy from file if configured, otherwise use the built-in default.
	activePolicy := policy.Default()
	if cfg.PolicyFile != "" {
		p, err := policy.Load(cfg.PolicyFile)
		if err != nil {
			return nil, fmt.Errorf("routing policy: %w", err)
		}
		activePolicy = p
		log.Infof("Routing policy loaded from %s", cfg.PolicyFile)
	} else {
		log.Info("No --policy-file configured — using built-in default (least-loaded, no filter, no fallback)")
	}

	evaluator := policy.NewEvaluator(store, counters)
	executor := policy.NewExecutor(agents, evaluator, activePolicy, cfg.MaxTriesPerStep, cfg.QueueDepth)
	executor.SetQueueMetrics(routerMetrics)

	// Load per-model policies from the policy model directory if configured.
	// effectiveGlobal / namedPolicies capture the policies actually in force so
	// the admission invariants can be checked once auth keys are known.
	effectiveGlobal := activePolicy
	var namedPolicies map[string]*policy.Policy
	var dirHashes map[string][32]byte
	if cfg.PolicyModelDir != "" {
		snap, err := policy.LoadDirSnapshot(cfg.PolicyModelDir)
		if err != nil {
			store.Close() //nolint:errcheck
			return nil, fmt.Errorf("policy model dir: %w", err)
		}
		if snap.Global != nil {
			if cfg.PolicyFile != "" {
				log.Warnf("Both --policy-file and _default.yaml in --policy-model-dir are set; _default.yaml takes precedence")
			}
			executor.SetPolicy(snap.Global)
			effectiveGlobal = snap.Global
			log.Infof("Global policy overridden by %s/_default.yaml", cfg.PolicyModelDir)
		}
		if len(snap.Named) > 0 {
			executor.SetNamedPoliciesFromSnapshot(snap.Named)
			namedPolicies = snap.Named
			log.Infof("Loaded %d named policies from %s", len(snap.Named), cfg.PolicyModelDir)
		}
		dirHashes = snap.Hashes
	}

	// Build HTTP auth providers from config (or defaults if AuthConfigFile is empty).
	apiProv, adminProv, keyRegistry, err := auth.ProvidersFromConfig(cfg)
	if err != nil {
		store.Close() //nolint:errcheck
		return nil, fmt.Errorf("auth providers: %w", err)
	}

	// Admission invariants that span the policy and auth configs (e.g. a
	// serverless key's input bucket must cover one maximum-size prompt). Neither
	// loader can check these alone, so they run here where both configs are in
	// hand. Re-reads auth.yaml (small, already proven parseable above) for the
	// raw key list.
	if err := checkAdmissionConfig(cfg, effectiveGlobal, namedPolicies); err != nil {
		store.Close() //nolint:errcheck
		return nil, fmt.Errorf("admission config: %w", err)
	}

	// Build the rate limiter. When QuotaBackend is "badger", pass the storage
	// instance so token counters are persisted across restarts.
	var quotaStore auth.DailyQuotaStore
	if cfg.QuotaBackend == "badger" {
		quotaStore = store // *BadgerStorage implements DailyQuotaStore
	}
	rateLimiter, err := auth.NewRateLimiter(cfg, quotaStore)
	if err != nil {
		store.Close() //nolint:errcheck
		return nil, fmt.Errorf("rate limiter: %w", err)
	}
	// Wire the today-usage gauge so hivenet_router_tenant_tokens_used_today is updated
	// on every successful token deduction, regardless of backend strategy.
	type tokenUsageReporter interface{ SetOnTokensUsed(func(string, int)) }
	if r, ok := rateLimiter.(tokenUsageReporter); ok {
		r.SetOnTokensUsed(routerMetrics.TenantSetTokensUsedToday)
	}
	// Wire the flush-error counter so diskDB write failures increment
	// hivenet_router_quota_backend_errors_total instead of being silently dropped.
	type flushErrorReporter interface{ SetOnFlushError(func()) }
	if r, ok := rateLimiter.(flushErrorReporter); ok {
		r.SetOnFlushError(routerMetrics.QuotaBackendError)
	}

	r := &Router{
		cfg:                  cfg,
		storage:              store,
		sessionManager:       sessionManager,
		jwtValidator:         jwtValidator,
		grpcTLSCert:          tlsCert,
		agents:               agents,
		executor:             executor,
		requestQueue:         requestQueue,
		metrics:              routerMetrics,
		counters:             counters,
		hardware:             hardwareStore,
		enginePunctual:       enginePunctualStore,
		registrationNotifier: registrationNotifier,
		policyDirHashes:      dirHashes,
		apiAuth:              auth.NewAtomicProvider(apiProv),
		adminAuth:            auth.NewAtomicProvider(adminProv),
		rateLimiter:          rateLimiter,
		minuteLimiter:        auth.NewMinuteRateLimiter(),
		keyRegistry:          keyRegistry,
	}
	// Refresh tenant quota gauges whenever the dynamic key registry changes,
	// so the Grafana $tenant_id variable picks up newly-pushed keys without
	// waiting for the first request from each tenant.
	if r.keyRegistry != nil {
		r.keyRegistry.SetOnChange(r.publishTenantQuotas)
	}
	return r, nil
}

// Start starts the router
func (r *Router) Start() error {
	log.Info("Starting Hivenet Router...")

	// Create libp2p host.
	// When P2PAnnounceAddr is set (e.g. behind Docker/NAT), AddrsFactory overrides
	// the addresses advertised to agents so they receive the public address instead
	// of the internal Docker/loopback address returned by the OS.
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%s", r.cfg.P2PListenAddr, r.cfg.P2PPort)
	p2pOpts := []libp2p.Option{libp2p.ListenAddrStrings(listenAddr)}
	if r.cfg.P2PAnnounceAddr != "" {
		announceMA, err := multiaddr.NewMultiaddr(r.cfg.P2PAnnounceAddr)
		if err != nil {
			return fmt.Errorf("invalid --p2p-announce-addr %q: %w", r.cfg.P2PAnnounceAddr, err)
		}
		p2pOpts = append(p2pOpts, libp2p.AddrsFactory(func(_ []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return []multiaddr.Multiaddr{announceMA}
		}))
		log.Infof("libp2p announce address override: %s", r.cfg.P2PAnnounceAddr)
	}

	// Configure the libp2p resource manager. We keep the memory- and
	// stream-scaled default scope limits (which still impose a real global
	// ceiling) but raise the per-source-IP connection limit. go-libp2p defaults
	// that limit to 8 connections per /32 IPv4 (and /56 IPv6); that is far too
	// low here because every agent reaches the router through a single shared
	// NAT/egress IP, so they all land in the same prefix bucket and the 9th
	// agent's Noise handshake is reset. P2PMaxConnsPerIP lifts the ceiling for
	// this trusted, authenticated fleet (agents pass JWT auth before they dial).
	rcMgrLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&rcMgrLimits)
	resourceManager, err := rcmgr.NewResourceManager(
		rcmgr.NewFixedLimiter(rcMgrLimits.AutoScale()),
		rcmgr.WithLimitPerSubnet(
			[]rcmgr.ConnLimitPerSubnet{{PrefixLength: 32, ConnCount: r.cfg.P2PMaxConnsPerIP}},
			[]rcmgr.ConnLimitPerSubnet{{PrefixLength: 56, ConnCount: r.cfg.P2PMaxConnsPerIP}},
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create libp2p resource manager: %w", err)
	}
	// Wrap so a per-source-IP rejection produces an actionable WARN; the limiter
	// is otherwise silent (no log/metric/trace), leaving only the agent-side
	// Noise handshake error as a signal.
	loggingRM := newLoggingResourceManager(resourceManager, r.cfg.P2PMaxConnsPerIP)
	p2pOpts = append(p2pOpts, libp2p.ResourceManager(loggingRM))

	p2pHost, err := libp2p.New(p2pOpts...)
	if err != nil {
		return fmt.Errorf("failed to create libp2p host: %w", err)
	}
	r.p2pHost = p2pHost

	// Release the libp2p peer slot immediately when the last connection to an
	// agent closes, instead of waiting up to RemoveAfter (30s) for the health
	// monitor to call ClosePeer. Without this, a peer that drops and reconnects
	// quickly can hit the resource manager's per-peer connection limit while the
	// stale scope is still open, causing every reconnect attempt to be rejected
	// with an immediate TCP FIN before the Noise handshake.
	r.p2pHost.Network().Notify(&libp2pnet.NotifyBundle{
		DisconnectedF: func(n libp2pnet.Network, conn libp2pnet.Conn) {
			peerID := conn.RemotePeer()
			if len(n.ConnsToPeer(peerID)) != 0 {
				return
			}
			// Fire the "unregistered" registration event before the slot is
			// closed — the watcher gets sub-second loss detection (<1s libp2p
			// drop notification vs. the 30s reaper). The agent stays in
			// AgentRegistry; only the registration-feed view changes here. The
			// reaper publishes a second "unregistered" later — the watcher's
			// 1s debounce collapses the pair to one annotation write.
			if agent := r.agents.Get(peerID); agent != nil {
				agent.RLock()
				dep, rep := agent.Metadata.DeploymentID, agent.Metadata.ReplicaID
				agent.RUnlock()
				r.registrationNotifier.Publish(domain.RegistrationEvent{
					EventType:    domain.RegistrationUnregistered,
					DeploymentID: dep,
					ReplicaID:    rep,
					AgentID:      peerID.String(),
					Timestamp:    time.Now().UTC(),
				})
			}
			go func() {
				// Re-check inside the goroutine: the peer may have reconnected
				// between the outer check and this execution. Closing a live
				// connection here would cause the exact flapping we are fixing.
				if len(n.ConnsToPeer(peerID)) == 0 {
					n.ClosePeer(peerID) //nolint:errcheck
				}
			}()
		},
	})

	// Create libp2p HTTP host.
	// All agent management endpoints are registered under a single protocol so
	// that agents can use one NamespacedClient for both /register and /heartbeat.
	r.httpHost = &p2phttp.Host{StreamHost: r.p2pHost}

	mux := http.NewServeMux()
	mux.HandleFunc("/register", r.handleAgentRegister)
	mux.HandleFunc("/heartbeat", r.handleAgentHeartbeat)
	mux.HandleFunc("/routing-signals", r.handleRoutingSignals)
	r.httpHost.SetHTTPHandler(routerProtocolID, mux)

	go r.httpHost.Serve() //nolint:errcheck

	log.Infof("libp2p address: %s", p2p.GetP2PAddress(r.p2pHost))

	// Build closed-source provider map from config (keyed by engine name).
	r.providers = make(map[string]provider.Provider, len(r.cfg.Providers))
	for _, pc := range r.cfg.Providers {
		prov, err := provider.New(provider.Config{Name: pc.Name, APIKey: pc.APIKey, Metrics: r.metrics})
		if err != nil {
			return fmt.Errorf("provider: %w", err)
		}
		r.providers[pc.Name] = prov
		log.Infof("Provider API key loaded: %s", pc.Name)
	}

	// Fail fast: if the global policy declares a fallback_provider, the API key must be present.
	if err := r.validateProviderPolicy(r.executor.GetPolicy()); err != nil {
		return err
	}
	// Same check for every named policy.
	for name, np := range r.executor.GetNamedPolicies() {
		if err := r.validateProviderPolicy(np); err != nil {
			return fmt.Errorf("named policy %q: %w", name, err)
		}
	}

	// Log the effective fallback provider declared by the active policy.
	if fp := r.executor.GetPolicy().FallbackProvider; fp != nil {
		log.Infof("Provider fallback active: %s (model: %s)", fp.Engine, fp.Model)
	} else {
		log.Info("Provider fallback: not configured in policy")
	}

	// Create request processor now that httpHost is ready
	r.processor = NewRequestProcessor(r.requestQueue, r.executor, r.httpHost, r.metrics, r.counters, r.cfg.MaxConcurrentForwards, r.providers, r.rateLimiter)

	// Watch for SIGHUP to reload the policy file from disk without restarting.
	go r.watchPolicyReload()

	processorCtx, processorCancel := context.WithCancel(context.Background())
	r.processorCancel = processorCancel

	if bs, ok := r.storage.(*storage.BadgerStorage); ok {
		bs.StartGC(processorCtx, r.cfg.FlushPeriod)
		log.Infof("BadgerDB GC started (interval: %v)", r.cfg.FlushPeriod)
	}

	r.counters.StartPeriodicFlush(processorCtx, r.cfg.UniversalFlushInterval)
	log.Infof("Universal counters periodic flush started (interval: %v)", r.cfg.UniversalFlushInterval)

	if bl, ok := r.rateLimiter.(*auth.BadgerLimiter); ok {
		bl.StartPeriodicFlush(processorCtx, r.cfg.UniversalFlushInterval)
		log.Infof("Quota BadgerLimiter periodic flush started (interval: %v)", r.cfg.UniversalFlushInterval)
	}

	r.metrics.ServeMetrics(r.cfg.MetricsPort)
	r.publishTenantQuotas()
	go r.startGRPCServer()
	go r.healthMonitor()
	go r.processor.Start(processorCtx)
	go r.startHTTPServer()

	go func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for range t.C {
			if n := r.sessionManager.CleanupExpired(); n > 0 {
				log.Infof("Session GC: removed %d expired sessions", n)
			}
		}
	}()

	log.Info("Router ready — waiting for agents to connect...")

	select {}
}

// Close gracefully shuts down router and its components.
// Shutdown order is critical:
//  1. Final FlushAll — persist any counters written since the last periodic flush.
//  2. Cancel processorCtx — stops the periodic-flush goroutine and the processor.
//  3. WaitFlush — block until the flush goroutine has fully exited.
//  4. Close libp2p — no new requests can arrive after this.
//  5. Close storage — safe now that no goroutine will write to it.
func (r *Router) Close() error {
	log.Info("Shutting down router...")

	// 1. Flush all in-process counters to diskDB before stopping anything.
	if r.counters != nil {
		log.Info("Flushing universal counters to diskDB...")
		r.counters.FlushAll()
	}
	if bl, ok := r.rateLimiter.(*auth.BadgerLimiter); ok {
		log.Info("Flushing quota counters to diskDB...")
		bl.Flush()
	}

	// 2. Stop processor and periodic-flush goroutines.
	if r.processorCancel != nil {
		r.processorCancel()
	}

	// 3. Wait for the periodic-flush goroutine to exit before closing storage.
	if r.counters != nil {
		r.counters.WaitFlush()
	}

	// 4. Close libp2p host and all associated streams.
	if r.p2pHost != nil {
		r.p2pHost.Close()
	}

	// 5. Close storage (safe — no goroutine can write to it now).
	return r.storage.Close()
}

// startGRPCServer initializes and runs the gRPC auth server.
func (r *Router) startGRPCServer() {
	authServer := grpc.NewAuthServer(
		r.cfg,
		r.jwtValidator,
		r.sessionManager,
		r.p2pHost,
	)
	if err := grpc.StartServer(r.cfg.GRPCPort, authServer, r.grpcTLSCert); err != nil {
		log.Errorf("gRPC server error: %v", err)
	}
}

// startHTTPServer initializes and runs the Gin HTTP API server.
func (r *Router) startHTTPServer() {
	// Guard the reset callback: pass nil when no counter store is configured so the
	// admin endpoint returns its clean "not available" 503 instead of panicking on a
	// nil receiver. (r.counters is non-nil on all normal paths.)
	var resetMetrics func() error
	if r.counters != nil {
		resetMetrics = r.counters.ResetAll
	}
	// The global occupancy controller publishes its Σw/count/budget as gauges.
	occupancy := admission.NewController(r.cfg.AdmitFraction, r.cfg.AdmitParkTimeout)
	occupancy.SetObserver(r.metrics.SetAdmissionOccupancy)
	handlers := api.NewHandlers(
		r.storage,
		r,
		r.requestQueue,
		r.cfg.RequestTimeout,
		r.executor,
		r.validateProviderPolicy,
		r.metrics.PolicyReload,
		r.rateLimiter,
		func(mc api.MetricContext) {
			r.metrics.TenantTokenLimited(mc.TenantID, mc.KeyID, mc.DeploymentID, mc.Phase)
			r.metrics.TenantRequestFailed(mc.TenantID, mc.KeyID, mc.DeploymentID, mc.Model)
		},
		func(mc api.MetricContext) {
			r.metrics.TenantSetLastRequestTimestamp(mc.TenantID, mc.KeyID, mc.DeploymentID)
		},
		func(mc api.MetricContext, seconds float64) {
			r.metrics.TenantObserveRequestDuration(mc.TenantID, mc.KeyID, mc.DeploymentID, mc.Model, seconds)
		},
		r.metrics.ObserveRequestDuration,
		r.keyRegistry,
		resetMetrics,
		r.agents.CountHealthyByModel,
		r, // RegistrationFeed — Router implements SubscribeRegistration
		occupancy,
		r.EnginePressureForModel,
		// Per-key occupancy share: no admit fraction (it is fairness, not box
		// safety) and no parking (a per-key breach is denied immediately).
		admission.NewController(1.0, 0),
		r.minuteLimiter,
		r.metrics.AdmissionRejected,
		tokenizer.NewEstimator(),
	)
	server := api.NewServer(handlers, r.cfg.HTTPPort, r.apiAuth, r.adminAuth, r.rateLimiter, r.metrics, r.agents.CountHealthyByModel, r.cfg.MaxRequestBytes)
	if err := server.Start(); err != nil {
		log.Errorf("HTTP server error: %v", err)
	}
}

// validateProviderPolicy checks that if the policy declares a fallback_provider,
// the corresponding provider is configured (API key is set). This is called both
// at startup and on SIGHUP policy reload to catch mismatches early.
func (r *Router) validateProviderPolicy(p *policy.Policy) error {
	if fp := p.FallbackProvider; fp != nil {
		if !provider.IsSupported(fp.Engine) {
			return fmt.Errorf("policy declares fallback_provider engine %q: unknown engine — supported: openai, anthropic", fp.Engine)
		}
		if _, ok := r.providers[fp.Engine]; !ok {
			engine := strings.ToLower(fp.Engine)
			switch engine {
			case "openai":
				return fmt.Errorf("policy declares fallback_provider engine %q but no API key is configured — set HIVENET_ROUTER_OPENAI_API_KEY", fp.Engine)
			case "anthropic":
				return fmt.Errorf("policy declares fallback_provider engine %q but no API key is configured — set HIVENET_ROUTER_ANTHROPIC_API_KEY", fp.Engine)
			default:
				return fmt.Errorf("policy declares fallback_provider engine %q but this engine is not supported; supported engines: openai, anthropic", fp.Engine)
			}
		}
	}
	return nil
}

// checkAdmissionAgainstKeys validates proposed policies against the API keys
// currently on disk, so a reload cannot introduce a serverless key whose input
// bucket no longer covers a model's context. A key-read error is logged and
// treated as "cannot validate" (nil) so an unrelated auth.yaml problem does not
// block a policy reload — that problem surfaces on the auth reload path instead.
func (r *Router) checkAdmissionAgainstKeys(policies []*policy.Policy) error {
	keys, err := admissionKeysFromConfig(r.cfg)
	if err != nil {
		log.Warnf("SIGHUP: could not read auth keys to validate policy admission invariants (%v) — skipping that check", err)
		return nil
	}
	return ValidateAdmissionInvariants(policies, keys)
}

// currentAdmissionPolicies returns the policies currently in force (global plus
// named) for cross-config re-validation on reload.
func (r *Router) currentAdmissionPolicies() []*policy.Policy {
	return gatherPolicies(r.executor.GetPolicy(), r.executor.GetNamedPolicies())
}

// watchPolicyReload listens for SIGHUP and reloads the policy file and/or
// the policy model directory from disk without restarting the router.
// Errors are logged and the previous policy stays active.
func (r *Router) watchPolicyReload() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)
	for range sigCh {
		if r.cfg.PolicyFile == "" && r.cfg.PolicyModelDir == "" && r.cfg.AuthConfigFile == "" {
			log.Info("SIGHUP received but neither --policy-file, --policy-model-dir, nor --auth-config-file is configured — ignoring")
			continue
		}

		// Reload auth providers on every SIGHUP. When --auth-config-file is set this
		// re-parses the file and re-reads HIVENET_ROUTER_ADMIN_API_KEYS from ENV, enabling
		// live key rotation without a restart. When no auth config file is configured
		// both sections default to mode=none — the reload is a no-op.
		r.reloadAuthProviders()
		// --policy-model-dir owns the global policy when configured (via _default.yaml).
		// --policy-file is only reloaded when the dir is not in use, so the dir always
		// retains full control and the two sources never race on the global policy.
		if r.cfg.PolicyModelDir != "" {
			r.reloadPolicyModelDir()
		} else if r.cfg.PolicyFile != "" {
			p, err := policy.Load(r.cfg.PolicyFile)
			if err != nil {
				log.Errorf("SIGHUP: policy reload failed (%v) — keeping previous policy active", err)
				r.metrics.PolicyReload("sighup", "error")
			} else if err := r.validateProviderPolicy(p); err != nil {
				log.Errorf("SIGHUP: policy reload rejected (%v) — keeping previous policy active", err)
				r.metrics.PolicyReload("sighup", "error")
			} else if err := r.checkAdmissionAgainstKeys([]*policy.Policy{p}); err != nil {
				log.Errorf("SIGHUP: policy reload rejected (%v) — keeping previous policy active", err)
				r.metrics.PolicyReload("sighup", "error")
			} else {
				r.executor.SetPolicy(p)
				r.metrics.PolicyReload("sighup", "success")
				log.Infof("SIGHUP: routing policy reloaded from %s", r.cfg.PolicyFile)
			}
		}
	}
}

// publishTenantQuotas pre-populates Prometheus quota gauges for every tenant
// defined in the auth config. This ensures the Grafana $tenant_id variable is
// populated within the first scrape (~5 s) instead of waiting for the first
// HTTP request. Called at startup and after every SIGHUP reload.
func (r *Router) publishTenantQuotas() {
	type tenantLister interface {
		Tenants() map[string]auth.QuotaLimits
	}
	lister, ok := r.apiAuth.Load().(tenantLister)
	if !ok {
		return
	}
	for tenantID, limits := range lister.Tenants() {
		// Always emit the flat-shape gauges. For per-model keys both values
		// are 0 (the QuotaConfig loader leaves RequestsPerMinute and
		// TokensPerDay at zero when per_model is set), so the gauge correctly
		// reads as "this tenant has no flat ceiling — look at the per-model
		// series instead."
		r.metrics.TenantSetQuotaLimit(tenantID, limits.RequestsPerMinute)
		r.metrics.TenantSetTPDLimit(tenantID, limits.TokensPerDay)
		// Seed per-model gauges so the Grafana (tenant_id, model) variable
		// populates within the first scrape. The RPM seed is per_replica
		// directly — at startup the live replica count is unknown — and the
		// QuotaMiddleware overwrites it with per_replica × live_replicas on
		// the first admission. The TPD seed is the configured absolute value.
		for model, entry := range limits.PerModel {
			r.metrics.TenantSetPerModelQuotaLimit(tenantID, model, entry.RequestsPerMinutePerReplica)
			r.metrics.TenantSetPerModelTPDLimit(tenantID, model, entry.TokensPerDay)
		}
		r.metrics.TenantInitCounters(tenantID)
	}
}

// reloadAuthProviders reloads auth providers from the configured auth config file.
// On success the atomic providers are swapped so in-flight requests are unaffected.
// On failure the previous providers remain active.
// If the router is running in dynamic key mode, the API provider (DynamicKeyProvider)
// is preserved while the admin provider is still reloaded.
// Switching auth modes (static ↔ dynamic) at runtime is rejected — it requires
// a process restart so the keyRegistry reference and Handlers wiring stay coherent.
func (r *Router) reloadAuthProviders() {
	apiProv, adminProv, newKeyRegistry, err := auth.ProvidersFromConfig(r.cfg)
	if err != nil {
		log.Errorf("SIGHUP: auth config reload failed (%v) — keeping previous providers active", err)
		return
	}
	if (r.keyRegistry == nil) != (newKeyRegistry == nil) {
		log.Errorf("SIGHUP: auth mode change between static and dynamic is not supported at runtime — restart the router; keeping previous providers active")
		return
	}
	if r.keyRegistry != nil {
		// DynamicKeyProvider managed via admin API — preserve it, only reload admin
		r.adminAuth.Swap(adminProv)
		log.Info("SIGHUP: admin auth provider reloaded (dynamic API key registry preserved)")
		return
	}
	// Re-check the cross-config invariants against the incoming keys before
	// swapping, so a reload cannot introduce a serverless key whose input bucket
	// no longer covers a model's context. Keep the previous providers on failure,
	// mirroring how any other invalid auth reload is rejected.
	if keys, err := admissionKeysFromConfig(r.cfg); err != nil {
		log.Errorf("SIGHUP: auth config re-read for validation failed (%v) — keeping previous providers active", err)
		return
	} else if err := ValidateAdmissionInvariants(r.currentAdmissionPolicies(), keys); err != nil {
		log.Errorf("SIGHUP: auth config reload rejected (%v) — keeping previous providers active", err)
		return
	}
	r.apiAuth.Swap(apiProv)
	r.adminAuth.Swap(adminProv)
	r.rateLimiter.Reset()
	r.minuteLimiter.Reset()
	r.publishTenantQuotas()
	if r.cfg.AuthConfigFile != "" {
		log.Infof("SIGHUP: auth providers reloaded from %s", r.cfg.AuthConfigFile)
	} else {
		log.Info("SIGHUP: auth providers reloaded (no auth config file — both sections use mode=none)")
	}
}

// reloadPolicyModelDir reloads all per-model policy files from cfg.PolicyModelDir.
// It diffs file hashes against the last-loaded snapshot and only applies changes.
// Per-model validation errors are logged and that model is skipped; other models
// are unaffected. Only called from the single watchPolicyReload goroutine.
func (r *Router) reloadPolicyModelDir() {
	snap, err := policy.LoadDirSnapshot(r.cfg.PolicyModelDir)
	if err != nil {
		log.Errorf("SIGHUP: policy model dir reload failed (%v)", err)
		r.metrics.PolicyReload("sighup", "error")
		return
	}

	// Detect any change by comparing file hashes.
	changed := len(snap.Hashes) != len(r.policyDirHashes)
	if !changed {
		for name, newHash := range snap.Hashes {
			if oldHash, ok := r.policyDirHashes[name]; !ok || oldHash != newHash {
				changed = true
				break
			}
		}
	}
	if !changed {
		log.Debug("SIGHUP: policy model dir unchanged — skipping reload")
		return
	}

	// Reject the whole dir reload if the proposed effective policy set would
	// break the cross-config invariants against the current keys — keep the
	// previous policies rather than silently cap context. proposedGlobal falls
	// back to the current global when _default.yaml is absent from the snapshot.
	proposedGlobal := snap.Global
	if proposedGlobal == nil {
		proposedGlobal = r.executor.GetPolicy()
	}
	if err := r.checkAdmissionAgainstKeys(gatherPolicies(proposedGlobal, snap.Named)); err != nil {
		log.Errorf("SIGHUP: policy model dir reload rejected (%v) — keeping previous policies active", err)
		r.metrics.PolicyReload("sighup", "error")
		return
	}

	reloadOK := true

	// Apply _default.yaml as new global policy if present; revert to built-in
	// default if it was present before but has now been deleted.
	_, prevHadDefault := r.policyDirHashes["_default.yaml"]
	if !prevHadDefault {
		_, prevHadDefault = r.policyDirHashes["_default.yml"]
	}
	if snap.Global != nil {
		if err := r.validateProviderPolicy(snap.Global); err != nil {
			log.Errorf("SIGHUP: _default.yaml validation failed (%v) — keeping previous global policy", err)
			reloadOK = false
		} else {
			r.executor.SetPolicy(snap.Global)
			log.Infof("SIGHUP: global policy reloaded from %s/_default.yaml", r.cfg.PolicyModelDir)
		}
	} else if prevHadDefault {
		// _default.yaml was deleted — revert all models to the built-in default.
		r.executor.SetPolicy(policy.Default())
		log.Infof("SIGHUP: _default.yaml deleted — global policy reverted to built-in default (least-loaded)")
	}

	// Build stem→hash maps for both the previous and new snapshots.
	// Used to distinguish unchanged files (established owners) from new/modified ones.
	stemHash := func(hashes map[string][32]byte) map[string][32]byte {
		m := make(map[string][32]byte, len(hashes))
		for filename, h := range hashes {
			if filename == "_default.yaml" || filename == "_default.yml" {
				continue
			}
			stem := strings.TrimSuffix(strings.TrimSuffix(filename, ".yaml"), ".yml")
			m[stem] = h
		}
		return m
	}
	oldStemHashes := stemHash(r.policyDirHashes)
	newStemHashes := stemHash(snap.Hashes)

	// Snapshot the currently-active named policies. Used both to seed the
	// established-owners set (files that exist but failed to re-parse keep their
	// models protected) and to restore those policies into valid after the loop.
	currentNamed := r.executor.GetNamedPolicies()

	// First pass: collect established owners — unchanged files plus files that
	// exist on disk but failed to re-parse (their old policy will be preserved).
	establishedOwners := make(map[string]string) // model → stem
	for stem, np := range snap.Named {
		newHash := newStemHashes[stem]
		oldHash, existed := oldStemHashes[stem]
		if existed && oldHash == newHash {
			for _, model := range np.Models {
				establishedOwners[model] = stem
			}
		}
	}
	// Also protect models owned by files that are readable but failed to parse.
	// Their file is in snap.Hashes (readable) but not in snap.Named (parse error).
	for oldStem, oldPolicy := range currentNamed {
		if _, inNamed := snap.Named[oldStem]; inNamed {
			continue // already handled above
		}
		_, yamlExists := snap.Hashes[oldStem+".yaml"]
		_, ymlExists := snap.Hashes[oldStem+".yml"]
		if yamlExists || ymlExists {
			for _, model := range oldPolicy.Models {
				establishedOwners[model] = oldStem
			}
		}
	}

	// Second pass: validate all files; reject new/modified ones that conflict with
	// established owners or fail provider validation.
	valid := make(map[string]*policy.Policy, len(snap.Named))
	for stem, np := range snap.Named {
		newHash := newStemHashes[stem]
		oldHash, existed := oldStemHashes[stem]
		unchanged := existed && oldHash == newHash

		if !unchanged {
			// New or modified file — check ownership conflict against established files.
			conflict := ""
			for _, model := range np.Models {
				if owner, taken := establishedOwners[model]; taken {
					conflict = fmt.Sprintf("model %q already claimed by %q", model, owner)
					break
				}
			}
			if conflict != "" {
				log.Errorf("SIGHUP: skipping %q — %s", stem, conflict)
				reloadOK = false
				continue
			}
		}

		if err := r.validateProviderPolicy(np); err != nil {
			log.Errorf("SIGHUP: named policy %q validation failed (%v) — skipping", stem, err)
			reloadOK = false
			continue
		}
		valid[stem] = np
	}

	// Restore policies for files that still exist on disk but are absent from
	// snap.Named (either a parse error or a conflict with another file).
	// Only files completely absent from snap.Hashes (deleted or unreadable) lose their policy.
	for oldStem, oldPolicy := range currentNamed {
		if _, alreadyValid := valid[oldStem]; alreadyValid {
			continue // successfully re-parsed — already in valid
		}
		_, yamlExists := snap.Hashes[oldStem+".yaml"]
		_, ymlExists := snap.Hashes[oldStem+".yml"]
		if yamlExists || ymlExists {
			if reason, conflicted := snap.Conflicted[oldStem]; conflicted {
				// File is valid YAML but lost a model ownership conflict.
				log.Infof("SIGHUP: keeping previous policy for %q (skipped due to conflict: %s)", oldStem, reason)
			} else {
				// File exists but has a parse or validation error.
				log.Infof("SIGHUP: keeping previous policy for %q (file exists but is currently invalid)", oldStem)
			}
			valid[oldStem] = oldPolicy
			reloadOK = false
		}
		// else: file was deleted (or unreadable) — policy is intentionally removed.
	}

	// Atomically replace the entire named policy map (deleted files are excluded).
	r.executor.SetNamedPoliciesFromSnapshot(valid)
	log.Infof("SIGHUP: named policies reloaded from %s (%d documents)", r.cfg.PolicyModelDir, len(valid))

	r.policyDirHashes = snap.Hashes
	if reloadOK {
		r.metrics.PolicyReload("sighup", "success")
	} else {
		r.metrics.PolicyReload("sighup", "error")
	}
}

// SubscribeRegistration implements api.RegistrationFeed. The returned channel
// receives every settled registration delta until the caller invokes the
// returned cancel function (which must be called — slot is reclaimed on
// Unsubscribe, never on GC).
func (r *Router) SubscribeRegistration() (<-chan domain.RegistrationEvent, func()) {
	sub := r.registrationNotifier.Subscribe()
	return sub.Events, func() { r.registrationNotifier.Unsubscribe(sub) }
}

// EnginePressureForModel returns the aggregate live engine pressure across the
// healthy agents serving model: the mean KV-cache utilization and the mean
// waiting-request count, each averaged over the agents that report it. A metric
// is nil when no healthy agent reports it (e.g. non-vLLM engines), so the
// front-door shed gate skips that dimension rather than treating a missing
// signal as zero pressure. The mean (not max) is used deliberately: a single hot
// replica is handled by routing's per-agent exclude_if, so shedding new
// admissions is warranted only when the pool as a whole is under pressure.
func (r *Router) EnginePressureForModel(model string) (kvUtil, waiting *float64) {
	var metrics []*domain.BackendMetrics
	for _, agent := range r.agents.ListByModel(model) {
		if !agent.IsHealthy() {
			continue
		}
		if ep, err := r.storage.GetEnginePunctual(agent.ID); err == nil && ep != nil {
			metrics = append(metrics, ep)
		}
	}
	return MeanEnginePressure(metrics)
}

// MeanEnginePressure averages the KV-cache utilization and waiting-request count
// over the given per-agent engine snapshots, ignoring nil metrics. A dimension
// that no snapshot reports is returned as nil (not zero), so the shed gate skips
// it rather than reading a missing signal as no pressure.
func MeanEnginePressure(metrics []*domain.BackendMetrics) (kvUtil, waiting *float64) {
	var kvSum, waitSum float64
	var kvN, waitN int
	for _, ep := range metrics {
		if ep == nil {
			continue
		}
		if ep.KVCacheUtilization != nil {
			kvSum += *ep.KVCacheUtilization
			kvN++
		}
		if ep.WaitingRequests != nil {
			waitSum += *ep.WaitingRequests
			waitN++
		}
	}
	if kvN > 0 {
		v := kvSum / float64(kvN)
		kvUtil = &v
	}
	if waitN > 0 {
		v := waitSum / float64(waitN)
		waiting = &v
	}
	return kvUtil, waiting
}

// GetRoutingTable implements api.RoutingTableProvider.
// It merges live in-memory agent state (health, active requests, last seen)
// with all universal counters (SRTT, success rate, token counts, etc.),
// the latest engine punctual snapshot (KV cache, TTFT, ITL, …), and the
// latest hardware snapshot (GPU, CPU, memory) for every registered agent.
func (r *Router) GetRoutingTable() []api.AgentView {
	agents := r.agents.List()
	views := make([]api.AgentView, 0, len(agents))

	for _, agent := range agents {
		// Read all mutable fields under a single lock to get a consistent snapshot.
		agent.RLock()
		peerID := agent.ID.String()
		metadata := api.AgentMetadataView{
			Model:         agent.Metadata.Model,
			Engine:        agent.Metadata.Engine,
			Region:        agent.Metadata.Region,
			Organization:  agent.Metadata.Organization,
			Machine:       agent.Metadata.Machine,
			Capacity:      agent.Metadata.Capacity,
			Tags:          agent.Metadata.Tags,
			HideLLM:       agent.Metadata.HideLLM,
			LLMPrettyName: agent.Metadata.LLMPrettyName,
			LLMInfo:       agent.Metadata.LLMInfo,
			Capability:    agent.Metadata.Capability,
			GPUModel:      agent.Metadata.GPUModel,
			DeploymentID:  agent.Metadata.DeploymentID,
			ReplicaID:     agent.Metadata.ReplicaID,
		}
		status := api.AgentStatusView{
			Healthy:        agent.Healthy,
			BackendHealthy: agent.BackendHealthy,
			ActiveRequests: agent.ActiveRequests,
			LastSeen:       agent.LastSeen,
		}
		agent.RUnlock()

		if metadata.Capacity > 0 {
			status.CapacityUtilization = float64(status.ActiveRequests) / float64(metadata.Capacity)
		}

		// All universal counters + latency from the in-process counter store.
		universal := api.AgentUniversalView{LatencyState: "UNKNOWN"}
		if stats, ok := r.counters.GetAgentStats(agent.ID); ok {
			universal.SuccessfulRequestsTotal = stats.SuccessfulRequests
			universal.FailedRequestsTotal = stats.FailedRequests
			universal.SuccessRate = stats.SuccessRate
			universal.InputTokensTotal = stats.InputTokens
			universal.OutputTokensTotal = stats.OutputTokens
			universal.RejectedRequestsTotal = stats.RejectedRequests
			universal.DisconnectionsTotal = stats.Disconnections
			universal.AgentFailuresTotal = stats.AgentFailures
			universal.BackendFailuresTotal = stats.BackendFailures
			if stats.SRTTInited {
				srttMs := stats.SRTTMs
				rttvarmMs := stats.RTTVARMs
				universal.SRTTMs = &srttMs
				universal.RTTVARMs = &rttvarmMs
				universal.LatencyState = "KNOWN"
			}
		}

		// Engine punctual snapshot from memDB (kv_cache, running/waiting reqs, TTFT, ITL).
		// Only created when at least one field is non-nil — prevents "engine": {} in the response.
		var engineView *api.AgentEngineView
		if ep, err := r.storage.GetEnginePunctual(agent.ID); err != nil {
			log.Warnf("GetRoutingTable: failed to read engine punctual for %s: %v", peerID[:8], err)
		} else if ep != nil {
			if ep.KVCacheUtilization != nil || ep.RunningRequests != nil || ep.WaitingRequests != nil ||
				ep.PreemptionsTotal != nil || ep.AvgTTFTSeconds != nil || ep.P90TTFTSeconds != nil ||
				ep.AvgITLSeconds != nil || ep.P90ITLSeconds != nil {
				engineView = &api.AgentEngineView{
					KVCacheUtilization: ep.KVCacheUtilization,
					RunningRequests:    ep.RunningRequests,
					WaitingRequests:    ep.WaitingRequests,
					PreemptionsTotal:   ep.PreemptionsTotal,
					AvgTTFTSeconds:     ep.AvgTTFTSeconds,
					P90TTFTSeconds:     ep.P90TTFTSeconds,
					AvgITLSeconds:      ep.AvgITLSeconds,
					P90ITLSeconds:      ep.P90ITLSeconds,
				}
			}
		}

		// Hardware snapshot from memDB (GPU, CPU, memory).
		// Timestamp is converted from Unix seconds to time.Time for a readable RFC3339 value.
		var hwView *api.AgentHardwareView
		if hw, err := r.storage.GetHardwareSnapshot(agent.ID); err != nil {
			log.Warnf("GetRoutingTable: failed to read hardware snapshot for %s: %v", peerID[:8], err)
		} else if hw != nil {
			hwView = &api.AgentHardwareView{
				GPUs:      hw.GPUs,
				CPU:       hw.CPU,
				Memory:    hw.Memory,
				Timestamp: time.Unix(hw.Timestamp, 0).UTC(),
			}
		}

		views = append(views, api.AgentView{
			PeerID:    peerID,
			Metadata:  metadata,
			Status:    status,
			Universal: universal,
			Engine:    engineView,
			Hardware:  hwView,
		})
	}

	return views
}

// handleAgentRegister handles agent self-registration via libp2p HTTP.
// The path seen here is already stripped of the protocol prefix by SetHTTPHandler.
func (r *Router) handleAgentRegister(w http.ResponseWriter, req *http.Request) {
	var payload agentRegistrationPayload
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	session, valid := r.sessionManager.ValidateSession(payload.SessionToken)
	if !valid {
		log.Warnf("handleAgentRegister: invalid or expired session token from %s", req.RemoteAddr)
		http.Error(w, "invalid or expired session token", http.StatusUnauthorized)
		return
	}

	peerID, err := peer.Decode(payload.PeerID)
	if err != nil {
		http.Error(w, "invalid peer ID", http.StatusBadRequest)
		return
	}

	// Bind the peer ID to the session so that heartbeat and routing-signal
	// handlers can resolve the agent in O(1) via session.PeerID instead of
	// scanning the full agent registry with ForEach.
	r.sessionManager.LinkPeerID(payload.SessionToken, peerID)

	// The agent-initiated connection this registration arrived on is the only
	// path the router has to the agent — inference streams are opened back over
	// it, never by dialing (agents advertise no dialable address). Protect it so
	// the connection manager cannot trim it when the fleet grows past the
	// high-watermark. Unprotected again when the health monitor removes the agent.
	r.p2pHost.ConnManager().Protect(peerID, connProtectTag)

	capability := session.Metadata.Capability
	if capability == "" {
		capability = domain.CapabilityLLM
	}
	metadata := domain.AgentMetadata{
		Model:         session.Metadata.Model,
		Capacity:      int(session.Metadata.Capacity),
		Version:       session.Metadata.Version,
		Region:        session.Metadata.Region,
		Tags:          session.Metadata.Tags,
		Engine:        session.Metadata.Engine,
		Organization:  session.Metadata.Organization,
		Machine:       session.Metadata.Machine,
		HideLLM:       session.Metadata.HideLlm,
		LLMPrettyName: session.Metadata.LlmPrettyName,
		LLMInfo:       session.Metadata.LlmInfo,
		Capability:    capability,
		GPUModel:      session.Metadata.GpuModel,
		DeploymentID:  payload.DeploymentID,
		ReplicaID:     payload.ReplicaID,
	}

	// Capture old session token before overwriting the registry entry.
	// Delete it after the new agent is fully registered to avoid a window
	// where neither token is valid (re-auth path).
	var oldToken string
	if existing := r.agents.Get(peerID); existing != nil {
		oldToken = existing.SessionToken
	}

	agent := domain.NewAgent(peerID, metadata, payload.SessionToken)
	r.agents.Register(agent)
	r.storage.RegisterAgent(peerID, metadata, payload.SessionToken) //nolint:errcheck // best-effort persistence; in-memory registry is authoritative
	r.counters.Bootstrap(peerID, metadata.Model, metadata.Engine, metadata.Organization, metadata.Machine)
	r.metrics.AgentRegistered(peerID.String(), metadata.Region, metadata.Engine, metadata.Model, strconv.Itoa(metadata.Capacity), metadata.Organization, metadata.Machine)
	// Fan out the settled "registered" event to every SSE subscriber on
	// /admin/registration-stream. Watchers consume this for sub-second
	// state propagation; the routing-table snapshot is the safety net.
	r.registrationNotifier.Publish(domain.RegistrationEvent{
		EventType:    domain.RegistrationRegistered,
		DeploymentID: metadata.DeploymentID,
		ReplicaID:    metadata.ReplicaID,
		AgentID:      peerID.String(),
		Timestamp:    time.Now().UTC(),
	})
	// New agent brings fresh capacity — wake any requests waiting in the queue for this model.
	r.executor.SignalCapacity(metadata.Model)

	// Delete the old session only after the new agent is fully registered.
	// A short grace period lets any in-flight routing-signal or heartbeat
	// requests that are already carrying the old token finish cleanly,
	// avoiding spurious "invalid or expired session token" warnings.
	if oldToken != "" {
		token := oldToken
		time.AfterFunc(2*time.Second, func() { r.sessionManager.DeleteSession(token) })
	}

	if oldToken == "" {
		log.Infof("libp2p HTTP: Agent registered: %s", peerID.String()[:8])
		log.Info("Agent metadata:")
		log.Infof("   - Model: %s", metadata.Model)
		log.Infof("   - Engine: %s", metadata.Engine)
		log.Infof("   - Capability: %s", metadata.Capability)
		log.Infof("   - Capacity: %d", metadata.Capacity)
		log.Infof("   - Version: %s", metadata.Version)
		log.Infof("   - Organization: %s", metadata.Organization)
		log.Infof("   - Machine: %s", metadata.Machine)
		log.Infof("Active agents: %d", r.agents.Count())
	} else {
		log.Debugf("libp2p HTTP: Agent re-registered (re-auth): %s", peerID.String()[:8])
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "registered"}) //nolint:errcheck
}

// handleAgentHeartbeat updates LastSeen for the agent identified by session token.
func (r *Router) handleAgentHeartbeat(w http.ResponseWriter, req *http.Request) {
	payload := agentHeartbeatPayload{BackendHealthy: true} // absent field defaults healthy; see agent.go heartbeatPayload
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	session, valid := r.sessionManager.ValidateSession(payload.SessionToken)
	if !valid {
		log.Warnf("handleAgentHeartbeat: invalid or expired session token from %s", req.RemoteAddr)
		http.Error(w, "invalid or expired session token", http.StatusUnauthorized)
		return
	}

	// O(1) lookup: session.PeerID was set by LinkPeerID during /register.
	a := r.agents.Get(session.PeerID)
	if a == nil {
		http.Error(w, "agent not found", http.StatusNotFound)
		return
	}

	// Region, model, engine, and capacity are immutable for the session lifetime —
	// read from the session rather than the agent to keep a single source of truth.
	peerIDStr := session.PeerID.String()
	region := session.Metadata.Region
	engine := session.Metadata.Engine
	model := session.Metadata.Model
	capacity := strconv.Itoa(int(session.Metadata.Capacity))
	organization := session.Metadata.Organization
	machine := session.Metadata.Machine

	// Always update LastSeen — the agent is alive and reachable regardless of backend state.
	a.UpdateLastSeen()

	prevBackendHealthy := a.SetBackendHealthy(payload.BackendHealthy)

	if payload.BackendHealthy {
		a.SetHealthy(true)
		if err := r.storage.UpdateAgentStatus(session.PeerID, true, time.Now()); err != nil {
			log.Warnf("handleAgentHeartbeat: storage update failed for %s: %v", peerIDStr, err)
		}
		r.metrics.AgentHealthUpdated(peerIDStr, region, engine, model, capacity, organization, machine, true)
	} else {
		a.SetHealthy(false)
		if err := r.storage.UpdateAgentStatus(session.PeerID, false, time.Now()); err != nil {
			log.Warnf("handleAgentHeartbeat: storage update failed for %s: %v", peerIDStr, err)
		}
		r.metrics.AgentHealthUpdated(peerIDStr, region, engine, model, capacity, organization, machine, false)
		// Only increment counter and log on the healthy→unhealthy transition.
		if prevBackendHealthy {
			r.counters.RecordBackendFailure(session.PeerID)
			log.Warnf("Agent %s reported backend unhealthy — removed from routing pool", peerIDStr[:8])
		}
	}

	if payload.Hardware != nil {
		r.hardware.Update(session.PeerID, region, model, engine, organization, machine, payload.Hardware)
	}
	if payload.BackendMetrics != nil {
		r.enginePunctual.Update(session.PeerID, model, engine, organization, machine, payload.BackendMetrics)
	}

	w.WriteHeader(http.StatusOK)
}

// handleRoutingSignals receives high-frequency metric pushes from agents (default 500ms).
// It updates engine and hardware snapshots in BadgerDB memDB and Prometheus gauges.
//
// Intentionally does NOT update LastSeen or health state — those are heartbeat concerns.
// This keeps health monitoring decoupled from metric freshness: a degraded agent that
// can still push metrics will not inadvertently reset its unhealthy timer.
func (r *Router) handleRoutingSignals(w http.ResponseWriter, req *http.Request) {
	var payload agentRoutingSignalPayload
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	session, valid := r.sessionManager.ValidateSession(payload.SessionToken)
	if !valid {
		log.Warnf("handleRoutingSignals: invalid or expired session token from %s", req.RemoteAddr)
		http.Error(w, "invalid or expired session token", http.StatusUnauthorized)
		return
	}

	// O(1) lookup: session.PeerID was set by LinkPeerID during /register.
	// Verify the agent is still in the registry (may have been removed by the health monitor).
	if r.agents.Get(session.PeerID) == nil {
		http.Error(w, "agent not found", http.StatusNotFound)
		return
	}

	region := session.Metadata.Region
	engine := session.Metadata.Engine
	model := session.Metadata.Model
	organization := session.Metadata.Organization
	machine := session.Metadata.Machine

	if payload.BackendMetrics != nil {
		r.enginePunctual.Update(session.PeerID, model, engine, organization, machine, payload.BackendMetrics)
	}
	if payload.Hardware != nil {
		r.hardware.Update(session.PeerID, region, model, engine, organization, machine, payload.Hardware)
	}

	w.WriteHeader(http.StatusOK)
}

// healthMonitor periodically checks agent heartbeat timestamps and removes dead agents.
func (r *Router) healthMonitor() {
	ticker := time.NewTicker(r.cfg.HealthCheckInterval)
	defer ticker.Stop()

	log.Infof("Health monitor started (check every %v, unhealthy after %v, remove after %v)",
		r.cfg.HealthCheckInterval, r.cfg.UnhealthyAfter, r.cfg.RemoveAfter)

	for range ticker.C {
		var toRemove []peer.ID

		// Read-only pass: mark unhealthy and collect dead agents.
		// Unregister must NOT be called here — ForEach holds RLock and
		// Unregister acquires write Lock, which deadlocks the same goroutine
		// and causes Go's RWMutex to block all subsequent readers too.
		r.agents.ForEach(func(id peer.ID, agent *domain.Agent) {
			lastSeen := func() time.Time {
				agent.RLock()
				defer agent.RUnlock()
				return agent.LastSeen
			}()

			if time.Since(lastSeen) > r.cfg.UnhealthyAfter {
				// Only count and log on the healthy→unhealthy transition.
				if agent.IsHealthy() {
					log.Warnf("Agent %s missed heartbeat, marking unhealthy", id.String()[:8])
					r.counters.RecordAgentFailure(id)
				}
				agent.SetHealthy(false)
				if err := r.storage.UpdateAgentStatus(id, false, lastSeen); err != nil {
					log.Warnf("healthMonitor: storage update failed for %s: %v", id, err)
				}
				r.metrics.AgentHealthUpdated(
					id.String(), agent.Metadata.Region, agent.Metadata.Engine, agent.Metadata.Model,
					strconv.Itoa(agent.Metadata.Capacity), agent.Metadata.Organization, agent.Metadata.Machine, false,
				)

				if time.Since(lastSeen) > r.cfg.RemoveAfter {
					toRemove = append(toRemove, id)
				}
			}
		})

		// Write pass: remove dead agents after ForEach has released RLock.
		// RecordDisconnect (called per agent below) already flushes that agent's
		// counters to diskDB individually — no need to FlushAll here.
		for _, id := range toRemove {
			agent := r.agents.Get(id)
			if agent == nil {
				continue // already removed by a concurrent unregister
			}

			// Re-check lastSeen under the agent's own lock: the agent may have
			// reconnected and refreshed its heartbeat between the ForEach pass
			// (which built toRemove) and now. Acting on a stale toRemove entry
			// would silently delete a live, freshly-registered agent.
			agent.RLock()
			lastSeen := agent.LastSeen
			agent.RUnlock()
			if time.Since(lastSeen) <= r.cfg.RemoveAfter {
				continue // agent reconnected in the gap — leave it alone
			}

			log.Warnf("Agent disconnected: %s", id.String()[:8])
			log.Warn("Agent metadata:")
			log.Warnf("   - Model: %s", agent.Metadata.Model)
			log.Warnf("   - Engine: %s", agent.Metadata.Engine)
			log.Warnf("   - Capacity: %d", agent.Metadata.Capacity)
			log.Warnf("   - Version: %s", agent.Metadata.Version)
			r.counters.RecordDisconnect(id)
			// Set agentInfo=0 before removing hardware gauges: ensures Grafana never
			// sees a window where hardware series are absent but the agent still appears online.
			r.metrics.AgentUnregistered(
				id.String(), agent.Metadata.Region, agent.Metadata.Engine, agent.Metadata.Model,
				strconv.Itoa(agent.Metadata.Capacity), agent.Metadata.Organization, agent.Metadata.Machine,
			)
			r.hardware.Unregister(id, agent.Metadata.Region, agent.Metadata.Model, agent.Metadata.Engine, agent.Metadata.Organization, agent.Metadata.Machine)
			r.enginePunctual.Unregister(id, agent.Metadata.Model, agent.Metadata.Engine, agent.Metadata.Organization, agent.Metadata.Machine)
			r.p2pHost.ConnManager().Unprotect(id, connProtectTag)
			r.p2pHost.Network().ClosePeer(id) //nolint:errcheck
			r.sessionManager.DeleteSession(agent.SessionToken)
			// Fan out the "unregistered" event before Unregister wipes the
			// metadata. Likely duplicates the libp2p Notifier (which fires on
			// connection drop ~15-30s earlier); the watcher's debounce
			// collapses near-duplicates.
			r.registrationNotifier.Publish(domain.RegistrationEvent{
				EventType:    domain.RegistrationUnregistered,
				DeploymentID: agent.Metadata.DeploymentID,
				ReplicaID:    agent.Metadata.ReplicaID,
				AgentID:      id.String(),
				Timestamp:    time.Now().UTC(),
			})
			r.agents.Unregister(id)
			r.storage.UnregisterAgent(id) //nolint:errcheck // best-effort cleanup; in-memory registry is authoritative
			r.counters.DeleteState(id)
			log.Infof("Active agents: %d", r.agents.Count())
		}
	}
}
