// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package agent implements the inference agent daemon that bridges a local
// inference server (vLLM, Ollama, etc.) with a remote Hivenet Router.
//
// This daemon is intentionally resilient and long-lived:
//
//   - It does NOT assume the backend is running at startup
//   - It does NOT assume the router is reachable
//   - It does NOT crash on transient failures
//
// Instead, it runs a retry loop that:
//  1. Waits for the backend to become healthy
//  2. Auto-discovers the model
//  3. Authenticates with the router (gRPC)
//  4. Starts a libp2p HTTP server for handling inference requests
//  5. Registers itself with the router via libp2p HTTP (NamespacedClient)
//  6. Sends periodic heartbeats to the router
//  7. On any failure, backs off and restarts
//
// The backend-specific logic (health check, model discovery, request forwarding)
// is abstracted behind the Engine interface, allowing different backends to be
// supported via the --engine flag.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/hardware"
	rpcClient "hivenet_router/internal/transport/grpc"
	"hivenet_router/internal/transport/p2p"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	p2phttp "github.com/libp2p/go-libp2p/p2p/http"
	"github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	pb "hivenet_router/proto"
)

var log = logging.Logger("agent")

const (
	// inferenceProtocolID is the protocol the agent exposes for inference requests.
	// Must match the ID the router uses in processor.go's NamespacedClient call.
	inferenceProtocolID = "/hivenet_router/inference/1.0.0"

	// routerProtocolID is the protocol the router exposes for management endpoints
	// (/register, /heartbeat). Must match the ID in router.go's SetHTTPHandler call.
	routerProtocolID = "/hivenet_router/router/1.0.0"

	backendPollInterval = 5 * time.Second
	retryBaseInterval   = 5 * time.Second
)

// Agent is the main agent type.
type Agent struct {
	cfg        *config.AgentConfig
	engine     Engine
	httpClient *http.Client
	identity   crypto.PrivKey // persistent libp2p identity — same peer ID across reconnects
	peerID     peer.ID        // derived from identity once at startup; stamped on agent-side spans so Tempo spanmetrics carry peer_id

	// Hardware metric collection — sampled in background, read on every heartbeat.
	collector  hardware.Collector
	snapshotMu sync.RWMutex
	snapshot   *domain.HardwareSnapshot
	samplerWg  sync.WaitGroup // ensures sampler exits before collector.Close() (NVML Shutdown)

	// Engine backend metrics — scraped by the fast poller goroutine at EngineSampleInterval.
	// Stored in an atomic pointer so sendHeartbeat reads with zero lock contention.
	// Nil until the first successful scrape or when the engine does not implement MetricsProvider.
	engineMetrics atomic.Pointer[domain.BackendMetrics]

	// backendHealthy is the result of the most recent periodic backend health check.
	// Initialised to true at the start of each session (backend was healthy at registration).
	// Updated every HardwareSampleInterval by runBackendHealthChecker.
	backendHealthy atomic.Bool

	// sessionToken holds the current session token for this agent session.
	// Set once per session in runUntilDisconnect and refreshed in-place by the
	// re-auth goroutine so sendHeartbeat and pushRoutingSignals always use a
	// valid token without interrupting the heartbeat cadence.
	sessionToken atomic.Value // stores string

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewAgent initializes a new agent instance with the given engine but does not start it.
// The hardware collector is auto-detected: NVML when available, NullCollector otherwise.
func NewAgent(cfg *config.AgentConfig, engine Engine) *Agent {
	return &Agent{
		cfg:       cfg,
		engine:    engine,
		collector: hardware.NewCollector(cfg.GPUDevicesFile),
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		stopCh: make(chan struct{}),
	}
}

// Stop signals the daemon to shut down gracefully.
func (a *Agent) Stop() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
}

// stopped is a non-blocking helper to check if the agent is stopping.
func (a *Agent) stopped() bool {
	select {
	case <-a.stopCh:
		return true
	default:
		return false
	}
}

// Run starts the agent daemon and implements the infinite retry loop.
func (a *Agent) Run() error {
	log.Info("Starting Hivenet Agent (daemon mode)")
	log.Infof("Engine: %s", a.engine.Name())
	log.Infof("Backend URL: %s", a.cfg.BackendURL)
	log.Infof("Router gRPC: %s", a.cfg.RouterGRPCAddr)
	log.Infof("Capacity: %d concurrent requests", a.cfg.Capacity)

	// Load or create a persistent libp2p identity so the peer ID is stable
	// across reconnects and process restarts.
	key, err := p2p.LoadOrCreateIdentity(a.cfg.IdentityPath)
	if err != nil {
		log.Errorf("Failed to load/create agent identity at %s: %v", a.cfg.IdentityPath, err)
		return err
	}
	a.identity = key
	peerID, err := peer.IDFromPrivateKey(key)
	if err != nil {
		log.Errorf("Failed to derive peer ID from identity: %v", err)
		return err
	}
	a.peerID = peerID
	log.Infof("Agent identity path: %s (peer_id=%s)", a.cfg.IdentityPath, peerID)

	// Start hardware sampler AFTER identity load so a failed identity load does not
	// race between a running sampler goroutine and collector.Close() (NVML Shutdown).
	// samplerWg ensures the sampler has fully exited before NVML is shut down.
	a.samplerWg.Add(1)
	go a.runHardwareSampler()

	// Start engine metrics poller if the engine implements MetricsProvider.
	// The poller scrapes the engine's /metrics endpoint at EngineSampleInterval (default 500ms)
	// and stores the result in engineMetrics for the next heartbeat to pick up.
	if provider, ok := a.engine.(MetricsProvider); ok {
		log.Infof("Engine %s supports metrics scraping — starting fast poller (interval %s)",
			a.engine.Name(), a.cfg.EngineSampleInterval)
		a.samplerWg.Add(1)
		go a.runEnginePoller(provider)
	}

	defer func() {
		a.samplerWg.Wait()
		a.collector.Close()
	}()

	for {
		if a.stopped() {
			log.Info("Agent stopped")
			return nil
		}

		// Step 1: Wait for backend and discover model
		model, err := a.waitForBackend()
		if err != nil {
			return err // only returns on stop
		}
		a.cfg.Model = model
		log.Infof("Model ready: %s", a.cfg.Model)

		// Step 2: Create libp2p node and start HTTP server for inference
		node, httpHost, err := a.startP2PServer()
		if err != nil {
			log.Warnf("Failed to start P2P server: %v, retrying in %v...", err, retryBaseInterval)
			if a.sleepOrStop(retryBaseInterval) {
				return nil
			}
			continue
		}

		// Step 3: Authenticate with router via gRPC
		log.Info("Authenticating with router...")
		authResp, err := a.authenticate(node.ID())
		if err != nil {
			log.Warnf("Authentication failed: %v, retrying in %v...", err, retryBaseInterval)
			node.Close()
			if a.sleepOrStop(retryBaseInterval) {
				return nil
			}
			continue
		}
		log.Info("Authentication successful")

		// Step 4: Connect to router peer and build a NamespacedClient for the router
		// management protocol so that /register and /heartbeat are called with the
		// correct protocol prefix prepended automatically.
		log.Info("Registering with router via libp2p HTTP...")
		routerClient, err := a.buildRouterClient(node, httpHost, authResp)
		if err != nil {
			log.Warnf("Failed to connect to router P2P: %v, retrying in %v...", err, retryBaseInterval)
			node.Close()
			if a.sleepOrStop(retryBaseInterval) {
				return nil
			}
			continue
		}

		registerCtx, registerCancel := context.WithTimeout(context.Background(), 30*time.Second)
		registerErr := a.registerWithRouter(registerCtx, routerClient, node, authResp.SessionToken)
		registerCancel()
		if registerErr != nil {
			log.Warnf("Registration failed: %v, retrying in %v...", registerErr, retryBaseInterval)
			node.Close()
			if a.sleepOrStop(retryBaseInterval) {
				return nil
			}
			continue
		}
		log.Info("Registration successful")
		log.Info("Ready for requests")

		// Step 5: Send heartbeats until the router is unreachable or we're stopped
		a.runUntilDisconnect(routerClient, node, authResp)

		log.Warnf("Disconnected, will retry in %v...", retryBaseInterval)
		node.Close()
		if a.sleepOrStop(retryBaseInterval) {
			return nil
		}
	}
}

// startP2PServer creates a libp2p node, mounts the inference handler on a single
// protocol using SetHTTPHandler + ServeMux, and starts serving.
// Using SetHTTPHandler (not SetHTTPHandlerAtPath) keeps the well-known path equal
// to the protocol ID, which is what NamespacedClient on the router side resolves.
func (a *Agent) startP2PServer() (host.Host, *p2phttp.Host, error) {
	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", a.cfg.P2PListenPort)
	node, err := p2p.NewHost(listenAddr, a.identity)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create libp2p node: %w", err)
	}
	log.Infof("Local Peer ID: %s...", node.ID().String()[:min(len(node.ID().String()), 16)])

	httpHost := &p2phttp.Host{StreamHost: node}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", a.chatHandler)
	mux.HandleFunc("/", a.proxyHandler)
	mux.HandleFunc("/health", a.healthHandler)
	httpHost.SetHTTPHandler(inferenceProtocolID, mux)

	go httpHost.Serve() //nolint:errcheck

	return node, httpHost, nil
}

// buildRouterClient parses the router's libp2p info from the auth response,
// connects to the router peer, and returns a NamespacedClient for routerProtocolID.
// Using NamespacedClient instead of NewConstrainedRoundTripper ensures the correct
// URL path prefix is prepended to every request, preventing the 301-redirect/body-loss
// issue that occurs with SetHTTPHandler's forced trailing-slash registration.
func (a *Agent) buildRouterClient(node host.Host, httpHost *p2phttp.Host, authResp *pb.AuthResponse) (*http.Client, error) {
	if len(authResp.Libp2PInfo.Addresses) == 0 {
		return nil, fmt.Errorf("no router addresses provided")
	}

	routerPeerID, err := peer.Decode(authResp.Libp2PInfo.PeerId)
	if err != nil {
		return nil, fmt.Errorf("invalid router peer ID: %w", err)
	}

	var routerAddrs []multiaddr.Multiaddr
	for _, addrStr := range authResp.Libp2PInfo.Addresses {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			log.Warnf("buildRouterClient: skipping invalid multiaddr %q: %v", addrStr, err)
			continue
		}
		routerAddrs = append(routerAddrs, addr)
	}
	if len(routerAddrs) == 0 {
		return nil, fmt.Errorf("no valid router addresses in auth response")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := node.Connect(ctx, peer.AddrInfo{ID: routerPeerID, Addrs: routerAddrs}); err != nil {
		return nil, fmt.Errorf("failed to connect to router: %w", err)
	}

	// This connection is the agent's only link to the router: the router opens
	// inference streams back over it instead of dialing the agent (the agent
	// advertises no dialable address). Protect it so libp2p's connection
	// manager can never trim it as idle.
	node.ConnManager().Protect(routerPeerID, "router")

	// NamespacedClient fetches /.well-known/libp2p from the router, resolves the
	// path prefix for routerProtocolID (e.g. /hivenet_router/router/1.0.0), trims the
	// trailing slash, and wraps the transport so every subsequent request URL has
	// that prefix prepended. The handler on the router side strips the same prefix
	// before the ServeMux routes to /register or /heartbeat.
	client, err := httpHost.NamespacedClient(
		routerProtocolID,
		peer.AddrInfo{ID: routerPeerID, Addrs: routerAddrs},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespaced client: %w", err)
	}

	return &client, nil
}

// registerWithRouter POSTs the agent's peer information to /register.
// The path is relative to the router protocol namespace; NamespacedClient
// prepends the protocol prefix automatically.
// ctx controls the total deadline for the HTTP round-trip; callers should
// always supply a timeout so a stalled libp2p connection cannot block
// indefinitely (see runReAuth for the session-renewal path).
func (a *Agent) registerWithRouter(ctx context.Context, routerClient *http.Client, node host.Host, sessionToken string) error {
	payload := map[string]any{
		"session_token": sessionToken,
		"peer_id":       node.ID().String(),
		"deployment_id": a.cfg.DeploymentID,
		"replica_id":    a.cfg.ReplicaID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal registration payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://router/register", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := routerClient.Do(req)
	if err != nil {
		return fmt.Errorf("registration request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("router rejected registration: HTTP %d", resp.StatusCode)
	}

	return nil
}

// runUntilDisconnect sends periodic heartbeats to the router's /heartbeat endpoint
// and starts the high-frequency routing-signal pusher for the duration of the session.
// node is required by the re-auth goroutine to re-register with the new session token.
// Returns when a heartbeat fails or the agent is stopped.
func (a *Agent) runUntilDisconnect(routerClient *http.Client, node host.Host, authResp *pb.AuthResponse) {
	interval := time.Duration(authResp.Config.HeartbeatInterval) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}

	// Initialise sessionToken so sendHeartbeat and pushRoutingSignals can read it.
	a.sessionToken.Store(authResp.SessionToken)

	sessionTTL := time.Duration(authResp.Config.SessionTtl) * time.Second
	if sessionTTL <= reAuthBefore {
		sessionTTL = time.Hour // fallback: zero (old router) or too-short value from misconfigured router
	}

	// pushDone is closed when this session ends, stopping all session-scoped goroutines.
	pushDone := make(chan struct{})
	go a.runRoutingSignalPusher(routerClient, pushDone)
	go a.runBackendHealthChecker(pushDone)
	go a.runReAuth(routerClient, node, pushDone, sessionTTL)
	defer close(pushDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			if err := a.sendHeartbeat(routerClient); err != nil {
				log.Warnf("Heartbeat failed: %v", err)
				return
			}
		}
	}
}

// routingSignalPayload is the JSON body sent to /routing-signals every RoutingSignalInterval.
// It carries only the instantaneous routing-critical snapshots — engine backend metrics
// and hardware state. It does NOT renew the session or update health state; the heartbeat
// remains the sole health/session mechanism.
type routingSignalPayload struct {
	SessionToken   string                   `json:"session_token"`
	BackendMetrics *domain.BackendMetrics   `json:"backend_metrics,omitempty"`
	Hardware       *domain.HardwareSnapshot `json:"hardware,omitempty"`
}

// runRoutingSignalPusher sends fresh engine and hardware snapshots to the router at
// RoutingSignalInterval (default 500ms), independently of the heartbeat cadence.
// Push failures are non-fatal: logged at debug level and skipped. The heartbeat
// provides a fallback — if all pushes fail the router still gets metrics every
// HeartbeatInterval. Exits when done or stopCh is closed.
func (a *Agent) runRoutingSignalPusher(routerClient *http.Client, done <-chan struct{}) {
	interval := a.cfg.RoutingSignalInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := a.pushRoutingSignals(routerClient); err != nil {
				log.Debugf("routing signal push: %v", err)
			}
		case <-done:
			return
		case <-a.stopCh:
			return
		}
	}
}

// pushRoutingSignals sends one routing-signal POST to /routing-signals.
// Returns immediately (no retry) if bm and snap are both nil.
func (a *Agent) pushRoutingSignals(routerClient *http.Client) error {
	a.snapshotMu.RLock()
	snap := a.snapshot
	a.snapshotMu.RUnlock()

	bm := a.engineMetrics.Load()
	if bm == nil && snap == nil {
		return nil // nothing scraped yet
	}

	tok, _ := a.sessionToken.Load().(string)
	body, err := json.Marshal(routingSignalPayload{
		SessionToken:   tok,
		BackendMetrics: bm,
		Hardware:       snap,
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	resp, err := routerClient.Post("http://router/routing-signals", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// backendFailureThreshold is the number of consecutive backend health check
// failures required before marking the backend unhealthy. Mirrors the router's
// heartbeat tolerance (UnhealthyAfter / HeartbeatInterval ≈ 3).
const backendFailureThreshold = 3

// runBackendHealthChecker periodically calls WaitForReady on the engine backend
// (e.g. GET /health on vLLM) and stores the result in backendHealthy.
// Requires backendFailureThreshold consecutive failures before flipping to false,
// tolerating transient errors (timeouts, momentary GC pauses, brief GPU swaps).
// A single success resets the counter and immediately flips back to true.
// Runs for the duration of the session — exits when done or stopCh is closed.
func (a *Agent) runBackendHealthChecker(done <-chan struct{}) {
	// Backend was confirmed healthy during waitForBackend — set initial state true.
	a.backendHealthy.Store(true)

	interval := a.cfg.HardwareSampleInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}

	var consecutiveFailures int

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-a.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := a.engine.WaitForReady(ctx, a.cfg.BackendURL, a.httpClient)
			cancel()

			if err == nil {
				if !a.backendHealthy.Load() {
					log.Infof("Backend health check recovered")
				}
				consecutiveFailures = 0
				a.backendHealthy.Store(true)
			} else {
				consecutiveFailures++
				switch {
				case consecutiveFailures < backendFailureThreshold:
					// Still building up — warn so the operator sees it coming.
					log.Warnf("Backend health check failed (%d/%d): %v",
						consecutiveFailures, backendFailureThreshold, err)
				case consecutiveFailures == backendFailureThreshold:
					// Threshold crossed — one final warning, then silence.
					log.Warnf("Backend marked unhealthy after %d consecutive failures — removed from routing pool",
						backendFailureThreshold)
					a.backendHealthy.Store(false)
					// consecutiveFailures > backendFailureThreshold: already unhealthy, stay silent.
				}
			}
		}
	}
}

// runHardwareSampler runs a background goroutine that collects hardware metrics
// at HardwareSampleInterval. The cached snapshot is included in every heartbeat.
// Runs for the full agent lifetime — exits when stopCh is closed.
func (a *Agent) runHardwareSampler() {
	defer a.samplerWg.Done()

	interval := a.cfg.HardwareSampleInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	// Sample immediately so the first heartbeat after connect carries real data.
	a.sampleHardware()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			a.sampleHardware()
		}
	}
}

// sampleHardware calls the collector and caches the result.
func (a *Agent) sampleHardware() {
	snap, err := a.collector.Collect()
	if err != nil {
		log.Warnf("hardware: Collect failed: %v", err)
		return
	}
	a.snapshotMu.Lock()
	a.snapshot = snap
	a.snapshotMu.Unlock()
}

// runEnginePoller runs a background goroutine that scrapes the engine's /metrics
// endpoint at EngineSampleInterval and stores the result atomically so that
// sendHeartbeat can read it with zero contention. Each scrape uses a 5-second
// context timeout; failures are logged only on state change (first failure and
// recovery) to avoid flooding logs at 500ms intervals. Exits when stopCh is closed.
func (a *Agent) runEnginePoller(provider MetricsProvider) {
	defer a.samplerWg.Done()

	interval := a.cfg.EngineSampleInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}

	// Scrape immediately so the first heartbeat after connect carries real data.
	// Treat the initial scrape as healthy baseline — vLLM may not expose /metrics
	// until the first request, so a failure here is not worth a warning yet.
	scrapeOK := a.scrapeEngineMetrics(provider)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			ok := a.scrapeEngineMetrics(provider)
			switch {
			case !ok && scrapeOK:
				// First failure after a healthy period — log WARN once.
				log.Warnf("Engine metrics scrape started failing — routing signals will carry stale data")
			case ok && !scrapeOK:
				// Recovery after one or more failures — log INFO once.
				log.Infof("Engine metrics scrape recovered")
			}
			scrapeOK = ok
		}
	}
}

// scrapeEngineMetrics performs a single scrape and stores the result.
// Returns true on success, false on error (error logged at DEBUG level by caller context).
func (a *Agent) scrapeEngineMetrics(provider MetricsProvider) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bm, err := provider.ScrapeMetrics(ctx, a.cfg.BackendURL, a.httpClient)
	if err != nil {
		log.Debugf("engine metrics scrape failed: %v", err)
		return false
	}
	a.engineMetrics.Store(bm)
	return true
}

// heartbeatPayload is the JSON body sent by the agent on each heartbeat.
// Hardware is nil (and omitted from JSON) only before the first sample
// completes or when Collect() keeps returning errors. CPU-only nodes
// (NullCollector) still populate this field — it carries CPU and memory
// metrics with an empty GPU slice.
// BackendMetrics is nil when the engine does not implement MetricsProvider
// (e.g. CustomEngine) or before the first successful scrape completes.
type heartbeatPayload struct {
	SessionToken   string                   `json:"session_token"`
	Hardware       *domain.HardwareSnapshot `json:"hardware,omitempty"`
	BackendMetrics *domain.BackendMetrics   `json:"backend_metrics,omitempty"`
	BackendHealthy bool                     `json:"backend_healthy"`
}

// sendHeartbeat sends a single heartbeat POST to /heartbeat with the latest
// hardware snapshot and engine backend metrics attached (relative to the router
// protocol namespace).
func (a *Agent) sendHeartbeat(routerClient *http.Client) error {
	a.snapshotMu.RLock()
	snap := a.snapshot
	a.snapshotMu.RUnlock()

	tok, _ := a.sessionToken.Load().(string)
	body, err := json.Marshal(heartbeatPayload{
		SessionToken:   tok,
		Hardware:       snap,
		BackendMetrics: a.engineMetrics.Load(),
		BackendHealthy: a.backendHealthy.Load(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal heartbeat payload: %w", err)
	}
	resp, err := routerClient.Post("http://router/heartbeat", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat rejected: HTTP %d", resp.StatusCode)
	}
	return nil
}

// chatHandler handles inference requests forwarded by the router via libp2p HTTP.
// The path received here is stripped of the inference protocol prefix by SetHTTPHandler.
func (a *Agent) chatHandler(w http.ResponseWriter, r *http.Request) {
	// Extract W3C trace context from incoming headers so this span becomes
	// a child of the router's forward_to_agent span.
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

	// Derive a request_id for log correlation with the router's audit/trace logs.
	// Prefer X-Request-ID forwarded from the original client headers; fall back to
	// the trace ID from the propagated W3C context (shared with the router spans).
	requestID := extractRequestID(r, ctx)

	tracer := otel.Tracer("hivenet-agent")
	ctx, span := tracer.Start(ctx, "chat_completion",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("peer_id", a.peerID.String()),
			attribute.String("model", a.cfg.Model),
			attribute.String("engine", a.engine.Name()),
			attribute.String("request.id", requestID),
		),
	)
	defer span.End()

	log.Infof("[request_id=%s] Chat completion request received (model=%s, engine=%s)",
		requestID, a.cfg.Model, a.engine.Name())

	// Read the entire body
	rawBytes, err := io.ReadAll(r.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to read request body")
		writeErrorJSON(w, domain.NewRouterError(
			domain.ErrCodeRequestInvalid,
			"failed to read request body",
			domain.SourceRouter,
		))
		return
	}
	if len(rawBytes) == 0 {
		span.SetStatus(codes.Error, "empty request body")
		writeErrorJSON(w, domain.NewRouterError(
			domain.ErrCodeRequestInvalid,
			"empty request body",
			domain.SourceRouter,
		))
		return
	}

	var chatReq domain.ChatRequest
	if err := json.Unmarshal(rawBytes, &chatReq); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid request")
		writeErrorJSON(w, domain.NewRouterError(
			domain.ErrCodeRequestInvalid,
			fmt.Sprintf("invalid request: %v", err),
			domain.SourceRouter,
		))
		return
	}
	chatReq.RawBytes = rawBytes

	// Record request attributes on the span.
	span.SetAttributes(
		attribute.Int("request.message_count", len(chatReq.Messages)),
		attribute.Int("request.max_tokens", chatReq.MaxTokens),
		attribute.Float64("request.temperature", chatReq.Temperature),
	)

	start := time.Now()

	if chatReq.Stream {
		// Streaming: pipe backend SSE directly to the libp2p response writer
		// without buffering. forwardStreamingResponse writes headers+body to w,
		// so we must not write anything to w after it returns on success.
		err := forwardStreamingResponse(ctx, w, a.cfg.BackendURL, a.httpClient,
			chatReq.RawBytes, a.engine.Name(),
			WithHttpHeader(r.Header.Clone()), WithPeerID(a.peerID.String()),
			WithStreamWriteTimeout(a.cfg.StreamWriteIdleTimeout))
		span.SetAttributes(attribute.Float64("duration_ms", float64(time.Since(start).Milliseconds())))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			var be *BackendError
			if errors.As(err, &be) {
				span.SetAttributes(attribute.Int("backend.status_code", be.HTTPStatus))
				writeErrorJSON(w, classifyBackendHTTPStatus(be.HTTPStatus, be.Body))
			} else {
				writeErrorJSON(w, domain.NewRouterError(
					domain.ErrCodeBackendUnavailable,
					fmt.Sprintf("backend unreachable: %v", err),
					domain.SourceBackend,
				))
			}
		}
		return
	}

	respBytes, responseHeader, err := a.engine.ForwardChat(ctx, a.cfg.BackendURL, a.httpClient, a.cfg.Model, chatReq,
		WithHttpHeader(r.Header.Clone()), WithPeerID(a.peerID.String()))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		var be *BackendError
		var routerErr *domain.RouterError
		if errors.As(err, &routerErr) {
			// Engine returned a structured error (e.g. Infinity rejecting chat completions).
			// Propagate as-is so the router receives the correct error code and message.
		} else if errors.As(err, &be) {
			span.SetAttributes(attribute.Int("backend.status_code", be.HTTPStatus))
			routerErr = classifyBackendHTTPStatus(be.HTTPStatus, be.Body)
		} else {
			// Transport-level failure (connection refused, context cancelled, etc.)
			routerErr = domain.NewRouterError(
				domain.ErrCodeBackendUnavailable,
				fmt.Sprintf("backend unreachable: %v", err),
				domain.SourceBackend,
			)
		}
		writeErrorJSON(w, routerErr)
		return
	}

	inferenceMs := float64(time.Since(start).Milliseconds())
	span.SetAttributes(attribute.Float64("duration_ms", inferenceMs))

	// Parse response to record token usage on the span.
	var chatResp domain.ChatResponse
	if json.Unmarshal(respBytes, &chatResp) == nil {
		span.SetAttributes(
			attribute.Int("response.prompt_tokens", chatResp.Usage.PromptTokens),
			attribute.Int("response.completion_tokens", chatResp.Usage.CompletionTokens),
			attribute.Int("response.total_tokens", chatResp.Usage.TotalTokens),
		)
		if chatResp.Usage.CompletionTokens > 0 {
			tokPerSec := float64(chatResp.Usage.CompletionTokens) / (inferenceMs / 1000.0)
			span.SetAttributes(attribute.Float64("response.tokens_per_second", tokPerSec))
		}
		if len(chatResp.Choices) > 0 && chatResp.Choices[0].FinishReason != "" {
			span.SetAttributes(attribute.String("response.finish_reason", chatResp.Choices[0].FinishReason))
		}
	}

	log.Infof("[request_id=%s] Response ready in %v", requestID, time.Since(start))

	if responseHeader != nil {
		domain.CopyHttpHeaders(w.Header(), responseHeader)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes) //nolint:errcheck
}

// extractRequestID returns a request identifier for log correlation.
// It checks the X-Request-ID header first (forwarded from the original client
// request by the router) and falls back to the trace ID from the propagated
// W3C trace context, which is shared across router and agent spans.
func extractRequestID(r *http.Request, ctx context.Context) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return "unknown"
}

// writeErrorJSON writes a structured RouterError as JSON with the appropriate HTTP status.
func writeErrorJSON(w http.ResponseWriter, re *domain.RouterError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(domain.HTTPStatusFor(re.Code))
	body, _ := json.Marshal(domain.RouterErrorResponse{Error: re})
	w.Write(body) //nolint:errcheck
}

// healthHandler reports that the agent is alive.
func (a *Agent) healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok")) //nolint:errcheck
}

// proxyHandler is the catch-all P2P handler. Any path that is not handled
// by a more specific handler (e.g. /v1/chat/completions) lands here and is
// forwarded transparently to the backend at the same path.
func (a *Agent) proxyHandler(w http.ResponseWriter, r *http.Request) {
	a.proxyToBackend(w, r, r.URL.Path)
}

// proxyToBackend proxies an incoming libp2p HTTP request to the backend at
// the given path and writes the raw response back to w.
func (a *Agent) proxyToBackend(w http.ResponseWriter, r *http.Request, path string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorJSON(w, domain.NewRouterError(domain.ErrCodeRequestInvalid, "failed to read request body", domain.SourceRouter))
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, a.cfg.BackendURL+path, bytes.NewReader(body))
	if err != nil {
		writeErrorJSON(w, domain.NewRouterError(domain.ErrCodeBackendError, "failed to build backend request", domain.SourceRouter))
		return
	}
	// Clone all incoming headers (preserves W3C trace context injected by the router).
	req.Header = r.Header.Clone()
	req.Header.Del("Content-Length") // Go's http client sets this from the body.
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		writeErrorJSON(w, domain.NewRouterError(domain.ErrCodeBackendUnavailable, fmt.Sprintf("backend unreachable: %v", err), domain.SourceBackend))
		return
	}
	defer resp.Body.Close()

	domain.CopyHttpHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	// Streaming (SSE) responses must be flushed per-chunk so the router and the
	// end client see tokens as they arrive instead of one buffered blob. The
	// native chat handler does this via forwardStreamingResponse; the generic
	// passthrough (e.g. /v1/messages) needs the same treatment here.
	dst := io.Writer(w)
	if domain.IsStreamingResponse(resp.Header) {
		dst = NewStreamWriter(w, a.cfg.StreamWriteIdleTimeout)
	}
	io.Copy(dst, resp.Body) //nolint:errcheck
}

// waitForBackend polls the backend server until it is healthy and a model is detected.
func (a *Agent) waitForBackend() (string, error) {
	if a.cfg.Model != "" {
		log.Infof("Waiting for %s backend to become available (model override: %s)...",
			a.engine.Name(), a.cfg.Model)
	} else {
		log.Infof("Waiting for %s backend to become available...", a.engine.Name())
	}

	ctx := context.Background()
	for {
		if a.stopped() {
			return "", fmt.Errorf("agent stopped")
		}

		if err := a.engine.WaitForReady(ctx, a.cfg.BackendURL, a.httpClient); err != nil {
			log.Warnf("Backend not ready: %v (retrying in %v)", err, backendPollInterval)
			if a.sleepOrStop(backendPollInterval) {
				return "", fmt.Errorf("agent stopped")
			}
			continue
		}

		if a.cfg.Model != "" {
			return a.cfg.Model, nil
		}

		models, err := a.engine.DiscoverModels(ctx, a.cfg.BackendURL, a.httpClient)
		if err != nil {
			log.Warnf("Backend healthy but no model yet: %v (retrying in %v)", err, backendPollInterval)
			if a.sleepOrStop(backendPollInterval) {
				return "", fmt.Errorf("agent stopped")
			}
			continue
		}

		return models[0], nil
	}
}

// authenticate performs gRPC authentication with the router.
func (a *Agent) authenticate(peerID peer.ID) (*pb.AuthResponse, error) {
	if a.cfg.JWTSecret == "" {
		return nil, fmt.Errorf("JWT secret is not configured — set --jwt-secret-file or HIVENET_ROUTER_JWT_SECRET")
	}
	if len(a.cfg.JWTSecret) < 32 {
		return nil, fmt.Errorf("JWT secret too short (%d bytes): minimum 32 bytes required for HS256 (RFC 7518 §3.2)", len(a.cfg.JWTSecret))
	}

	_, expectedKey, err := auth.DeriveGRPCCredentials([]byte(a.cfg.JWTSecret))
	if err != nil {
		return nil, fmt.Errorf("derive gRPC TLS credentials: %w", err)
	}

	client, err := rpcClient.NewClient(a.cfg.RouterGRPCAddr, expectedKey)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	jwtToken, err := auth.CreateToken(
		"agent-"+peerID.String(),
		1*time.Hour,
		[]byte(a.cfg.JWTSecret),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT: %w", err)
	}

	authResp, err := client.AuthenticateWithTimeout(jwtToken, &pb.AgentMetadata{
		Model:         a.cfg.Model,
		Capacity:      int32(a.cfg.Capacity),
		Version:       a.cfg.Version,
		Region:        a.cfg.Region,
		Tags:          a.cfg.Tags,
		Engine:        a.engine.Name(),
		Organization:  a.cfg.Organization,
		Machine:       a.cfg.Machine,
		HideLlm:       a.cfg.HideLLM,
		LlmPrettyName: a.cfg.LLMPrettyName,
		LlmInfo:       a.cfg.LLMInfo,
		Capability:    a.cfg.Capability,
		GpuModel:      a.cfg.GPUModel,
	}, 30*time.Second)

	if err != nil {
		return nil, fmt.Errorf("authentication request failed: %v", err)
	}

	if !authResp.Success {
		return nil, fmt.Errorf("authentication rejected: %s", authResp.Message)
	}

	log.Infof("Session token: %s...", authResp.SessionToken[:min(len(authResp.SessionToken), 6)])
	return authResp, nil
}

// reAuthBefore is how long before token expiry to trigger re-authentication.
const reAuthBefore = 5 * time.Minute

// runReAuth renews the session token before it expires so that sendHeartbeat and
// pushRoutingSignals keep working without interruption. It re-authenticates via
// gRPC and re-registers with the router to link the new session token to this
// agent's peer ID. Runs for the duration of the session — exits when done or
// stopCh is closed.
func (a *Agent) runReAuth(routerClient *http.Client, node host.Host, done <-chan struct{}, sessionTTL time.Duration) {
	timer := time.NewTimer(sessionTTL - reAuthBefore)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			newAuthResp, err := a.authenticate(node.ID())
			if err != nil {
				log.Warnf("Re-authentication failed: %v — retrying in 30s", err)
				timer.Reset(30 * time.Second)
				continue
			}
			reAuthCtx, reAuthCancel := context.WithTimeout(context.Background(), 30*time.Second)
			reAuthErr := a.registerWithRouter(reAuthCtx, routerClient, node, newAuthResp.SessionToken)
			reAuthCancel()
			if reAuthErr != nil {
				log.Warnf("Re-registration failed: %v — retrying in 30s", reAuthErr)
				timer.Reset(30 * time.Second)
				continue
			}
			a.sessionToken.Store(newAuthResp.SessionToken)
			log.Info("Session token renewed")
			if newTTL := time.Duration(newAuthResp.Config.SessionTtl) * time.Second; newTTL > reAuthBefore {
				sessionTTL = newTTL
			}
			timer.Reset(sessionTTL - reAuthBefore)
		case <-done:
			return
		case <-a.stopCh:
			return
		}
	}
}

// sleepOrStop sleeps for the given duration but exits early if agent is stopped.
func (a *Agent) sleepOrStop(d time.Duration) bool {
	select {
	case <-a.stopCh:
		return true
	case <-time.After(d):
		return false
	}
}
