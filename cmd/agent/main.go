// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package main is the entry point for the Hivenet Agent daemon.
//
// The agent runs as a long-lived daemon process that:
//  1. Polls the local inference server until it becomes healthy and a model is detected
//  2. Authenticates with the router via gRPC and connects over libp2p
//  3. Forwards inference requests from the router to the local backend
//  4. Automatically reconnects on any failure (backend down, router disconnect, etc.)
//
// The agent supports multiple backends via the --engine flag:
//   - vllm (default): vLLM inference server
//   - ollama: Ollama inference server
//   - sglang: SGLang inference server
//   - llamacpp: llama.cpp inference server (launch with --metrics)
//   - infinity: Infinity server (embedding + reranking models)
//   - custom: any OpenAI-compatible server (requires --model and --health-url)
//
// Usage:
//
//	./hivenet-agent [flags]
//	./hivenet-agent --engine vllm --backend-url http://localhost:8888
//	./hivenet-agent --engine ollama --backend-url http://localhost:8888
//	./hivenet-agent --engine ollama --model gpt-oss:20b --backend-url http://localhost:8888
//	./hivenet-agent --engine custom --model mistral-7b --health-url http://localhost:1234/health --backend-url http://localhost:1234
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/logger"
	"hivenet_router/internal/tracing"
	"hivenet_router/internal/transport/p2p"
)

var log = logging.Logger("agent")

func main() {
	cfg := config.DefaultAgentConfig()

	// Engine selection
	engineName := flag.String("engine", cfg.Engine, "Inference engine: vllm, ollama, sglang, or custom")

	// Backend URL
	backendURL := flag.String("backend-url", cfg.BackendURL, "Backend inference server URL")

	// Model override — if set, skips auto-detection
	modelOverride := flag.String("model", "", "Model name (overrides auto-detection; required for --engine custom)")

	// Health URL — custom endpoint for health checking (required for --engine custom)
	healthURL := flag.String("health-url", "", "Health check URL (required for --engine custom)")

	// Standard agent flags
	capacity := flag.Int("capacity", cfg.Capacity, "Maximum concurrent requests")
	version := flag.String("version", cfg.Version, "Agent version")
	region := flag.String("region", cfg.Region, "Deployment region")
	organization := flag.String("organization", cfg.Organization, "Cloud/compute provider hosting this agent (e.g. AWS, OVH, Hivenet Compute)")
	machine := flag.String("machine", cfg.Machine, "Machine name within the organization")
	routerGRPC := flag.String("router-grpc", cfg.RouterGRPCAddr, "Router gRPC address")
	routerP2P := flag.String("router-p2p", cfg.RouterP2PAddr, "Router libp2p address")
	tagsStr := flag.String("tags", strings.Join(cfg.Tags, ","), "Comma-separated tags")
	httpTimeout := flag.Duration("http-timeout", cfg.HTTPTimeout, "HTTP timeout for backend requests")
	streamWriteTimeout := flag.Duration("stream-write-timeout", cfg.StreamWriteIdleTimeout, "Rolling per-chunk deadline for writing a streaming response back to the router; bounds how long a stalled write may block before the stream is released (0 disables)")
	identityPath := flag.String("identity-path", cfg.IdentityPath, "Path to the persistent libp2p private key file (stable peer ID across restarts)")
	p2pListenPort := flag.Int("p2p-listen-port", cfg.P2PListenPort, "TCP port for the agent's libp2p node (0 = random; set a fixed port when running in Docker)")
	hardwareSampleInterval := flag.Duration("hardware-sample-interval", cfg.HardwareSampleInterval, "How often the background hardware sampler collects GPU/CPU/memory metrics")
	engineSampleInterval := flag.Duration("engine-sample-interval", cfg.EngineSampleInterval, "How often the engine metrics poller scrapes the backend /metrics endpoint (vLLM, SGLang, llama.cpp)")
	routingSignalInterval := flag.Duration("routing-signal-interval", cfg.RoutingSignalInterval, "How often the agent pushes fresh engine+hardware metrics to the router's /routing-signals endpoint (default 500ms)")
	jwtSecretFile := flag.String("jwt-secret-file", "", "Path to file containing the HMAC-SHA256 secret (env: HIVENET_ROUTER_JWT_SECRET)")
	gpuDevicesFile := flag.String("gpu-devices-file", "", "Path to a file containing the GPU UUIDs assigned to the engine (NVIDIA_VISIBLE_DEVICES format). When set, hardware metrics are restricted to those GPUs only.")
	gpuModel := flag.String("gpu-model", os.Getenv("HIVENET_ROUTER_GPU_MODEL"), "Hardware identifier used as routing metadata (e.g. RTX4090, RTX5090). Env: HIVENET_ROUTER_GPU_MODEL")
	deploymentID := flag.String("deployment-id", os.Getenv("HIVENET_ROUTER_DEPLOYMENT_ID"), "Identifier of the logical deployment this agent serves. Env: HIVENET_ROUTER_DEPLOYMENT_ID")
	replicaID := flag.String("replica-id", os.Getenv("HIVENET_ROUTER_REPLICA_ID"), "Identifier of this replica; with --deployment-id it forms a stable join key for external schedulers. Env: HIVENET_ROUTER_REPLICA_ID")
	capability := flag.String("capability", cfg.Capability, "Inference capability: llm (default), embedding, reranker")
	hideLLM := flag.Bool("hide-llm", cfg.HideLLM, "Exclude this agent from the /v1/models listing (agent still accepts requests)")
	llmPrettyName := flag.String("llm-pretty-name", cfg.LLMPrettyName, "Human-readable display name for the model (e.g. \"xLAM 8B\")")
	llmInfo := flag.String("llm-info", cfg.LLMInfo, "Short description of the model's capabilities (e.g. \"A small model useful for FC\")")
	flag.Parse()

	// Apply parsed values
	cfg.Engine = *engineName
	cfg.BackendURL = *backendURL
	cfg.Model = *modelOverride
	cfg.HealthURL = *healthURL
	cfg.Capacity = *capacity
	cfg.Version = *version
	cfg.Region = *region
	cfg.Organization = *organization
	cfg.Machine = *machine
	cfg.RouterGRPCAddr = *routerGRPC
	cfg.RouterP2PAddr = *routerP2P
	cfg.HTTPTimeout = *httpTimeout
	cfg.StreamWriteIdleTimeout = *streamWriteTimeout
	cfg.IdentityPath = *identityPath
	cfg.P2PListenPort = *p2pListenPort
	cfg.HardwareSampleInterval = *hardwareSampleInterval
	cfg.EngineSampleInterval = *engineSampleInterval
	cfg.RoutingSignalInterval = *routingSignalInterval
	cfg.GPUDevicesFile = *gpuDevicesFile
	cfg.GPUModel = *gpuModel
	cfg.DeploymentID = *deploymentID
	cfg.ReplicaID = *replicaID
	cfg.Capability = *capability
	cfg.HideLLM = *hideLLM
	cfg.LLMPrettyName = *llmPrettyName
	cfg.LLMInfo = *llmInfo

	// Load or generate the persistent libp2p identity so every subsequent log line
	// can be tagged with the agent's peer_id via go-log labels. The agent also
	// calls LoadOrCreateIdentity later in Run(); the second read is idempotent
	// (same key file), so there's no duplicated state — just a tiny extra file
	// read at startup.
	//
	// Logging is not yet configured at this point, so errors go to stderr directly.
	identityKey, err := p2p.LoadOrCreateIdentity(cfg.IdentityPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load/create agent identity at %s: %v\n", cfg.IdentityPath, err)
		os.Exit(1)
	}
	peerID, err := peer.IDFromPrivateKey(identityKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to derive peer ID from identity: %v\n", err)
		os.Exit(1)
	}

	logger.Setup(map[string]string{"peer_id": peerID.String()})

	log.Infof("Hivenet Agent (daemon) peer_id=%s", peerID)

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

	if *tagsStr != "" {
		cfg.Tags = strings.Split(*tagsStr, ",")
	}

	// Select and validate engine
	var engine agent.Engine
	switch cfg.Engine {
	case "vllm":
		engine = &agent.VLLMEngine{}
	case "ollama":
		engine = &agent.OllamaEngine{}
	case "sglang":
		engine = &agent.SGLangEngine{}
	case "llamacpp":
		engine = &agent.LlamaCPPEngine{}
	case "infinity":
		engine = &agent.InfinityEngine{}
	case "custom":
		if cfg.Model == "" {
			log.Error("Error: --model is required for --engine custom")
			os.Exit(1)
		}
		if cfg.HealthURL == "" {
			log.Error("Error: --health-url is required for --engine custom")
			os.Exit(1)
		}
		engine = &agent.CustomEngine{HealthURL: cfg.HealthURL}
	default:
		log.Errorf("Error: unknown engine %q (supported: vllm, ollama, sglang, llamacpp, infinity, custom)", cfg.Engine)
		os.Exit(1)
	}

	// Validate capability value.
	switch cfg.Capability {
	case "llm", "embedding", "reranker":
		// valid
	default:
		log.Errorf("Error: unknown capability %q (supported: llm, embedding, reranker)", cfg.Capability)
		os.Exit(1)
	}

	// Validate capability/engine combination.
	// Ollama has no native /v1/rerank endpoint (PR pending, not merged).
	// All other engines (vllm, sglang, llamacpp, infinity, custom) support reranking.
	if cfg.Capability == "reranker" && cfg.Engine == "ollama" {
		log.Error("Error: --capability reranker is not supported with --engine ollama (no /v1/rerank endpoint)")
		os.Exit(1)
	}

	// Initialize OpenTelemetry tracing.
	// Reads OTEL_EXPORTER_OTLP_ENDPOINT env var; noop when unset.
	shutdownTracer, err := tracing.Init(context.Background(), "hivenet-agent", cfg.Version)
	if err != nil {
		log.Fatalf("Failed to initialize tracing: %v", err)
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		log.Infof("Tracing enabled — exporting to %s", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	} else {
		log.Info("Tracing disabled — set OTEL_EXPORTER_OTLP_ENDPOINT to enable")
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	a := agent.NewAgent(cfg, engine)

	go func() {
		<-sigCh
		log.Info("Shutdown signal received, stopping agent...")
		a.Stop()
		if err := shutdownTracer(context.Background()); err != nil {
			log.Errorf("Error shutting down tracer: %v", err)
		}
	}()

	if err := a.Run(); err != nil {
		log.Errorf("Agent error: %v", err)
		os.Exit(1)
	}
}
