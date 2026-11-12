// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/router"
)

// siteRouter is one router of a multi-router site that a test can stop.
type siteRouter struct {
	baseURL string
	cfg     *config.Config
	stop    func()
}

func startSiteRouter(t *testing.T) *siteRouter {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	return startSiteRouterOn(t, cfg)
}

// startSiteRouterOn starts a router on the ports in cfg (to restart one).
func startSiteRouterOn(t *testing.T, cfg *config.Config) *siteRouter {
	t.Helper()
	t.Setenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN", "true")
	cfg.P2PListenAddr = "127.0.0.1"
	cfg.JWTSecret = jwtSecret // one secret for the whole site
	cfg.DiskDBPath = filepath.Join(t.TempDir(), "badger")
	r, err := router.New(cfg)
	if err != nil {
		t.Fatalf("router.New: %v", err)
	}
	go r.Start() //nolint:errcheck
	var once sync.Once
	stop := func() { once.Do(func() { r.Close() }) } //nolint:errcheck
	t.Cleanup(stop)
	base := "http://127.0.0.1" + cfg.HTTPPort
	waitFor(t, 15*time.Second, "router HTTP API ready", func() bool {
		resp, err := http.Get(base + "/v1/models")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return &siteRouter{baseURL: base, cfg: cfg, stop: stop}
}

// startMultiHomeAgent runs one agent connected to every router given.
func startMultiHomeAgent(t *testing.T, routers []*siteRouter, backendURL string, capacity int, extra ...string) {
	t.Helper()
	cfg := config.DefaultAgentConfig()
	cfg.JWTSecret = jwtSecret
	cfg.Model = stubModel
	cfg.Engine = "custom"
	cfg.BackendURL = backendURL
	cfg.HealthURL = backendURL + "/health"
	for _, r := range routers {
		cfg.Routers = append(cfg.Routers, "127.0.0.1"+r.cfg.GRPCPort)
	}
	cfg.Routers = append(cfg.Routers, extra...)
	cfg.IdentityPath = filepath.Join(t.TempDir(), "agent.key")
	cfg.Capacity = capacity
	cfg.Region = "e2e"
	a := agent.NewAgent(cfg, &agent.CustomEngine{HealthURL: cfg.HealthURL})
	go a.Run() //nolint:errcheck
	t.Cleanup(a.Stop)
}

func chatOK(t *testing.T, baseURL string) {
	t.Helper()
	resp, err := postChat(baseURL, false)
	if err != nil {
		t.Fatalf("chat via %s: %v", baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("chat via %s: HTTP %d: %s", baseURL, resp.StatusCode, b)
	}
}

// One agent registers with every router of the site; any router serves it,
// and losing one router leaves the others serving with no gap.
func TestAgentServesEveryRouterOfTheSite(t *testing.T) {
	backend := startStubBackend(t)
	routers := []*siteRouter{startSiteRouter(t), startSiteRouter(t), startSiteRouter(t)}
	startMultiHomeAgent(t, routers, backend.URL, 10)

	for i, r := range routers {
		waitFor(t, 20*time.Second, fmt.Sprintf("agent registered with router %d", i), func() bool {
			return modelHealthy(r.baseURL)
		})
	}
	for _, r := range routers {
		chatOK(t, r.baseURL)
	}

	routers[0].stop()
	for _, r := range routers[1:] {
		for i := 0; i < 3; i++ {
			chatOK(t, r.baseURL) // no re-registration needed: the sessions are independent
		}
	}

	chatOK(t, routers[1].baseURL)
}

// A router that is down when the agent starts (or restarts later) is retried
// with backoff; the agent registers as soon as it answers, while serving the
// routers already up.
func TestAgentConnectsToRouterThatComesLater(t *testing.T) {
	backend := startStubBackend(t)
	up := startSiteRouter(t)
	late := config.DefaultConfig()
	late.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	late.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	late.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	late.P2PPort = fmt.Sprintf("%d", freePort(t))
	startMultiHomeAgent(t, []*siteRouter{up}, backend.URL, 10, "127.0.0.1"+late.GRPCPort)

	waitFor(t, 20*time.Second, "agent registered with the router that is up", func() bool { return modelHealthy(up.baseURL) })
	chatOK(t, up.baseURL)

	time.Sleep(3 * time.Second) // the agent is retrying the missing router
	r := startSiteRouterOn(t, late)
	waitFor(t, 40*time.Second, "agent registered with the router that came later", func() bool { return modelHealthy(r.baseURL) })
	chatOK(t, r.baseURL)
	chatOK(t, up.baseURL)
}

// The agent's declared capacity is ignored: a router forwards more requests at
// once than the agent declared, and occupancy admission is what bounds them.
func TestDeclaredCapacityIsIgnored(t *testing.T) {
	var cur, peak atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/health") || strings.HasSuffix(r.URL.Path, "/models") {
			w.WriteHeader(http.StatusOK)
			return
		}
		n := cur.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(300 * time.Millisecond)
		cur.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"x","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	}))
	t.Cleanup(backend.Close)

	r := startSiteRouter(t)
	startMultiHomeAgent(t, []*siteRouter{r}, backend.URL, 1)
	waitFor(t, 20*time.Second, "agent registered", func() bool { return modelHealthy(r.baseURL) })

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); chatOK(t, r.baseURL) }()
	}
	wg.Wait()
	if peak.Load() < 2 {
		t.Fatalf("backend saw at most %d request at once: the declared capacity of 1 still caps the agent", peak.Load())
	}
}

// A router that goes away mid-request can no longer receive the response: the
// agent stops the request so the engine frees its KV cache at once, instead of
// finishing work no one will read. Routers rely on this before handing a lost
// router's occupancy credits to the others.
func TestAgentStopsRequestsOfLostRouter(t *testing.T) {
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.ReadAll(r.Body) // the server notices a closed client only once the body is read
		started <- struct{}{}
		select {
		case <-r.Context().Done():
			stopped <- struct{}{}
		case <-time.After(30 * time.Second):
		}
	}))
	t.Cleanup(backend.Close)

	r := startSiteRouter(t)
	startMultiHomeAgent(t, []*siteRouter{r}, backend.URL, 0)
	waitFor(t, 20*time.Second, "agent registered", func() bool { return modelHealthy(r.baseURL) })

	go func() {
		if resp, err := postChat(r.baseURL, false); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the request never reached the backend")
	}
	r.stop()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("the backend kept running the request of a router that is gone")
	}
}
