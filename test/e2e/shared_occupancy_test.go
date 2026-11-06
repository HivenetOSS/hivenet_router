// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"hivenet_router/internal/config"
)

// Two routers with shared counters and one agent connected to both: the
// occupancy budget holds across the routers (the backend never runs more
// than it allows), and a router that shuts down hands its credits to the
// other at once.
func TestOccupancySharedAcrossRouters(t *testing.T) {
	mr := miniredis.RunT(t)
	var cur, peak atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/health") || strings.HasSuffix(r.URL.Path, "/models") {
			w.WriteHeader(http.StatusOK)
			return
		}
		n := cur.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(400 * time.Millisecond)
		cur.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"x","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel)
	}))
	t.Cleanup(backend.Close)

	policyDir := t.TempDir()
	// Two requests at once for the whole site (one replica).
	if err := os.WriteFile(filepath.Join(policyDir, "stub.yaml"),
		[]byte("models:\n  - "+stubModel+"\nmax_inflight: 2\nrouting_policy:\n  strategy: least-loaded\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	newRouter := func(id string) *siteRouter {
		cfg := config.DefaultConfig()
		cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
		cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
		cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
		cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
		cfg.PolicyModelDir = policyDir
		cfg.AdmitParkTimeout = 0
		cfg.SyncStore = "valkey"
		cfg.SyncValkeyAddrs = mr.Addr()
		cfg.SyncInterval = 100 * time.Millisecond
		cfg.RouterID = id
		return startSiteRouterOn(t, cfg)
	}
	routers := []*siteRouter{newRouter("r0"), newRouter("r1")}
	startMultiHomeAgent(t, routers, backend.URL, 10)
	for i, r := range routers {
		waitFor(t, 20*time.Second, fmt.Sprintf("agent registered with router %d", i), func() bool { return modelHealthy(r.baseURL) })
	}

	burst := func(targets []*siteRouter, n int) (ok int64) {
		var wg sync.WaitGroup
		var okN atomic.Int64
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(r *siteRouter) {
				defer wg.Done()
				resp, err := postChat(r.baseURL, false)
				if err == nil {
					if resp.StatusCode == http.StatusOK {
						okN.Add(1)
					}
					resp.Body.Close()
				}
			}(targets[i%len(targets)])
		}
		wg.Wait()
		return okN.Load()
	}

	if ok := burst(routers, 8); ok < 2 {
		t.Fatalf("only %d of 8 admitted", ok)
	}
	if peak.Load() > 2 {
		t.Fatalf("backend ran %d requests at once; the site budget is 2", peak.Load())
	}

	// r0 shuts down holding credits and hands them back, so r1 does not wait
	// for the request deadline.
	if ok := burst(routers[:1], 2); ok == 0 {
		t.Fatal("r0 admitted nothing")
	}
	routers[0].stop()
	waitFor(t, 10*time.Second, "r1 to serve two requests at once", func() bool {
		peak.Store(0)
		return burst(routers[1:], 2) == 2
	})
}
