// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/config"
)

// When the router times a request out (504), the agent stops the backend call
// at the same moment: the router has released the request's occupancy, so an
// engine still running it would hold capacity no one counts. libp2p does not
// tell the agent the router gave up; the router sends the time left.
func TestRouterTimeoutStopsBackendRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	stopped := make(chan time.Time, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.ReadAll(r.Body)
		started <- struct{}{}
		select {
		case <-r.Context().Done():
			stopped <- time.Now()
		case <-time.After(30 * time.Second):
		}
	}))
	t.Cleanup(backend.Close)
	cfg := config.DefaultConfig()
	cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
	cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
	cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
	cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
	cfg.RequestTimeout = 2 * time.Second
	r := startSiteRouterOn(t, cfg)
	startMultiHomeAgent(t, []*siteRouter{r}, backend.URL, 0)
	waitFor(t, 20*time.Second, "agent registered", func() bool { return modelHealthy(r.baseURL) })
	t0 := time.Now()
	go func() {
		if resp, err := postChat(r.baseURL, false); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	select {
	case at := <-stopped:
		if d := at.Sub(t0); d > 4*time.Second {
			t.Fatalf("backend stopped %v after the request, long after the 2s timeout", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("router timed out the request but the backend kept running it")
	}
}
