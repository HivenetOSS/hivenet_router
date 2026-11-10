// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"hivenet_router/internal/config"
)

// An agent given one router of a site finds the others: every router that
// advertises its gRPC address is handed to the agent, at authentication and
// in every heartbeat reply, so a router added later gets the agent too.
func TestAgentDiscoversEveryRouterOfTheSite(t *testing.T) {
	mr := miniredis.RunT(t)
	// An existing site: its store's data is older than the settle window.
	mr.Set("hivenet:{default}:born", strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10)) //nolint:errcheck
	backend := startStubBackend(t)
	newRouter := func(id string) *siteRouter {
		cfg := config.DefaultConfig()
		cfg.HTTPPort = fmt.Sprintf(":%d", freePort(t))
		cfg.GRPCPort = fmt.Sprintf(":%d", freePort(t))
		cfg.MetricsPort = fmt.Sprintf(":%d", freePort(t))
		cfg.P2PPort = fmt.Sprintf("%d", freePort(t))
		cfg.SyncStore = "valkey"
		cfg.SyncValkeyAddrs = mr.Addr()
		cfg.SyncInterval = 100 * time.Millisecond
		cfg.HeartbeatInterval = time.Second
		cfg.RouterID = id
		cfg.AdvertiseGRPCAddr = "127.0.0.1" + cfg.GRPCPort
		return startSiteRouterOn(t, cfg)
	}
	r0, r1 := newRouter("r0"), newRouter("r1")
	time.Sleep(300 * time.Millisecond) // both routers are in the store

	// The agent is given r0 only.
	startMultiHomeAgent(t, []*siteRouter{r0}, backend.URL, 10)
	for i, r := range []*siteRouter{r0, r1} {
		waitFor(t, 20*time.Second, fmt.Sprintf("agent registered with router %d", i), func() bool { return modelHealthy(r.baseURL) })
	}
	chatOK(t, r1.baseURL)

	// A router added later is learned from a heartbeat reply.
	r2 := newRouter("r2")
	waitFor(t, 20*time.Second, "agent registered with the router added later", func() bool { return modelHealthy(r2.baseURL) })
	chatOK(t, r2.baseURL)
}
