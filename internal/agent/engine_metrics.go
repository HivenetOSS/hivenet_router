// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package agent — engine_metrics.go defines the optional MetricsProvider interface
// that inference engines can implement to expose real-time backend metrics.
//
// The agent type-asserts the active engine against MetricsProvider at startup.
// Engines that implement it (VLLMEngine, SGLangEngine) have their
// ScrapeMetrics method called by the fast poller goroutine every EngineSampleInterval.
// Engines that do not implement it (CustomEngine) are silently skipped — zero changes
// required to the engine implementation.
package agent

import (
	"context"
	"net/http"

	"hivenet_router/internal/domain"
)

// MetricsProvider is an optional interface implemented by engines that expose
// a real-time metrics endpoint. It is type-asserted at runtime:
//
//	if provider, ok := engine.(MetricsProvider); ok { ... }
//
// Returning (nil, nil) is valid and means the scrape found no data.
// Errors are non-fatal: the heartbeat proceeds without backend metrics.
type MetricsProvider interface {
	// ScrapeMetrics fetches and parses the engine's metrics endpoint.
	// baseURL is the backend server URL (e.g. http://localhost:8888).
	// The context carries the scrape timeout (default 5s).
	// Pointer fields in BackendMetrics are nil when a metric is unavailable.
	ScrapeMetrics(ctx context.Context, baseURL string, httpClient *http.Client) (*domain.BackendMetrics, error)
}
