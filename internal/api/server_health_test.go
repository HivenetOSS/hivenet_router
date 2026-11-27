// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// server_health_test pins the liveness contract: the public /health probe
// must stay independent of the request pipeline — in particular of the
// logging pipeline. When the container's stdout stops draining (the audit
// log is the main volume on it, and a storage hang can stall the drain), the
// deferred access-log write (which runs for every request that goes through
// the global middleware) blocks the probe's response and liveness kills a
// router that is otherwise serving fine. Registering /health before
// engine.Use means a stalled log writer can never hold the probe's response.
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"hivenet_router/internal/metrics"
)

// stalledLogWriter records that a write happened and blocks until unblocked.
type stalledLogWriter struct {
	hit     atomic.Bool
	unblock chan struct{}
}

func (w *stalledLogWriter) Write(p []byte) (int, error) {
	w.hit.Store(true)
	<-w.unblock
	return len(p), nil
}

// TestHealthSurvivesStalledLogWriter proves the probe does not share the
// logging pipeline: with gin's log writer stalled, a request that goes
// through the middleware blocks (control), while /health still answers.
//
// The stalled writer must be installed BEFORE NewServer: gin's
// LoggerWithFormatter snapshots gin.DefaultWriter into the middleware at
// construction time, so a later swap would not reach the pipeline under test.
func TestHealthSurvivesStalledLogWriter(t *testing.T) {
	w := &stalledLogWriter{unblock: make(chan struct{})}
	origWriter := gin.DefaultWriter
	gin.DefaultWriter = w
	defer func() { gin.DefaultWriter = origWriter }()

	// Release the stalled writer no matter how the test exits: if a t.Fatal
	// fired before the normal release, the control goroutine would otherwise
	// stay blocked for the rest of the test binary.
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(w.unblock) }) }
	t.Cleanup(unblock)

	// Liveness reads no Handlers fields, so an empty Handlers suffices here.
	srv := NewServer(&Handlers{}, "0", nil, nil, nil, metrics.NewRouterMetrics(), nil, 0)

	// Control: a route that goes through the global middleware must block on
	// the stalled writer (the deferred access-log write).
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		rr := httptest.NewRecorder()
		srv.engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/nonexistent", nil))
	}()

	// Wait until the control request actually stalled the pipeline; if the
	// writer is never hit the test would pass vacuously.
	deadline := time.Now().Add(2 * time.Second)
	for !w.hit.Load() {
		if time.Now().After(deadline) {
			t.Fatal("stalled writer was never hit — control request did not go through the logging pipeline; test is vacuous")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// /health must answer while the log pipeline is stalled.
	healthDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rr := httptest.NewRecorder()
		srv.engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health", nil))
		healthDone <- rr
	}()

	select {
	case rr := <-healthDone:
		if rr.Code != http.StatusOK {
			t.Fatalf("GET /health during stalled log writer: status = %d, want %d", rr.Code, http.StatusOK)
		}
		var body map[string]string
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("GET /health body %q: %v", rr.Body.String(), err)
		}
		if body["status"] != "ok" {
			t.Fatalf("GET /health body = %q, want status ok", rr.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GET /health did not answer within 3s while the log pipeline was stalled — the probe still depends on logging")
	}

	// Release the control request and let it finish before the test ends.
	unblock()
	select {
	case <-controlDone:
	case <-time.After(2 * time.Second):
		t.Fatal("control request did not complete after unblocking the log writer")
	}
}
