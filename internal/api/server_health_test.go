// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// server_health_test pins the liveness contract under a stalled log drain:
// when the container's stdout stops draining — the audit log is
// the main volume on it, and a storage hang can stall the drain — the
// router must keep serving, and in particular /health must keep answering:
//
//   - /health is registered BEFORE the global middleware (v0.1.8), so the
//     probe does no log writes at all.
//   - the access logger writes to a bounded, non-blocking writer over
//     stdout (internal/logging), so a request that goes through the
//     middleware drops its log line (counted) instead of blocking the
//     request's goroutine.
package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bounded "hivenet_router/internal/logging"
	"hivenet_router/internal/metrics"
)

// stalledLogWriter records that a write happened and blocks until unblocked.
// It simulates a stalled container log drain: the pipe stops being read, so
// any synchronous write to it parks until the drain recovers.
type stalledLogWriter struct {
	hit     atomic.Bool
	unblock chan struct{}
}

func (w *stalledLogWriter) Write(p []byte) (int, error) {
	w.hit.Store(true)
	<-w.unblock
	return len(p), nil
}

// TestHealthSurvivesStalledLogWriter proves the full contract with the
// access log's drain stalled: a request that goes through the middleware
// completes (its access line is queued/dropped, never blocks), the drop is
// counted, and /health answers.
//
// The stalled writer is installed as the BOUNDED writer's underlying via
// the accessLogSink hook: NewServer builds its access logger around
// accessLogSink(), so the swap must happen before NewServer.
func TestHealthSurvivesStalledLogWriter(t *testing.T) {
	w := &stalledLogWriter{unblock: make(chan struct{})}
	var sink *bounded.BoundedWriter
	origSink := accessLogSink
	accessLogSink = func() io.Writer {
		sink = bounded.NewBounded(w, 4)
		return sink
	}
	defer func() { accessLogSink = origSink }()

	// Release the stalled writer no matter how the test exits: if a t.Fatal
	// fired before the normal release, the bounded writer's drain goroutine
	// would otherwise stay blocked on it for the rest of the test binary.
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(w.unblock) }) }
	t.Cleanup(unblock)

	// Liveness reads no Handlers fields, so an empty Handlers suffices here.
	srv := NewServer(&Handlers{}, "0", nil, nil, nil, metrics.NewRouterMetrics(), nil, 0)

	// Control: requests that go through the global middleware must complete
	// while the drain is stalled. The bounded queue (4 lines) absorbs the
	// first few; the rest are dropped — but the requests themselves must
	// never block on their log write (before the bounded writer this
	// control blocked forever, which is what stalled the probes).
	const controls = 10
	dones := make([]chan struct{}, controls)
	for i := range dones {
		dones[i] = make(chan struct{})
	}
	for i := range dones {
		i := i
		go func() {
			defer close(dones[i])
			rr := httptest.NewRecorder()
			srv.engine.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/nonexistent", nil))
		}()
	}
	deadline := time.Now().Add(3 * time.Second)
	for i, d := range dones {
		select {
		case <-d:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("control request %d did not complete within 3s of the stalled drain — the access-log write still blocks the pipeline", i)
		}
	}

	// Self-verification: the stalled underlying was actually reached (the
	// bounded writer queued at least one line to it), and the queue overflow
	// dropped lines (10 access lines against a queue of 4).
	if !w.hit.Load() {
		t.Fatal("stalled writer was never hit — the access log never reached the drain; test is vacuous")
	}
	if sink.DroppedLines() == 0 {
		t.Fatalf("DroppedLines = 0 with %d access lines against a queue of 4 — the drop path was not exercised", controls)
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

	// Release the drain and let the bounded writer flush the queued lines.
	unblock()
}
