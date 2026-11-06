// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/admission"

	"github.com/gin-gonic/gin"
)

func releasedCtx(holdFor time.Duration) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set(ctxKeyHoldDeadline, time.Now().Add(holdFor))
	return c
}

// TestAdmitOccupancy_ReleasedSeesNewReplicas checks that a released request
// waiting for room is admitted when replicas join, not only when a
// reservation is released.
func TestAdmitOccupancy_ReleasedSeesNewReplicas(t *testing.T) {
	ctrl := admission.NewController(1.0, 0)
	var replicas atomic.Int64
	replicas.Store(1)
	pool := func() (int, int) { return 100 * int(replicas.Load()), 0 }

	first, _ := admitOccupancy(releasedCtx(time.Minute), ctrl, "m", 100, false, pool)
	if first == nil {
		t.Fatal("first request not admitted")
	}
	defer first.Release()

	go func() {
		time.Sleep(100 * time.Millisecond)
		replicas.Store(2) // a second agent registers; nothing is released
	}()
	start := time.Now()
	res, fits := admitOccupancy(releasedCtx(time.Minute), ctrl, "m", 100, false, pool)
	if res == nil || !fits {
		t.Fatalf("second request: admitted %v, fits %v; want admitted once the replica joined", res != nil, fits)
	}
	defer res.Release()
	if waited := time.Since(start); waited > 3*admitRecheck {
		t.Errorf("admitted after %v, want within a recheck of the replica joining", waited)
	}
}

// TestAdmitOccupancy_ReleasedTooLarge checks that a released request larger
// than the whole budget is refused at once, as one that can never fit.
func TestAdmitOccupancy_ReleasedTooLarge(t *testing.T) {
	ctrl := admission.NewController(1.0, 0)
	pool := func() (int, int) { return 100, 0 }
	start := time.Now()
	res, fits := admitOccupancy(releasedCtx(time.Minute), ctrl, "m", 500, false, pool)
	if res != nil || fits {
		t.Fatalf("admitted %v, fits %v; want refused as never fitting", res != nil, fits)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Error("a request that can never fit waited")
	}
}
