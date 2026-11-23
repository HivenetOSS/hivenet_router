// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"hivenet_router/internal/api"
	"hivenet_router/internal/wake"
)

// fakeDemandFeed is an in-process DemandFeed the test controls.
type fakeDemandFeed struct {
	snapshot []wake.DemandEntry
	changes  chan wake.DemandEntry
}

func (f *fakeDemandFeed) SubscribeDemand() ([]wake.DemandEntry, <-chan wake.DemandEntry, func()) {
	return f.snapshot, f.changes, func() {}
}

func newDemandHandlers() *api.Handlers {
	return api.NewHandlers(
		nil, nil, nil, time.Second,
		nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil,
	)
}

// TestDemandStream_SnapshotThenChange checks the wire shape: an
// `event: snapshot` frame with every model, then one `data:` frame per
// change.
func TestDemandStream_SnapshotThenChange(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ts := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	feed := &fakeDemandFeed{
		snapshot: []wake.DemandEntry{{Model: "qwen", Pending: 0, Active: 0, TS: ts}},
		changes:  make(chan wake.DemandEntry, 2),
	}
	h := newDemandHandlers()
	h.SetDemandFeed(feed)
	router := gin.New()
	router.GET("/admin/demand-stream", h.DemandStream)
	srv := httptest.NewServer(router)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/admin/demand-stream", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	frames := make(chan string, 4)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		var cur []string
		for sc.Scan() {
			line := sc.Text()
			if line == "" {
				frames <- strings.Join(cur, "\n")
				cur = nil
				continue
			}
			cur = append(cur, line)
		}
	}()
	frame := func() string {
		select {
		case f := <-frames:
			return f
		case <-time.After(2 * time.Second):
			t.Fatal("no SSE frame within 2s")
			return ""
		}
	}

	first := frame()
	require.Contains(t, first, "event: snapshot")
	require.Contains(t, first, `"models":[{"model":"qwen","pending":0,"active":0,"ts":"2026-11-23T12:00:00Z"}]`)

	feed.changes <- wake.DemandEntry{Model: "qwen", Pending: 1, Active: 0, TS: ts}
	second := frame()
	require.Equal(t, `data: {"model":"qwen","pending":1,"active":0,"ts":"2026-11-23T12:00:00Z"}`, second)

	feed.changes <- wake.DemandEntry{Model: "qwen", Pending: 0, Active: 1, LastRouted: ts, TS: ts}
	third := frame()
	require.Equal(t, `data: {"model":"qwen","pending":0,"active":1,"last_routed":"2026-11-23T12:00:00Z","ts":"2026-11-23T12:00:00Z"}`, third)

	close(feed.changes) // the feed dropped this subscriber: the stream ends
	select {
	case f, ok := <-frames:
		require.False(t, ok && f != "", "unexpected frame %q", f)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDemandStream_NotImplementedWithoutHold checks that the endpoint is
// off (501) when hold at zero is disabled.
func TestDemandStream_NotImplementedWithoutHold(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/admin/demand-stream", newDemandHandlers().DemandStream)
	srv := httptest.NewServer(router)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/admin/demand-stream")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}
