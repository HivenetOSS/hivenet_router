// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
)

// fakeFeed is a tiny in-process RegistrationFeed for the SSE handler test.
// It returns a channel the test controls.
type fakeFeed struct {
	events chan domain.RegistrationEvent
	cancel func()
}

func newFakeFeed() *fakeFeed {
	return &fakeFeed{
		events: make(chan domain.RegistrationEvent, 4),
		cancel: func() {},
	}
}

func (f *fakeFeed) SubscribeRegistration() (<-chan domain.RegistrationEvent, func()) {
	return f.events, f.cancel
}

// the SSE handler emits one `data: {json}\n\n` frame per published
// event in the wire shape external consumers expect.
func TestRegistrationStream_EmitsSSEFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)

	feed := newFakeFeed()
	h := api.NewHandlers(
		nil, nil, nil, time.Second,
		nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, feed,
		nil, nil, nil, nil,
		nil, nil,
	)

	router := gin.New()
	router.GET("/admin/registration-stream", h.RegistrationStream)
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Open the stream.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/admin/registration-stream", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// Push an event and read one SSE frame.
	feed.events <- domain.RegistrationEvent{
		EventType:    domain.RegistrationRegistered,
		DeploymentID: "dep-1",
		ReplicaID:    "dep-1-0",
		AgentID:      "12D3KooW...",
		Timestamp:    time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC),
	}

	// Read in a goroutine so the test can bound how long it waits independent
	// of the SSE stream remaining open.
	type result struct {
		frame string
		err   error
	}
	read := make(chan result, 1)
	go func() {
		buf := make([]byte, 4096)
		var collected string
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				collected += string(buf[:n])
				if strings.Contains(collected, "\n\n") {
					read <- result{frame: collected}
					return
				}
			}
			if err != nil {
				read <- result{frame: collected, err: err}
				return
			}
		}
	}()

	var frame string
	select {
	case r := <-read:
		frame = r.frame
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive SSE frame within 2s")
	}

	require.Contains(t, frame, "data: ")
	require.Contains(t, frame, `"event_type":"registered"`)
	require.Contains(t, frame, `"deployment_id":"dep-1"`)
	require.Contains(t, frame, `"replica_id":"dep-1-0"`)
}

// when no RegistrationFeed is wired, the handler returns 501 — the
// stream is opt-in to keep static deployments unaffected.
func TestRegistrationStream_NotImplementedWhenNoFeed(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := api.NewHandlers(
		nil, nil, nil, time.Second,
		nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, // registrationFeed = nil
		nil, // admission controller = nil
		nil, // engine-pressure provider = nil
		nil, // key admission = nil
		nil, // minute limiter = nil
		nil, // admission reject callback = nil
		nil, // estimator = nil
	)

	router := gin.New()
	router.GET("/admin/registration-stream", h.RegistrationStream)
	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/admin/registration-stream")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}
