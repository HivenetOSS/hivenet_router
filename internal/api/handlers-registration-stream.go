// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// RegistrationStream handles GET /admin/registration-stream.
//
// Serves a Server-Sent Events feed of agent-registration deltas — one
// `data: <json>\n\n` frame per settled change. External consumers
// subscribe to this feed for sub-second
// loss detection (via the router's libp2p Notifier) and pairs it with the
// /admin/routing-table snapshot as the resync safety net.
//
// Long-lived connection: the handler blocks until either the client
// disconnects or the subscriber's channel closes. A periodic `:` keep-alive
// comment is emitted to keep intermediaries from idling the stream out.
func (h *Handlers) RegistrationStream(c *gin.Context) {
	if h.registrationFeed == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "registration feed not configured"})
		return
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming not supported by ResponseWriter"})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)
	flusher.Flush()

	events, cancel := h.registrationFeed.SubscribeRegistration()
	defer cancel()

	// Keep-alive cadence: 25s sits comfortably below the typical 30s idle
	// timeout an L7 ingress imposes; the comment frame is invisible to the
	// SSE parser and keeps the connection warm during quiet periods.
	keepalive := time.NewTicker(25 * time.Second)
	defer keepalive.Stop()

	ctx := c.Request.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			if _, err := fmt.Fprintf(c.Writer, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, ok := <-events:
			if !ok {
				return // notifier closed the channel
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				// Malformed event would point at a producer bug — log and
				// skip rather than tear down the whole stream.
				log.Warnf("RegistrationStream: failed to marshal event: %v", err)
				continue
			}
			if _, err := fmt.Fprintf(c.Writer, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
