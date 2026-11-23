// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"hivenet_router/internal/wake"

	"github.com/gin-gonic/gin"
)

// DemandFeed is implemented by the router: the live pending demand of every
// model that may be held here while it wakes from zero.
type DemandFeed interface {
	// SubscribeDemand returns the current demand of every model and a
	// channel of later changes, closed when the subscriber falls behind or
	// the router stops. cancel must be called.
	SubscribeDemand() (snapshot []wake.DemandEntry, changes <-chan wake.DemandEntry, cancel func())
}

// SetDemandFeed enables GET /admin/demand-stream. Call once during wiring,
// before the server starts.
func (h *Handlers) SetDemandFeed(f DemandFeed) { h.demandFeed = f }

// demandSnapshot is the first frame of the demand stream.
type demandSnapshot struct {
	TS     time.Time          `json:"ts"`
	Models []wake.DemandEntry `json:"models"`
}

// DemandStream handles GET /admin/demand-stream.
//
// Serves a Server-Sent Events feed of the pending demand, for an external
// autoscaler that should wake a model as soon as a request is held for it
// instead of at its next poll of the metrics. The first frame is an
// `event: snapshot` with every model that may be held here; every later
// frame is one model's change (`{model, pending, active, ts}`, `removed`
// when the model left the sibling map). The router only reports numbers: it
// never starts or stops a model.
//
// The stream ends when the client falls too far behind; it then reconnects
// and gets a fresh snapshot.
func (h *Handlers) DemandStream(c *gin.Context) {
	if h.demandFeed == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "hold at zero is not enabled"})
		return
	}
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming not supported by ResponseWriter"})
		return
	}

	snapshot, changes, cancel := h.demandFeed.SubscribeDemand()
	defer cancel()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)

	first, err := json.Marshal(demandSnapshot{TS: time.Now().UTC(), Models: snapshot})
	if err != nil {
		return
	}
	if _, err := fmt.Fprintf(c.Writer, "event: snapshot\ndata: %s\n\n", first); err != nil {
		return
	}
	flusher.Flush()

	// Same cadence as the registration stream: below a typical 30 s
	// ingress idle timeout.
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
		case ev, ok := <-changes:
			if !ok {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(c.Writer, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
