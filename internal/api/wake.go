// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"hivenet_router/internal/admission"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/wake"

	"github.com/gin-gonic/gin"
)

// WakeConfig configures holding requests for a model that is scaled to zero.
type WakeConfig struct {
	// Budget is the default hold budget, used when the model's placement in
	// the sibling map has no hold_budget.
	Budget time.Duration
	// KeepAlive is how long a request is held before the router starts
	// sending keepalive bytes, and the interval between them. 0 disables
	// keepalives.
	KeepAlive time.Duration
}

// statusClientClosedRequest is the status logged for a request whose client
// left before it was answered (the nginx convention).
const statusClientClosedRequest = 499

// wakeRetryAfter is the Retry-After of a model_waking answer: the model is
// still waking, so a retry soon is likely to find it ready.
const wakeRetryAfter = 30 * time.Second

// SetWake enables holding requests for models that are scaled to zero here.
// Call once during wiring, before the server starts. A nil holder leaves it
// disabled. It needs cross-site redirect (SetRedirect): the sibling map says
// which models may be held here.
func (h *Handlers) SetWake(holder *wake.Holder, cfg WakeConfig) {
	h.wake, h.wakeCfg = holder, cfg
}

// QuotaReplicas returns the replica count the per-model RPM quota scales
// with: the healthy agents, but at least 1 for a model that may be held here
// while it wakes. At zero replicas the quota is skipped because the request
// fails at once; a held request is served later, so it is charged as if one
// replica served it, and a key cannot hold requests past its quota.
func (h *Handlers) QuotaReplicas(healthy func(model string) int) func(model string) int {
	if healthy == nil {
		return nil
	}
	return func(model string) int {
		n := healthy(model)
		if n == 0 && h.wake != nil && h.redirect != nil && h.redirect.Map.WakeableHere(model, h.redirect.Self) {
			return 1
		}
		return n
	}
}

// WakeHold holds a request for a model that is scaled to zero here until an
// agent serving it is healthy, then lets it continue as a normal request.
//
// A request is held when the sibling map places its model, for the
// endpoint's kind, on this policloud with scale_from_zero, and no healthy
// agent here serves it. RedirectDecision runs first: a warm sibling gets the
// request with a 307 before anything is held. The hold runs after the quota
// check and before the handler, so a key over its quota is refused at once
// and every capacity-based gate sees the agents that woke.
//
// The hold lasts at most the placement's hold_budget (WakeConfig.Budget when
// it has none). When the budget runs out, or the model already holds the
// maximum number of requests, the answer is 503 model_waking with a
// Retry-After. Each held request counts once in the pending demand, the
// signal an autoscaler uses to wake the model.
//
// After WakeConfig.KeepAlive the router sends keepalive bytes so clients and
// proxies do not close an idle connection: an SSE comment for a streaming
// request, a space before the JSON body otherwise. That sends the 200 status,
// so an error after it is written in the body: as a final SSE error event
// for a streaming request, as the JSON error envelope otherwise.
func (h *Handlers) WakeHold() gin.HandlerFunc {
	return func(c *gin.Context) {
		kind := endpointKinds[c.Request.URL.Path]
		if h.wake == nil || h.redirect == nil || h.healthyAgentCount == nil || kind == "" {
			c.Next()
			return
		}
		model := peekModel(c)
		if model == "" || !modelAllowed(c, model) || h.healthyAgentCount(model) > 0 {
			c.Next()
			return
		}
		budget, ok := h.redirect.Map.Wakeable(model, kind, h.redirect.Self)
		if !ok {
			c.Next()
			return
		}
		if budget == 0 {
			budget = h.wakeCfg.Budget
		}

		resp := newHeldResponse(c)
		deadline := time.Now().Add(budget)
		outcome := h.wake.Hold(c.Request.Context(), model, callerIdentity(c), c.GetString("request_id"), wake.Options{
			Budget:      budget,
			KeepAlive:   h.wakeCfg.KeepAlive,
			OnKeepAlive: resp.keepAlive,
		})
		switch outcome {
		case wake.Woken:
			// Released requests line up for the woken capacity instead of
			// being refused by it: the admission gate lets them wait until
			// their hold budget ends, and the capacity queue takes them past
			// its depth (the hold limits already bound how many there are).
			c.Set(ctxKeyHoldDeadline, deadline)
			c.Request = c.Request.WithContext(policy.WithReleasedFromHold(c.Request.Context()))
			c.Next()
		case wake.Gone:
			// The client left: nothing reaches it. 499 (client closed the
			// request) is for the audit log and the HTTP metrics.
			c.AbortWithStatus(statusClientClosedRequest)
		default: // wake.Expired, wake.Full
			msg := fmt.Sprintf("model %s is waking up; retry in %d seconds", model, int(wakeRetryAfter.Seconds()))
			if outcome == wake.Full {
				msg = fmt.Sprintf("model %s is waking up and too many requests are already waiting for it; retry in %d seconds", model, int(wakeRetryAfter.Seconds()))
			}
			resp.fail(msg)
		}
	}
}

// ctxKeyHoldDeadline holds, for a request released by a wake, the time its
// hold budget ends.
const ctxKeyHoldDeadline = "wake_hold_deadline"

// admitOccupancy admits a request against ctrl. A request released by a wake
// waits for room until its hold budget ends, not only the short default park:
// it already waited for the model, and the burst of a wake is expected.
func admitOccupancy(c *gin.Context, ctrl *admission.Controller, key string, footprint int, grows bool, budgetTokens, maxInflight int) *admission.Reservation {
	if v, ok := c.Get(ctxKeyHoldDeadline); ok {
		if wait := time.Until(v.(time.Time)); wait > 0 {
			return ctrl.AdmitWithin(c.Request.Context(), key, footprint, grows, budgetTokens, maxInflight, wait)
		}
	}
	return ctrl.Admit(c.Request.Context(), key, footprint, grows, budgetTokens, maxInflight)
}

// rejectReleased answers a request released by a wake that still found no
// room when its hold budget ended: 503 model_waking with Retry-After, the
// same answer as a hold that expires, rather than a 429 after a long wait.
// It reports false, writing nothing, for any other request.
func rejectReleased(c *gin.Context, model string) bool {
	if _, ok := c.Get(ctxKeyHoldDeadline); !ok {
		return false
	}
	c.Header("Retry-After", strconv.Itoa(int(wakeRetryAfter.Seconds())))
	rerr := domain.NewRouterError(domain.ErrCodeModelWaking,
		fmt.Sprintf("model %s woke up but is still at capacity; retry in %d seconds", model, int(wakeRetryAfter.Seconds())),
		domain.SourceRouter)
	rerr.RetryAfter = int(wakeRetryAfter.Seconds())
	c.Set(auditKeyErrorCode, string(rerr.Code))
	setRootSpanError(c, rerr.Message)
	c.JSON(http.StatusServiceUnavailable, domain.RouterErrorResponse{Error: rerr})
	return true
}

// heldResponse is the response of a held request: whether keepalives have
// already sent the 200 status, and how to frame an error after that.
type heldResponse struct {
	c         *gin.Context
	stream    bool // the client asked for an SSE stream
	anthropic bool // Anthropic Messages framing for SSE errors
	committed bool // the status and headers are sent
}

func newHeldResponse(c *gin.Context) *heldResponse {
	r := &heldResponse{c: c, anthropic: c.Request.URL.Path == "/v1/messages"}
	if body, ok := cachedBody(c); ok {
		var peek struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(body, &peek)
		r.stream = peek.Stream
	}
	return r
}

// keepAlive sends one keepalive, sending the 200 status first if needed.
func (r *heldResponse) keepAlive() error {
	w := r.c.Writer
	if !r.committed {
		if r.stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(http.StatusOK)
		r.committed = true
		// From here on the status on the wire is 200. heldWriter records the
		// status the rest of the request sets, for the audit log and the
		// HTTP metrics, and frames errors in the body.
		r.c.Writer = &heldWriter{ResponseWriter: w, resp: r}
		w = r.c.Writer
	}
	ka := " "
	if r.stream {
		ka = ": keep-alive\n\n"
	}
	if _, err := w.WriteString(ka); err != nil {
		return err
	}
	w.Flush()
	return nil
}

// fail answers model_waking: a 503 with Retry-After, or the error in the
// body when keepalives already sent the status.
func (r *heldResponse) fail(msg string) {
	c := r.c
	c.Set(auditKeyErrorCode, string(domain.ErrCodeModelWaking))
	setRootSpanError(c, msg)
	rerr := domain.NewRouterError(domain.ErrCodeModelWaking, msg, domain.SourceRouter)
	rerr.RetryAfter = int(wakeRetryAfter.Seconds())
	body, _ := json.Marshal(domain.RouterErrorResponse{Error: rerr})
	if !r.committed {
		c.Header("Retry-After", strconv.Itoa(rerr.RetryAfter))
	}
	// After a keepalive, heldWriter records the 503 and frames the body.
	c.Data(http.StatusServiceUnavailable, "application/json", body)
	c.Writer.Flush()
	c.Abort()
}

// errorBody frames an error envelope for a response whose 200 status is
// already sent: unchanged for JSON, as a final SSE event for a stream.
func (r *heldResponse) errorBody(body []byte) []byte {
	if !r.stream {
		return body
	}
	var env domain.RouterErrorResponse
	if json.Unmarshal(body, &env) != nil || env.Error == nil {
		env.Error = domain.NewRouterError(domain.ErrCodeBackendError, string(bytes.TrimSpace(body)), domain.SourceBackend)
	}
	if r.anthropic {
		frame, _ := json.Marshal(map[string]any{
			"type":  "error",
			"error": map[string]string{"type": "api_error", "message": string(env.Error.Code) + ": " + env.Error.Message},
		})
		return []byte("event: error\ndata: " + string(frame) + "\n\n")
	}
	frame, _ := json.Marshal(env)
	return []byte("data: " + string(frame) + "\n\n")
}

// heldWriter is the response writer of a request released after keepalives
// sent its 200 status. The status can no longer change: it records the
// status the handler sets (for the audit log) and writes an error body in
// the framing the client expects.
type heldWriter struct {
	gin.ResponseWriter
	resp   *heldResponse
	status int
}

func (w *heldWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}

func (w *heldWriter) WriteHeaderNow() {}

func (w *heldWriter) Status() int {
	if w.status != 0 {
		return w.status
	}
	return w.ResponseWriter.Status()
}

func (w *heldWriter) Write(b []byte) (int, error) {
	if w.status >= http.StatusBadRequest {
		if _, err := w.ResponseWriter.Write(w.resp.errorBody(b)); err != nil {
			return 0, err
		}
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (w *heldWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
