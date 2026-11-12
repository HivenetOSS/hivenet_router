// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/metrics"
	"hivenet_router/internal/redirect"
	"hivenet_router/internal/wake"

	"github.com/gin-gonic/gin"
)

// wakeSite is site b of fleetDoc, where chat model m is placed with
// scale_from_zero, with hold at zero enabled.
type wakeSite struct {
	*testSite
	holder  *wake.Holder
	charged *atomic.Int64 // per-minute quota charges

	mu      sync.Mutex
	healthy map[string]int
	status  int // the status the last request recorded
	h       *api.Handlers
}

func (s *wakeSite) recorded() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *wakeSite) count(model string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthy[model]
}

// wakeUp makes model healthy and tells the holder, as agent registration does.
func (s *wakeSite) wakeUp(model string) {
	s.mu.Lock()
	s.healthy[model] = 1
	s.mu.Unlock()
	s.holder.Wake(model)
}

// waitHeld waits until n distinct requests are held for model.
func (s *wakeSite) waitHeld(t *testing.T, model string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.holder.Demand()[model] != n {
		if time.Now().After(deadline) {
			t.Fatalf("demand for %s = %d, want %d", model, s.holder.Demand()[model], n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// atomicLimiter counts per-minute quota charges from concurrent requests.
type atomicLimiter struct {
	fakeLimiter
	n atomic.Int64
}

func (l *atomicLimiter) AllowRequest(string, string, string, int) (bool, int, error) {
	l.n.Add(1)
	return true, -1, nil
}

// newWakeSite builds the site; allowed restricts key-1 (redirectKey) to
// those models (none = every model).
func newWakeSite(t *testing.T, cfg api.WakeConfig, maxHeld int, doc redirect.MapDoc, allowed ...string) *wakeSite {
	t.Helper()
	s := &wakeSite{testSite: &testSite{queue: make(chan *domain.PendingRequest, 8)}, healthy: map[string]int{}}
	s.rd = &redirect.Redirector{Self: "b", Map: redirect.NewMap(testDomains), Signer: testSigner(t), Replay: redirect.NewReplayGuard()}
	push(t, doc, s.testSite)
	s.holder = wake.NewHolder(wake.Limits{PerModel: maxHeld, PerCaller: maxHeld, Total: 1000}, s.count, nil)
	s.holder.SetPollInterval(time.Hour) // only wakeUp releases

	keys := testKeys(t, allowed...)
	lim := &atomicLimiter{fakeLimiter: fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000}}
	s.charged = &lim.n
	h := api.NewHandlers(
		nil, nil, s.queue, 2*time.Second,
		nil, nil, nil, lim,
		nil, nil, nil, nil, keys, nil,
		s.count,
		nil, nil, nil, nil, nil, nil, nil,
	)
	h.SetRedirect(s.rd)
	h.SetWake(s.holder, cfg)
	s.h = h
	e := gin.New()
	e.Use(func(c *gin.Context) { // what the audit log and the HTTP metrics read
		c.Next()
		s.mu.Lock()
		s.status = c.Writer.Status()
		s.mu.Unlock()
	})
	e.Use(api.RequestIDMiddleware())
	inference := e.Group("/v1", api.AuthMiddleware(redirect.Wrap(s.rd, keys, keys))).
		Group("", api.BodyLimitMiddleware(1<<20), h.RedirectTarget(), h.AliasMiddleware(),
			h.RedirectDecision(), api.QuotaMiddlewareWithReplicas(lim, metrics.NewRouterMetrics(), h.QuotaReplicas(s.count)), h.WakeHold())
	inference.POST("/chat/completions", h.Passthrough)
	inference.POST("/messages", h.Passthrough)
	inference.POST("/messages/count_tokens", h.Passthrough)
	inference.POST("/embeddings", h.Embeddings)
	s.engine = e
	return s
}

var (
	noKeepAlive = api.WakeConfig{Budget: time.Minute}
	streamBody  = []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hello"}],"max_tokens":5}`)
)

func result(t *testing.T, done chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case w := <-done:
		return w
	case <-time.After(3 * time.Second):
		t.Fatal("request did not finish")
		return nil
	}
}

// TestWakeHold_ReleasesOnWake checks the main path: a request for a model
// scaled to zero here is held, counted in the demand, and served by the agent
// that wakes, under its own request ID, with the quota charged once.
func TestWakeHold_ReleasesOnWake(t *testing.T) {
	s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
	done := sendAsync(s.testSite, chatPath, chatBody, bearer())
	s.waitHeld(t, "m", 1)
	select {
	case p := <-s.queue:
		t.Fatalf("request %s was queued while the model is scaled to zero", p.ID)
	default:
	}

	s.wakeUp("m")
	p := serveOne(t, s.testSite)
	w := result(t, done)
	if w.Code != http.StatusOK || w.Body.String() != `{"served":true}` {
		t.Fatalf("got %d %s, want 200 from the woken agent", w.Code, w.Body)
	}
	if id := w.Header().Get("X-Request-ID"); p.ID != id {
		t.Errorf("served request ID %q, response %q: want the same", p.ID, id)
	}
	if n := s.charged.Load(); n != 1 {
		t.Errorf("quota charged %d times, want 1", n)
	}
	if d := s.holder.Demand(); len(d) != 0 {
		t.Errorf("demand after release = %v, want empty", d)
	}
}

// TestWakeHold_WakeReleasesEveryRequest checks that one wake releases every
// held request, not just the first.
func TestWakeHold_WakeReleasesEveryRequest(t *testing.T) {
	s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
	var dones []chan *httptest.ResponseRecorder
	for range 5 {
		dones = append(dones, sendAsync(s.testSite, chatPath, chatBody, bearer()))
	}
	s.waitHeld(t, "m", 5)
	s.wakeUp("m")
	for range 5 {
		serveOne(t, s.testSite)
	}
	for i, d := range dones {
		if w := result(t, d); w.Code != http.StatusOK {
			t.Fatalf("request %d: %d %s", i, w.Code, w.Body)
		}
	}
}

// TestWakeHold_ClientRetryCountsOnce checks that held requests resending the
// same client ID count once in the pending demand, although each gets its
// own router request ID, and that a request without a client ID counts on
// its own.
func TestWakeHold_ClientRetryCountsOnce(t *testing.T) {
	s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
	retry := map[string]string{"Authorization": "Bearer " + redirectKey, "X-Request-ID": "6f1d3a52-0c4b-4d2e-9a8f-2b7c5e1d9a00"}
	var dones []chan *httptest.ResponseRecorder
	for range 3 {
		dones = append(dones, sendAsync(s.testSite, chatPath, chatBody, retry))
	}
	dones = append(dones, sendAsync(s.testSite, chatPath, chatBody, bearer()))
	s.waitHeld(t, "m", 2)
	time.Sleep(100 * time.Millisecond) // let any late copy be held too
	if d := s.holder.Demand()["m"]; d != 2 {
		t.Fatalf("demand = %d, want 2: three copies of one client request and one other request", d)
	}
	s.wakeUp("m")
	ids := map[string]bool{}
	for range 4 {
		ids[serveOne(t, s.testSite).ID] = true
	}
	if len(ids) != 4 {
		t.Errorf("%d distinct request IDs served, want 4: the router mints one per request", len(ids))
	}
	for i, d := range dones {
		if w := result(t, d); w.Code != http.StatusOK {
			t.Fatalf("request %d: %d %s", i, w.Code, w.Body)
		}
	}
}

// TestWakeHold_BudgetExpires checks the placement's hold_budget and the 503
// model_waking answer.
func TestWakeHold_BudgetExpires(t *testing.T) {
	doc := fleetDoc(1)
	for i := range doc.Placements {
		if doc.Placements[i].Site == "b" && doc.Placements[i].Model == "m" {
			doc.Placements[i].HoldBudget = "100ms" // overrides the 1m default
		}
	}
	s := newWakeSite(t, noKeepAlive, 10, doc)
	start := time.Now()
	w := send(s.testSite, chatPath, chatBody, bearer())
	if d := time.Since(start); d < 100*time.Millisecond || d > time.Second {
		t.Errorf("held %v, want about the 100ms hold_budget", d)
	}
	assertModelWaking(t, w)
}

// TestWakeHold_FullRefusesAtOnce checks the per-model cap.
func TestWakeHold_FullRefusesAtOnce(t *testing.T) {
	s := newWakeSite(t, noKeepAlive, 1, fleetDoc(1))
	held := sendAsync(s.testSite, chatPath, chatBody, bearer())
	s.waitHeld(t, "m", 1)

	start := time.Now()
	w := send(s.testSite, chatPath, chatBody, bearer())
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("refusal took %v, want immediate", d)
	}
	assertModelWaking(t, w)

	s.wakeUp("m")
	serveOne(t, s.testSite)
	if w := result(t, held); w.Code != http.StatusOK {
		t.Fatalf("held request: %d %s", w.Code, w.Body)
	}
}

func assertModelWaking(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503; body %s", w.Code, w.Body)
	}
	if ra := w.Header().Get("Retry-After"); ra != "30" {
		t.Errorf("Retry-After %q, want 30", ra)
	}
	var env domain.RouterErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil || env.Error == nil {
		t.Fatalf("body %s: %v", w.Body, err)
	}
	if env.Error.Code != domain.ErrCodeModelWaking || env.Error.Source != domain.SourceRouter || env.Error.RetryAfter != 30 {
		t.Errorf("error %+v, want model_waking from the router with retry_after 30", env.Error)
	}
}

// TestWakeHold_NotHeld checks every case where the request is not held.
func TestWakeHold_NotHeld(t *testing.T) {
	noScale := fleetDoc(1)
	for i := range noScale.Placements {
		if noScale.Placements[i].Site == "b" && noScale.Placements[i].Model == "m" {
			no := false
			noScale.Placements[i].ScaleFromZero = &no
		}
	}
	cases := []struct {
		name   string
		doc    redirect.MapDoc
		awake  bool
		header map[string]string
		want   int // status; 0 = queued for an agent
	}{
		{"an agent is healthy", fleetDoc(1), true, bearer(), 0},
		{"placed without scale_from_zero", noScale, false, bearer(), 0},
		{"a sibling in scope serves it: redirect first", fleetDoc(1), false, scoped("all"), http.StatusTemporaryRedirect},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newWakeSite(t, noKeepAlive, 10, tc.doc)
			if tc.awake {
				s.wakeUp("m")
			}
			done := sendAsync(s.testSite, chatPath, chatBody, tc.header)
			if tc.want == 0 {
				serveOne(t, s.testSite)
				tc.want = http.StatusOK
			}
			if w := result(t, done); w.Code != tc.want {
				t.Fatalf("status %d, want %d; body %s", w.Code, tc.want, w.Body)
			}
			if d := s.holder.Demand(); len(d) != 0 {
				t.Errorf("demand %v, want nothing held", d)
			}
		})
	}

	t.Run("count_tokens is never held", func(t *testing.T) {
		s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
		done := sendAsync(s.testSite, "/v1/messages/count_tokens",
			[]byte(`{"model":"m","messages":[{"role":"user","content":"hello"}],"max_tokens":5}`), bearer())
		serveOne(t, s.testSite) // goes on at once, as before hold at zero
		if w := result(t, done); w.Code != http.StatusOK {
			t.Fatalf("status %d, want 200; body %s", w.Code, w.Body)
		}
		if d := s.holder.Demand(); len(d) != 0 {
			t.Errorf("demand %v, want nothing held", d)
		}
	})

	t.Run("model the key may not use", func(t *testing.T) {
		s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1), "other-model")
		w := send(s.testSite, chatPath, chatBody, bearer())
		if w.Code != http.StatusForbidden {
			t.Fatalf("status %d, want 403 at once; body %s", w.Code, w.Body)
		}
		if d := s.holder.Demand(); len(d) != 0 {
			t.Errorf("demand %v, want nothing held", d)
		}
	})

	t.Run("endpoint of another kind", func(t *testing.T) {
		// The map places m here as a chat model only.
		s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
		done := sendAsync(s.testSite, "/v1/embeddings", []byte(`{"model":"m","input":"x"}`), bearer())
		serveOne(t, s.testSite)
		if w := result(t, done); w.Code != http.StatusOK {
			t.Fatalf("status %d, want 200; body %s", w.Code, w.Body)
		}
	})
}

// TestWakeHold_KeepAlive checks the bytes a held request receives and that
// the body stays valid for the client after the release.
func TestWakeHold_KeepAlive(t *testing.T) {
	cfg := api.WakeConfig{Budget: time.Minute, KeepAlive: 20 * time.Millisecond}

	t.Run("json", func(t *testing.T) {
		s := newWakeSite(t, cfg, 10, fleetDoc(1))
		done := sendAsync(s.testSite, chatPath, chatBody, bearer())
		s.waitHeld(t, "m", 1)
		time.Sleep(70 * time.Millisecond)
		s.wakeUp("m")
		serveOne(t, s.testSite)
		w := result(t, done)
		body := w.Body.String()
		if w.Code != http.StatusOK || !strings.HasPrefix(body, " ") || strings.TrimLeft(body, " ") != `{"served":true}` {
			t.Fatalf("got %d %q, want 200, leading spaces, then the response", w.Code, body)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type %q, want application/json", ct)
		}
		var v map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
			t.Errorf("body is not valid JSON for the client: %v", err)
		}
	})

	t.Run("stream", func(t *testing.T) {
		s := newWakeSite(t, cfg, 10, fleetDoc(1))
		done := sendAsync(s.testSite, chatPath, streamBody, bearer())
		s.waitHeld(t, "m", 1)
		time.Sleep(70 * time.Millisecond)
		s.wakeUp("m")
		serveOne(t, s.testSite)
		w := result(t, done)
		if !strings.HasPrefix(w.Body.String(), ": keep-alive\n\n") {
			t.Fatalf("body %q, want SSE keep-alive comments first", w.Body)
		}
		if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
			t.Errorf("Content-Type %q, want text/event-stream", ct)
		}
	})

	t.Run("no keepalive before the delay", func(t *testing.T) {
		s := newWakeSite(t, api.WakeConfig{Budget: time.Minute, KeepAlive: time.Hour}, 10, fleetDoc(1))
		done := sendAsync(s.testSite, chatPath, chatBody, bearer())
		s.waitHeld(t, "m", 1)
		s.wakeUp("m")
		serveOne(t, s.testSite)
		if w := result(t, done); w.Body.String() != `{"served":true}` {
			t.Fatalf("body %q, want the response alone", w.Body)
		}
	})
}

// TestWakeHold_ErrorsAfterKeepAlive checks that an error after keepalives
// sent the 200 status still reaches the client in a form it can read.
func TestWakeHold_ErrorsAfterKeepAlive(t *testing.T) {
	expiring := fleetDoc(1)
	for i := range expiring.Placements {
		if expiring.Placements[i].Site == "b" && expiring.Placements[i].Model == "m" {
			expiring.Placements[i].HoldBudget = "100ms"
		}
	}
	cfg := api.WakeConfig{Budget: time.Minute, KeepAlive: 20 * time.Millisecond}

	t.Run("budget expires on a stream", func(t *testing.T) {
		s := newWakeSite(t, cfg, 10, expiring)
		w := send(s.testSite, chatPath, streamBody, bearer())
		body := w.Body.String()
		if w.Code != http.StatusOK || !strings.HasPrefix(body, ": keep-alive") ||
			!strings.HasSuffix(body, "\n\n") || !strings.Contains(body, `data: {"error":{"code":"model_waking"`) {
			t.Fatalf("got %d %q, want keepalives then a model_waking SSE error event", w.Code, body)
		}
		if st := s.recorded(); st != http.StatusServiceUnavailable {
			t.Errorf("recorded status %d, want 503 for the audit log and metrics", st)
		}
	})

	t.Run("budget expires on an Anthropic stream", func(t *testing.T) {
		s := newWakeSite(t, cfg, 10, expiring)
		w := send(s.testSite, "/v1/messages", streamBody, bearer())
		if body := w.Body.String(); !strings.Contains(body, "event: error\ndata: {\"error\":{\"message\":\"model_waking: ") {
			t.Fatalf("body %q, want an Anthropic error event", body)
		}
	})

	t.Run("budget expires on JSON", func(t *testing.T) {
		s := newWakeSite(t, cfg, 10, expiring)
		w := send(s.testSite, chatPath, chatBody, bearer())
		var env domain.RouterErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil || env.Error == nil || env.Error.Code != domain.ErrCodeModelWaking {
			t.Fatalf("body %q, want leading spaces then the model_waking envelope", w.Body)
		}
	})

	t.Run("routing fails after the release", func(t *testing.T) {
		s := newWakeSite(t, cfg, 10, fleetDoc(1))
		done := sendAsync(s.testSite, chatPath, streamBody, bearer())
		s.waitHeld(t, "m", 1)
		time.Sleep(50 * time.Millisecond)
		s.wakeUp("m")
		select {
		case p := <-s.queue:
			p.Error <- domain.NewRouterError(domain.ErrCodeNoCapacity, "all agents are busy", domain.SourceRouter)
		case <-time.After(2 * time.Second):
			t.Fatal("request was not queued")
		}
		w := result(t, done)
		if body := w.Body.String(); !strings.HasSuffix(body, `data: {"error":{"code":"no_capacity","message":"all agents are busy","source":"router"}}`+"\n\n") {
			t.Fatalf("body %q, want the error as a final SSE event", body)
		}
		if st := s.recorded(); st != http.StatusServiceUnavailable {
			t.Errorf("recorded status %d, want 503", st)
		}
	})
}

// TestWakeHold_QuotaCountsOneReplica checks that the per-model RPM quota,
// which scales with the healthy replicas and is skipped at zero, counts at
// least one replica for a model that may be held here: a held request is
// charged like any other.
func TestWakeHold_QuotaCountsOneReplica(t *testing.T) {
	s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1, placement("x", redirect.KindChat, "a")))
	count := s.h.QuotaReplicas(s.count)
	ctxFor := func(path string) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, path, nil)
		return c
	}
	chat, embed := ctxFor(chatPath), ctxFor("/v1/embeddings")
	if n := count(chat, "m"); n != 1 {
		t.Errorf("m at zero, wakeable here: %d replicas, want 1", n)
	}
	if n := count(embed, "m"); n != 0 {
		t.Errorf("m at zero, not wakeable here for embeddings: %d replicas, want 0 (the request fails at once)", n)
	}
	if n := count(chat, "x"); n != 0 {
		t.Errorf("x at zero, not placed here: %d replicas, want 0 (the request fails at once)", n)
	}
	s.wakeUp("m")
	if n := count(chat, "m"); n != 1 {
		t.Errorf("m awake: %d replicas, want the healthy count 1", n)
	}
}

// TestWakeHold_ClientGoneRecords499 checks the status recorded for a client
// that leaves during the hold.
func TestWakeHold_ClientGoneRecords499(t *testing.T) {
	s := newWakeSite(t, noKeepAlive, 10, fleetDoc(1))
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodPost, chatPath, bytes.NewReader(chatBody)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+redirectKey)
	done := make(chan struct{})
	go func() { s.engine.ServeHTTP(httptest.NewRecorder(), r); close(done) }()
	s.waitHeld(t, "m", 1)
	cancel()
	<-done
	if st := s.recorded(); st != 499 {
		t.Errorf("recorded status %d, want 499", st)
	}
	if d := s.holder.Demand(); len(d) != 0 {
		t.Errorf("demand %v, want empty", d)
	}
}

// TestWakeHold_ReleasedWaitsForCapacity checks a request released by a wake
// that still waits for capacity: it stays in the pending demand, and gets
// keepalives so the connection is not idle, until the model answers it.
func TestWakeHold_ReleasedWaitsForCapacity(t *testing.T) {
	s := newWakeSite(t, api.WakeConfig{Budget: time.Minute, KeepAlive: 20 * time.Millisecond}, 10, fleetDoc(1))
	done := sendAsync(s.testSite, chatPath, chatBody, bearer())
	s.waitHeld(t, "m", 1)
	s.wakeUp("m") // released before any keepalive

	// Nothing serves the queued request yet: it waits for capacity.
	time.Sleep(90 * time.Millisecond)
	if n := s.holder.Demand()["m"]; n != 1 {
		t.Errorf("demand while the released request waits = %d, want 1", n)
	}
	serveOne(t, s.testSite)
	w := result(t, done)
	body := w.Body.String()
	if w.Code != http.StatusOK || !strings.HasPrefix(body, " ") || strings.TrimLeft(body, " ") != `{"served":true}` {
		t.Fatalf("got %d %q, want 200, keepalive spaces sent while waiting, then the response", w.Code, body)
	}
	if d := s.holder.Demand(); len(d) != 0 {
		t.Errorf("demand after the answer = %v, want empty", d)
	}
}
