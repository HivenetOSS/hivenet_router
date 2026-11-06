// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

// Tenant-isolation checklist (test/isolation/CHECKLIST.md), end to end: a
// real router with two static-key tenants and a real agent.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/config"
)

const (
	isoAdminKey = "sk-e2e-admin"
	isoKeyA     = "sk-e2e-tenant-a"
	isoKeyB     = "sk-e2e-tenant-b"
)

// isoAuthYAML: tenant-a and tenant-b, one key each.
var isoAuthYAML = writeAuthYAML(
	testKey{Token: isoKeyA, Owner: "tenant-a", Name: "a"},
	testKey{Token: isoKeyB, Owner: "tenant-b", Name: "b"},
)

var markerRe = regexp.MustCompile(`marker-[a-z]+-\d+`)

// startEchoBackend answers chat completions with the marker found in the
// request (marker-<tenant>-<n>), streaming or not, after a short delay so
// that concurrent requests overlap inside the router and agent.
func startEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Stream bool `json:"stream"`
		}
		_ = json.Unmarshal(body, &req)
		marker := markerRe.FindString(string(body))
		time.Sleep(20 * time.Millisecond)
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fl, _ := w.(http.Flusher)
			for _, part := range []string{marker[:len(marker)/2], marker[len(marker)/2:]} {
				fmt.Fprintf(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":%q,"choices":[{"index":0,"delta":{"content":%q},"finish_reason":null}]}`+"\n\n", stubModel, part)
				if fl != nil {
					fl.Flush()
				}
				time.Sleep(5 * time.Millisecond)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"c","object":"chat.completion","created":1,"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, stubModel, marker)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// chatAs sends one chat completion with the given key and returns the
// assistant text (joined across SSE chunks when streaming).
func chatAs(base, key, content string, stream bool) (int, string, error) {
	payload := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":10,"stream":%t}`,
		stubModel, content, stream)
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(payload))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if !stream {
		var out struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		raw, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(raw, &out); err != nil || len(out.Choices) == 0 {
			return resp.StatusCode, string(raw), nil
		}
		return resp.StatusCode, out.Choices[0].Message.Content, nil
	}
	var text strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || line == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(line), &chunk) == nil && len(chunk.Choices) > 0 {
			text.WriteString(chunk.Choices[0].Delta.Content)
		}
	}
	return resp.StatusCode, text.String(), sc.Err()
}

// startIsoStack starts a router with the two-tenant auth.yaml and one agent
// on the given backend, and waits until tenant-a can see the model.
func startIsoStack(t *testing.T, backendURL string, capacity int) string {
	t.Helper()
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, backendURL, filepath.Join(t.TempDir(), "agent.key"), func(c *config.AgentConfig) {
		c.Capacity = capacity
	})
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, isoKeyA, stubModel)
	})
	return base
}

// TestIsolation_ConcurrentResponsesNotCrossWired: two tenants send many
// concurrent requests, streaming and not, through one router and one agent.
// Every response carries its own request's marker, never another request's
// (in particular never the other tenant's).
func TestIsolation_ConcurrentResponsesNotCrossWired(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base := startIsoStack(t, startEchoBackend(t).URL, 64)

	const perTenant = 25
	var wg sync.WaitGroup
	errs := make(chan string, 2*perTenant)
	for _, tenant := range []struct{ name, key string }{{"a", isoKeyA}, {"b", isoKeyB}} {
		for i := range perTenant {
			wg.Add(1)
			go func() {
				defer wg.Done()
				marker := fmt.Sprintf("marker-%s-%d", tenant.name, i)
				status, got, err := chatAs(base, tenant.key, "echo "+marker, i%2 == 0)
				switch {
				case err != nil:
					errs <- fmt.Sprintf("%s: %v", marker, err)
				case status != http.StatusOK:
					errs <- fmt.Sprintf("%s: status %d body %q", marker, status, got)
				case got != marker:
					errs <- fmt.Sprintf("%s: got response %q (another request's answer)", marker, got)
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// postAs sends one raw chat-completions body with the given key and returns
// the status and response body.
func postAs(t *testing.T, base, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// TestIsolation_TwoTenantsQuotaE2E: one router, one agent, two tenants with
// two keys each. A key exhausting its request rate or its daily token budget
// is refused with 429; the other key of the same tenant and the other
// tenant's keys keep being served.
func TestIsolation_TwoTenantsQuotaE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	const (
		rpmA1, rpmA2, rpmB1 = "sk-q-a1", "sk-q-a2", "sk-q-b1"
		tpdA, tpdB          = "sk-q-a-tpd", "sk-q-b-tpd"
	)
	auth := writeAuthYAML(
		testKey{Token: rpmA1, Owner: "tenant-a", Name: "a1", RPM: 2},
		testKey{Token: rpmA2, Owner: "tenant-a", Name: "a2", RPM: 2},
		testKey{Token: rpmB1, Owner: "tenant-b", Name: "b1", RPM: 2},
		// 170 tokens/day: one request reserving 150 output tokens on a ~14
		// token prompt fits; after it is charged (prompt + 1 output token)
		// the next identical request no longer does.
		testKey{Token: tpdA, Owner: "tenant-a", Name: "a-tpd", TPD: 170},
		testKey{Token: tpdB, Owner: "tenant-b", Name: "b-tpd", TPD: 170},
	)
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: auth, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, startStubBackend(t).URL, filepath.Join(t.TempDir(), "agent.key"), nil)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, rpmB1, stubModel)
	})
	small := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":5}`, stubModel)

	t.Run("requests per minute", func(t *testing.T) {
		for i := range 2 {
			if code, body := postAs(t, base, rpmA1, small); code != http.StatusOK {
				t.Fatalf("a1 request %d: %d %s", i, code, body)
			}
		}
		if code, _ := postAs(t, base, rpmA1, small); code != http.StatusTooManyRequests {
			t.Fatalf("a1 third request: %d, want 429", code)
		}
		if code, body := postAs(t, base, rpmA2, small); code != http.StatusOK {
			t.Errorf("a2 (same tenant, own bucket) after a1 hit its limit: %d %s", code, body)
		}
		if code, body := postAs(t, base, rpmB1, small); code != http.StatusOK {
			t.Errorf("b1 (other tenant) after a1 hit its limit: %d %s", code, body)
		}
	})

	t.Run("tokens per day", func(t *testing.T) {
		big := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":%q}],"max_tokens":150}`,
			stubModel, strings.Repeat("x", 40))
		if code, body := postAs(t, base, tpdA, big); code != http.StatusOK {
			t.Fatalf("a-tpd first request: %d %s", code, body)
		}
		if code, body := postAs(t, base, tpdA, big); code != http.StatusTooManyRequests || !strings.Contains(body, "token") {
			t.Fatalf("a-tpd second request: %d %s, want 429 (daily budget spent)", code, body)
		}
		if code, body := postAs(t, base, tpdB, big); code != http.StatusOK {
			t.Errorf("b-tpd (other tenant) after a-tpd spent its budget: %d %s", code, body)
		}
		if code, body := postAs(t, base, rpmA2, small); code != http.StatusOK {
			t.Errorf("a2 (same tenant, no daily limit) after a-tpd spent its budget: %d %s", code, body)
		}
	})
}

// tenantSeries sums a Prometheus counter's samples per tenant_id and lists
// the key_id values seen per tenant.
func tenantSeries(t *testing.T, metricsURL, name string) (sum map[string]float64, keys map[string]map[string]bool) {
	t.Helper()
	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sum, keys = map[string]float64{}, map[string]map[string]bool{}
	label := func(line, l string) string {
		m := regexp.MustCompile(l + `="([^"]*)"`).FindStringSubmatch(line)
		if m == nil {
			return ""
		}
		return m[1]
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		var v float64
		if _, err := fmt.Sscan(line[strings.LastIndex(line, " ")+1:], &v); err != nil {
			continue
		}
		tenant, key := label(line, "tenant_id"), label(line, "key_id")
		sum[tenant] += v
		if keys[tenant] == nil {
			keys[tenant] = map[string]bool{}
		}
		keys[tenant][key] = true
	}
	return sum, keys
}

// TestIsolation_TenantMetricsAttribution: a tenant's requests move only that
// tenant's hivenet_tenant_* series, labelled with its own key ID (a static
// key's derived ID, not "anonymous").
func TestIsolation_TenantMetricsAttribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	startAgentWith(t, rcfg, startStubBackend(t).URL, filepath.Join(t.TempDir(), "agent.key"), nil)
	waitFor(t, 30*time.Second, "agent registered and healthy", func() bool {
		return modelHealthyAs(base, isoKeyA, stubModel)
	})
	metricsURL := "http://127.0.0.1" + rcfg.MetricsPort + "/metrics"
	const success = "hivenet_tenant_requests_success_total"
	small := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"ping"}],"max_tokens":5}`, stubModel)

	for range 3 {
		if code, body := postAs(t, base, isoKeyA, small); code != http.StatusOK {
			t.Fatalf("tenant-a request: %d %s", code, body)
		}
	}
	waitFor(t, 5*time.Second, "tenant-a success counter at 3", func() bool {
		sum, _ := tenantSeries(t, metricsURL, success)
		return sum["tenant-a"] == 3
	})
	if sum, _ := tenantSeries(t, metricsURL, success); sum["tenant-b"] != 0 {
		t.Errorf("tenant-b success %v after only tenant-a sent requests, want 0", sum["tenant-b"])
	}

	if code, body := postAs(t, base, isoKeyB, small); code != http.StatusOK {
		t.Fatalf("tenant-b request: %d %s", code, body)
	}
	waitFor(t, 5*time.Second, "tenant-b success counter at 1", func() bool {
		sum, _ := tenantSeries(t, metricsURL, success)
		return sum["tenant-b"] == 1
	})
	sum, keys := tenantSeries(t, metricsURL, success)
	if sum["tenant-a"] != 3 {
		t.Errorf("tenant-a success %v after tenant-b's request, want still 3", sum["tenant-a"])
	}
	for tenant, token := range map[string]string{"tenant-a": isoKeyA, "tenant-b": isoKeyB} {
		want := derivedStaticKeyID(token)
		if !keys[tenant][want] || keys[tenant]["anonymous"] {
			t.Errorf("%s key_id labels %v, want only its own %q", tenant, keys[tenant], want)
		}
	}
}

// derivedStaticKeyID is the key ID the router derives for a static key.
func derivedStaticKeyID(token string) string {
	h := sha256.Sum256([]byte(token))
	id := sha256.Sum256([]byte("hivenet-router/key-id/v1\x00" + hex.EncodeToString(h[:])))
	return "static-" + hex.EncodeToString(id[:8])
}
