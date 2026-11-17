// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/agent"
	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// errCanary is the prompt the error tests send; the echo backend puts it in
// its error bodies, as engines often do.
const errCanary = "canary-echoed-prompt-3b7e1d"

// startEchoErrorBackend answers like an engine that echoes the input in its
// error bodies: a prompt containing "status-NNN" gets HTTP NNN with the
// prompt in the body, as JSON for a 400 and a 500, as plain text for a 502.
// "shape-anthropic" answers in vLLM's Anthropic-shaped error
// ({"type":"error","error":{...}}), and "shape-code" in an OpenAI-shaped
// error with a string code the router knows (context_length_exceeded): both
// nest the message where the router's own error envelope has it.
func startEchoErrorBackend(t *testing.T, models ...string) *httptest.Server {
	t.Helper()
	status := regexp.MustCompile(`status-(\d{3})`)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		data := make([]string, len(models))
		for i, m := range models {
			data[i] = fmt.Sprintf(`{"id":%q,"object":"model"}`, m)
		}
		fmt.Fprintf(w, `{"object":"list","data":[%s]}`, strings.Join(data, ","))
	})
	fail := func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m := status.FindSubmatch(body)
		if m == nil {
			w.WriteHeader(http.StatusTeapot)
			return
		}
		code := 0
		fmt.Sscan(string(m[1]), &code) //nolint:errcheck
		prompt := string(body)
		if code == http.StatusBadGateway {
			w.WriteHeader(code)
			fmt.Fprintf(w, "upstream failed on input: %s", prompt)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		quoted, _ := json.Marshal("bad input: " + prompt)
		switch {
		case strings.Contains(prompt, "shape-anthropic"):
			fmt.Fprintf(w, `{"type":"error","error":{"type":"api_error","message":%s}}`, quoted)
		case strings.Contains(prompt, "shape-code"):
			fmt.Fprintf(w, `{"error":{"code":"context_length_exceeded","message":%s}}`, quoted)
		default:
			fmt.Fprintf(w, `{"object":"error","message":%s,"type":"BadRequestError"}`, quoted)
		}
	}
	for _, p := range []string{"/v1/chat/completions", "/v1/messages", "/v1/embeddings"} {
		mux.HandleFunc(p, fail)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// sendEchoedErrors sends, as tenant-a, requests the echo backend answers with
// the prompt in an error body: retried 5xx (500 and 502) on chat, streaming
// chat and /v1/messages, a 500 on embeddings and a 400 on chat. It returns
// the bodies the client got.
func sendEchoedErrors(t *testing.T, base string) []string {
	t.Helper()
	chat := func(code int, stream bool) string {
		return fmt.Sprintf(`{"model":"chat-echo","stream":%v,"messages":[{"role":"user","content":"%s status-%d"}],"max_tokens":5}`, stream, errCanary, code)
	}
	calls := []struct{ path, body string }{
		{"/v1/chat/completions", chat(500, false)},
		{"/v1/chat/completions", chat(502, false)},
		{"/v1/chat/completions", chat(500, true)},
		{"/v1/chat/completions", chat(502, true)},
		{"/v1/chat/completions", chat(400, false)},
		{"/v1/messages", fmt.Sprintf(`{"model":"chat-echo","max_tokens":5,"messages":[{"role":"user","content":"%s status-500"}]}`, errCanary)},
		{"/v1/messages", fmt.Sprintf(`{"model":"chat-echo","max_tokens":5,"messages":[{"role":"user","content":"%s status-502"}]}`, errCanary)},
		{"/v1/embeddings", fmt.Sprintf(`{"model":"embed-echo","input":"%s status-500"}`, errCanary)},
		// Error bodies nesting the message as the router's envelope does:
		// retried (500) and not (a known code), on messages and embeddings.
		{"/v1/messages", fmt.Sprintf(`{"model":"chat-echo","max_tokens":5,"messages":[{"role":"user","content":"%s status-500 shape-anthropic"}]}`, errCanary)},
		{"/v1/messages", fmt.Sprintf(`{"model":"chat-echo","max_tokens":5,"messages":[{"role":"user","content":"%s status-400 shape-code"}]}`, errCanary)},
		{"/v1/embeddings", fmt.Sprintf(`{"model":"embed-echo","input":"%s status-500 shape-code"}`, errCanary)},
		{"/v1/chat/completions", fmt.Sprintf(`{"model":"chat-echo","messages":[{"role":"user","content":"%s status-400 shape-code"}],"max_tokens":5}`, errCanary)},
	}
	var got []string
	for _, c := range calls {
		req, _ := http.NewRequest(http.MethodPost, base+c.path, strings.NewReader(c.body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+isoKeyA)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 400 && !strings.Contains(string(body), "error") {
			t.Fatalf("%s %s: status %d, want an error", c.path, c.body, resp.StatusCode)
		}
		// Every error reaches the client with a code and a source the
		// router knows: the backend's, or the router's own once retries
		// ran out. Never a backend body passed off as the router's error.
		var env domain.RouterErrorResponse
		if json.Unmarshal(body, &env) == nil && env.Error != nil &&
			(!domain.KnownErrorCode(env.Error.Code) || (env.Error.Source != domain.SourceBackend && env.Error.Source != domain.SourceRouter)) {
			t.Errorf("%s %s: client got code %q source %q, want a known code and source", c.path, c.body, env.Error.Code, env.Error.Source)
		}
		got = append(got, string(body))
	}
	time.Sleep(200 * time.Millisecond) // let the last log lines and spans land
	return got
}

// startEchoErrorStack starts a router with one chat and one embedding agent
// on the echo backend.
func startEchoErrorStack(t *testing.T) string {
	t.Helper()
	base, rcfg := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey})
	backend := startEchoErrorBackend(t, "chat-echo", "embed-echo")
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "chat-echo", domain.CapabilityLLM)
	startEngineAgent(t, rcfg, backend.URL, &agent.VLLMEngine{}, "embed-echo", domain.CapabilityEmbedding)
	for _, m := range []string{"chat-echo", "embed-echo"} {
		waitFor(t, 30*time.Second, m+" registered and healthy", func() bool { return modelHealthyAs(base, isoKeyA, m) })
	}
	return base
}

// TestIsolation_BackendErrorsContentFree: a backend error whose body echoes
// the prompt (4xx or 5xx, retried or not, streaming or not, chat, messages or
// embeddings) never puts the prompt in a router or agent log: the audit log,
// the application log at debug level, or the access log. The client still
// gets the backend's explanation.
func TestIsolation_BackendErrorsContentFree(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base := startEchoErrorStack(t)
	bodies := sendEchoedErrors(t, base)
	if !strings.Contains(strings.Join(bodies, "\n"), errCanary) {
		t.Error("no client got the backend's error message; the router must still return it")
	}
	for _, path := range []string{auditLogPath(), appLogPath(), accessLogPath()} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 {
			t.Errorf("%s is empty; the check would prove nothing", filepath.Base(path))
		}
		if i := strings.Index(string(b), errCanary); i >= 0 {
			start := max(0, strings.LastIndex(string(b[:i]), "\n"))
			end := i + strings.Index(string(b[i:])+"\n", "\n")
			t.Errorf("%s contains the echoed prompt: %s", filepath.Base(path), b[start:end])
		}
	}
}

// TestIsolation_SpansContentFree: the router's and the agent's spans carry the
// tenant (tenant.id), and no span status or span event carries the prompt a
// backend echoed in its error body, so nothing reaches the trace backend.
func TestIsolation_SpansContentFree(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		tp.Shutdown(context.Background()) //nolint:errcheck
	})

	base := startEchoErrorStack(t)
	sendEchoedErrors(t, base)

	spans := rec.Ended()
	if len(spans) == 0 {
		t.Fatal("no span recorded; the check would prove nothing")
	}
	tenantTagged, errored := false, 0
	for _, s := range spans {
		for _, kv := range s.Attributes() {
			if kv.Key == attribute.Key("tenant.id") && kv.Value.AsString() == "tenant-a" {
				tenantTagged = true
			}
			if strings.Contains(kv.Value.String(), errCanary) {
				t.Errorf("span %q attribute %s carries the echoed prompt", s.Name(), kv.Key)
			}
		}
		if s.Status().Description != "" {
			errored++
		}
		if strings.Contains(s.Status().Description, errCanary) {
			t.Errorf("span %q status carries the echoed prompt: %s", s.Name(), s.Status().Description)
		}
		for _, ev := range s.Events() {
			for _, kv := range ev.Attributes {
				if strings.Contains(kv.Value.String(), errCanary) {
					t.Errorf("span %q event %q attribute %s carries the echoed prompt", s.Name(), ev.Name, kv.Key)
				}
			}
		}
	}
	if !tenantTagged {
		t.Error("no span carries tenant.id = tenant-a")
	}
	if errored == 0 {
		t.Error("no span has an error status; the check would prove nothing")
	}
}

// TestAudit_RouterIDOnEveryLine: with --router-id set, every audit line names
// the router that wrote it, so the lines of the routers of a site, written to
// one shared log, can be told apart (a retried or redirected request appears
// on more than one).
func TestAudit_RouterIDOnEveryLine(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e stack in -short mode")
	}
	base, _ := startRouterWith(t, routerOpts{AuthYAML: isoAuthYAML, AdminKey: isoAdminKey, Mutate: func(c *config.Config) {
		c.RouterID = "router-e2e-audit"
	}})
	_, rid := postFull(t, base, isoKeyA, "/v1/chat/completions", `{"model":"none","messages":[]}`)
	time.Sleep(100 * time.Millisecond)
	line, ok := auditLines(t)[rid]
	if !ok {
		t.Fatalf("no audit line for request %s", rid)
	}
	if line["router_id"] != "router-e2e-audit" {
		t.Errorf("audit line router_id %v, want router-e2e-audit", line["router_id"])
	}
}
