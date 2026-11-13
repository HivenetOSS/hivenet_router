// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"hivenet_router/internal/config"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/prefixhash"
)

const (
	testSecret = "agent-site-secret-at-least-32-bytes!"
	fakeStart  = 7 // the fake template's message-start token
)

// fakeVLLM renders messages as [start, role, content bytes..., end] each,
// plus [start, 99] as the generation prompt, and returns the prompt's tokens
// (and per-chunk token IDs) when asked with return_token_ids.
type fakeVLLM struct {
	mu      sync.Mutex
	bodies  []map[string]any
	headers []http.Header
}

func render(msgs []any, gen bool) []int {
	var toks []int
	for _, m := range msgs {
		mm := m.(map[string]any)
		toks = append(toks, fakeStart, len(mm["role"].(string)))
		for _, c := range []byte(fmt.Sprint(mm["content"])) {
			toks = append(toks, 100+int(c))
		}
		toks = append(toks, 8)
	}
	if gen {
		toks = append(toks, fakeStart, 99)
	}
	return toks
}

func (f *fakeVLLM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	f.headers = append(f.headers, r.Header.Clone())
	f.mu.Unlock()
	msgs, _ := body["messages"].([]any)
	switch r.URL.Path {
	case "/tokenize":
		gen, _ := body["add_generation_prompt"].(bool)
		json.NewEncoder(w).Encode(map[string]any{"tokens": render(msgs, gen)}) //nolint:errcheck
	case "/v1/chat/completions":
		ids, _ := body["return_token_ids"].(bool)
		prompt, _ := json.Marshal(render(msgs, true))
		if stream, _ := body["stream"].(bool); stream {
			w.Header().Set("Content-Type", "text/event-stream")
			first := `{"id":"c","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]`
			if ids {
				first += `,"prompt_token_ids":` + string(prompt)
			}
			fmt.Fprintf(w, "data: %s}\n\n", first)
			for _, tok := range []string{"he", "llo"} {
				chunk := `{"id":"c","choices":[{"index":0,"delta":{"content":"` + tok + `"}`
				if ids {
					chunk += `,"token_ids":[1]`
				}
				fmt.Fprintf(w, "data: %s}]}\n\n", chunk)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		resp := `{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"token_ids":[1,2]}],"usage":{"prompt_tokens":5}`
		if ids {
			resp += `,"prompt_token_ids":` + string(prompt)
		}
		fmt.Fprint(w, resp+"}")
	}
}

func newPrefixAgent(t *testing.T) (*Agent, *fakeVLLM) {
	t.Helper()
	fake := &fakeVLLM{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	a := &Agent{
		cfg:        &config.AgentConfig{BackendURL: srv.URL, Model: "m", JWTSecret: testSecret},
		engine:     &VLLMEngine{},
		httpClient: srv.Client(),
		stopCh:     make(chan struct{}),
	}
	a.initPrefix()
	if a.prefix == nil || a.prefix.startToken != fakeStart {
		t.Fatalf("prefix state %+v, want start token %d", a.prefix, fakeStart)
	}
	return a, fake
}

func chatBody(stream bool, extra string) string {
	long := strings.Repeat("x", 300)
	return fmt.Sprintf(`{"model":"m","stream":%t%s,"messages":[{"role":"system","content":"%s"},{"role":"user","content":"%s"}]}`, stream, extra, long, long)
}

func callChat(t *testing.T, a *Agent, body string, anchors bool) (*httptest.ResponseRecorder, []uint64) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	var want []uint64
	if anchors {
		var cr domain.ChatRequest
		_ = json.Unmarshal([]byte(body), &cr)
		want, _ = prefixhash.DeriveKeys([]byte("router-key-irrelevant-for-the-agent")).Anchors("m", "", &cr)
		req.Header.Set(domain.HeaderPrefixAnchors, prefixhash.EncodeAnchors(want))
	}
	rec := httptest.NewRecorder()
	a.chatHandler(rec, req)
	return rec, want
}

func TestPrefix_StreamingReportsBlocksAndStripsTokens(t *testing.T) {
	a, fake := newPrefixAgent(t)
	rec, anchors := callChat(t, a, chatBody(true, ""), true)
	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", rec.Code, rec.Body)
	}
	out := rec.Body.String()
	if strings.Contains(out, "token_ids") {
		t.Errorf("token IDs reached the router: %s", out)
	}
	if !strings.Contains(out, `"content":"he"`) || !strings.Contains(out, "[DONE]") {
		t.Errorf("stream altered: %s", out)
	}
	fake.mu.Lock()
	body, hdr := fake.bodies[len(fake.bodies)-1], fake.headers[len(fake.headers)-1]
	fake.mu.Unlock()
	if body["return_token_ids"] != true {
		t.Error("the agent did not ask vLLM for the prompt tokens")
	}
	if hdr.Get(domain.HeaderPrefixAnchors) != "" {
		t.Error("the anchor header reached the engine")
	}
	entries, _ := a.prefix.since(0)
	if len(entries) != 1 {
		t.Fatalf("%d entries queued, want 1", len(entries))
	}
	e := entries[0]
	tokens := render([]any{map[string]any{"role": "system", "content": strings.Repeat("x", 300)}, map[string]any{"role": "user", "content": strings.Repeat("x", 300)}}, true)
	if !slices.Equal(e.Anchors, anchors) || len(e.Blocks) != len(tokens)/prefixhash.BlockTokens {
		t.Errorf("entry: %d anchors (want %d), %d blocks (want %d)", len(e.Anchors), len(anchors), len(e.Blocks), len(tokens)/prefixhash.BlockTokens)
	}
	// The system message is 303 tokens: 4 full blocks end before the user message.
	if !slices.Equal(e.Ends, []int{303 / prefixhash.BlockTokens, 606 / prefixhash.BlockTokens}) {
		t.Errorf("ends %v", e.Ends)
	}
}

func TestPrefix_NonStreaming(t *testing.T) {
	a, _ := newPrefixAgent(t)
	rec, _ := callChat(t, a, chatBody(false, ""), true)
	if strings.Contains(rec.Body.String(), "token_ids") {
		t.Errorf("token IDs reached the router: %s", rec.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not valid JSON after stripping: %v: %s", err, rec.Body)
	}
	if entries, _ := a.prefix.since(0); len(entries) != 1 {
		t.Errorf("%d entries, want 1", len(entries))
	}
}

// A client that asks for token IDs itself gets them; the agent still learns.
// A client that refuses them is respected. Without the header nothing changes.
func TestPrefix_ClientChoiceAndNoHeader(t *testing.T) {
	a, fake := newPrefixAgent(t)
	rec, _ := callChat(t, a, chatBody(true, `,"return_token_ids":true`), true)
	if !strings.Contains(rec.Body.String(), "prompt_token_ids") {
		t.Error("the client asked for token IDs and lost them")
	}
	if entries, _ := a.prefix.since(0); len(entries) != 1 {
		t.Errorf("%d entries, want 1", len(entries))
	}
	callChat(t, a, chatBody(true, `,"return_token_ids":false`), true)
	callChat(t, a, chatBody(true, ""), false)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, b := range fake.bodies[len(fake.bodies)-2:] {
		if b["return_token_ids"] == true {
			t.Error("token IDs requested against the client's choice or without anchors")
		}
	}
	if entries, _ := a.prefix.since(0); len(entries) != 1 {
		t.Errorf("%d entries, want still 1", len(entries))
	}
}

// Engines without prompt tokens report nothing.
func TestPrefix_OnlyEnginesWithTokens(t *testing.T) {
	a := &Agent{cfg: &config.AgentConfig{JWTSecret: testSecret}, engine: &OllamaEngine{}}
	a.initPrefix()
	if a.prefix != nil {
		t.Error("ollama cannot return prompt tokens: no prefix state expected")
	}
}

func TestRemoveField(t *testing.T) {
	for _, tc := range []struct{ in, key, want string }{
		{`{"a":1,"token_ids":[1,2],"b":2}`, `"token_ids"`, `{"a":1,"b":2}`},
		{`{"token_ids":[1,2],"b":2}`, `"token_ids"`, `{"b":2}`},
		{`{"a":1,"token_ids":null}`, `"token_ids"`, `{"a":1}`},
		{`{"a":"say \"token_ids\":[1]","b":1}`, `"token_ids"`, `{"a":"say \"token_ids\":[1]","b":1}`},
		{`{"a":1}`, `"token_ids"`, `{"a":1}`},
		{`{"a":1, "token_ids" : [ 1 , 2 ] }`, `"token_ids"`, `{"a":1 }`},
	} {
		if got := string(removeField([]byte(tc.in), tc.key)); got != tc.want {
			t.Errorf("removeField(%s) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestMessageStartFromProbes(t *testing.T) {
	if _, err := messageStartFromProbes([]int{1, 2, 3}, []int{1, 2, 3, 4, 5}); err == nil {
		t.Error("a second message with no common opening token must fail")
	}
	if tok, err := messageStartFromProbes([]int{7, 1, 8}, []int{7, 1, 8, 7, 2, 8}); err != nil || tok != 7 {
		t.Errorf("got %d, %v; want 7", tok, err)
	}
}
