// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package provider_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/provider"
)

// TestProviderFallback_NeverSendsCacheSalt: the OpenAI and Anthropic
// fallback adapters build their request from the parsed fields, never from
// the forwarded body, so a cache_salt in that body (the router's per-tenant
// salt, or a client's) never leaves for a third-party provider.
func TestProviderFallback_NeverSendsCacheSalt(t *testing.T) {
	for name, reply := range map[string]string{
		"openai":    `{"id":"c","object":"chat.completion","model":"x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{}}`,
		"anthropic": `{"id":"m","model":"x","stop_reason":"end_turn","content":[{"type":"text","text":"ok"}],"usage":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var raw string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				raw = string(b)
				io.WriteString(w, reply) //nolint:errcheck
			}))
			defer srv.Close()
			p, err := provider.New(provider.Config{Name: name, APIKey: "k", BaseURL: srv.URL})
			if err != nil {
				t.Fatal(err)
			}
			req := chatReq("m", userMsg("hello"))
			req.RawBytes = []byte(`{"model":"m","cache_salt":"tenant-salt","messages":[{"role":"user","content":"hello"}]}`)
			if _, err := p.Complete(context.Background(), req, "x"); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			var sent map[string]any
			if err := json.Unmarshal([]byte(raw), &sent); err != nil {
				t.Fatalf("sent body %q: %v", raw, err)
			}
			if _, has := sent["cache_salt"]; has || strings.Contains(raw, "tenant-salt") {
				t.Errorf("%s fallback sent a cache salt: %s", name, raw)
			}
		})
	}
}
