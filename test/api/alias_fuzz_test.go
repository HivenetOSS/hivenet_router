// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"hivenet_router/internal/api"
)

// FuzzAliasMiddlewareModelRewrite drives the alias body rewrite with arbitrary
// bodies. Whatever the input, the middleware must not panic, and a request it
// lets through must reach the handler as the client sent it, except that when
// an alias was resolved the value of the top-level "model" key (the last one,
// as encoding/json reads it) becomes the routed model and nothing else moves.
func FuzzAliasMiddlewareModelRewrite(f *testing.F) {
	for _, seed := range []string{
		`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`,
		`{"messages":[{"role":"user","content":"hi \"model\": {x}"}],"stream":false,"model":"auto"}`,
		`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":"coder","messages":[{"role":"user","content":"hi"}],"model":"auto"}`,
		`{"model":"auto","messages":[{"role":"user","content":"hi"}],"model":"coder"}`,
		"{ \"model\" :\n \"auto\" ,\t\"messages\" : [ {\"role\":\"user\",\"content\":\"hi\"} ] }",
		`{"model":"auto","messages":[{"role":"user","content":"Traceback (most recent call last)"}]}`,
		`{"model":"auto","messages":[{"role":"user","content":[{"type":"text","text":"a"}]}],"extra":{"model":"x"}}`,
		`{"model":"chat","messages":[]}`,
		`{"model":"auto"`,
		`[]`,
	} {
		f.Add([]byte(seed))
	}
	h := newAliasHandlers(f, nil)
	f.Fuzz(func(t *testing.T, body []byte) {
		var out seen
		w := post(aliasRouter(h, noAuth, &out), "/v1/chat/completions", string(body))
		if w.Code != http.StatusOK || !out.called || out.body == nil {
			return // rejected (or passed through without a cached body)
		}
		routed := w.Header().Get(api.HeaderRoutedModel)
		if routed == "" {
			if !bytes.Equal(out.body, body) {
				t.Fatalf("concrete-model body changed:\n in  %q\n out %q", body, out.body)
			}
			return
		}
		if !json.Valid(out.body) {
			t.Fatalf("rewritten body is not valid JSON: %q (in %q)", out.body, body)
		}
		var in, got map[string]json.RawMessage
		if err := json.Unmarshal(body, &in); err != nil {
			t.Fatalf("an alias was resolved from a body encoding/json rejects: %q: %v", body, err)
		}
		if err := json.Unmarshal(out.body, &got); err != nil {
			t.Fatalf("rewritten body: %v", err)
		}
		var model string
		if err := json.Unmarshal(got["model"], &model); err != nil || model != routed {
			t.Fatalf("model = %q (err %v), routed %q; out %q", model, err, routed, out.body)
		}
		quoted, _ := json.Marshal(routed)
		if len(out.body)-len(body) != len(quoted)-len(in["model"]) {
			t.Fatalf("more than the model value changed:\n in  %q\n out %q", body, out.body)
		}
		for k, v := range in {
			if k != "model" && !bytes.Equal(got[k], v) {
				t.Fatalf("key %q changed: %s -> %s", k, v, got[k])
			}
		}
	})
}
