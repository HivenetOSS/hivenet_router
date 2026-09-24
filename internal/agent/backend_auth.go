// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import "net/http"

// BackendTransport returns the RoundTripper the agent uses for every request
// to its inference backend: readiness and health checks, model discovery,
// metrics scrapes, chat (streaming or not) and the generic proxy path.
//
// When apiKey is non-empty each request is sent with
// "Authorization: Bearer <apiKey>" and without any "X-Api-Key" header,
// replacing whatever credentials the client sent to the router. This lets an
// agent front a backend that requires its own key (vLLM, SGLang or llama.cpp
// started with --api-key, or any authenticated OpenAI-compatible endpoint),
// and keeps the caller's router API key from ever reaching the backend.
//
// When apiKey is empty, base is returned unchanged and requests go out
// exactly as before. A nil base means http.DefaultTransport.
func BackendTransport(base http.RoundTripper, apiKey string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if apiKey == "" {
		return base
	}
	return &backendAuthTransport{base: base, bearer: "Bearer " + apiKey}
}

type backendAuthTransport struct {
	base   http.RoundTripper
	bearer string
}

// RoundTrip sets the backend credentials on a clone of req; a RoundTripper
// must not modify the caller's request.
func (t *backendAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Del("X-Api-Key")
	r.Header.Set("Authorization", t.bearer)
	return t.base.RoundTrip(r)
}
