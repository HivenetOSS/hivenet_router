// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

// BackendTransport returns the RoundTripper the agent uses for every request
// to its inference backend: readiness and health checks, model discovery,
// metrics scrapes, chat (streaming or not) and the generic proxy path.
//
// When apiKey is non-empty each request is sent without any "X-Api-Key"
// header, and with "Authorization: Bearer <apiKey>" replacing whatever
// credentials the client sent to the router. This lets an agent front a
// backend that requires its own key (vLLM, SGLang or llama.cpp started with
// --api-key, or any authenticated OpenAI-compatible endpoint), and keeps the
// caller's router API key from ever reaching the backend.
//
// The key is only sent to the origins (scheme, host and port) of backendURLs:
// the backend URL and, for the custom engine, its health URL. A request to
// any other origin, such as a redirect the backend issues, goes out with no
// Authorization header at all. Go's http.Client already drops Authorization
// on a redirect to another host, but it compares host names only (ports are
// ignored) and this transport runs again on every hop, so the check has to
// live here. An entry that does not parse as an absolute URL matches nothing.
//
// When apiKey is empty, base is returned unchanged and requests go out
// exactly as before. A nil base means http.DefaultTransport.
func BackendTransport(base http.RoundTripper, apiKey string, backendURLs ...string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if apiKey == "" {
		return base
	}
	origins := make(map[string]bool, len(backendURLs))
	for _, raw := range backendURLs {
		if u, err := url.Parse(raw); err == nil && u.Host != "" {
			origins[originOf(u)] = true
		}
	}
	return &backendAuthTransport{base: base, bearer: "Bearer " + apiKey, origins: origins}
}

type backendAuthTransport struct {
	base    http.RoundTripper
	bearer  string
	origins map[string]bool // originOf the URLs allowed to receive the key
}

// RoundTrip sets the backend credentials on a clone of req; a RoundTripper
// must not modify the caller's request.
func (t *backendAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Del("X-Api-Key")
	if t.origins[originOf(r.URL)] {
		r.Header.Set("Authorization", t.bearer)
	} else {
		r.Header.Del("Authorization")
	}
	return t.base.RoundTrip(r)
}

// originOf returns u's scheme, host and port in canonical form: lower case,
// with the scheme's default port made explicit, so "http://Backend" and
// "http://backend:80" compare equal.
func originOf(u *url.URL) string {
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	return scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}
