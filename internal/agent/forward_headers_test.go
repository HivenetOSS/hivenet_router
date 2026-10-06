// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"net/http"
	"testing"
)

// Even if a router forwarded the client's credentials, the agent must not pass
// them to the backend, and must not change the router's request headers.
func TestForwardableHeadersDropClientCredentials(t *testing.T) {
	in := http.Header{}
	in.Set("Authorization", "Bearer sk-router-key")
	in.Set("X-Api-Key", "sk-router-key")
	in.Set("Content-Type", "application/json")

	out := forwardableHeaders(in)

	if out.Get("Authorization") != "" || out.Get("X-Api-Key") != "" {
		t.Fatal("client credentials must not reach the backend")
	}
	if out.Get("Content-Type") != "application/json" {
		t.Fatal("other headers must be kept")
	}
	if in.Get("Authorization") == "" {
		t.Fatal("the incoming headers must not be modified")
	}
}
