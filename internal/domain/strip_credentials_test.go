// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"net/http"
	"testing"
)

// The client's router credentials must be removed before headers are forwarded
// to agents and backends; other headers must stay.
func TestStripClientCredentials(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer sk-router-key")
	h.Set("Proxy-Authorization", "Basic x")
	h.Set("X-Api-Key", "sk-router-key")
	h.Set("Cookie", "session=1")
	h.Set("Anthropic-Version", "2023-06-01")
	h.Set("X-Request-Id", "abc")

	StripClientCredentials(h)

	for _, k := range []string{"Authorization", "Proxy-Authorization", "X-Api-Key", "Cookie"} {
		if h.Get(k) != "" {
			t.Errorf("%s must be removed", k)
		}
	}
	for _, k := range []string{"Anthropic-Version", "X-Request-Id"} {
		if h.Get(k) == "" {
			t.Errorf("%s must be kept", k)
		}
	}
}
