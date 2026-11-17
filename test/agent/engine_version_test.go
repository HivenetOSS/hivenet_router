// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/agent"
)

// TestEngineVersion_ReadFromBackend: vLLM, SGLang and llama.cpp report their
// version (or build) from the endpoint each one serves it on; SGLang falls
// back to the older /get_server_info; a backend without one is an error,
// which the agent reports as an unknown version.
func TestEngineVersion_ReadFromBackend(t *testing.T) {
	serve := func(routes map[string]string) string {
		mux := http.NewServeMux()
		for path, body := range routes {
			mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
		}
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		return srv.URL
	}
	for _, tc := range []struct {
		name    string
		engine  agent.VersionReporter
		routes  map[string]string
		want    string
		wantErr bool
	}{
		{"vllm", &agent.VLLMEngine{}, map[string]string{"/version": `{"version":"0.31.0"}`}, "0.31.0", false},
		{"sglang", &agent.SGLangEngine{}, map[string]string{"/server_info": `{"tp_size":1,"version":"0.5.11"}`}, "0.5.11", false},
		{"sglang, older endpoint", &agent.SGLangEngine{}, map[string]string{"/get_server_info": `{"version":"0.5.9"}`}, "0.5.9", false},
		{"llama.cpp", &agent.LlamaCPPEngine{}, map[string]string{"/props": `{"total_slots":1,"build_info":"b11795-261700b"}`}, "b11795-261700b", false},
		{"no version endpoint", &agent.VLLMEngine{}, map[string]string{}, "", true},
		{"no version field", &agent.SGLangEngine{}, map[string]string{"/server_info": `{"tp_size":1}`}, "", true},
		{"version too long is cut", &agent.VLLMEngine{}, map[string]string{"/version": `{"version":"` + strings.Repeat("1", 100) + `"}`}, strings.Repeat("1", 64), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.engine.EngineVersion(context.Background(), serve(tc.routes), http.DefaultClient)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("EngineVersion = %q, %v; want %q, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
