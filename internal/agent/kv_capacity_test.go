// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// vLLM's KV capacity is num_gpu_blocks × block_size from cache_config_info;
// other *_block_size labels of hybrid models are not mistaken for it.
func TestVLLMKVCapacityFromCacheConfig(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`# HELP vllm:cache_config_info Information of the LLMEngine CacheConfig
# TYPE vllm:cache_config_info gauge
vllm:cache_config_info{block_size="1568",cache_dtype="fp8",mamba_block_size="1568",num_gpu_blocks="628",num_cpu_blocks="0"} 1.0
vllm:kv_cache_usage_perc{model_name="m"} 0.25
`)) //nolint:errcheck
	}))
	defer srv.Close()
	m, err := (&VLLMEngine{}).ScrapeMetrics(context.Background(), srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if m.KVCapacityTokens == nil || *m.KVCapacityTokens != 628*1568 {
		t.Fatalf("KV capacity %v, want %d", m.KVCapacityTokens, 628*1568)
	}
}

// When vLLM reports kv_cache_size_tokens it is used: for a hybrid model it is
// well below num_gpu_blocks × block_size (measured on vLLM 0.31 with a hybrid
// 27B model: 52,043 tokens, against 84,672 from the blocks).
func TestVLLMKVCapacityPrefersReportedTokens(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`vllm:cache_config_info{block_size="784",kv_cache_size_tokens="52043",num_gpu_blocks="108",mamba_block_size="784"} 1.0
`)) //nolint:errcheck
	}))
	defer srv.Close()
	m, err := (&VLLMEngine{}).ScrapeMetrics(context.Background(), srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if m.KVCapacityTokens == nil || *m.KVCapacityTokens != 52043 {
		t.Fatalf("KV capacity %v, want 52043", m.KVCapacityTokens)
	}
}

func TestVLLMKVCapacityAbsent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("vllm:num_requests_running 2\n")) //nolint:errcheck
	}))
	defer srv.Close()
	m, err := (&VLLMEngine{}).ScrapeMetrics(context.Background(), srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if m.KVCapacityTokens != nil {
		t.Fatalf("KV capacity %v without cache_config_info, want nil", *m.KVCapacityTokens)
	}
}

// With data parallelism vLLM reports one cache_config_info series per engine:
// the capacity is their sum, and a repeated series is counted once.
func TestVLLMKVCapacitySumsEngines(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`vllm:cache_config_info{block_size="16",engine="0",num_gpu_blocks="1000"} 1.0
vllm:cache_config_info{block_size="16",engine="1",num_gpu_blocks="1000"} 1.0
vllm:cache_config_info{block_size="16",engine="1",num_gpu_blocks="1000"} 1.0
`)) //nolint:errcheck
	}))
	defer srv.Close()
	m, err := (&VLLMEngine{}).ScrapeMetrics(context.Background(), srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if m.KVCapacityTokens == nil || *m.KVCapacityTokens != 2*16000 {
		t.Fatalf("KV capacity %v, want %d", m.KVCapacityTokens, 2*16000)
	}
}
