// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// VersionReporter is an optional Engine capability: it reads the version (or
// build) of the backend the agent fronts. The agent sends it to the router at
// registration, which needs it where a feature depends on the engine version:
// SGLang isolates its prefix cache per cache_salt only from v0.5.11, and
// llama.cpp's /v1/messages takes llama.cpp parameters only from build b11795.
type VersionReporter interface {
	EngineVersion(ctx context.Context, baseURL string, httpClient *http.Client) (string, error)
}

// maxEngineVersionLen bounds the version an agent reports: it only names a
// release or build, and the router stores it with the agent.
const maxEngineVersionLen = 64

// engineVersionTimeout bounds one version read at startup.
const engineVersionTimeout = 5 * time.Second

// EngineVersion reads vLLM's version from GET /version ({"version": "..."}).
func (e *VLLMEngine) EngineVersion(ctx context.Context, baseURL string, httpClient *http.Client) (string, error) {
	return readVersionField(ctx, httpClient, baseURL+"/version", "version")
}

// EngineVersion reads SGLang's version from GET /server_info, or from its
// older name /get_server_info.
func (e *SGLangEngine) EngineVersion(ctx context.Context, baseURL string, httpClient *http.Client) (string, error) {
	v, err := readVersionField(ctx, httpClient, baseURL+"/server_info", "version")
	if err == nil {
		return v, nil
	}
	return readVersionField(ctx, httpClient, baseURL+"/get_server_info", "version")
}

// EngineVersion reads llama.cpp's build from GET /props (build_info,
// "b<build>-<commit>").
func (e *LlamaCPPEngine) EngineVersion(ctx context.Context, baseURL string, httpClient *http.Client) (string, error) {
	return readVersionField(ctx, httpClient, baseURL+"/props", "build_info")
}

// readVersionField GETs url and returns the string member field of its JSON
// object, trimmed and bounded.
func readVersionField(ctx context.Context, httpClient *http.Client, url, field string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, engineVersionTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s returned status %d", url, resp.StatusCode)
	}
	var doc map[string]json.RawMessage
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&doc); err != nil {
		return "", fmt.Errorf("%s: %w", url, err)
	}
	var v string
	if err := json.Unmarshal(doc[field], &v); err != nil || strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("%s has no %q", url, field)
	}
	v = strings.TrimSpace(v)
	if len(v) > maxEngineVersionLen {
		v = v[:maxEngineVersionLen]
	}
	return v, nil
}
