// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"net/http"
	"time"
)

type EngineOption func(*EngineOptions)

type EngineOptions struct {
	HttpHeader http.Header
	PeerID     string // stamped on forward_to_backend spans so Tempo spanmetrics carry peer_id
	// StreamWriteTimeout bounds how long a single streaming-response chunk may block
	// while being written back to the router. It is applied as a rolling (per-chunk)
	// deadline so a stalled reader cannot leave the write blocked forever. 0 disables it.
	StreamWriteTimeout time.Duration
}

// BackendHeader returns a copy of the client headers to send to the backend.
// When apiKey is non-empty the client's credentials are replaced by it
// (Authorization: Bearer <apiKey>, x-api-key removed), so a caller's router key
// never reaches an authenticated backend and the backend sees the agent's key.
// When apiKey is empty the headers are forwarded unchanged.
func BackendHeader(src http.Header, apiKey string) http.Header {
	h := src.Clone()
	if h == nil {
		h = http.Header{}
	}
	if apiKey != "" {
		h.Del("X-Api-Key")
		h.Set("Authorization", "Bearer "+apiKey)
	}
	return h
}

func WithHttpHeader(header http.Header) EngineOption {
	return func(opts *EngineOptions) {
		opts.HttpHeader = header
	}
}

func WithPeerID(peerID string) EngineOption {
	return func(opts *EngineOptions) {
		opts.PeerID = peerID
	}
}

// WithStreamWriteTimeout sets the rolling per-chunk write deadline for streaming
// responses. See EngineOptions.StreamWriteTimeout.
func WithStreamWriteTimeout(d time.Duration) EngineOption {
	return func(opts *EngineOptions) {
		opts.StreamWriteTimeout = d
	}
}
