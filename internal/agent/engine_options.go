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
