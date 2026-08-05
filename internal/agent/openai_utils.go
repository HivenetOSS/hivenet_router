// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"hivenet_router/internal/domain"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func forwardOpenAIChatCompletion(ctx context.Context, baseURL string, httpClient *http.Client, model string, reqBody []byte, engine string, options ...EngineOption) ([]byte, http.Header, error) {
	opts := &EngineOptions{}
	for _, opt := range options {
		opt(opts)
	}

	tracer := otel.Tracer("hivenet-agent")
	ctx, span := tracer.Start(ctx, "forward_to_backend",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("peer_id", opts.PeerID),
			attribute.String("backend.url", baseURL),
			attribute.String("engine", engine),
			attribute.String("model", model),
		),
	)
	defer span.End()

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, fmt.Errorf("failed to create %s request: %v", engine, err)
	}
	if opts.HttpHeader != nil {
		httpRequest.Header = opts.HttpHeader
	}
	resp, err := httpClient.Do(httpRequest)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, fmt.Errorf("%s request failed: %v", engine, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, fmt.Errorf("failed to read %s response: %v", engine, err)
	}
	span.SetAttributes(attribute.Int("backend.status_code", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		span.SetStatus(codes.Error, fmt.Sprintf("backend returned HTTP %d", resp.StatusCode))
		return nil, resp.Header.Clone(), &BackendError{HTTPStatus: resp.StatusCode, Body: body}
	}
	return body, resp.Header.Clone(), nil
}

// forwardStreamingResponse sends a streaming chat completion request to the backend
// and pipes the SSE response directly to w chunk-by-chunk, flushing after each write.
// Returns a BackendError if the backend returns a non-200 status before any bytes
// are written to w, so the caller can still write an error response.
func forwardStreamingResponse(ctx context.Context, w http.ResponseWriter, baseURL string, httpClient *http.Client, reqBody []byte, engine string, options ...EngineOption) error {
	opts := &EngineOptions{}
	for _, opt := range options {
		opt(opts)
	}

	tracer := otel.Tracer("hivenet-agent")
	ctx, span := tracer.Start(ctx, "forward_to_backend",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("peer_id", opts.PeerID),
			attribute.String("backend.url", baseURL),
			attribute.String("engine", engine),
			attribute.Bool("streaming", true),
		),
	)
	defer span.End()

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to create %s request: %v", engine, err)
	}
	if opts.HttpHeader != nil {
		httpRequest.Header = opts.HttpHeader
	}
	resp, err := httpClient.Do(httpRequest)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("%s streaming request failed: %v", engine, err)
	}
	defer resp.Body.Close()

	span.SetAttributes(attribute.Int("backend.status_code", resp.StatusCode))
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		span.SetStatus(codes.Error, fmt.Sprintf("backend returned HTTP %d", resp.StatusCode))
		return &BackendError{HTTPStatus: resp.StatusCode, Body: body}
	}

	domain.CopyHttpHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.WriteHeader(http.StatusOK)

	dst := NewStreamWriter(w, opts.StreamWriteTimeout)
	// Capture the copy error so an abandoned stream (reader gone, or the rolling write
	// deadline fired) is observable instead of silent. The response headers are already
	// sent, so we cannot return an error to the client — record it on the span and let
	// the handler return, which closes the libp2p stream and releases its rcmgr scope.
	if _, err := io.Copy(dst, resp.Body); err != nil {
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("stream.aborted", true))
	}
	return nil
}

// NewStreamWriter wraps w for streaming SSE responses: it flushes after every write
// and, when idleTimeout > 0, applies a rolling per-chunk write deadline so a stalled
// reader cannot leave the underlying libp2p write blocked forever. Returns w unchanged
// if it is not an http.Flusher. Exported so the streaming write behaviour can be tested
// in isolation.
func NewStreamWriter(w http.ResponseWriter, idleTimeout time.Duration) io.Writer {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return w
	}
	return &flushingWriter{
		ResponseWriter: w,
		flusher:        flusher,
		rc:             http.NewResponseController(w),
		idleTimeout:    idleTimeout,
	}
}

// flushingWriter wraps an http.ResponseWriter and flushes after every Write so
// SSE chunks reach the client as soon as they arrive from the backend.
//
// It also applies a rolling write deadline before each chunk: if the reader (the
// router → end client) stops reading, the libp2p stream's send window fills and the
// underlying yamux write would otherwise block forever (no context cancellation, since
// the libp2p HTTP server ignores EOF). The deadline makes a stalled write fail so the
// handler can return and free the stream. It is reset on every chunk, so a healthy
// stream never trips it.
type flushingWriter struct {
	http.ResponseWriter
	flusher     http.Flusher
	rc          *http.ResponseController
	idleTimeout time.Duration
}

func (fw *flushingWriter) Write(p []byte) (n int, err error) {
	if fw.idleTimeout > 0 && fw.rc != nil {
		// Best-effort: if the underlying writer does not support deadlines this returns
		// an error and we proceed as before (verified supported for libp2p streams).
		_ = fw.rc.SetWriteDeadline(time.Now().Add(fw.idleTimeout))
	}
	n, err = fw.ResponseWriter.Write(p)
	if n > 0 {
		fw.flusher.Flush()
	}
	return
}
