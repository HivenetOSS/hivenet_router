// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package logging provides the bounded, non-blocking sink that every
// router log writer (audit log, gin access log, stdlib log) writes to
// instead of writing synchronously to the container's stdout/stderr.
//
// When a storage hang stalls the container's log drain, the
// stdout/stderr pipes stop being read. Any log writer that blocks on a
// full pipe blocks its caller — a request goroutine blocked on a log
// write cannot serve /health or any other endpoint, and liveness kills a
// router that is otherwise fine. BoundedWriter queues lines in memory
// and drops them (counted) when the queue is full, so a stalled drain
// can never block the application: the drain goroutine parks on the
// stuck pipe, everything else keeps running, and lines flow again when
// the drain recovers.
//
// The go-log (GOLOG) subsystem is NOT routed through BoundedWriter: it
// writes to stderr through its own zap core and exposes no injectable
// writer. Those call sites may park on a stalled drain, but the request
// path (audit, access, stdlib) is fully protected.
package logging

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultQueueSize is the number of log lines a BoundedWriter holds in
// memory while its underlying writer is stalled. A stalled drain that
// outlives the queue drops lines (counted via DroppedLines) rather than
// blocking the writer.
const DefaultQueueSize = 8192

// closeFlushTimeout bounds how long Close waits to flush the queue to a
// stalled underlying writer. A shutdown must not hang on a dead pipe:
// whatever the queue still holds after this window is lost.
const closeFlushTimeout = 5 * time.Second

// syncTimeout bounds how long Sync waits for the drain goroutine to catch
// up with a stalled underlying writer.
const syncTimeout = 5 * time.Second

// BoundedWriter is an io.Writer that never blocks. Lines are copied into
// a bounded queue; a dedicated drain goroutine writes them to the
// underlying writer. When the queue is full — or the writer is closed —
// lines are dropped and counted instead of blocking the caller.
type BoundedWriter struct {
	underlying io.Writer
	ch         chan []byte
	done       chan struct{}

	droppedBytes atomic.Int64
	droppedLines atomic.Int64
	sent         atomic.Int64 // lines enqueued
	written      atomic.Int64 // lines the drain processed (written or error-dropped)
	closed       atomic.Bool
	closeOnce    sync.Once
}

// NewBounded returns a BoundedWriter over underlying with a queue of
// queueSize lines (DefaultQueueSize when queueSize <= 0) and starts its
// drain goroutine. The caller owns underlying: BoundedWriter never closes
// it. Call Close at shutdown to stop accepting lines and flush what the
// queue still holds (bounded by closeFlushTimeout).
func NewBounded(underlying io.Writer, queueSize int) *BoundedWriter {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	b := &BoundedWriter{
		underlying: underlying,
		ch:         make(chan []byte, queueSize),
		done:       make(chan struct{}),
	}
	go b.drain()
	return b
}

// Write enqueues p for the drain goroutine and returns immediately: it
// never blocks, even when the underlying writer is stalled and the queue
// is full (the line is then dropped and counted). A zero-length write is
// a no-op. Errors are never reported: log sinks must not fail requests.
func (b *BoundedWriter) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	if b.closed.Load() {
		b.droppedBytes.Add(int64(n))
		b.droppedLines.Add(1)
		return n, nil
	}
	line := make([]byte, n)
	copy(line, p)
	select {
	case b.ch <- line:
		b.sent.Add(1)
	default:
		// Queue full: the underlying writer is stalled (or the queue is
		// undersized). Drop rather than block — a stalled drain must
		// never hold a request goroutine.
		b.droppedBytes.Add(int64(n))
		b.droppedLines.Add(1)
	}
	return n, nil
}

// Sync waits up to syncTimeout for the drain goroutine to process every
// line enqueued so far, so the underlying writer holds all of them
// (in order) when Sync returns. It returns promptly for a healthy
// underlying writer and after the timeout for a stalled one, so it can
// never hang a caller. Callers must not enqueue new lines while Sync is
// waiting (it settles on the last sent/written pair it observes).
// Typical use: a test that reads the log file after a batch of requests.
func (b *BoundedWriter) Sync() {
	deadline := time.Now().Add(syncTimeout)
	for {
		if b.written.Load() >= b.sent.Load() {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// DroppedLines returns how many lines were dropped because the queue was
// full or the writer was closed. A non-zero value in steady state means
// the log drain is stalled (or the queue is undersized for the volume).
func (b *BoundedWriter) DroppedLines() int64 {
	return b.droppedLines.Load()
}

// DroppedBytes returns the total byte count of dropped lines.
func (b *BoundedWriter) DroppedBytes() int64 {
	return b.droppedBytes.Load()
}

// Close stops accepting new lines and waits up to closeFlushTimeout for
// the drain goroutine to flush the queue to the underlying writer. When
// the underlying writer is stalled the remaining queue is abandoned
// (counted as not written) so shutdown cannot hang on a dead pipe. Safe
// to call multiple times.
func (b *BoundedWriter) Close() error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.ch)
	})
	select {
	case <-b.done:
	case <-time.After(closeFlushTimeout):
	}
	return nil
}

// drain writes queued lines to the underlying writer until the queue is
// closed and drained. A stuck underlying writer parks this goroutine —
// by design: it is the only goroutine allowed to block on the pipe.
func (b *BoundedWriter) drain() {
	defer close(b.done)
	for line := range b.ch {
		if _, err := b.underlying.Write(line); err != nil {
			// The underlying writer is gone (EPIPE, closed fd, ...):
			// nothing more can be written. Count the line and everything
			// still queued as dropped, then exit.
			b.droppedBytes.Add(int64(len(line)))
			b.droppedLines.Add(1)
			b.written.Add(1) // processed, so Sync does not wait for it
			for rest := range b.ch {
				b.droppedBytes.Add(int64(len(rest)))
				b.droppedLines.Add(1)
				b.written.Add(1)
			}
			return
		}
		b.written.Add(1)
	}
}
