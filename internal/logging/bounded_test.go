// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package logging

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// blockingWriter blocks in Write until released. It simulates a stalled
// container log drain: the pipe stops being read, so any synchronous
// write to it parks until the drain recovers.
type blockingWriter struct {
	mu       sync.Mutex
	got      [][]byte
	blocking chan struct{} // closed to release all blocked writes
	release  chan struct{} // closed to unblock the test
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{blocking: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	cp := make([]byte, len(p))
	copy(cp, p)
	w.got = append(w.got, cp)
	w.mu.Unlock()
	<-w.blocking // park until the drain recovers (or the test releases)
	return len(p), nil
}

func (w *blockingWriter) releaseForever() {
	close(w.blocking)
}

func (w *blockingWriter) lines() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([][]byte, len(w.got))
	copy(out, w.got)
	return out
}

// TestBoundedNeverBlocks is the non-blocking contract: with a stalled
// underlying writer, Write must never block — it drops lines (counted)
// once the queue fills, and the caller keeps running.
func TestBoundedNeverBlocks(t *testing.T) {
	underlying := newBlockingWriter()
	b := NewBounded(underlying, 4)

	const n = 200
	line := make([]byte, 100)
	for i := range line {
		line[i] = 'x'
	}

	start := time.Now()
	for i := 0; i < n; i++ {
		if w, err := b.Write(line); w != 100 || err != nil {
			t.Fatalf("Write %d = (%d, %v), want (100, nil)", i, w, err)
		}
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("200 Writes against a stalled writer took %v — Write blocked", elapsed)
	}

	// Queue of 4: the drain parks on the first line, so at most 4 lines
	// were accepted and the rest dropped. Allow off-by-one slack for the
	// drain's timing.
	drops := b.DroppedLines()
	if drops < n-5 {
		t.Fatalf("DroppedLines = %d, want >= %d (queue 4 of %d writes)", drops, n-5, n)
	}
	if b.DroppedBytes() != drops*100 {
		t.Fatalf("DroppedBytes = %d, want %d", b.DroppedBytes(), drops*100)
	}
}

// TestBoundedFlushesInOrderAfterRecovery: lines queued while the drain
// was stalled flow to the underlying writer — in order, intact — as soon
// as the drain recovers.
func TestBoundedFlushesInOrderAfterRecovery(t *testing.T) {
	underlying := newBlockingWriter()
	b := NewBounded(underlying, 64)

	const n = 10
	for i := 0; i < n; i++ {
		if _, err := b.Write([]byte("line " + string(rune('a'+i)))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	underlying.releaseForever()

	deadline := time.After(5 * time.Second)
	for {
		got := underlying.lines()
		if len(got) >= n {
			for i := 0; i < n; i++ {
				want := "line " + string(rune('a'+i))
				if string(got[i]) != want {
					t.Fatalf("line %d = %q, want %q (ordering broken)", i, got[i], want)
				}
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("only %d/%d lines reached the underlying writer after recovery", len(underlying.lines()), n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestBoundedCloseIsBounded: Close against a permanently stalled writer
// must return within the flush window, so shutdown cannot hang on a dead
// pipe.
func TestBoundedCloseIsBounded(t *testing.T) {
	underlying := newBlockingWriter()
	b := NewBounded(underlying, 4)
	if _, err := b.Write([]byte("pending\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	start := time.Now()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > closeFlushTimeout+2*time.Second {
		t.Fatalf("Close against a stalled writer took %v, want < %v", elapsed, closeFlushTimeout+2*time.Second)
	}
	// Idempotent.
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestBoundedWriteAfterClose: writes after Close drop (counted) instead
// of panicking or blocking on the closed queue.
func TestBoundedWriteAfterClose(t *testing.T) {
	underlying := newBlockingWriter()
	b := NewBounded(underlying, 4)
	_ = b.Close()
	if w, err := b.Write([]byte("after close")); w != len("after close") || err != nil {
		t.Fatalf("Write after Close = (%d, %v), want (n, nil)", w, err)
	}
	if b.DroppedLines() != 1 {
		t.Fatalf("DroppedLines after close-write = %d, want 1", b.DroppedLines())
	}
}

// TestBoundedUnderlyingErrorDrops: when the underlying writer errors
// (EPIPE, closed fd), the drain counts the rest of the queue as dropped
// and exits without blocking the process.
func TestBoundedUnderlyingErrorDrops(t *testing.T) {
	errWriter := errorWriter{err: errTestWriter}
	b := NewBounded(errWriter, 8)
	for i := 0; i < 10; i++ {
		if _, err := b.Write([]byte("line\n")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	// The drain should observe the error and exit; give it a beat, then
	// Close must return promptly (the drain goroutine is already gone or
	// about to finish).
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if b.DroppedLines() < 1 {
		t.Fatalf("DroppedLines = %d, want >= 1 (error path must count drops)", b.DroppedLines())
	}
}

type errorWriter struct{ err error }

func (e errorWriter) Write(p []byte) (int, error) { return 0, e.err }

var errTestWriter = errValue{msg: "sink gone"}

type errValue struct{ msg string }

func (e errValue) Error() string { return e.msg }

// TestBoundedPassthroughUnstalled: with a healthy underlying writer the
// writer is a transparent pass-through — nothing is dropped.
func TestBoundedPassthroughUnstalled(t *testing.T) {
	var mu sync.Mutex
	var got bytes.Buffer
	b := NewBounded(&syncWriter{mu: &mu, buf: &got}, DefaultQueueSize)
	for i := 0; i < 100; i++ {
		if _, err := b.Write([]byte("ok\n")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got.Len() != 100*3 {
		t.Fatalf("underlying received %d bytes, want %d", got.Len(), 100*3)
	}
	if b.DroppedLines() != 0 {
		t.Fatalf("DroppedLines = %d, want 0 for a healthy writer", b.DroppedLines())
	}
}

type syncWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// TestBoundedSync: with a slow underlying writer, Sync returns only after
// every enqueued line has reached it, in order.
func TestBoundedSync(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	b := NewBounded(&slowWriter{mu: &mu, lines: &lines, delay: 5 * time.Millisecond}, 64)
	const n = 20
	for i := 0; i < n; i++ {
		line := fmt.Sprintf("line-%02d\n", i)
		if _, err := b.Write([]byte(line)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	b.Sync()
	mu.Lock()
	defer mu.Unlock()
	if len(lines) != n {
		t.Fatalf("underlying received %d lines after Sync, want %d", len(lines), n)
	}
	for i, got := range lines {
		if want := fmt.Sprintf("line-%02d\n", i); got != want {
			t.Fatalf("line %d = %q, want %q (ordering broken)", i, got, want)
		}
	}
}

type slowWriter struct {
	mu    *sync.Mutex
	lines *[]string
	delay time.Duration
}

func (s *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(s.delay)
	s.mu.Lock()
	*s.lines = append(*s.lines, string(p))
	s.mu.Unlock()
	return len(p), nil
}
