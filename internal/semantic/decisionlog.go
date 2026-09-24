// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DecisionRecord is one line of the decision log (JSONL). It is both the audit
// trail for "why did this request go there" and the dataset for later
// evaluation and learning.
type DecisionRecord struct {
	TS            time.Time           `json:"ts"`
	RequestID     string              `json:"request_id,omitempty"`
	Path          string              `json:"path"`
	KeyID         string              `json:"key_id,omitempty"`
	Alias         string              `json:"alias"`
	ResolvedModel string              `json:"resolved_model,omitempty"`
	Route         string              `json:"route,omitempty"`
	Source        string              `json:"source"`
	RouteScores   map[string]float64  `json:"route_scores,omitempty"`
	RouteMatches  map[string][]string `json:"route_matches,omitempty"`
	Features      map[string]float64  `json:"features,omitempty"`
	FilteredOut   map[string]string   `json:"filtered_out,omitempty"`
	TaskKey       string              `json:"task_key,omitempty"`
	DecisionUS    int64               `json:"decision_us"`
	Error         string              `json:"error,omitempty"`
}

// DecisionLog appends DecisionRecords to a file as JSON lines from a single
// background writer. Write never blocks the request path: when the buffer is
// full the record is dropped and counted. A nil *DecisionLog is a valid no-op.
type DecisionLog struct {
	ch      chan DecisionRecord
	f       *os.File
	wg      sync.WaitGroup
	dropped atomic.Int64
	once    sync.Once
}

// OpenDecisionLog opens (appending) path and starts the writer.
func OpenDecisionLog(path string, buffer int) (*DecisionLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("semantic: open decision log %q: %w", path, err)
	}
	if buffer <= 0 {
		buffer = 4096
	}
	l := &DecisionLog{ch: make(chan DecisionRecord, buffer), f: f}
	l.wg.Add(1)
	go l.run()
	return l, nil
}

func (l *DecisionLog) run() {
	defer l.wg.Done()
	w := bufio.NewWriter(l.f)
	enc := json.NewEncoder(w)
	for rec := range l.ch {
		_ = enc.Encode(rec) // Encode appends '\n'
		if len(l.ch) == 0 {
			_ = w.Flush() // flush when idle so the file is tail-able in real time
		}
	}
	_ = w.Flush()
}

// Write enqueues rec without blocking.
func (l *DecisionLog) Write(rec DecisionRecord) {
	if l == nil {
		return
	}
	select {
	case l.ch <- rec:
	default:
		l.dropped.Add(1)
	}
}

// Dropped returns how many records were dropped because the buffer was full.
func (l *DecisionLog) Dropped() int64 {
	if l == nil {
		return 0
	}
	return l.dropped.Load()
}

// Close drains pending records and closes the file. Safe to call more than once.
// Write must not be called after Close.
func (l *DecisionLog) Close() error {
	if l == nil {
		return nil
	}
	var err error
	l.once.Do(func() {
		close(l.ch)
		l.wg.Wait()
		err = l.f.Close()
	})
	return err
}
