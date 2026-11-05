// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"hivenet_router/internal/semantic"
)

// TestDecisionLog: records land as JSON lines; a nil log is a no-op.
func TestDecisionLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decisions.jsonl")
	l, err := semantic.OpenDecisionLog(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	l.Write(semantic.DecisionRecord{Alias: "auto", ResolvedModel: "big", Route: "agentic", Source: "scored",
		RouteScores: map[string]float64{"agentic": 0.8}, DecisionUS: 42})
	l.Write(semantic.DecisionRecord{Alias: "auto", Source: "default", Error: "no eligible"})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []semantic.DecisionRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec semantic.DecisionRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		lines = append(lines, rec)
	}
	if len(lines) != 2 || lines[0].ResolvedModel != "big" || lines[0].RouteScores["agentic"] != 0.8 {
		t.Errorf("records = %+v", lines)
	}
	var nilLog *semantic.DecisionLog
	nilLog.Write(semantic.DecisionRecord{}) // must not panic
	_ = nilLog.Close()
}

// TestDecisionLog_WriteDuringClose: requests still in flight at shutdown may
// write while Close runs, or after it; that must never panic.
func TestDecisionLog_WriteDuringClose(t *testing.T) {
	l, err := semantic.OpenDecisionLog(filepath.Join(t.TempDir(), "d.jsonl"), 8)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				l.Write(semantic.DecisionRecord{Alias: "auto"})
			}
		}()
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	l.Write(semantic.DecisionRecord{Alias: "late"}) // after Close: dropped, no panic
	if l.Dropped() == 0 {
		t.Error("the late write should be counted as dropped")
	}
}

// TestDecisionLog_FileMode: the decision log names tenants and keys, so it is
// owner-only like the audit log: created with mode 0600, and a file created
// earlier with a wider mode is narrowed to 0600 when opened.
func TestDecisionLog_FileMode(t *testing.T) {
	for name, pre := range map[string]bool{"new file": false, "existing world-readable file": true} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "decisions.jsonl")
			if pre {
				if err := os.WriteFile(path, nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			dl, err := semantic.OpenDecisionLog(path, 4)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { dl.Close() }) //nolint:errcheck
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if perm := fi.Mode().Perm(); perm != 0o600 {
				t.Errorf("decision log mode %o, want 600", perm)
			}
		})
	}
}
