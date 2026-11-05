// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package isolation_test holds the tenant-isolation checklist
// (CHECKLIST.md) and the guard that keeps it in sync with the tests.
package isolation_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	// testFuncRe finds test and fuzz function declarations in Go sources.
	testFuncRe = regexp.MustCompile(`(?m)^func ((?:Test|Fuzz)\w+)\(`)
	// testNameRe finds test and fuzz names quoted in the checklist.
	testNameRe = regexp.MustCompile("`((?:Test|Fuzz)\\w+)`")
	// lineNumRe matches the "#" cell of a checklist row, e.g. "2.10".
	lineNumRe = regexp.MustCompile(`^\s*\d+\.\d+\s*$`)
)

// declaredTests returns every Test*/Fuzz* function under test/.
func declaredTests(t *testing.T) map[string]string {
	t.Helper()
	found := map[string]string{}
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range testFuncRe.FindAllStringSubmatch(string(src), -1) {
			found[m[1]] = path
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// TestChecklist_LinesHaveTests: every test named on a "done" line of
// CHECKLIST.md exists under test/, and every TestIsolation_* function is
// listed in CHECKLIST.md, so the checklist cannot silently drift from the
// suite.
func TestChecklist_LinesHaveTests(t *testing.T) {
	raw, err := os.ReadFile("CHECKLIST.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	declared := declaredTests(t)

	rows := 0
	for _, line := range strings.Split(doc, "\n") {
		cells := strings.Split(line, "|")
		// | # | Line | Status | Tests |  → 6 cells with the outer empties.
		if len(cells) != 6 || !lineNumRe.MatchString(cells[1]) {
			continue
		}
		rows++
		status := strings.TrimSpace(cells[3])
		if status != "done" {
			continue
		}
		names := testNameRe.FindAllStringSubmatch(cells[4], -1)
		if len(names) == 0 {
			t.Errorf("line %s is done but names no test", strings.TrimSpace(cells[1]))
		}
		for _, n := range names {
			if _, ok := declared[n[1]]; !ok {
				t.Errorf("line %s names %s, which does not exist under test/", strings.TrimSpace(cells[1]), n[1])
			}
		}
	}
	if rows == 0 {
		t.Fatal("no checklist lines parsed; did the table format change?")
	}

	for name, path := range declared {
		if strings.HasPrefix(name, "TestIsolation_") && !strings.Contains(doc, "`"+name+"`") {
			t.Errorf("%s (%s) is not listed in CHECKLIST.md", name, path)
		}
	}
}
