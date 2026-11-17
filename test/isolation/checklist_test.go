// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package isolation_test holds the tenant-isolation checklist
// (CHECKLIST.md) and the guard that keeps it in sync with the tests.
package isolation_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	// testNameRe is a test or fuzz function name.
	testNameRe = regexp.MustCompile(`^(?:Test|Fuzz)\w+$`)
	// codeSpanRe finds the backticked tokens of a cell.
	codeSpanRe = regexp.MustCompile("`([^`]*)`")
	// rowStartRe marks a checklist row ("| 2.10 | ..."), counted on its own so
	// a row the cell parser cannot read is reported instead of skipped.
	rowStartRe = regexp.MustCompile(`^\|\s*\d+\.\d+\s*\|`)
	// statusRe is the status vocabulary of the legend.
	statusRe = regexp.MustCompile(`^(?:done|planned \((?:PR-\d+(?:, PR-\d+)*|#\d+(?:–#\d+)?)\)|Epic [A-Z]|infra|platform|out of scope)$`)
)

// testInfo is one Test*/Fuzz* function under test/.
type testInfo struct {
	path    string
	skipped bool // its body starts with an unconditional Skip
}

// declaredTests returns every Test*/Fuzz* function under test/.
func declaredTests(t *testing.T) map[string]testInfo {
	t.Helper()
	found := map[string]testInfo{}
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		funcs, err := testFuncs(path, src)
		if err != nil {
			return err
		}
		for name, skipped := range funcs {
			found[name] = testInfo{path: path, skipped: skipped}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// testFuncs parses one Go test file and returns its Test*/Fuzz* functions,
// each with whether its body starts with an unconditional call to Skip,
// Skipf or SkipNow on its *testing.T or *testing.F parameter: such a test
// proves nothing, so a "done" line may not cite it.
func testFuncs(path string, src []byte) (map[string]bool, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !testNameRe.MatchString(fn.Name.Name) {
			continue
		}
		out[fn.Name.Name] = startsWithSkip(fn)
	}
	return out, nil
}

// startsWithSkip reports whether a test skips itself unconditionally: its
// body opens with a Skip, Skipf or SkipNow call on its own *testing.T (or
// *testing.F / *testing.B), possibly after Helper and Parallel calls, which
// change nothing about whether the test runs. A Skip under a condition, or
// after any other statement, does not count.
func startsWithSkip(fn *ast.FuncDecl) bool {
	params := fn.Type.Params.List
	if fn.Body == nil || len(params) != 1 || len(params[0].Names) != 1 {
		return false
	}
	recvName := params[0].Names[0].Name
	for _, stmt := range fn.Body.List {
		switch method := recvCall(stmt, recvName); method {
		case "Helper", "Parallel":
			continue
		case "Skip", "Skipf", "SkipNow":
			return true
		default:
			return false
		}
	}
	return false
}

// recvCall returns the method name when stmt is a bare call of a method on
// the identifier recv (recv.Method(...)), "" otherwise.
func recvCall(stmt ast.Stmt, recv string) string {
	expr, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return ""
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok {
		return ""
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	if id, ok := sel.X.(*ast.Ident); !ok || id.Name != recv {
		return ""
	}
	return sel.Sel.Name
}

// row is one checklist line: | # | Line | Status | Tests |.
type row struct {
	lineNo                   int
	num, text, status, tests string
}

// splitRow splits a markdown table row on its unescaped pipes ("\|" stays
// inside a cell) and trims each cell. The empty cells outside the outer
// pipes are dropped.
func splitRow(line string) []string {
	var cells []string
	var cur strings.Builder
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '\\' && i+1 < len(line) && line[i+1] == '|':
			cur.WriteString(`\|`)
			i++
		case line[i] == '|':
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(line[i])
		}
	}
	cells = append(cells, strings.TrimSpace(cur.String()))
	if len(cells) >= 2 && cells[0] == "" && cells[len(cells)-1] == "" {
		cells = cells[1 : len(cells)-1]
	}
	return cells
}

// parseChecklist returns the rows of doc, and a problem for every row the
// parser cannot read (wrong number of cells, usually an unescaped "|").
func parseChecklist(doc string) (rows []row, problems []string) {
	for i, line := range strings.Split(doc, "\n") {
		if !rowStartRe.MatchString(line) {
			continue
		}
		cells := splitRow(line)
		if len(cells) != 4 {
			problems = append(problems, fmt.Sprintf("line %d: %d cells, want 4 (# | Line | Status | Tests); escape a | inside a cell as \\|", i+1, len(cells)))
			continue
		}
		rows = append(rows, row{lineNo: i + 1, num: cells[0], text: cells[1], status: cells[2], tests: cells[3]})
	}
	return rows, problems
}

// checkChecklist returns every way doc disagrees with the declared tests:
//
//   - every row parses and has a status from the legend;
//   - every "done" row cites at least one test, and every backticked token in
//     its Tests cell is a test that exists under test/ and is not skipped
//     (a file name or a skipped test proves nothing);
//   - every TestIsolation_* function is cited somewhere in doc.
func checkChecklist(doc string, declared map[string]testInfo) []string {
	rows, problems := parseChecklist(doc)
	if len(rows) == 0 {
		return append(problems, "no checklist rows parsed; did the table format change?")
	}
	for _, r := range rows {
		if !statusRe.MatchString(r.status) {
			problems = append(problems, fmt.Sprintf("line %s: status %q is not in the legend", r.num, r.status))
		}
		if r.status != "done" {
			continue
		}
		tokens := codeSpanRe.FindAllStringSubmatch(r.tests, -1)
		if len(tokens) == 0 {
			problems = append(problems, fmt.Sprintf("line %s is done but names no test", r.num))
		}
		for _, tok := range tokens {
			name := tok[1]
			info, ok := declared[name]
			switch {
			case !testNameRe.MatchString(name):
				problems = append(problems, fmt.Sprintf("line %s is done but cites %q, which is not a test: cite the tests that prove it", r.num, name))
			case !ok:
				problems = append(problems, fmt.Sprintf("line %s names %s, which does not exist under test/", r.num, name))
			case info.skipped:
				problems = append(problems, fmt.Sprintf("line %s is done but %s (%s) is skipped", r.num, name, info.path))
			}
		}
	}
	for name, info := range declared {
		if strings.HasPrefix(name, "TestIsolation_") && !strings.Contains(doc, "`"+name+"`") {
			problems = append(problems, fmt.Sprintf("%s (%s) is not listed in CHECKLIST.md", name, info.path))
		}
	}
	return problems
}

// TestChecklist_LinesHaveTests: CHECKLIST.md agrees with the suite (see
// checkChecklist), so the checklist cannot silently drift from the tests.
func TestChecklist_LinesHaveTests(t *testing.T) {
	raw, err := os.ReadFile("CHECKLIST.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range checkChecklist(string(raw), declaredTests(t)) {
		t.Error(p)
	}
}

// TestChecklistGuard_RejectsBadRows: the guard itself refuses each way a
// checklist can claim more than the tests prove.
func TestChecklistGuard_RejectsBadRows(t *testing.T) {
	declared := map[string]testInfo{
		"TestReal":           {path: "x_test.go"},
		"TestSkipped":        {path: "x_test.go", skipped: true},
		"TestIsolation_Real": {path: "x_test.go"},
	}
	const header = "| # | Line | Status | Tests |\n|---|---|---|---|\n"
	good := header + "| 1.1 | A line with an escaped \\| pipe. | done | `TestReal`, `TestIsolation_Real` |\n" +
		"| 1.2 | Later. | planned (PR-3) | `TestNotYet` |\n| 1.3 | Elsewhere. | Epic L | none |\n"
	if p := checkChecklist(good, declared); len(p) != 0 {
		t.Fatalf("a valid checklist was rejected: %v", p)
	}
	for _, tc := range []struct{ name, row, want string }{
		{"unescaped pipe", "| 2.1 | a | b | done | `TestReal` |", "cells"},
		{"file instead of test", "| 2.1 | a | done | `TestReal`, `handlers_models_test.go` |", "not a test"},
		{"skipped test", "| 2.1 | a | done | `TestSkipped` |", "is skipped"},
		{"unknown test", "| 2.1 | a | done | `TestMissing` |", "does not exist"},
		{"no test", "| 2.1 | a | done | none |", "names no test"},
		{"status not in legend", "| 2.1 | a | Done | `TestReal` |", "not in the legend"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := checkChecklist(good+tc.row+"\n", declared)
			if !strings.Contains(strings.Join(p, "\n"), tc.want) {
				t.Errorf("problems %v, want one mentioning %q", p, tc.want)
			}
		})
	}
	if p := checkChecklist(header+"| 1.1 | a | done | `TestReal` |\n", declared); !strings.Contains(strings.Join(p, "\n"), "TestIsolation_Real") {
		t.Errorf("an unlisted TestIsolation_ function was not reported: %v", p)
	}
}

// TestChecklistGuard_DetectsSkippedTests: only a Skip that runs
// unconditionally as the first statement makes a test count as skipped.
func TestChecklistGuard_DetectsSkippedTests(t *testing.T) {
	src := `package x
import "testing"
func TestSkip(t *testing.T)      { t.Skip("later") }
func TestSkipf(t *testing.T)     { t.Skipf("later %d", 1) }
func TestSkipNow(t *testing.T)   { t.SkipNow() }
func FuzzSkip(f *testing.F)      { f.Skip() }
func TestConditional(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
}
func TestSkipLater(t *testing.T) { t.Log("x"); t.Skip() }
func TestOther(t *testing.T)     { other.Skip() }
func TestHelperSkip(t *testing.T)   { t.Helper(); t.Skip("later") }
func TestParallelSkip(t *testing.T) { t.Parallel(); t.Skip("later") }
func TestBothSkip(t *testing.T)     { t.Parallel(); t.Helper(); t.SkipNow() }
func TestParallelOnly(t *testing.T) { t.Parallel() }
func TestOtherParallel(t *testing.T) { other.Parallel(); t.Skip() }
`
	got, err := testFuncs("x_test.go", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{
		"TestSkip": true, "TestSkipf": true, "TestSkipNow": true, "FuzzSkip": true,
		"TestConditional": false, "TestSkipLater": false, "TestOther": false,
		// Helper and Parallel calls before the Skip change nothing.
		"TestHelperSkip": true, "TestParallelSkip": true, "TestBothSkip": true,
		"TestParallelOnly": false, "TestOtherParallel": false,
	} {
		if got[name] != want {
			t.Errorf("%s: skipped=%v, want %v", name, got[name], want)
		}
	}
}
