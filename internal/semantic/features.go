// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Features holds the per-request values signals produce. Numeric features are
// keyed by the names in policy.KnownSignalFeatures; the text window is what
// keyword rules match against.
type Features struct {
	values map[string]float64
	// window is the lower-cased text of the last RecentTurns human turns
	// (already stripped of marker blocks by ParseView). Keyword rules match here.
	window string
}

// Value returns a numeric feature and whether it was set.
func (f *Features) Value(name string) (float64, bool) {
	v, ok := f.values[name]
	return v, ok
}

// Set records a numeric feature. Signals call it from Extract.
func (f *Features) Set(name string, v float64) {
	if f.values == nil {
		f.values = make(map[string]float64, 20)
	}
	f.values[name] = v
}

// Values returns a copy of all numeric features (for the decision log).
func (f *Features) Values() map[string]float64 {
	out := make(map[string]float64, len(f.values))
	for k, v := range f.values {
		out[k] = v
	}
	return out
}

// Window returns the lower-cased text keyword rules match against.
func (f *Features) Window() string { return f.window }

// ExtractOptions are the alias-level knobs that shape feature extraction.
type ExtractOptions struct {
	RecentTurns int
}

// Signal computes request features. It is the plug point for later signal
// families (embedding similarity, classifiers on fleet agents); Phase 1 ships
// only Structural.
type Signal interface {
	Name() string
	Extract(v *RequestView, opts ExtractOptions, f *Features)
}

// Structural derives features from the request's shape and text, in-process,
// with no model calls: tool activity (Switchyard-style), conversation size,
// modalities, output constraints, and lexical code/stack-trace/math markers
// (LiteLLM-heuristic-style) over the recent human turns.
//
// Every name in policy.KnownSignalFeatures except estimated_input_tokens (which
// the resolver sets from the tokenizer) is set by Extract, even when zero.
type Structural struct{}

func (Structural) Name() string { return "structural" }

func (Structural) Extract(v *RequestView, opts ExtractOptions, f *Features) {
	var toolResultTurns, toolErrors, assistantToolTurns, turns, images int
	for i := range v.Messages {
		m := &v.Messages[i]
		images += m.Images
		if m.ToolResults > 0 {
			toolResultTurns++
		}
		toolErrors += m.ToolErrors
		if m.Role == "assistant" && m.ToolCalls > 0 {
			assistantToolTurns++
		}
		if m.IsUserTurn() {
			turns++
		}
	}
	f.Set("has_tools", b2f(v.ToolCount > 0))
	f.Set("tool_count", float64(v.ToolCount))
	f.Set("tool_result_turns", float64(toolResultTurns))
	f.Set("tool_error_count", float64(toolErrors))
	f.Set("assistant_tool_call_turns", float64(assistantToolTurns))
	f.Set("turn_count", float64(turns))
	f.Set("message_count", float64(len(v.Messages)))
	f.Set("system_prompt_bytes", float64(len(v.System)))
	f.Set("request_bytes", float64(v.RequestBytes))
	f.Set("image_count", float64(images))
	f.Set("needs_structured_output", b2f(v.StructuredOut))
	f.Set("needs_reasoning", b2f(v.ReasoningAsked))

	recent := recentUserText(v, opts.RecentTurns)
	f.Set("code_presence", b2f(hasCode(recent)))
	f.Set("stack_trace", b2f(stackTraceRe.MatchString(recent)))
	f.Set("math_presence", b2f(mathRe.MatchString(recent)))
	f.window = strings.ToLower(recent)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// recentUserText joins the text of the last n human turns (oldest first).
func recentUserText(v *RequestView, n int) string {
	if n <= 0 {
		return ""
	}
	var picked []string
	for i := len(v.Messages) - 1; i >= 0 && len(picked) < n; i-- {
		if m := &v.Messages[i]; m.IsUserTurn() {
			picked = append(picked, m.Text)
		}
	}
	var b strings.Builder
	for i := len(picked) - 1; i >= 0; i-- {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(picked[i])
	}
	return b.String()
}

// stripMarked removes every block enclosed by an open/close marker pair
// (inclusive). An unterminated block is removed to the end of the text. An
// empty close marker removes just the open marker (the loader rejects it).
func stripMarked(s string, markers []string) string {
	for i := 0; i+1 < len(markers); i += 2 {
		open, closeTag := markers[i], markers[i+1]
		if open == "" || !strings.Contains(s, open) {
			continue
		}
		var b strings.Builder
		rest := s
		for {
			j := strings.Index(rest, open)
			if j < 0 {
				b.WriteString(rest)
				break
			}
			b.WriteString(rest[:j])
			k := strings.Index(rest[j+len(open):], closeTag)
			if k < 0 {
				break
			}
			rest = rest[j+len(open)+k+len(closeTag):]
		}
		s = b.String()
	}
	return s
}

var (
	stackTraceRe = regexp.MustCompile(`Traceback \(most recent call last\)|(?m)^\s+at [\w$.<>]+\(.*:\d+\)|panic: |goroutine \d+ \[|Exception in thread|(?m)^\w*(Error|Exception): |error\[E\d{4}\]|npm ERR!|(?m)^\s+File ".*", line \d+`)

	codeLineRe = regexp.MustCompile(`(?m)^\s*(def |func |class |import |from \S+ import |#include|package |public |private |const |let |var |fn |SELECT |CREATE TABLE )|[;{]\s*$|=>|::|\(\)\s*\{`)

	mathRe = regexp.MustCompile(`\$\$|\\\(|\\\[|\\(frac|int|sum|sqrt|partial|lim|infty|alpha|beta|theta|lambda|sigma|mathbb)\b|[∫∑√∂∞≤≥≈∇]|\b[a-zA-Z]\^\d|\bd[a-z]/d[a-z]\b`)
)

// failureProbeBytes bounds how much of each tool result the failure heuristic
// reads. Failures announce themselves at the start ("Error: ...") or the end
// ("exit code 1", a trailing traceback header), and agentic histories carry
// hundreds of KB of tool output, so scanning whole results would dominate the
// decision latency. Plain string checks, not regexp, for the same reason.
const failureProbeBytes = 256

// failureMarkers are matched anywhere in the probed head/tail (lower-cased).
var failureMarkers = []string{"traceback (most recent call last)", "command failed", "panic: "}

// looksLikeFailure reports whether tool output reads like a failure. Used for
// OpenAI "tool" messages, which have no is_error flag, and to corroborate
// Anthropic tool_result blocks.
func looksLikeFailure(s string) bool {
	if s == "" {
		return false
	}
	head, tail := s, ""
	if len(s) > 2*failureProbeBytes {
		head, tail = s[:failureProbeBytes], s[len(s)-failureProbeBytes:]
	}
	h := strings.ToLower(strings.TrimLeft(head, " \t\r\n"))
	for _, p := range []string{"error", "exception", "fatal"} {
		if strings.HasPrefix(h, p) && boundaryAfter(h, len(p)) {
			return true
		}
	}
	return probeFailure(h) || (tail != "" && probeFailure(strings.ToLower(tail)))
}

func probeFailure(lower string) bool {
	for _, m := range failureMarkers {
		if strings.Contains(lower, m) {
			return true
		}
	}
	// "exit code 1" / "exit status: 2" — any non-zero code.
	for _, m := range []string{"exit code", "exit status"} {
		for from := 0; ; {
			i := strings.Index(lower[from:], m)
			if i < 0 {
				break
			}
			j := from + i + len(m)
			for j < len(lower) && (lower[j] == ' ' || lower[j] == ':') {
				j++
			}
			if j < len(lower) && lower[j] >= '1' && lower[j] <= '9' {
				return true
			}
			from = from + i + 1
		}
	}
	return false
}

// hasCode: a fenced block, or at least two code-looking lines.
func hasCode(s string) bool {
	if strings.Contains(s, "```") {
		return true
	}
	return len(codeLineRe.FindAllStringIndex(s, 2)) >= 2
}

// containsKeyword reports whether kw (already lower-cased) occurs in text
// (lower-cased) at word boundaries, so "pr" does not match "prompt".
func containsKeyword(text, kw string) bool {
	if kw == "" {
		return false
	}
	for from := 0; ; {
		i := strings.Index(text[from:], kw)
		if i < 0 {
			return false
		}
		start := from + i
		end := start + len(kw)
		if boundaryBefore(text, start) && boundaryAfter(text, end) {
			return true
		}
		from = start + 1
		if from >= len(text) {
			return false
		}
	}
}

func boundaryBefore(s string, i int) bool {
	if i == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:i])
	return !isWordRune(r)
}

func boundaryAfter(s string, i int) bool {
	if i >= len(s) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(s[i:])
	return !isWordRune(r)
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
