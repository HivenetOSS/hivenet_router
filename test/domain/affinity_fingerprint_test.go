// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Behavioural tests for domain.AffinityFingerprint — the targeted body parse
// that keys agent affinity when no task/session header is present: end user,
// system prompt and first human turn, in both dialects, degrading to empty
// fields instead of failing.
package domain_test

import (
	"strings"
	"testing"

	"hivenet_router/internal/domain"
)

func TestAffinityFingerprint_EndUserDialect(t *testing.T) {
	cases := []struct {
		name string
		path string
		body string
		want string
	}{
		{
			"openai top-level user",
			"/v1/chat/completions",
			`{"model":"m","user":"alice","messages":[{"role":"user","content":"hi"}]}`,
			"alice",
		},
		{
			"anthropic metadata.user_id",
			"/v1/messages",
			`{"model":"m","metadata":{"user_id":"bob"},"messages":[{"role":"user","content":"hi"}]}`,
			"bob",
		},
		{
			"openai dialect prefers user over metadata",
			"/v1/chat/completions",
			`{"user":"alice","metadata":{"user_id":"bob"},"messages":[{"role":"user","content":"hi"}]}`,
			"alice",
		},
		{
			"anthropic dialect prefers metadata.user_id",
			"/v1/messages",
			`{"user":"alice","metadata":{"user_id":"bob"},"messages":[{"role":"user","content":"hi"}]}`,
			"bob",
		},
		{
			"anthropic falls back to user when metadata absent",
			"/v1/messages",
			`{"user":"alice","messages":[{"role":"user","content":"hi"}]}`,
			"alice",
		},
		{
			"malformed user degrades to metadata",
			"/v1/chat/completions",
			`{"user":42,"metadata":{"user_id":"bob"},"messages":[{"role":"user","content":"hi"}]}`,
			"bob",
		},
		{
			"malformed metadata degrades to empty (never an error)",
			"/v1/messages",
			`{"metadata":"nope","messages":[{"role":"user","content":"hi"}]}`,
			"",
		},
		{
			"no declared user",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":"hi"}]}`,
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eu, _, _ := domain.AffinityFingerprint([]byte(tc.body), tc.path)
			if eu != tc.want {
				t.Errorf("endUser = %q, want %q", eu, tc.want)
			}
		})
	}
}

func TestAffinityFingerprint_System(t *testing.T) {
	cases := []struct {
		name string
		path string
		body string
		want string
	}{
		{
			"anthropic top-level system string",
			"/v1/messages",
			`{"system":"be brief","messages":[{"role":"user","content":"hi"}]}`,
			"be brief",
		},
		{
			"anthropic top-level system as text blocks",
			"/v1/messages",
			`{"system":[{"type":"text","text":"part one"},{"type":"text","text":"part two"}],"messages":[{"role":"user","content":"hi"}]}`,
			"part one\npart two",
		},
		{
			"openai system and developer turns",
			"/v1/chat/completions",
			`{"messages":[{"role":"system","content":"rules"},{"role":"developer","content":"style"},{"role":"user","content":"hi"}]}`,
			"rules\nstyle",
		},
		{
			"top-level system plus system turn are joined",
			"/v1/chat/completions",
			`{"system":"outer","messages":[{"role":"system","content":"inner"},{"role":"user","content":"hi"}]}`,
			"outer\ninner",
		},
		{
			"no system at all",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":"hi"}]}`,
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, sys, _ := domain.AffinityFingerprint([]byte(tc.body), tc.path)
			if sys != tc.want {
				t.Errorf("system = %q, want %q", sys, tc.want)
			}
		})
	}
}

func TestAffinityFingerprint_FirstUserText(t *testing.T) {
	cases := []struct {
		name string
		path string
		body string
		want string
	}{
		{
			"plain string content",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":"hello there"}]}`,
			"hello there",
		},
		{
			"text parts are joined",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":[{"type":"text","text":"line one"},{"type":"text","text":"line two"}]}]}`,
			"line one\nline two",
		},
		{
			"anthropic content blocks",
			"/v1/messages",
			`{"messages":[{"role":"user","content":[{"type":"text","text":"claude question"}]}]}`,
			"claude question",
		},
		{
			"first human turn wins over later ones",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"reply"},{"role":"user","content":"second"}]}`,
			"first",
		},
		{
			"leading assistant messages are skipped",
			"/v1/chat/completions",
			`{"messages":[{"role":"assistant","content":"preamble"},{"role":"user","content":"actual question"}]}`,
			"actual question",
		},
		{
			"tool-only user turn is not a human turn",
			"/v1/messages",
			`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]},{"role":"user","content":"the real ask"}]}`,
			"the real ask",
		},
		{
			"role tool messages are excluded",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":"question"},{"role":"tool","content":"tool says hi"},{"role":"user","content":"follow-up"}]}`,
			"question",
		},
		{
			"user turn mixing tool results and text counts as human",
			"/v1/messages",
			`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"},{"type":"text","text":"and now do X"}]}]}`,
			"and now do X",
		},
		{
			"malformed content degrades to empty, no panic",
			"/v1/chat/completions",
			`{"messages":[{"role":"user","content":42}]}`,
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, first := domain.AffinityFingerprint([]byte(tc.body), tc.path)
			if first != tc.want {
				t.Errorf("firstUserText = %q, want %q", first, tc.want)
			}
		})
	}
}

// TestAffinityFingerprint_MalformedBody: the fingerprint never fails — a body
// the router would have rejected upstream still yields empty fields (the key
// then falls back to whatever header the client sent).
func TestAffinityFingerprint_MalformedBody(t *testing.T) {
	for _, body := range []string{"", "{", "not json", "[]", "null"} {
		eu, sys, first := domain.AffinityFingerprint([]byte(body), "/v1/chat/completions")
		if eu != "" || sys != "" || first != "" {
			t.Errorf("body %q → (%q,%q,%q), want all empty", body, eu, sys, first)
		}
	}
}

// TestAffinityFingerprint_PiStampChangesSystem: the pi harness re-stamps
// "Snapshot session_start at …" in the system prompt every turn. The
// fingerprint must reflect the CURRENT system text (that churn is exactly why
// header-keyed conversations exist), while the first human turn stays stable.
func TestAffinityFingerprint_PiStampChangesSystem(t *testing.T) {
	bodyAt := func(stamp string) []byte {
		return []byte(`{"messages":[{"role":"system","content":"Snapshot session_start at ` + stamp + `"},{"role":"user","content":"same ask"}]}`)
	}
	_, sys1, first1 := domain.AffinityFingerprint(bodyAt("10:00:01"), "/v1/chat/completions")
	_, sys2, first2 := domain.AffinityFingerprint(bodyAt("10:00:02"), "/v1/chat/completions")

	if sys1 == sys2 {
		t.Errorf("stamped system text should differ per turn, both %q", sys1)
	}
	if first1 != first2 {
		t.Errorf("first human turn must stay stable: %q vs %q", first1, first2)
	}
	if !strings.Contains(sys1, "10:00:01") {
		t.Errorf("system = %q, want the turn-1 stamp", sys1)
	}
}
