// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package domain_test contains black-box tests for the prompt token estimator
// used by the front-door budget check, and for the message-slice extraction it
// relies on (which intentionally drops non-text content).
package domain_test

import (
	"testing"

	"hivenet_router/internal/domain"
)

// TestEstimateTokens locks the estimation formula: sum(len(msg)/4) per entry plus
// a flat 4-token overhead per entry.
func TestEstimateTokens(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want int
	}{
		{"empty slice", nil, 0},
		{"single empty string", []string{""}, 4},            // 0/4 + 1*4
		{"single 8-char", []string{"abcdefgh"}, 2 + 4},      // 8/4 + 4
		{"two 4-char", []string{"abcd", "abcd"}, 1 + 1 + 8}, // (1+1) + 2*4
	}
	for _, c := range cases {
		if got := domain.EstimateTokens(c.in); got != c.want {
			t.Errorf("%s: EstimateTokens(%v) = %d, want %d", c.name, c.in, got, c.want)
		}
	}
}

// TestGetMessageSlice_DropsNonText documents the behaviour that motivates the
// per-message floor in the budget check: only text content is returned, so an
// image-only message contributes nothing and an image-only conversation yields an
// empty slice (which would estimate to 0 without the floor).
func TestGetMessageSlice_DropsNonText(t *testing.T) {
	// Mixed message: one text part + one image part, plus a plain-string message.
	msgs := []domain.ChatCompletionMessage{
		{Role: "user", Content: []domain.ContentPart{
			{Type: "text", Text: "hello"},
			{Type: "image_url", ImageURL: &domain.ImageURLConfig{}},
		}},
		{Role: "user", Content: "world"},
	}
	got := domain.GetMessageSlice(msgs)
	want := []string{"hello", "world"} // image part dropped
	if len(got) != len(want) {
		t.Fatalf("GetMessageSlice returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("GetMessageSlice[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Image-only conversation → empty slice → would estimate to 0 (hence the floor).
	imageOnly := []domain.ChatCompletionMessage{
		{Role: "user", Content: []domain.ContentPart{{Type: "image_url", ImageURL: &domain.ImageURLConfig{}}}},
	}
	if got := domain.GetMessageSlice(imageOnly); len(got) != 0 {
		t.Errorf("image-only message should yield an empty slice, got %v", got)
	}
	if est := domain.EstimateTokens(domain.GetMessageSlice(imageOnly)); est != 0 {
		t.Errorf("image-only estimate should be 0 (compensated by the floor), got %d", est)
	}
}
