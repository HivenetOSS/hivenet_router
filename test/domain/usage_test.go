// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"encoding/json"
	"testing"

	"hivenet_router/internal/domain"
)

// TestUsage_UnmarshalOpenAIShape verifies the OpenAI usage shape still decodes
// exactly as before the Anthropic normalization was added.
func TestUsage_UnmarshalOpenAIShape(t *testing.T) {
	var u domain.Usage
	if err := json.Unmarshal([]byte(`{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}`), &u); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if u.PromptTokens != 10 || u.CompletionTokens != 5 || u.TotalTokens != 15 {
		t.Errorf("got %+v, want 10/5/15", u)
	}
}

// TestUsage_UnmarshalAnthropicShape verifies the Anthropic /v1/messages usage
// shape (input_tokens/output_tokens, no total) is normalized into the OpenAI
// fields with a synthesized total — without this every Anthropic response read
// as zero usage, so the dialect silently escaped output metering, OTPM
// charging, the occupancy true-up, and estimator learning.
func TestUsage_UnmarshalAnthropicShape(t *testing.T) {
	var u domain.Usage
	if err := json.Unmarshal([]byte(`{"input_tokens":42,"output_tokens":7}`), &u); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if u.PromptTokens != 42 || u.CompletionTokens != 7 {
		t.Errorf("got prompt=%d completion=%d, want 42/7", u.PromptTokens, u.CompletionTokens)
	}
	if u.TotalTokens != 49 {
		t.Errorf("total must be synthesized: got %d, want 49", u.TotalTokens)
	}
}

// TestUsage_UnmarshalEmpty verifies an absent/empty usage object stays zero, so
// "usage present" checks (TotalTokens > 0) still fall back to estimation.
func TestUsage_UnmarshalEmpty(t *testing.T) {
	var u domain.Usage
	if err := json.Unmarshal([]byte(`{}`), &u); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if u.PromptTokens != 0 || u.CompletionTokens != 0 || u.TotalTokens != 0 {
		t.Errorf("empty usage must stay zero, got %+v", u)
	}
}
