// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/tokenizer"
)

func pngDataURL(t *testing.T, w, h int) string {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewGray(image.Rect(0, 0, w, h))); err != nil {
		t.Fatal(err)
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(b.Bytes())
}

// TestPassthrough_ImageEstimateLearned: a chat request with an image is
// estimated at the default per image and charged that to the daily budget;
// once the backend has counted the image, the next request with an image of
// that size is estimated at what it counted, and the ratio is reported.
func TestPassthrough_ImageEstimateLearned(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	lim := &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000}
	h := newTestHandlers(q, lim)
	var ratios []float64
	h.SetImageEstimator(tokenizer.NewImageEstimator(1000), func(model string, ratio float64) {
		if model == "vlm" {
			ratios = append(ratios, ratio)
		}
	})
	body := fmt.Sprintf(`{"model":"vlm","max_tokens":5,"messages":[{"role":"user","content":[{"type":"text","text":"what is in the picture?"},{"type":"image_url","image_url":{"url":%q}}]}]}`,
		pngDataURL(t, 640, 480))

	send := func() *domain.PendingRequest {
		c, w := newCtx("/v1/chat/completions", []byte(body))
		c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})
		done := make(chan struct{})
		go func() { h.Passthrough(c); close(done) }()
		var pending *domain.PendingRequest
		select {
		case pending = <-q:
		case <-time.After(time.Second):
			t.Fatal("request not queued")
		}
		text := pending.EstimatedInputTokens - pending.EstimatedImageTokens
		prompt := text + 393 // what the backend counts for the image
		pending.Response <- &domain.ChatResponse{
			RawBytes: []byte(`{}`),
			Usage:    domain.Usage{PromptTokens: prompt, CompletionTokens: 1, TotalTokens: prompt + 1},
		}
		<-done
		if w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
		return pending
	}

	first := send()
	if first.EstimatedImageTokens != 1000 {
		t.Errorf("first request: image estimate %d, want the default 1000", first.EstimatedImageTokens)
	}
	if lim.chargedInput != first.EstimatedInputTokens+5 || first.HeldInputTokens != first.EstimatedInputTokens {
		t.Errorf("daily budget held %d (prompt %d), want the estimate with the image, %d, plus max_tokens 5",
			lim.chargedInput, first.HeldInputTokens, first.EstimatedInputTokens)
	}
	if first.ReservedInputTokens != first.EstimatedInputTokens {
		t.Errorf("reserved %d, want %d (nothing learned: upper = point)", first.ReservedInputTokens, first.EstimatedInputTokens)
	}

	second := send()
	if second.EstimatedImageTokens != 393 {
		t.Errorf("second request: image estimate %d, want the learned 393", second.EstimatedImageTokens)
	}
	if len(ratios) != 2 || ratios[0] != 0.393 || ratios[1] != 1 {
		t.Errorf("reported ratios %v, want [0.393 1]", ratios)
	}
}

// TestPassthrough_ImageURLNeverFetched: an image given by URL is estimated
// without fetching it (the URL points nowhere reachable).
func TestPassthrough_ImageURLNeverFetched(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newTestHandlers(q, &fakeLimiter{remaining: 1_000_000, inAllowed: true, inRemaining: 1_000_000})
	h.SetImageEstimator(tokenizer.NewImageEstimator(700), nil)
	body := `{"model":"vlm","max_tokens":5,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://192.0.2.1/never.png"}}]}]}`
	c, _ := newCtx("/v1/chat/completions", []byte(body))
	c.Set("quota_limits", auth.QuotaLimits{TokensPerDay: 1_000_000})
	done := make(chan struct{})
	start := time.Now()
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if time.Since(start) > 500*time.Millisecond {
			t.Errorf("queued after %v: the URL looks fetched", time.Since(start))
		}
		if pending.EstimatedImageTokens != 700 {
			t.Errorf("image estimate %d, want the default 700", pending.EstimatedImageTokens)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{}`)}
	case <-time.After(2 * time.Second):
		t.Fatal("request not queued")
	}
	<-done
}
