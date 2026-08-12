// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package api_test contains black-box tests for the per-request hard caps
// (max_input_tokens and images_max) enforced before a request is queued: an
// over-cap request is a clean 400 input_too_long, while an at-cap request and
// any max_tokens value pass through untouched (the router applies no output
// clamp).
package api_test

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"hivenet_router/internal/api"
	"hivenet_router/internal/domain"
	"hivenet_router/internal/policy"
)

// newCapHandlers builds a Handlers whose executor serves global as the policy
// for every model. The limiter is nil and no quota is set, so the daily-budget
// check short-circuits and a request that clears the caps reaches the queue.
func newCapHandlers(q chan *domain.PendingRequest, global *policy.Policy) *api.Handlers {
	exec := policy.NewExecutor(nil, nil, global, 0, 0)
	return api.NewHandlers(
		nil, nil, q, 2*time.Second,
		exec, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, nil,
	)
}

// textBody builds a chat-completions body with one user message whose content
// is contentLen 'a' characters, optionally carrying a max_tokens field (omitted
// when maxTokens <= 0). EstimateTokens for such a body is contentLen/4 + 4.
func textBody(contentLen, maxTokens int) []byte {
	content := strings.Repeat("a", contentLen)
	if maxTokens > 0 {
		return fmt.Appendf(nil, `{"model":"m","messages":[{"role":"user","content":%q}],"max_tokens":%d}`, content, maxTokens)
	}
	return fmt.Appendf(nil, `{"model":"m","messages":[{"role":"user","content":%q}]}`, content)
}

// imageBody builds a body with one user message carrying n image_url parts.
func imageBody(n int) []byte {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = `{"type":"image_url","image_url":{"url":"http://x/y.png"}}`
	}
	return fmt.Appendf(nil, `{"model":"m","messages":[{"role":"user","content":[%s]}]}`, strings.Join(parts, ","))
}

// TestB1_InputOverCap_400 verifies a prompt one token over max_input_tokens is
// rejected with 400 input_too_long before queueing.
func TestB1_InputOverCap_400(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 10})
	// contentLen 28 → 28/4 + 4 = 11 tokens, over the cap of 10.
	c, w := newCtx("/v1/chat/completions", textBody(28, 0))

	h.Passthrough(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an over-cap prompt, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), string(domain.ErrCodeInputTooLong)) {
		t.Errorf("expected error code %q in body, got %s", domain.ErrCodeInputTooLong, w.Body.String())
	}
	if len(q) != 0 {
		t.Errorf("an over-cap request must not be queued, queue depth=%d", len(q))
	}
}

// TestB1_InputAtCap_Passes verifies a prompt exactly at max_input_tokens is
// admitted (the cap is inclusive) and reaches the queue.
func TestB1_InputAtCap_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 10})
	// contentLen 24 → 24/4 + 4 = 10 tokens, exactly the cap.
	c, w := newCtx("/v1/chat/completions", textBody(24, 0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("an at-cap prompt must pass B1 and be enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for an at-cap prompt, got %d", w.Code)
	}
}

// TestB1_ImagesOverCap_400 verifies a request carrying more images than
// images_max is rejected with 400 input_too_long.
func TestB1_ImagesOverCap_400(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{ImagesMax: 2})
	c, w := newCtx("/v1/chat/completions", imageBody(3))

	h.Passthrough(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for too many images, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), string(domain.ErrCodeInputTooLong)) {
		t.Errorf("expected error code %q in body, got %s", domain.ErrCodeInputTooLong, w.Body.String())
	}
	if len(q) != 0 {
		t.Errorf("an over-cap request must not be queued, queue depth=%d", len(q))
	}
}

// TestB1_ImagesAcrossMessages_400 verifies the image cap counts images summed
// across all messages, not per message: two messages of two images each exceed
// an images_max of 3.
func TestB1_ImagesAcrossMessages_400(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{ImagesMax: 3})
	img := `{"type":"image_url","image_url":{"url":"http://x/y.png"}}`
	body := fmt.Appendf(nil, `{"model":"m","messages":[{"role":"user","content":[%s,%s]},{"role":"user","content":[%s,%s]}]}`, img, img, img, img)
	c, w := newCtx("/v1/chat/completions", body)

	h.Passthrough(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when images summed across messages exceed the cap, got %d", w.Code)
	}
	if len(q) != 0 {
		t.Errorf("an over-cap request must not be queued, queue depth=%d", len(q))
	}
}

// TestB1_MixedTextAndImage_TextCounted verifies the token cap still sees the text
// of a multimodal message that also carries an image: the text alone is over
// max_input_tokens, so the request is a 400 even though an image shares the
// message (the text part must not be dropped just because an image is present).
func TestB1_MixedTextAndImage_TextCounted(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 10, ImagesMax: 4})
	text := strings.Repeat("a", 28) // 28/4 + 4 = 11 tokens, over the cap of 10
	body := fmt.Appendf(nil, `{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":%q},{"type":"image_url","image_url":{"url":"http://x/y.png"}}]}]}`, text)
	c, w := newCtx("/v1/chat/completions", body)

	h.Passthrough(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("text in a multimodal message must still count toward the token cap; expected 400, got %d", w.Code)
	}
	if len(q) != 0 {
		t.Errorf("an over-cap request must not be queued, queue depth=%d", len(q))
	}
}

// TestB1_ImagesAtCap_Passes verifies a request with exactly images_max images
// is admitted.
func TestB1_ImagesAtCap_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{ImagesMax: 2})
	c, w := newCtx("/v1/chat/completions", imageBody(2))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("an at-cap image count must pass B1 and be enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for an at-cap image request, got %d", w.Code)
	}
}

// TestB1_ImageOnlyUnderCaps_Passes verifies a textless (image-only) request is
// not spuriously rejected by the input-token cap: an image carries no estimable
// text, so the text cap cannot see it — images are bounded by images_max, not by
// max_input_tokens. With both caps set and the image count within images_max the
// request must pass, locking in the two-cap division against a "floor the
// estimate" change that would provide no real bound.
func TestB1_ImageOnlyUnderCaps_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 10, ImagesMax: 4})
	c, w := newCtx("/v1/chat/completions", imageBody(2))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("a textless image request within images_max must pass B1")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for an image-only request under both caps, got %d", w.Code)
	}
}

// TestB1_NoMaxTokens_BodyUnchanged verifies B1 passes a request that declares no
// max_tokens and injects none: the forwarded body carries no max_tokens key and
// the parsed request has both output fields zero.
func TestB1_NoMaxTokens_BodyUnchanged(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 100_000})
	c, w := newCtx("/v1/chat/completions", textBody(24, 0)) // no max_tokens

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if pending.Request.MaxTokens != 0 || pending.Request.MaxCompletionTokens != 0 {
			t.Errorf("B1 must not inject an output cap; got max_tokens=%d max_completion_tokens=%d",
				pending.Request.MaxTokens, pending.Request.MaxCompletionTokens)
		}
		if bytes.Contains(pending.Request.RawBytes, []byte("max_tokens")) {
			t.Errorf("forwarded body must not contain a max_tokens key, got %s", pending.Request.RawBytes)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestB1_LargeDeclaredMaxTokens_Passes verifies a large declared max_tokens does
// not trip B1: B1 bounds input and images only, never output. Aggregate pool
// safety for large max_tokens is a separate admission concern.
func TestB1_LargeDeclaredMaxTokens_Passes(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 100_000})
	c, w := newCtx("/v1/chat/completions", textBody(24, 200_000))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if pending.Request.MaxTokens != 200_000 {
			t.Errorf("declared max_tokens must be forwarded unchanged, got %d", pending.Request.MaxTokens)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("a large declared max_tokens must still pass B1")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestB1_NoOutputClamp_Forwarded verifies the forwarded body for a request with
// a declared max_tokens is passed through verbatim — B1 neither shrinks nor
// removes the value.
func TestB1_NoOutputClamp_Forwarded(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{MaxInputTokens: 100_000, ImagesMax: 4})
	body := textBody(24, 50_000)
	c, w := newCtx("/v1/chat/completions", body)

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		if !bytes.Equal(pending.Request.RawBytes, body) {
			t.Errorf("forwarded body must be verbatim; sent %s got %s", body, pending.Request.RawBytes)
		}
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("request was not enqueued")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestB1_UnsetCaps_NoOp verifies a policy that declares neither cap (both zero)
// leaves every request untouched — the reserved-mode / no-limits regression.
func TestB1_UnsetCaps_NoOp(t *testing.T) {
	q := make(chan *domain.PendingRequest, 1)
	h := newCapHandlers(q, &policy.Policy{}) // no caps
	c, w := newCtx("/v1/chat/completions", textBody(4_000, 0))

	done := make(chan struct{})
	go func() { h.Passthrough(c); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("with no caps set, a large request must still pass")
	}
	<-done
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with caps unset, got %d", w.Code)
	}
}

// TestB1_PerModelPolicyCap verifies the cap is resolved per model: a named
// policy claiming model "capped" enforces its cap, while a request for another
// model falls back to the (uncapped) global policy.
func TestB1_PerModelPolicyCap(t *testing.T) {
	exec := policy.NewExecutor(nil, nil, &policy.Policy{}, 0, 0)
	if err := exec.SetNamedPolicy("capped", &policy.Policy{Models: []string{"capped"}, MaxInputTokens: 10}); err != nil {
		t.Fatalf("SetNamedPolicy: %v", err)
	}
	q := make(chan *domain.PendingRequest, 1)
	h := api.NewHandlers(nil, nil, q, 2*time.Second, exec, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)

	// Over-cap request for the capped model → 400.
	over := fmt.Appendf(nil, `{"model":"capped","messages":[{"role":"user","content":%q}]}`, strings.Repeat("a", 28))
	c, w := newCtx("/v1/chat/completions", over)
	h.Passthrough(c)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for the capped model, got %d", w.Code)
	}

	// Same-size request for an unclaimed model → global (uncapped) → passes.
	free := fmt.Appendf(nil, `{"model":"other","messages":[{"role":"user","content":%q}]}`, strings.Repeat("a", 28))
	c2, w2 := newCtx("/v1/chat/completions", free)
	done := make(chan struct{})
	go func() { h.Passthrough(c2); close(done) }()
	select {
	case pending := <-q:
		pending.Response <- &domain.ChatResponse{RawBytes: []byte(`{"ok":true}`)}
	case <-time.After(time.Second):
		t.Fatal("an unclaimed model must fall back to the uncapped global policy")
	}
	<-done
	if w2.Code != http.StatusOK {
		t.Errorf("expected 200 for the uncapped model, got %d", w2.Code)
	}
}
