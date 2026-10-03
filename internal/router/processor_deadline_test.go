// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
)

// TestIsRequestDeadlineError pins the two deadline forms the forward path can
// produce, the message-string safety net, and — just as important — that
// genuine transport failures do NOT match (they must keep routing through the
// agent_disconnected eviction path).
func TestIsRequestDeadlineError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"context deadline", context.DeadlineExceeded, true},
		{"os deadline bare", os.ErrDeadlineExceeded, true},
		{"url.Error wrapping os deadline", &url.Error{
			Op:  "Post",
			URL: "/hivenet_router/inference/1.0.0/v1/chat/completions",
			Err: os.ErrDeadlineExceeded,
		}, true},
		{"message-only wrapper", errors.New(`Post "/hivenet_router/inference/1.0.0/v1/chat/completions": i/o deadline reached`), true},
		{"connection reset", errors.New("read: connection reset by peer"), false},
		{"no addresses", errors.New("failed to dial: no addresses"), false},
		{"stream reset", errors.New("transport error: sent go away, code: 0"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRequestDeadlineError(tc.err); got != tc.want {
				t.Fatalf("isRequestDeadlineError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
