// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"testing"
	"time"
)

// TestRequestDeadlinePassed pins the request-timeout classification: only an
// expired request deadline counts. A deadline-shaped transport error while the
// request still has budget (e.g. libp2phttp's own stream-open timeout) must
// stay on the connection-failure path so the request fails over to another
// agent instead of returning 504 with most of its budget left.
func TestRequestDeadlinePassed(t *testing.T) {
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	// Deadline instant reached but the context timer may not have fired yet:
	// the stream read deadline can surface first.
	justNow, cancelJustNow := context.WithDeadline(context.Background(), time.Now())
	defer cancelJustNow()

	budgetLeft, cancelBudgetLeft := context.WithTimeout(context.Background(), time.Minute)
	defer cancelBudgetLeft()

	cancelled, cancel := context.WithTimeout(context.Background(), time.Minute)
	cancel()

	cases := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{"deadline expired", expired, true},
		{"deadline instant reached", justNow, true},
		{"budget left (stream-open timeout)", budgetLeft, false},
		{"cancelled, not expired", cancelled, false},
		{"no deadline", context.Background(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := requestDeadlinePassed(tc.ctx); got != tc.want {
				t.Fatalf("requestDeadlinePassed = %v, want %v", got, tc.want)
			}
		})
	}
}
