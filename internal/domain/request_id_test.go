// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"testing"
	"time"
)

// TestForwardID checks that every forward attempt sends the backend its own
// ID, all starting with the request ID.
func TestForwardID(t *testing.T) {
	p := NewPendingRequest("rid", &ChatRequest{}, time.Second)
	for _, want := range []string{"rid", "rid-2", "rid-3"} {
		if got := p.ForwardID(); got != want {
			t.Errorf("ForwardID() = %q, want %q", got, want)
		}
	}
}
