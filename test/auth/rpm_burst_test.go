// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth_test

import (
	"testing"

	"hivenet_router/internal/auth"
)

// TestRPM_CertifiedBurstWindow verifies the RPM burst is the certified short
// window when configured, instead of a full minute's quota. A 60/min rate with a
// 10s window bursts 10, not 60.
func TestRPM_CertifiedBurstWindow(t *testing.T) {
	l := auth.NewInMemoryLimiter()
	l.SetRPMBurstWindow(10) // 10s of a 60/min rate → burst 10

	admitted := 0
	for i := 0; i < 20; i++ {
		if ok, _, _ := l.AllowRequest("t", "m", 60); ok {
			admitted++
		}
	}
	if admitted != 10 {
		t.Errorf("a 10s certified burst of a 60/min rate must admit 10, got %d", admitted)
	}

	// The legacy default (no window) bursts a full minute — 6× the certified burst.
	def := auth.NewInMemoryLimiter()
	admittedDef := 0
	for i := 0; i < 20; i++ {
		if ok, _, _ := def.AllowRequest("t", "m", 60); ok {
			admittedDef++
		}
	}
	if admittedDef != 20 {
		t.Errorf("the legacy full-minute burst must admit all 20, got %d", admittedDef)
	}
}

// TestTinyFloodStoppedByRPM_NotITPM verifies RPM and ITPM are complementary axes:
// a flood of tiny (100-token) requests is stopped by the request-rate cap while
// the input-token-rate cap has ample room, so RPM — not ITPM — is the flood guard.
func TestTinyFloodStoppedByRPM_NotITPM(t *testing.T) {
	rpm := auth.NewInMemoryLimiter()    // default burst = rpm = 96
	itpm := auth.NewMinuteRateLimiter() // 520,000 tokens/min

	rpmAdmitted, itpmAdmitted := 0, 0
	for i := 0; i < 300; i++ {
		if ok, _, _ := rpm.AllowRequest("t", "m", 96); ok {
			rpmAdmitted++
		}
		if itpm.AllowInputTokens("t", "m", 520_000, 100) {
			itpmAdmitted++
		}
	}
	if rpmAdmitted >= 300 {
		t.Errorf("RPM must stop the tiny-request flood; admitted %d/300", rpmAdmitted)
	}
	if itpmAdmitted != 300 {
		t.Errorf("ITPM must NOT stop the flood (520000/100 = 5200 capacity); admitted %d/300", itpmAdmitted)
	}
}
