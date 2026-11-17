// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "testing"

func TestPoolKey(t *testing.T) {
	if k := PoolKey("m", SharedPool); k != "m" {
		t.Errorf("shared pool key %q, want the model name", k)
	}
	for _, tc := range [][2]string{{"m", ""}, {"Qwen/Qwen2.5-7B", "acme"}, {"m", "a.b_c-1"}} {
		if m, p := SplitPoolKey(PoolKey(tc[0], tc[1])); m != tc[0] || p != tc[1] {
			t.Errorf("round trip %v: got %q %q", tc, m, p)
		}
	}
	if PoolKey("m", "acme") == PoolKey("m", "other") || PoolKey("m", "acme") == "m" {
		t.Error("pools of one model must have distinct keys")
	}
}

func TestValidReservation(t *testing.T) {
	for _, ok := range []string{"acme", "tenant-42", "a.b_c", "A1"} {
		if !ValidReservation(ok) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"", "has space", "a/b", "a\x1fb", "é", string(make([]byte, 65))} {
		if ValidReservation(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}
