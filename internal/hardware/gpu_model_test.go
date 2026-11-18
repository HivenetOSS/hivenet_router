// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package hardware

import "testing"

func TestCommonGPUModel(t *testing.T) {
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{[]string{"NVIDIA GeForce RTX 5090", "NVIDIA GeForce RTX 5090"}, "NVIDIA GeForce RTX 5090"},
		{[]string{"NVIDIA GeForce RTX 5090", "NVIDIA RTX PRO 6000 Blackwell"}, ""},
		{nil, ""},
		{[]string{" ", ""}, ""},
	} {
		got, why := commonGPUModel(tc.names)
		if got != tc.want || (got == "" && why == "") {
			t.Errorf("commonGPUModel(%q) = %q, %q; want %q and a reason when empty", tc.names, got, why, tc.want)
		}
	}
}

func TestParseGPUDevices(t *testing.T) {
	got := parseGPUDevices("GPU-a, GPU-b\nall\nnone\n")
	if len(got) != 2 || !got["GPU-a"] || !got["GPU-b"] {
		t.Errorf("parseGPUDevices = %v, want GPU-a and GPU-b", got)
	}
}
