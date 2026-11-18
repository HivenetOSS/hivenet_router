// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package hardware

import (
	"fmt"
	"os"
	"strings"

	nvml "github.com/NVIDIA/go-nvml/pkg/nvml"
)

// DetectGPUModel returns the model name NVML reports (e.g. "NVIDIA GeForce
// RTX 5090") when every GPU the agent uses is the same model: the GPUs listed
// in gpuDevicesFile, or every visible GPU when it is empty. Otherwise it
// returns "" and why: no NVML, no GPU, the devices file not written yet, or
// GPUs of several models (the agent cannot tell which one its engine uses).
func DetectGPUModel(gpuDevicesFile string) (model, why string) {
	var allowed map[string]bool
	if gpuDevicesFile != "" {
		data, err := os.ReadFile(gpuDevicesFile)
		if err != nil {
			return "", "the GPU devices file is not readable yet"
		}
		allowed = parseGPUDevices(string(data))
	}
	if ret := nvml.Init(); ret != nvml.SUCCESS {
		return "", "NVML unavailable"
	}
	defer nvml.Shutdown() //nolint:errcheck
	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return "", "NVML cannot count the GPUs"
	}
	var names []string
	for i := 0; i < count; i++ {
		dev, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			continue
		}
		if allowed != nil {
			uuid, ret := dev.GetUUID()
			if ret != nvml.SUCCESS || !allowed[uuid] {
				continue
			}
		}
		if name, ret := dev.GetName(); ret == nvml.SUCCESS {
			names = append(names, name)
		}
	}
	return commonGPUModel(names)
}

// parseGPUDevices reads an NVIDIA_VISIBLE_DEVICES value: GPU UUIDs separated
// by commas or new lines; "all" and "none" name no UUID.
func parseGPUDevices(s string) map[string]bool {
	out := map[string]bool{}
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' }) {
		if u := strings.TrimSpace(part); u != "" && u != "all" && u != "none" {
			out[u] = true
		}
	}
	return out
}

// commonGPUModel is the one model name in names, or "" and why.
func commonGPUModel(names []string) (string, string) {
	seen := map[string]bool{}
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			seen[n] = true
		}
	}
	switch len(seen) {
	case 0:
		return "", "no GPU found"
	case 1:
		for n := range seen {
			return n, ""
		}
	}
	return "", fmt.Sprintf("%d GPU models on this host", len(seen))
}
