// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package hardware_test verifies the Collector construction/sampling path that
// works on any node (CPU + memory via gopsutil). NVML GPU sampling requires real
// NVIDIA hardware and is exercised only on GPU nodes — NewCollector transparently
// falls back to the CPU/memory-only collector when NVML is unavailable, which is
// the path covered here.
package hardware_test

import (
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/hardware"
)

func TestNewCollector_CollectsCPUAndMemory(t *testing.T) {
	c := hardware.NewCollector("") // NVML if present, else CPU/memory-only fallback
	defer c.Close()

	// Collect samples CPU + memory (always available via gopsutil) and, when NVML
	// is present, GPUs — so this must succeed on a plain CI runner with no GPU.
	snap, err := c.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if snap == nil {
		t.Fatal("Collect returned nil snapshot")
	}
	// Memory is always available regardless of GPU presence.
	if snap.Memory.TotalBytes <= 0 {
		t.Errorf("Memory.TotalBytes = %d, want > 0", snap.Memory.TotalBytes)
	}
	if snap.Memory.UsedPercent < 0 || snap.Memory.UsedPercent > 100 {
		t.Errorf("Memory.UsedPercent = %v, want 0..100", snap.Memory.UsedPercent)
	}
	if snap.CPU.UsagePercent < 0 {
		t.Errorf("CPU.UsagePercent = %v, want >= 0", snap.CPU.UsagePercent)
	}
	// GPUs must be a non-nil slice (possibly empty on a CPU-only node).
	if snap.GPUs == nil {
		t.Error("GPUs slice must be non-nil (empty on CPU-only nodes, not nil)")
	}
	if snap.Timestamp <= 0 {
		t.Errorf("Timestamp = %d, want > 0", snap.Timestamp)
	}
}

func TestCollector_CloseIsIdempotentEnough(t *testing.T) {
	// Close must be safe to call; on the null collector it is a no-op.
	c := hardware.NewCollector("")
	c.Close()
}

// fakeCollector demonstrates the Collector seam consumers use to inject
// deterministic hardware readings in their own tests (no GPU required).
type fakeCollector struct{ snap *domain.HardwareSnapshot }

func (f fakeCollector) Collect() (*domain.HardwareSnapshot, error) { return f.snap, nil }
func (f fakeCollector) Close()                                     {}

var _ hardware.Collector = fakeCollector{}

func TestCollector_InterfaceSeam(t *testing.T) {
	want := &domain.HardwareSnapshot{
		CPU:    domain.CPUMetric{UsagePercent: 12.5},
		Memory: domain.MemoryMetric{UsedPercent: 40, TotalBytes: 1 << 30},
		GPUs:   []domain.GPUMetric{{Index: 0, UtilPercent: 88, TemperatureC: 65}},
	}
	// Through the interface, Collect returns exactly the injected snapshot, letting
	// downstream tests assert on GPU fields (e.g. util%) with zero real hardware.
	var c hardware.Collector = fakeCollector{snap: want}
	got, err := c.Collect()
	if err != nil || got != want {
		t.Fatalf("fake Collector round-trip failed: %v", err)
	}
	if got.GPUs[0].UtilPercent != 88 {
		t.Errorf("GPU util = %v, want 88", got.GPUs[0].UtilPercent)
	}
}
