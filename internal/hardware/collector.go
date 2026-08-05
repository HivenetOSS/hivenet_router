// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package hardware provides hardware metric collection for the agent daemon.
// It abstracts GPU (NVIDIA NVML) and system (CPU, memory via gopsutil) sampling
// behind a single Collector interface.
//
// The router never imports this package — it only receives domain.HardwareSnapshot
// via the heartbeat payload. Only the agent binary incurs the CGO/NVML dependency.
package hardware

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"hivenet_router/internal/domain"

	nvml "github.com/NVIDIA/go-nvml/pkg/nvml"
	logging "github.com/ipfs/go-log/v2"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("hardware")

// Collector samples hardware metrics from the local node.
// Implementations must be safe for concurrent use.
type Collector interface {
	// Collect returns a fresh hardware snapshot. GPU slice is empty on CPU-only nodes.
	Collect() (*domain.HardwareSnapshot, error)

	// Close releases any resources held by the collector (e.g. NVML handle).
	Close()
}

// NewCollector auto-detects the best available GPU backend.
// It tries NVML first; if unavailable it falls back to NullCollector (CPU/memory only).
// This is the only constructor callers should use.
//
// gpuDevicesFile is an optional path to a file containing GPU UUIDs (the value of
// NVIDIA_VISIBLE_DEVICES written by the engine at startup). When non-empty, only the
// listed GPUs are reported; when empty all visible GPUs are reported (default behaviour).
func NewCollector(gpuDevicesFile string) Collector {
	// Seed the CPU baseline. cpu.Percent with interval=0 computes usage since the
	// last call; the very first call has no prior baseline and typically returns
	// garbage (0 or near-100%). Discarding this warm-up result ensures the first
	// real Collect() call reports accurate values.
	_, _ = cpu.Percent(0, false)

	if ret := nvml.Init(); ret != nvml.SUCCESS {
		log.Warnf("hardware: NVML unavailable (%s) — GPU metrics disabled, reporting CPU/memory only", nvml.ErrorString(ret))
		return &nullCollector{}
	}
	log.Info("hardware: NVML initialised — GPU metrics enabled")
	return &nvmlCollector{gpuDevicesFile: gpuDevicesFile}
}

// nvmlCollector reads GPU metrics via NVIDIA NVML and system metrics via gopsutil.
type nvmlCollector struct {
	gpuDevicesFile string
	// filterMu guards the lazy-loaded filter state during initialization.
	// Once filterLoaded is true, allowedUUIDs becomes immutable and no lock is needed.
	filterMu     sync.Mutex
	allowedUUIDs map[string]struct{}
	filterLoaded bool // true when file was successfully parsed and filter is definitive
}

// loadAllowedUUIDsLocked reads the GPU devices file and populates allowedUUIDs.
// The file contains the value of NVIDIA_VISIBLE_DEVICES as written by the engine
// (e.g. "GPU-abc123" or "GPU-abc123,GPU-def456"). Returns true if the file was
// successfully parsed (definitive state: UUIDs present, "all", or "none"), false if
// the file is not ready (missing or empty) — caller should retry on next Collect().
//
// The caller must hold c.filterMu before calling this method.
func (c *nvmlCollector) loadAllowedUUIDsLocked() bool {
	data, err := os.ReadFile(c.gpuDevicesFile)
	if err != nil {
		return false // file not yet written by the engine — try again next cycle
	}
	c.allowedUUIDs = make(map[string]struct{})
	for _, part := range strings.FieldsFunc(string(data), func(r rune) bool { return r == ',' || r == '\n' || r == '\r' }) {
		uuid := strings.TrimSpace(part)
		if uuid != "" && uuid != "all" && uuid != "none" {
			c.allowedUUIDs[uuid] = struct{}{}
		}
	}
	// File was successfully parsed. Mark as loaded regardless of UUID count.
	// An empty allowedUUIDs map means "none" (report zero GPUs).
	c.filterLoaded = true
	if len(c.allowedUUIDs) > 0 {
		log.Infof("hardware: GPU filter loaded — restricting metrics to %d GPU(s) from %q", len(c.allowedUUIDs), c.gpuDevicesFile)
	} else {
		log.Info("hardware: GPU filter loaded — no GPUs assigned (NVIDIA_VISIBLE_DEVICES=none or empty)")
	}
	return true
}

// Collect samples all available GPUs plus CPU and memory.
// Individual GPU errors are logged and skipped rather than failing the whole snapshot.
func (c *nvmlCollector) Collect() (*domain.HardwareSnapshot, error) {
	// Lazy-load GPU UUID filter from file on first successful read.
	// The mutex ensures only one goroutine loads the filter even if Collect() is called concurrently.
	// If the file is not ready yet, we retry on the next cycle and report all visible GPUs.
	if c.gpuDevicesFile != "" {
		c.filterMu.Lock()
		if !c.filterLoaded {
			c.loadAllowedUUIDsLocked()
		}
		c.filterMu.Unlock()
	}

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("nvml DeviceGetCount: %s", nvml.ErrorString(ret))
	}

	gpus := make([]domain.GPUMetric, 0, count)
	for i := 0; i < count; i++ {
		dev, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			log.Warnf("hardware: skipping GPU %d: %s", i, nvml.ErrorString(ret))
			continue
		}

		// Apply GPU filter if loaded.
		// - filterLoaded=false: file not ready yet, report all GPUs (will retry next cycle).
		// - filterLoaded=true, allowedUUIDs empty: "none" was specified, skip all GPUs.
		// - filterLoaded=true, allowedUUIDs has entries: report only matching GPUs.
		if c.filterLoaded {
			if len(c.allowedUUIDs) == 0 {
				continue // "none" — skip all GPUs
			}
			uuid, ret := dev.GetUUID()
			if ret != nvml.SUCCESS {
				log.Warnf("hardware: GPU %d: cannot read UUID, skipping: %s", i, nvml.ErrorString(ret))
				continue
			}
			if _, ok := c.allowedUUIDs[uuid]; !ok {
				continue
			}
		}

		failures := 0

		var utilPct float64
		if util, ret := nvml.DeviceGetUtilizationRates(dev); ret == nvml.SUCCESS {
			utilPct = float64(util.Gpu)
		} else {
			failures++
		}

		var vramUsed, vramFree, vramTotal int64
		if memInfo, ret := nvml.DeviceGetMemoryInfo(dev); ret == nvml.SUCCESS {
			vramUsed = int64(memInfo.Used)
			vramFree = int64(memInfo.Free)
			vramTotal = int64(memInfo.Total)
		} else {
			failures++
		}

		var tempC float64
		if temp, ret := nvml.DeviceGetTemperature(dev, nvml.TEMPERATURE_GPU); ret == nvml.SUCCESS {
			tempC = float64(temp)
		} else {
			failures++
		}

		// NVML returns power in milliwatts — convert to watts.
		var powerW float64
		if power, ret := nvml.DeviceGetPowerUsage(dev); ret == nvml.SUCCESS {
			powerW = float64(power) / 1000.0
		} else {
			failures++
		}

		if failures == 4 {
			log.Warnf("hardware: GPU %d: all NVML sub-queries failed — all metrics will be zero", i)
		}

		gpus = append(gpus, domain.GPUMetric{
			Index:          i,
			UtilPercent:    utilPct,
			VRAMUsedBytes:  vramUsed,
			VRAMFreeBytes:  vramFree,
			VRAMTotalBytes: vramTotal,
			TemperatureC:   tempC,
			PowerWatts:     powerW,
		})
	}

	snap, err := collectCPUMemory()
	if err != nil {
		return nil, err
	}
	snap.GPUs = gpus
	return snap, nil
}

func (c *nvmlCollector) Close() {
	if ret := nvml.Shutdown(); ret != nvml.SUCCESS {
		log.Warnf("hardware: NVML shutdown error: %s", nvml.ErrorString(ret))
	}
}

// nullCollector reports only CPU and memory. Used on CPU-only nodes or when NVML is unavailable.
type nullCollector struct{}

func (c *nullCollector) Collect() (*domain.HardwareSnapshot, error) {
	return collectCPUMemory()
}

func (c *nullCollector) Close() {}

// collectCPUMemory gathers CPU utilization and system memory via gopsutil.
// Returns a snapshot with an empty GPU slice; callers fill it in if needed.
func collectCPUMemory() (*domain.HardwareSnapshot, error) {
	// cpu.Percent with interval=0 returns the usage since the last call (non-blocking).
	cpuPcts, err := cpu.Percent(0, false)
	if err != nil {
		return nil, fmt.Errorf("cpu.Percent: %w", err)
	}
	var cpuPct float64
	if len(cpuPcts) > 0 {
		cpuPct = cpuPcts[0]
	}

	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return nil, fmt.Errorf("mem.VirtualMemory: %w", err)
	}

	return &domain.HardwareSnapshot{
		GPUs: []domain.GPUMetric{},
		CPU:  domain.CPUMetric{UsagePercent: cpuPct},
		Memory: domain.MemoryMetric{
			UsedPercent:    vmStat.UsedPercent,
			AvailableBytes: int64(vmStat.Available),
			TotalBytes:     int64(vmStat.Total),
		},
		Timestamp: time.Now().Unix(),
	}, nil
}
