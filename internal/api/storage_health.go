// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// healthCheckPayload is what the write probe persists into the data directory.
// The content is irrelevant; what matters is that the bytes reach the volume
// (the write must survive a flush) and read back unchanged.
const healthCheckPayload = "hivenet-router storage health check\n"

// StorageHealth checks that the directory backing the router's persistent
// storage (the BadgerDB data directory — the PVC mount point in Kubernetes
// deployments) is accessible. GET /health calls Verify on every probe: when
// the volume is faulted, detached, or read-only, the probe returns 503, the
// pod drops out of Ready, and the owning InferenceEndpoint leaves state
// Active (HAI-404).
type StorageHealth struct {
	mu           sync.Mutex
	dataDir      string
	volumeName   string // operator-supplied (HIVENET_ROUTER_STORAGE_VOLUME_NAME); empty = unknown
	storageClass string // operator-supplied (HIVENET_ROUTER_STORAGE_CLASS); empty = unknown
}

// NewStorageHealth creates a StorageHealth for dataDir. volumeName and
// storageClass are optional identifiers included in the warning log when the
// check fails, so operators can correlate the fault with the pod's volume.
func NewStorageHealth(dataDir, volumeName, storageClass string) *StorageHealth {
	return &StorageHealth{dataDir: dataDir, volumeName: volumeName, storageClass: storageClass}
}

// DataDir returns the checked directory.
func (s *StorageHealth) DataDir() string { return s.dataDir }

// VolumeName returns the configured volume name (the PVC name in Kubernetes
// deployments), falling back to the mount source of the data directory (e.g.
// /dev/longhorn/pvc-<uuid>) when none is configured, and "unknown" when the
// data directory sits on the container's root filesystem.
func (s *StorageHealth) VolumeName() string {
	if s.volumeName != "" {
		return s.volumeName
	}
	if src := mountSource(s.dataDir); src != "" {
		return src
	}
	return "unknown"
}

// StorageClass returns the configured storage class, or "unknown" when unset.
// A storage class cannot be detected from inside the container, so it is only
// known when the orchestrator configures it (HIVENET_ROUTER_STORAGE_CLASS).
func (s *StorageHealth) StorageClass() string {
	if s.storageClass != "" {
		return s.storageClass
	}
	return "unknown"
}

// Verify checks that the data directory is usable: it must exist and be a
// directory, and a small file must be writable there, flushed to the volume,
// read back byte-for-byte, and removed. The flush is the decisive step — a
// faulted volume still accepts writes into the page cache and fails only when
// the data is pushed out, so a check without a sync would report a broken
// volume as healthy. Returns nil when storage is accessible.
//
// The probe file is created alongside the BadgerDB files but never touches
// them, so concurrent BadgerDB I/O is unaffected.
func (s *StorageHealth) Verify() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, err := os.Stat(s.dataDir)
	if err != nil {
		return fmt.Errorf("data directory %q unavailable: %w", s.dataDir, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("data directory %q is not a directory", s.dataDir)
	}

	f, err := os.CreateTemp(s.dataDir, ".hivenet-healthcheck-")
	if err != nil {
		return fmt.Errorf("write probe: %w", err)
	}
	path := f.Name()
	if _, err := f.Write([]byte(healthCheckPayload)); err != nil {
		abortProbe(f, path)
		return fmt.Errorf("write probe: %w", err)
	}
	if err := f.Sync(); err != nil {
		abortProbe(f, path)
		return fmt.Errorf("write probe (volume flush): %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write probe: %w", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("read probe: %w", err)
	}
	if !bytes.Equal(data, []byte(healthCheckPayload)) {
		_ = os.Remove(path)
		return fmt.Errorf("read probe: read back %d bytes, want %d", len(data), len(healthCheckPayload))
	}

	// A cleanup failure is itself a storage fault — the volume cannot
	// reliably service writes.
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("write probe (cleanup): %w", err)
	}
	return nil
}

// abortProbe closes a probe file whose write or flush failed and removes it.
// Both are best-effort: the underlying storage fault is already being
// reported, and an orphan probe file is harmless (each check uses a fresh
// name, and a volume that faults on write faults on create just as soon).
func abortProbe(f *os.File, path string) {
	_ = f.Close()
	_ = os.Remove(path)
}

// mountSource returns the device serving the mount that contains dataDir, as
// parsed from /proc/mounts — e.g. "/dev/longhorn/pvc-<uuid>" for a Longhorn
// volume. It is the best volume identifier available when no explicit volume
// name is configured, and matches what operators see in Longhorn and kubectl.
// Returns "" on non-Linux systems or when no covering mount exists (the data
// directory lives on the container's root filesystem).
func mountSource(dataDir string) string {
	if runtime.GOOS != "linux" {
		return ""
	}
	content, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return ""
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return ""
	}
	best, bestLen := "", -1
	for _, line := range strings.Split(string(content), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		mountPoint := fields[1]
		if abs == mountPoint || strings.HasPrefix(abs, mountPoint+string(filepath.Separator)) {
			if len(mountPoint) > bestLen {
				best, bestLen = fields[0], len(mountPoint)
			}
		}
	}
	return best
}
