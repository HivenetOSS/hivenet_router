// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package storage_test exercises BadgerStorage end-to-end against real (temp-dir)
// BadgerDB instances: CRUD per key prefix, the (nil,nil) "not found" contract,
// disk persistence across reopen, stats counting, and the token-quota codec.
package storage_test

import (
	"testing"
	"time"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/storage"

	"github.com/libp2p/go-libp2p/core/peer"
)

// newStore opens a BadgerStorage rooted at a fresh temp dir (auto-removed) and
// registers Close as cleanup, so each test gets an isolated instance.
func newStore(t *testing.T, ttl time.Duration) *storage.BadgerStorage {
	t.Helper()
	s, err := storage.NewBadgerStorage(t.TempDir(), ttl)
	if err != nil {
		t.Fatalf("NewBadgerStorage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// fptr returns a pointer to f, for populating the optional *float64 metric fields.
func fptr(f float64) *float64 { return &f }

// TestAgentLifecycle walks a single agent through register → status update →
// unregister, plus the no-op-on-missing contract for status updates.
func TestAgentLifecycle(t *testing.T) {
	s := newStore(t, 0)
	pid := peer.ID("agent-1")
	meta := domain.AgentMetadata{Model: "qwen", Capacity: 4, Region: "eu", Engine: "vllm"}

	if err := s.RegisterAgent(pid, meta, "tok"); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}

	agents, err := s.ListAgents()
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("want 1 agent, got %d", len(agents))
	}
	// Metadata round-trips and a freshly registered agent starts healthy.
	if agents[0].Model != "qwen" || agents[0].Capacity != 4 || !agents[0].IsHealthy {
		t.Errorf("unexpected registration: %+v", agents[0])
	}

	// UpdateAgentStatus flips health and lastSeen.
	ts := time.Now().Add(-time.Minute).Truncate(time.Second)
	if err := s.UpdateAgentStatus(pid, false, ts); err != nil {
		t.Fatalf("UpdateAgentStatus: %v", err)
	}
	agents, _ = s.ListAgents()
	if agents[0].IsHealthy {
		t.Error("agent should be unhealthy after status update")
	}

	// Unregister removes it.
	if err := s.UnregisterAgent(pid); err != nil {
		t.Fatalf("UnregisterAgent: %v", err)
	}
	agents, _ = s.ListAgents()
	if len(agents) != 0 {
		t.Errorf("want 0 agents after unregister, got %d", len(agents))
	}

	// UpdateAgentStatus on a missing agent is a no-op, not an error.
	if err := s.UpdateAgentStatus(pid, true, time.Now()); err != nil {
		t.Errorf("UpdateAgentStatus on missing agent should be nil, got %v", err)
	}
}

// TestUniversalPunctual_CRUD exercises the get/set/delete cycle for the memDB
// universal-punctual prefix, including the (nil,nil) missing-key contract.
func TestUniversalPunctual_CRUD(t *testing.T) {
	s := newStore(t, 0)
	pid := peer.ID("p")

	// Missing key is not an error; it returns a nil value.
	got, err := s.GetUniversalPunctual(pid)
	if err != nil || got != nil {
		t.Fatalf("missing key must be (nil,nil), got (%v,%v)", got, err)
	}

	in := &domain.AgentUniversalPunctual{DisconnectionCount: 3, RejectedRequests: 7, CapacityUtilization: 0.5}
	if err := s.SetUniversalPunctual(pid, in); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Value survives the encode/decode round-trip.
	got, _ = s.GetUniversalPunctual(pid)
	if got == nil || got.DisconnectionCount != 3 || got.RejectedRequests != 7 {
		t.Errorf("roundtrip mismatch: %+v", got)
	}

	if err := s.DeleteUniversalPunctual(pid); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// After delete the key is gone (nil again).
	got, _ = s.GetUniversalPunctual(pid)
	if got != nil {
		t.Errorf("expected nil after delete, got %+v", got)
	}
}

// TestUniversalHistory_CRUDAndReset covers the diskDB history prefix and the
// bulk ResetUniversalHistory that clears the whole prefix.
func TestUniversalHistory_CRUDAndReset(t *testing.T) {
	s := newStore(t, 0)
	pid := peer.ID("p")

	got, err := s.GetUniversalHistory(pid)
	if err != nil || got != nil {
		t.Fatalf("missing history must be (nil,nil), got (%v,%v)", got, err)
	}

	// SRTT/RTTVAR are the warm-start values that must survive round-trips.
	in := &domain.AgentUniversalHistory{SuccessfulRequests: 100, FailedRequests: 5, SRTT: 42.5, RTTVAR: 3.2}
	if err := s.SetUniversalHistory(pid, in); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, _ = s.GetUniversalHistory(pid)
	if got == nil || got.SuccessfulRequests != 100 || got.SRTT != 42.5 {
		t.Errorf("roundtrip mismatch: %+v", got)
	}

	// ResetUniversalHistory wipes the prefix.
	if err := s.ResetUniversalHistory(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	got, _ = s.GetUniversalHistory(pid)
	if got != nil {
		t.Errorf("history should be gone after reset, got %+v", got)
	}
}

// TestHardwareAndEnginePunctual covers two memDB prefixes in one test: the
// hardware snapshot and the engine-punctual metrics (which use *float64 fields).
func TestHardwareAndEnginePunctual(t *testing.T) {
	s := newStore(t, 0)
	pid := peer.ID("p")

	// Hardware snapshot roundtrip + delete.
	if snap, err := s.GetHardwareSnapshot(pid); err != nil || snap != nil {
		t.Fatalf("missing hw must be (nil,nil), got (%v,%v)", snap, err)
	}
	if err := s.SetHardwareSnapshot(pid, &domain.HardwareSnapshot{Timestamp: 123}); err != nil {
		t.Fatalf("SetHardwareSnapshot: %v", err)
	}
	snap, _ := s.GetHardwareSnapshot(pid)
	if snap == nil || snap.Timestamp != 123 {
		t.Errorf("hw roundtrip mismatch: %+v", snap)
	}
	if err := s.DeleteHardwareSnapshot(pid); err != nil {
		t.Fatalf("DeleteHardwareSnapshot: %v", err)
	}
	// Deleting a missing snapshot is a no-op.
	if err := s.DeleteHardwareSnapshot(pid); err != nil {
		t.Errorf("delete missing hw should be nil, got %v", err)
	}

	// Engine punctual roundtrip + delete (pointer fields).
	m := &domain.BackendMetrics{KVCacheUtilization: fptr(0.8), RunningRequests: fptr(3)}
	if err := s.SetEnginePunctual(pid, m); err != nil {
		t.Fatalf("SetEnginePunctual: %v", err)
	}
	// Pointer field must survive round-trip as a non-nil, correctly-valued pointer.
	got, _ := s.GetEnginePunctual(pid)
	if got == nil || got.KVCacheUtilization == nil || *got.KVCacheUtilization != 0.8 {
		t.Errorf("engine roundtrip mismatch: %+v", got)
	}
	if err := s.DeleteEnginePunctual(pid); err != nil {
		t.Fatalf("DeleteEnginePunctual: %v", err)
	}
	if got, _ := s.GetEnginePunctual(pid); got != nil {
		t.Errorf("expected nil engine metrics after delete, got %+v", got)
	}
}

// TestTokensUsed_Codec covers the token-quota counter: missing→0, round-trip,
// and rejection of negative counts.
func TestTokensUsed_Codec(t *testing.T) {
	s := newStore(t, 0)
	key := "quotaTPD:tenant-a:19500"

	// Missing key → (0, nil).
	if n, err := s.GetTokensUsed(key); err != nil || n != 0 {
		t.Fatalf("missing token key must be (0,nil), got (%d,%v)", n, err)
	}
	if err := s.SetTokensUsed(key, 1234, time.Hour); err != nil {
		t.Fatalf("SetTokensUsed: %v", err)
	}
	// Stored count reads back exactly.
	if n, _ := s.GetTokensUsed(key); n != 1234 {
		t.Errorf("token roundtrip = %d, want 1234", n)
	}
	// Negative count is rejected.
	if err := s.SetTokensUsed(key, -1, time.Hour); err == nil {
		t.Error("negative token count must be rejected")
	}
}

// TestDiskPersistenceAcrossReopen proves diskDB survives a Close/reopen on the
// same path — the warm-start guarantee for SRTT history.
func TestDiskPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	pid := peer.ID("persist")

	// First instance writes history, then closes (flushing to disk).
	s1, err := storage.NewBadgerStorage(dir, 0)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := s1.SetUniversalHistory(pid, &domain.AgentUniversalHistory{SRTT: 99.9}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	s1.Close()

	// Second instance reopens the same dir and must see the earlier write.
	s2, err := storage.NewBadgerStorage(dir, 0)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer s2.Close()
	got, _ := s2.GetUniversalHistory(pid)
	if got == nil || got.SRTT != 99.9 {
		t.Errorf("history did not persist across reopen: %+v", got)
	}
}

// TestStats verifies per-prefix entry counts and reported TTL. Seed: 2 agents
// (metadata x2), but engine-punctual and universal-history only for a1 (x1 each).
func TestStats(t *testing.T) {
	s := newStore(t, 30*24*time.Hour)
	s.RegisterAgent(peer.ID("a1"), domain.AgentMetadata{Model: "m"}, "t")
	s.RegisterAgent(peer.ID("a2"), domain.AgentMetadata{Model: "m"}, "t")
	s.SetEnginePunctual(peer.ID("a1"), &domain.BackendMetrics{RunningRequests: fptr(1)})
	s.SetUniversalHistory(peer.ID("a1"), &domain.AgentUniversalHistory{SuccessfulRequests: 1})

	st := s.Stats()
	// Both agents registered → 2 metadata entries in memDB.
	if st.MemDB.MetadataCount != 2 {
		t.Errorf("MetadataCount = %d, want 2", st.MemDB.MetadataCount)
	}
	// Only a1 has engine metrics.
	if st.MemDB.EngPunctualCount != 1 {
		t.Errorf("EngPunctualCount = %d, want 1", st.MemDB.EngPunctualCount)
	}
	// Only a1 has universal history (on diskDB).
	if st.DiskDB.UnivHistoryCount != 1 {
		t.Errorf("UnivHistoryCount = %d, want 1", st.DiskDB.UnivHistoryCount)
	}
	// TTL is reported in days, matching the 30-day store config.
	if st.DiskDB.EntryTTLDays != 30 {
		t.Errorf("EntryTTLDays = %d, want 30", st.DiskDB.EntryTTLDays)
	}
	// GC hasn't been started in this test, so the interval reads as such.
	if st.GCInterval != "not started" {
		t.Errorf("GCInterval = %q, want 'not started'", st.GCInterval)
	}
}
