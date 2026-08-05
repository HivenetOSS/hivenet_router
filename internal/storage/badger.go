// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"hivenet_router/internal/domain"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Key-schema constants shared across all storage tickets.
// Every key in BadgerDB is prefixed by one of these to partition the namespace.
//
//	memDB  (in-memory):  metadata:, universalPunctual:, enginePunctual:, hardwareSnapshot:
//	diskDB (persistent): universalHistory:
const (
	PrefixMetadata         = "metadata:"          // agent identity snapshot       — Ticket 1
	PrefixUnivPunctual     = "universalPunctual:" // session-scoped counters        — Ticket 2a (memDB)
	PrefixUnivHistory      = "universalHistory:"  // lifetime counters + RTT        — Ticket 2b (diskDB)
	PrefixEngPunctual      = "enginePunctual:"    // engine real-time snapshots    — Ticket 3
	PrefixHardwareSnapshot = "hardwareSnapshot:"  // latest GPU/CPU/memory reading — HAI-65 (memDB)
	PrefixQuotaTPD         = "quotaTPD:"          // per-tenant daily token counter — HAI-109 (diskDB)
)

// BadgerStorage implements RoutingStorage using two BadgerDB instances:
//   - memDB:  WithInMemory(true), holds metadata / universal / enginePunctual keys.
//     Lost on router restart — acceptable, agents re-register.
//   - diskDB: persistent on disk, holds universalHistory keys.
//     Survives restarts so SRTT history is never cold-started.
type BadgerStorage struct {
	memDB  *badger.DB
	diskDB *badger.DB

	// TTL applied to every diskDB write. Zero means no expiry.
	diskTTL time.Duration

	// GC tracking — protected by gcMu.
	gcMu       sync.Mutex
	lastGCAt   time.Time
	gcInterval time.Duration
}

// BadgerStats is returned by Stats() and exposed via GET /admin/storage.
type BadgerStats struct {
	MemDB  MemDBStats  `json:"mem_db"`
	DiskDB DiskDBStats `json:"disk_db"`

	GCInterval string `json:"gc_interval"` // e.g. "5s", or "not started"
	LastGCAt   string `json:"last_gc_at"`  // RFC3339, or "never"
}

// MemDBStats holds key counts for the in-memory BadgerDB.
type MemDBStats struct {
	MetadataCount         int64 `json:"metadata_count"`
	UnivPunctualCount     int64 `json:"univ_punctual_count"`
	EngPunctualCount      int64 `json:"eng_punctual_count"`
	HardwareSnapshotCount int64 `json:"hardware_snapshot_count"`
}

// DiskDBStats holds key counts and size for the persistent BadgerDB.
type DiskDBStats struct {
	UnivHistoryCount int64 `json:"univ_history_count"`
	LSMSizeBytes     int64 `json:"lsm_size_bytes"`
	VLogSizeBytes    int64 `json:"vlog_size_bytes"`
	EntryTTLDays     int   `json:"entry_ttl_days"` // 0 means no expiry
}

// NewBadgerStorage opens both BadgerDB instances.
// diskDBPath is the directory for the persistent (universalHistory) database.
// diskTTL is applied to every diskDB write; pass 0 to disable expiry.
func NewBadgerStorage(diskDBPath string, diskTTL time.Duration) (*BadgerStorage, error) {
	memOpts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	memDB, err := badger.Open(memOpts)
	if err != nil {
		return nil, fmt.Errorf("open in-memory BadgerDB: %w", err)
	}

	diskOpts := badger.DefaultOptions(diskDBPath).WithLogger(nil)
	diskDB, err := badger.Open(diskOpts)
	if err != nil {
		memDB.Close() //nolint:errcheck
		return nil, fmt.Errorf("open disk BadgerDB at %s: %w", diskDBPath, err)
	}

	return &BadgerStorage{memDB: memDB, diskDB: diskDB, diskTTL: diskTTL}, nil
}

// SetDiskEntry writes a key-value pair to diskDB, applying the configured TTL when non-zero.
// All Ticket 4 writes to diskDB should go through this helper.
func (s *BadgerStorage) SetDiskEntry(key, val []byte) error {
	return s.diskDB.Update(func(txn *badger.Txn) error {
		if s.diskTTL > 0 {
			return txn.SetEntry(badger.NewEntry(key, val).WithTTL(s.diskTTL))
		}
		return txn.Set(key, val)
	})
}

// RegisterAgent writes an AgentRegistration under metadata:{peerID} in memDB.
func (s *BadgerStorage) RegisterAgent(peerID peer.ID, metadata domain.AgentMetadata, sessionToken string) error {
	reg := &domain.AgentRegistration{
		PeerID:    peerID.String(),
		Model:     metadata.Model,
		Capacity:  metadata.Capacity,
		Region:    metadata.Region,
		IsHealthy: true,
		LastSeen:  time.Now(),
		Metadata:  metadata,
	}

	val, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("marshal agent registration: %w", err)
	}

	key := []byte(PrefixMetadata + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

// UnregisterAgent deletes the metadata:{peerID} entry from memDB.
func (s *BadgerStorage) UnregisterAgent(peerID peer.ID) error {
	key := []byte(PrefixMetadata + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// UpdateAgentStatus updates IsHealthy and LastSeen for a registered agent in memDB.
// It performs a read-modify-write inside a single BadgerDB transaction.
// Returns nil if the agent is not found (already removed — acceptable race condition).
func (s *BadgerStorage) UpdateAgentStatus(peerID peer.ID, isHealthy bool, lastSeen time.Time) error {
	key := []byte(PrefixMetadata + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil // already removed, no-op
		}
		if err != nil {
			return fmt.Errorf("get agent: %w", err)
		}

		var reg domain.AgentRegistration
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &reg)
		}); err != nil {
			return fmt.Errorf("unmarshal agent: %w", err)
		}

		reg.IsHealthy = isHealthy
		reg.LastSeen = lastSeen

		val, err := json.Marshal(reg)
		if err != nil {
			return fmt.Errorf("marshal agent: %w", err)
		}
		return txn.Set(key, val)
	})
}

// ListAgents scans the metadata: prefix in memDB and returns all registered agents.
// Only currently connected agents appear here; diskDB (engineHistory) is not consulted.
func (s *BadgerStorage) ListAgents() ([]*domain.AgentRegistration, error) {
	var agents []*domain.AgentRegistration
	prefix := []byte(PrefixMetadata)

	err := s.memDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var reg domain.AgentRegistration
			if err := it.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &reg)
			}); err != nil {
				return fmt.Errorf("unmarshal agent at key %s: %w", it.Item().Key(), err)
			}
			agents = append(agents, &reg)
		}
		return nil
	})

	return agents, err
}

// GetUniversalPunctual reads the session-scoped counters for peerID from memDB.
// Returns (nil, nil) if no entry exists yet.
func (s *BadgerStorage) GetUniversalPunctual(peerID peer.ID) (*domain.AgentUniversalPunctual, error) {
	key := []byte(PrefixUnivPunctual + peerID.String())
	var p domain.AgentUniversalPunctual
	err := s.memDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &p)
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return &p, err
}

// SetUniversalPunctual writes session-scoped counters for peerID to memDB.
func (s *BadgerStorage) SetUniversalPunctual(peerID peer.ID, p *domain.AgentUniversalPunctual) error {
	val, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal universalPunctual: %w", err)
	}
	key := []byte(PrefixUnivPunctual + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

// DeleteUniversalPunctual removes the session-scoped counters for peerID from memDB.
// Called after flushing to history on agent disconnect.
func (s *BadgerStorage) DeleteUniversalPunctual(peerID peer.ID) error {
	key := []byte(PrefixUnivPunctual + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// GetUniversalHistory reads lifetime counters for peerID from diskDB.
// Returns (nil, nil) if no entry exists yet (first time this agent is seen).
func (s *BadgerStorage) GetUniversalHistory(peerID peer.ID) (*domain.AgentUniversalHistory, error) {
	key := []byte(PrefixUnivHistory + peerID.String())
	var h domain.AgentUniversalHistory
	err := s.diskDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &h)
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return &h, err
}

// SetUniversalHistory writes lifetime counters for peerID to diskDB.
// Uses SetDiskEntry so the configured TTL is applied automatically.
func (s *BadgerStorage) SetUniversalHistory(peerID peer.ID, h *domain.AgentUniversalHistory) error {
	val, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("marshal universalHistory: %w", err)
	}
	key := []byte(PrefixUnivHistory + peerID.String())
	return s.SetDiskEntry(key, val)
}

// GetHardwareSnapshot reads the latest hardware reading for peerID from memDB.
// Returns (nil, nil) if no entry exists yet.
func (s *BadgerStorage) GetHardwareSnapshot(peerID peer.ID) (*domain.HardwareSnapshot, error) {
	key := []byte(PrefixHardwareSnapshot + peerID.String())
	var snap domain.HardwareSnapshot
	err := s.memDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &snap)
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return &snap, err
}

// SetHardwareSnapshot writes the latest hardware reading for peerID to memDB.
// Called on every heartbeat that carries a non-nil snapshot.
func (s *BadgerStorage) SetHardwareSnapshot(peerID peer.ID, snap *domain.HardwareSnapshot) error {
	val, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal hardwareSnapshot: %w", err)
	}
	key := []byte(PrefixHardwareSnapshot + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

// DeleteHardwareSnapshot removes the hardware snapshot for peerID from memDB.
// Called when the agent disconnects and is removed from the routing table.
func (s *BadgerStorage) DeleteHardwareSnapshot(peerID peer.ID) error {
	key := []byte(PrefixHardwareSnapshot + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// GetEnginePunctual reads the latest engine backend metrics for peerID from memDB.
// Returns (nil, nil) if no entry exists yet.
func (s *BadgerStorage) GetEnginePunctual(peerID peer.ID) (*domain.BackendMetrics, error) {
	key := []byte(PrefixEngPunctual + peerID.String())
	var m domain.BackendMetrics
	err := s.memDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &m)
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return &m, err
}

// SetEnginePunctual writes the latest engine backend metrics for peerID to memDB.
// Called on every heartbeat that carries a non-nil BackendMetrics snapshot.
func (s *BadgerStorage) SetEnginePunctual(peerID peer.ID, m *domain.BackendMetrics) error {
	val, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal enginePunctual: %w", err)
	}
	key := []byte(PrefixEngPunctual + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

// DeleteEnginePunctual removes the engine backend metrics for peerID from memDB.
// Called when the agent disconnects and is removed from the routing table.
func (s *BadgerStorage) DeleteEnginePunctual(peerID peer.ID) error {
	key := []byte(PrefixEngPunctual + peerID.String())
	return s.memDB.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// GetTokensUsed reads the daily token count for the given key from diskDB.
// Returns (0, nil) if the key does not exist yet (first use of the day).
// Key format: "quotaTPD:{tenantID}:{dayIndex}"
func (s *BadgerStorage) GetTokensUsed(key string) (int64, error) {
	var count int64
	err := s.diskDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid token count value: expected 8 bytes, got %d", len(val))
			}
			count = int64(binary.BigEndian.Uint64(val))
			return nil
		})
	})
	return count, err
}

// SetTokensUsed writes the daily token count for the given key to diskDB with
// the specified TTL. A 48 h TTL is recommended so a counter written late at
// night remains readable after midnight if the router restarts.
// Key format: "quotaTPD:{tenantID}:{dayIndex}"
func (s *BadgerStorage) SetTokensUsed(key string, count int64, ttl time.Duration) error {
	if count < 0 {
		return fmt.Errorf("SetTokensUsed: count must be non-negative, got %d", count)
	}
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(count))
	return s.diskDB.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), val)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// ResetUniversalHistory deletes every universalHistory: key from diskDB, clearing
// all persisted lifetime per-agent counters. DropPrefix is an efficient bulk delete.
// Only the universalHistory: prefix is removed — quotaTPD: (billing) and all memDB
// keys are left intact.
func (s *BadgerStorage) ResetUniversalHistory() error {
	return s.diskDB.DropPrefix([]byte(PrefixUnivHistory))
}

// Close shuts down both BadgerDB instances. Both are attempted even if the first fails.
func (s *BadgerStorage) Close() error {
	return errors.Join(s.memDB.Close(), s.diskDB.Close())
}

// StartGC starts a background goroutine that runs BadgerDB value-log GC on diskDB
// at the given interval. Should be called once after the router starts.
// The goroutine exits cleanly when ctx is cancelled (router shutdown) or when
// diskDB is closed — whichever comes first.
func (s *BadgerStorage) StartGC(ctx context.Context, interval time.Duration) {
	s.gcMu.Lock()
	s.gcInterval = interval
	s.gcMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := s.diskDB.RunValueLogGC(0.5)
				if err == badger.ErrDBClosed {
					return // DB shut down — exit cleanly
				}
				// ErrNoRewrite means nothing to reclaim — not an error worth logging
				s.gcMu.Lock()
				s.lastGCAt = time.Now()
				s.gcMu.Unlock()
			}
		}
	}()
}

// Stats returns a snapshot of key counts and disk usage for both DB instances.
// Used by GET /admin/storage.
func (s *BadgerStorage) Stats() BadgerStats {
	var st BadgerStats

	st.MemDB.MetadataCount, _ = s.countKeys(s.memDB, PrefixMetadata)
	st.MemDB.UnivPunctualCount, _ = s.countKeys(s.memDB, PrefixUnivPunctual)
	st.MemDB.EngPunctualCount, _ = s.countKeys(s.memDB, PrefixEngPunctual)
	st.MemDB.HardwareSnapshotCount, _ = s.countKeys(s.memDB, PrefixHardwareSnapshot)

	st.DiskDB.UnivHistoryCount, _ = s.countKeys(s.diskDB, PrefixUnivHistory)
	lsm, vlog := s.diskDB.Size()
	st.DiskDB.LSMSizeBytes = lsm
	st.DiskDB.VLogSizeBytes = vlog
	if s.diskTTL > 0 {
		// Convert TTL from duration to days using integer division.
		// Any fractional days are truncated (e.g. 29.5d -> 29d); this matches the
		// current configuration, which only allows integer day values.
		st.DiskDB.EntryTTLDays = int(s.diskTTL / (24 * time.Hour))
	}

	s.gcMu.Lock()
	lastGC := s.lastGCAt
	interval := s.gcInterval
	s.gcMu.Unlock()

	if lastGC.IsZero() {
		st.LastGCAt = "never"
	} else {
		st.LastGCAt = lastGC.UTC().Format(time.RFC3339)
	}
	if interval == 0 {
		st.GCInterval = "not started"
	} else {
		st.GCInterval = interval.String()
	}

	return st
}

// countKeys performs a keys-only prefix scan and returns the count.
func (s *BadgerStorage) countKeys(db *badger.DB, prefix string) (int64, error) {
	var count int64
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // keys-only — much faster
		it := txn.NewIterator(opts)
		defer it.Close()

		p := []byte(prefix)
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}
