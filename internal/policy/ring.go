// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"encoding/binary"
	"slices"
	"strconv"
	"sync"

	"hivenet_router/internal/domain"

	"github.com/cespare/xxhash/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

// hashRing is a consistent-hash ring over the agents of one model: every agent
// takes vnodes positions, xxhash(peerID ‖ i). Positions depend only on the
// peer IDs, so every router that serves the same agents builds the same ring.
// Immutable once built.
type hashRing struct {
	fingerprint uint64 // of the agent set it was built from (poolFingerprint)
	agents      int    // distinct agents on the ring
	points      []uint64
	owners      []peer.ID // owners[i] holds points[i]
}

// ringKey identifies a ring: one per model and number of virtual nodes.
type ringKey struct {
	model  string
	vnodes int
}

// ringCache holds the latest ring of each model. A ring is rebuilt only when
// the model's agent set changes (an agent joins or leaves), never when an
// agent's health or load changes: those agents are skipped during the walk.
type ringCache struct {
	m sync.Map // ringKey → *hashRing
}

// get returns the ring of model over pool, rebuilding it when pool differs from
// the agents the cached ring was built from. Concurrent rebuilds are harmless:
// they build the same ring.
func (c *ringCache) get(model string, vnodes int, pool []*domain.Agent) *hashRing {
	fp := poolFingerprint(pool)
	k := ringKey{model: model, vnodes: vnodes}
	if v, ok := c.m.Load(k); ok {
		if r := v.(*hashRing); r.fingerprint == fp && r.agents == len(pool) {
			return r
		}
	}
	r := buildRing(pool, vnodes)
	r.fingerprint = fp
	c.m.Store(k, r)
	return r
}

// poolFingerprint hashes the set of peer IDs, independent of their order.
func poolFingerprint(pool []*domain.Agent) uint64 {
	var sum, xor uint64
	for _, a := range pool {
		h := xxhash.Sum64String(string(a.ID))
		sum += h
		xor ^= h
	}
	return sum ^ (xor << 1)
}

func buildRing(pool []*domain.Agent, vnodes int) *hashRing {
	type point struct {
		pos   uint64
		owner peer.ID
	}
	pts := make([]point, 0, len(pool)*vnodes)
	seen := make(map[peer.ID]struct{}, len(pool))
	buf := make([]byte, 0, 64)
	for _, a := range pool {
		if _, dup := seen[a.ID]; dup {
			continue
		}
		seen[a.ID] = struct{}{}
		for i := range vnodes {
			buf = append(buf[:0], a.ID...)
			buf = append(buf, '#')
			buf = strconv.AppendInt(buf, int64(i), 10)
			pts = append(pts, point{pos: xxhash.Sum64(buf), owner: a.ID})
		}
	}
	// Ties (vanishingly rare) are broken by peer ID so every router agrees.
	slices.SortFunc(pts, func(a, b point) int {
		if a.pos != b.pos {
			if a.pos < b.pos {
				return -1
			}
			return 1
		}
		switch {
		case a.owner < b.owner:
			return -1
		case a.owner > b.owner:
			return 1
		}
		return 0
	})
	r := &hashRing{agents: len(seen), points: make([]uint64, len(pts)), owners: make([]peer.ID, len(pts))}
	for i, p := range pts {
		r.points[i], r.owners[i] = p.pos, p.owner
	}
	return r
}

// keyPoint maps a routing key (a SHA-256 digest, uniform) onto the ring.
func keyPoint(key []byte) uint64 {
	if len(key) >= 8 {
		return binary.BigEndian.Uint64(key)
	}
	return xxhash.Sum64(key)
}

// walk visits each agent once, clockwise from point, until visit returns true.
func (r *hashRing) walk(point uint64, visit func(peer.ID) bool) {
	n := len(r.points)
	if n == 0 {
		return
	}
	start, _ := slices.BinarySearch(r.points, point)
	seen := make(map[peer.ID]struct{}, r.agents)
	for i := range n {
		id := r.owners[(start+i)%n]
		if _, done := seen[id]; done {
			continue
		}
		seen[id] = struct{}{}
		if visit(id) || len(seen) == r.agents {
			return
		}
	}
}
