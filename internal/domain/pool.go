// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "strings"

// A pool is the replicas of one model that serve the same clients: the
// shared pool (agents with no reserved_for, any key without a reservation)
// or one reserved pool (agents with reserved_for = R, only keys with
// reservation = R). Admission, wait queues and the healthy-replica count are
// kept per pool, so a reserved pool's load never sizes or fills the shared
// one, and the reverse.

// SharedPool is the pool name of the agents reserved for no one.
const SharedPool = ""

// poolSep separates model and pool in a pool key; it cannot appear in a
// reservation (ValidReservation) and is not printable, so a key never
// collides with a model name.
const poolSep = "\x1f"

// PoolKey is the internal key of a model's pool: the model name itself for
// the shared pool, so everything keyed by model before pools existed keeps
// its key.
func PoolKey(model, pool string) string {
	if pool == SharedPool {
		return model
	}
	return model + poolSep + pool
}

// SplitPoolKey returns the model and pool of a PoolKey.
func SplitPoolKey(key string) (model, pool string) {
	if m, p, ok := strings.Cut(key, poolSep); ok {
		return m, p
	}
	return key, SharedPool
}

// ValidReservation reports whether r is a usable reservation name: 1–64
// characters among letters, digits, '.', '_' and '-'.
func ValidReservation(r string) bool {
	if len(r) == 0 || len(r) > 64 {
		return false
	}
	for _, c := range r {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}
