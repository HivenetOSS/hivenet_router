// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"context"
	"time"
)

// saltFingerprintTicks is how many sync intervals pass between two
// comparisons of the cache-salt secret's fingerprint (30 s by default).
const saltFingerprintTicks = 30

// saltOff is what a router without the cache salt publishes in place of a
// fingerprint (a fingerprint is 16 hex characters, never this).
const saltOff = "off"

// checkSaltFingerprints publishes this router's cache-salt secret fingerprint
// (saltOff without the cache salt) in the shared store and compares it with
// the other live routers' every saltFingerprintTicks sync intervals. A router
// started with another secret gives each tenant a second prefix-cache
// namespace: its requests through that router stop reusing its own cache,
// with no error anywhere. A router without the salt is worse: its requests
// share one cache namespace and one prefix index for every tenant, and its
// prefix anchors use another key. The count of routers that differ (each
// once, by its newest run: see shared.ShareValue, so a router in a restart
// loop is not counted per restart) is exported as
// hivenet_cache_salt_secret_mismatch and logged when it changes. It warns
// rather than refuses: during a rolling rotation of the secret, old and new
// routers run side by side.
func (r *Router) checkSaltFingerprints(ctx context.Context) {
	fp := saltOff
	if r.salter != nil {
		fp = r.salter.Fingerprint()
	}
	interval := saltFingerprintTicks * r.cfg.SyncInterval
	t := time.NewTimer(0)
	defer t.Stop()
	var last saltMismatch
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		values, err := r.shared.ShareValue(ctx, "cache-salt-fingerprint", fp, 3*interval)
		if err != nil {
			// Not connected yet, or Valkey is away: try again on the next
			// sync interval rather than a whole check interval later.
			t.Reset(r.cfg.SyncInterval)
			continue
		}
		m := compareFingerprints(fp, values)
		r.metrics.SetCacheSaltSecretMismatch(m.secret + m.off + m.on)
		if m != last {
			m.log(fp == saltOff)
		}
		last = m
		t.Reset(interval)
	}
}

// saltMismatch counts the routers whose cache salt differs from this one's:
// another secret, salt off (this one on), salt on (this one off).
type saltMismatch struct{ secret, off, on int }

func compareFingerprints(fp string, values map[string]string) saltMismatch {
	var m saltMismatch
	for _, v := range values {
		switch {
		case v == fp:
		case fp == saltOff:
			m.on++
		case v == saltOff:
			m.off++
		default:
			m.secret++
		}
	}
	return m
}

func (m saltMismatch) log(thisOff bool) {
	switch {
	case m == saltMismatch{}:
		log.Infof("Every router of the site uses the same cache-salt setting again")
		return
	case thisOff && m.on > 0:
		log.Warnf("%d router(s) of the site use the cache salt and this one does not: requests through this router share every tenant's prefix cache and prefix index. Set --cache-salt (or --multi-tenant) the same way on every router of the site", m.on)
	}
	if m.off > 0 {
		log.Warnf("%d router(s) of the site run without the cache salt: requests through them share every tenant's prefix cache and prefix index. Set --cache-salt (or --multi-tenant) the same way on every router of the site", m.off)
	}
	if m.secret > 0 {
		log.Warnf("%d router(s) of the site use another cache-salt secret: a tenant's requests through them do not share its prefix cache. Use the same --cache-salt-secret-file on every router of the site", m.secret)
	}
}
