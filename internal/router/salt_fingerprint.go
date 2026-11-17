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

// checkSaltFingerprints publishes this router's cache-salt secret fingerprint
// in the shared store and compares it with the other live routers' every
// saltFingerprintTicks sync intervals. A router started with another secret
// gives each tenant a second prefix-cache namespace: its requests through
// that router stop reusing its own cache, with no error anywhere. The count
// of such routers (each once, by its newest run: see shared.ShareValue, so
// a router in a restart loop is not counted per restart) is exported as
// hivenet_cache_salt_secret_mismatch and
// logged when it changes. It warns rather than refuses: during a rolling
// rotation of the secret, old and new routers run side by side.
func (r *Router) checkSaltFingerprints(ctx context.Context) {
	fp := r.salter.Fingerprint()
	interval := saltFingerprintTicks * r.cfg.SyncInterval
	t := time.NewTimer(0)
	defer t.Stop()
	last := 0
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
		n := 0
		for _, v := range values {
			if v != fp {
				n++
			}
		}
		r.metrics.SetCacheSaltSecretMismatch(n)
		switch {
		case n > 0 && n != last:
			log.Warnf("%d router(s) of the site use another cache-salt secret: a tenant's requests through them do not share its prefix cache. Use the same --cache-salt-secret-file on every router of the site", n)
		case n == 0 && last > 0:
			log.Infof("Every router of the site uses the same cache-salt secret again")
		}
		last = n
		t.Reset(interval)
	}
}
