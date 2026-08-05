// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package router

import (
	"strings"
	"sync"
	"time"

	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

// connLimitLogThrottle is the minimum interval between per-IP rejection warnings
// for the same source IP. Agents retry their dial every ~5s, so without this a
// single locked-out agent would emit a warning every few seconds.
const connLimitLogThrottle = time.Minute

// loggingResourceManager wraps a libp2p ResourceManager and surfaces an
// actionable WARN whenever an inbound connection is rejected by the per-source-IP
// connection limiter.
//
// This rejection is otherwise invisible: go-libp2p's connLimiter returns an error
// ("connections per ip limit exceeded for <addr>") but, unlike the scope-based
// limits, does NOT log it, emit a trace event, or increment any metric. The
// dialing agent only sees its Noise handshake reset ("failed to negotiate
// security protocol: EOF"), so without this wrapper the router gives the operator
// no signal at all that the fleet has outgrown --p2p-max-conns-per-ip.
type loggingResourceManager struct {
	libp2pnet.ResourceManager
	maxConnsPerIP int

	mu         sync.Mutex
	lastLogged map[string]time.Time // source IP -> last warning time (throttle)
}

// newLoggingResourceManager wraps rm so that per-source-IP connection rejections
// are logged at WARN, throttled per IP.
func newLoggingResourceManager(rm libp2pnet.ResourceManager, maxConnsPerIP int) *loggingResourceManager {
	return &loggingResourceManager{
		ResourceManager: rm,
		maxConnsPerIP:   maxConnsPerIP,
		lastLogged:      make(map[string]time.Time),
	}
}

func (m *loggingResourceManager) OpenConnection(dir libp2pnet.Direction, usefd bool, endpoint multiaddr.Multiaddr) (libp2pnet.ConnManagementScope, error) {
	scope, err := m.ResourceManager.OpenConnection(dir, usefd, endpoint)
	// The connLimiter wraps the source address in its error string; match on the
	// stable prefix rather than the formatted suffix.
	if err != nil && strings.Contains(err.Error(), "connections per ip limit exceeded") {
		ip := ipFromMultiaddr(endpoint)
		if m.shouldLog(ip) {
			log.Warnf("libp2p: rejected inbound connection from %s — per-source-IP "+
				"connection limit (%d) reached. If the agent fleet grew, raise "+
				"--p2p-max-conns-per-ip (keep it >= 2x the number of agents behind "+
				"this egress IP).", endpoint, m.maxConnsPerIP)
		}
	}
	return scope, err
}

// shouldLog reports whether a warning for ip is due, and records the time if so.
func (m *loggingResourceManager) shouldLog(ip string) bool {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if last, ok := m.lastLogged[ip]; ok && now.Sub(last) < connLimitLogThrottle {
		return false
	}
	m.lastLogged[ip] = now
	return true
}

// ipFromMultiaddr extracts the IPv4/IPv6 component of a multiaddr for throttling
// and log readability, falling back to the full multiaddr string.
func ipFromMultiaddr(m multiaddr.Multiaddr) string {
	if v, err := m.ValueForProtocol(multiaddr.P_IP4); err == nil {
		return v
	}
	if v, err := m.ValueForProtocol(multiaddr.P_IP6); err == nil {
		return v
	}
	return m.String()
}
