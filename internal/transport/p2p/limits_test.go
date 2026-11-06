// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package p2p

import (
	"testing"

	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

// Stream counts are lifted at every scope; connection limits are kept.
func TestUnlimitedStreamsKeepsConnLimits(t *testing.T) {
	limits := rcmgr.DefaultLimits
	base := limits.AutoScale()
	got := UnlimitedStreams(base).ToPartialLimitConfig()
	for name, l := range map[string]rcmgr.ResourceLimits{
		"system": got.System, "protocol": got.ProtocolDefault,
		"protocol-peer": got.ProtocolPeerDefault, "peer": got.PeerDefault,
	} {
		if l.Streams != rcmgr.Unlimited || l.StreamsInbound != rcmgr.Unlimited || l.StreamsOutbound != rcmgr.Unlimited {
			t.Errorf("%s streams still limited: %+v", name, l)
		}
	}
	if got.System.Conns != base.ToPartialLimitConfig().System.Conns {
		t.Errorf("system connection limit changed: %v → %v", base.ToPartialLimitConfig().System.Conns, got.System.Conns)
	}
}
