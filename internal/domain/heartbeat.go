// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

// HeartbeatReply is the router's answer to an agent's heartbeat. Routers lists
// the site's live routers that advertise the address agents reach them at;
// older agents ignore the body. Shared by both sides so the JSON names cannot
// drift apart.
type HeartbeatReply struct {
	Routers []SiteRouter `json:"routers,omitempty"`
}

// SiteRouter is one router of the site in a HeartbeatReply.
type SiteRouter struct {
	GRPCAddr string `json:"grpc_addr"`
	PeerID   string `json:"peer_id"`
}
