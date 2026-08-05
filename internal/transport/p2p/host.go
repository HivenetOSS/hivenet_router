// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package p2p

import (
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// LoadOrCreateIdentity loads a persistent Ed25519 private key from path.
// If the file does not exist, a new key is generated and saved to path.
// The key is stored as raw protobuf bytes (libp2p wire format) with mode 0600.
// Returns the key regardless of whether saving succeeded — the caller always
// gets a usable identity even if the filesystem is read-only.
func LoadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	if data, err := os.ReadFile(path); err == nil {
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("identity file %s is corrupt: %w", path, err)
		}
		return priv, nil
	}

	// Generate a new Ed25519 keypair.
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate identity key: %w", err)
	}

	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return priv, nil // key is usable; marshalling failure is non-fatal
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return priv, nil // key is usable; write failure is non-fatal
	}
	return priv, nil
}

// NewHost creates a new libp2p Host node.
// identity may be nil, in which case libp2p generates a fresh random key
// (different peer ID on every call — only suitable for tests).
// No announce-address override is supported: the router never dials agents —
// it opens inference streams back over the agent→router connection — so the
// listen addresses libp2p shares via identify are informational only, and no
// announce address is needed even behind NAT/Docker.
func NewHost(listenAddr string, identity crypto.PrivKey) (host.Host, error) {
	opts := []libp2p.Option{libp2p.ListenAddrStrings(listenAddr)}
	if identity != nil {
		opts = append(opts, libp2p.Identity(identity))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}
	return h, nil
}

// GetP2PAddress returns a single full multiaddress for the host including the PeerID.
// Steps:
// 1. Wrap the host's PeerID and listen addresses into a peer.AddrInfo
// 2. Convert AddrInfo to multiaddrs
// 3. Return the first address (if any)
func GetP2PAddress(h host.Host) string {
	peerInfo := peer.AddrInfo{
		ID:    h.ID(),    // The host's unique PeerID
		Addrs: h.Addrs(), // All multiaddresses the host is listening on
	}

	// Convert AddrInfo to one or more full /p2p/<peerID> addresses
	addrs, _ := peer.AddrInfoToP2pAddrs(&peerInfo)
	if len(addrs) > 0 {
		return addrs[0].String() // Return first available address
	}
	return "" // Host has no addresses
}

// GetAddressesWithPeerID returns all the host's listen addresses
// **with the PeerID appended** in the /p2p/<peerID> format.
//
// This is useful for:
// - Advertising multiple reachable addresses to remote peers
// - Ensuring peers can connect to whichever address is reachable
func GetAddressesWithPeerID(h host.Host) []string {
	peerID := h.ID().String()
	var addresses []string

	for _, addr := range h.Addrs() {
		// Append /p2p/<peerID> so remote peers know which peer to dial
		fullAddr := fmt.Sprintf("%s/p2p/%s", addr.String(), peerID)
		addresses = append(addresses, fullAddr)
	}

	return addresses
}
