// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package config

import (
	"fmt"
	"net"
	"strconv"
)

// ValidAdvertiseAddr checks the address agents dial to reach this router: a
// host:port with a real host (not empty, not 0.0.0.0 or ::, which would make
// every agent dial itself) and a numeric port.
func ValidAdvertiseAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("must be host:port, got %q", addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port must be a number from 1 to 65535, got %q", addr)
	}
	if host == "" {
		return fmt.Errorf("needs the host agents reach this router at, got %q", addr)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("%s is a listen address, not one agents can dial: give the router's reachable host, got %q", host, addr)
	}
	return nil
}
