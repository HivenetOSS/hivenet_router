// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import "net/http"

// NoAuthProvider allows all requests through with a fixed "default" tenant ID.
// Intended for local development and private networks only.
// A warning is logged at wiring time by ProvidersFromConfig so it appears
// in the startup banner; no per-request logging is done here.
type NoAuthProvider struct{}

// NewNoAuthProvider creates a NoAuthProvider.
func NewNoAuthProvider() *NoAuthProvider {
	return &NoAuthProvider{}
}

// Authenticate always succeeds and returns TenantID "default" with no quota limits.
func (n *NoAuthProvider) Authenticate(_ *http.Request) (*AuthResult, error) {
	return &AuthResult{TenantID: "default"}, nil
}
