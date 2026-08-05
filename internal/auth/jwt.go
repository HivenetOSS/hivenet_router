// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// issuerName is the expected JWT issuer for all tokens in this system.
// CreateToken and ValidateToken must reference this constant to stay in sync.
const issuerName = "hivenet-router"

// JWTValidator handles JWT token validation using HMAC-SHA256 signature verification.
type JWTValidator struct {
	secretKey []byte
}

// NewJWTValidator creates a new JWT validator that verifies tokens signed with the
// provided HMAC-SHA256 secret.
func NewJWTValidator(secretKey []byte) *JWTValidator {
	return &JWTValidator{secretKey: secretKey}
}

// ValidateToken validates a JWT token signature and standard claims (exp, iat, iss),
// returning the subject (agent ID) on success.
func (v *JWTValidator) ValidateToken(tokenString string) (string, error) {
	token, err := jwt.Parse(
		[]byte(tokenString),
		jwt.WithKey(jwa.HS256(), v.secretKey),
		jwt.WithValidate(true),
		jwt.WithIssuer(issuerName),
	)
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}
	sub, _ := token.Subject()
	if sub == "" {
		return "", fmt.Errorf("token missing required 'sub' claim")
	}
	return sub, nil
}

// CreateToken generates an HMAC-SHA256-signed JWT for the given subject with the
// specified TTL. The token carries only standard claims (sub, iss, iat, exp).
// Agent capability metadata (model, capacity, engine, etc.) is transmitted
// separately via the gRPC request body and is agent-asserted, not router-enforced.
func CreateToken(subject string, ttl time.Duration, secretKey []byte) (string, error) {
	b := jwt.NewBuilder().
		Subject(subject).
		Issuer(issuerName).
		IssuedAt(time.Now()).
		Expiration(time.Now().Add(ttl))

	token, err := b.Build()
	if err != nil {
		return "", fmt.Errorf("failed to build JWT: %w", err)
	}

	signed, err := jwt.Sign(token, jwt.WithKey(jwa.HS256(), secretKey))
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT: %w", err)
	}
	return string(signed), nil
}
