// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import "github.com/google/uuid"

// CanonicalRequestID returns raw in canonical UUID form (lowercase, no
// prefix or braces) and true, or "" and false when raw is not a UUID. It is
// the one rule for request IDs: the API middleware applies it to the
// client's ID and the redirect target to the IDs a token carries.
func CanonicalRequestID(raw string) (string, bool) {
	u, err := uuid.Parse(raw)
	if err != nil {
		return "", false
	}
	return u.String(), true
}
