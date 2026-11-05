// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"fmt"
	"unicode/utf8"
)

// validateIdentifier rejects a key ID or owner (tenant) that could alias
// another key's state or corrupt logs. Rate-limit buckets join identifiers
// with NUL (see bucketKey), so an owner or ID containing NUL could address
// another key's counters. Control characters and DEL would also break log
// lines and metric labels. Spaces and any printable Unicode are allowed.
func validateIdentifier(label, field, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("auth: %s: %s must be valid UTF-8", label, field)
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("auth: %s: %s must not contain control characters (found %U)", label, field, r)
		}
	}
	return nil
}
