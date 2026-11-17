// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Identities the router assigns itself. A key can never claim one (see
// validateOwner and validateKeyID), so a key's counters, cache salt, metric
// labels and audit lines can never be confused with these callers'.
const (
	// NoAuthTenantID is the tenant of every request in auth mode none.
	NoAuthTenantID = "default"
	// AdminTenantID is the tenant of every request made with an admin key.
	AdminTenantID = "admin"
	// AnonymousID names a caller with no tenant or key ID (auth mode none)
	// in metric labels and log lines.
	AnonymousID = "anonymous"

	// staticKeyIDPrefix and adminKeyIDPrefix start the key IDs the router
	// derives for static auth.yaml keys and for admin keys (derivedKeyID).
	staticKeyIDPrefix = "static-"
	adminKeyIDPrefix  = "admin-"
)

// reservedTenantIDs are the tenants the router assigns itself.
var reservedTenantIDs = []string{NoAuthTenantID, AdminTenantID, AnonymousID}

// validateOwner rejects an owner (tenant) that is not a safe identifier or
// that names a tenant the router assigns itself. Compared case-insensitively:
// "Admin" would read as the admin tenant in a log or on a dashboard.
func validateOwner(label, field, s string) error {
	if err := validateIdentifier(label, field, s); err != nil {
		return err
	}
	for _, r := range reservedTenantIDs {
		if strings.EqualFold(s, r) {
			return fmt.Errorf("auth: %s: %s %q is reserved for the router's own callers", label, field, s)
		}
	}
	return nil
}

// validateKeyID rejects a dynamic key ID that is not a safe identifier, that
// is the no-auth ID, or that starts like an ID the router derives itself
// ("static-", "admin-"): in a metric label or an audit line it would read as
// a static key, an admin key or an anonymous caller. Compared with Unicode
// case folding, as for owners: "ſtatic-x" (long s) reads as "static-x".
func validateKeyID(label, field, s string) error {
	if err := validateIdentifier(label, field, s); err != nil {
		return err
	}
	if strings.EqualFold(s, AnonymousID) {
		return fmt.Errorf("auth: %s: %s %q is reserved for the router's own callers", label, field, s)
	}
	for _, p := range []string{staticKeyIDPrefix, adminKeyIDPrefix} {
		if hasFoldPrefix(s, p) {
			return fmt.Errorf("auth: %s: %s %q starts with %q, which the router reserves for the IDs it derives", label, field, s, p)
		}
	}
	return nil
}

// hasFoldPrefix reports whether s starts with prefix under Unicode case
// folding (strings.EqualFold), comparing as many runes as prefix has: a
// folded rune can be longer in bytes than the ASCII letter it folds to.
func hasFoldPrefix(s, prefix string) bool {
	n := utf8.RuneCountInString(prefix)
	i := 0
	for j := range s {
		if n == 0 {
			break
		}
		n--
		_, size := utf8.DecodeRuneInString(s[j:])
		i = j + size
	}
	return n == 0 && strings.EqualFold(s[:i], prefix)
}

// validateIdentifier rejects a key ID or owner (tenant) that could alias
// another key's state or pass for another identifier in a log line or on a
// dashboard. Rate-limit buckets join identifiers with NUL (see bucketKey), so
// an owner or ID containing NUL could address another key's counters. The
// rest would make two identifiers look the same, or change how a log line
// reads:
//   - control characters (C0 and C1, NEL included);
//   - every character Unicode says renders as nothing (Default_Ignorable_Code_Point:
//     format characters such as bidirectional overrides, zero-width
//     characters and the byte-order mark, variation selectors, tag
//     characters, the Hangul fillers and the combining grapheme joiner);
//   - line and paragraph separators;
//   - whitespace other than the plain space U+0020 (a no-break or
//     ideographic space reads as a space), and whitespace at either end
//     ("admin " reads as "admin").
//
// Plain spaces inside and every other printable character are allowed.
func validateIdentifier(label, field, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("auth: %s: %s must be valid UTF-8", label, field)
	}
	for _, r := range s {
		switch {
		case unicode.In(r, unicode.Cc, unicode.Zl, unicode.Zp):
			return fmt.Errorf("auth: %s: %s must not contain control or line-separator characters (found %U)", label, field, r)
		case isInvisible(r):
			return fmt.Errorf("auth: %s: %s must not contain invisible characters (found %U)", label, field, r)
		case r != ' ' && unicode.Is(unicode.White_Space, r):
			return fmt.Errorf("auth: %s: %s must not contain whitespace other than a plain space (found %U)", label, field, r)
		}
	}
	if strings.TrimSpace(s) != s {
		return fmt.Errorf("auth: %s: %s must not start or end with a space", label, field)
	}
	return nil
}

// isInvisible reports whether r is a Unicode Default_Ignorable_Code_Point,
// which renderers display as nothing: format characters (Cf), variation
// selectors, and the other default-ignorable code points (Hangul fillers,
// combining grapheme joiner, tag and reserved ranges).
func isInvisible(r rune) bool {
	return unicode.In(r, unicode.Cf, unicode.Variation_Selector, unicode.Other_Default_Ignorable_Code_Point)
}

// normalizeKeyHash returns a key hash in the form keys are looked up by:
// lowercase hex. It must be a SHA-256 digest, 64 hex characters in either
// case; anything else could never match a presented key, so it is refused
// when the key is loaded rather than answered with 401 forever.
func normalizeKeyHash(label, h string) (string, error) {
	if h == "" {
		return "", fmt.Errorf("auth: %s: key_hash must not be empty", label)
	}
	h = strings.ToLower(h)
	if len(h) != 64 {
		return "", fmt.Errorf("auth: %s: key_hash must be 64 hex characters (SHA-256), got %d", label, len(h))
	}
	for _, c := range h {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("auth: %s: key_hash contains non-hex character %q", label, c)
		}
	}
	return h, nil
}
