// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"encoding/json"
	"fmt"
)

// ExternalFallback is a key's own account at an external provider (bring
// your own key): when every local policy step is exhausted, the request is
// sent to Provider's Model with the client's APIKey. A key without one gets
// no external fallback.
type ExternalFallback struct {
	Provider string `json:"provider"` // "openai" or "anthropic"
	Model    string `json:"model"`
	APIKey   Secret `json:"api_key"` // marshals to its preview only
}

// Secret holds a credential that must never be shown: it prints, formats
// and marshals as a short preview ("sk-...KJ4") or "[redacted]", and only
// Reveal returns the value, for the one call that sends it to the provider.
type Secret struct{ value string }

// NewSecret wraps a credential.
func NewSecret(v string) Secret { return Secret{value: v} }

// Reveal returns the credential. Call it only to send it where it belongs.
func (s Secret) Reveal() string { return s.value }

// Empty reports whether no credential is held.
func (s Secret) Empty() bool { return s.value == "" }

// Preview shows the first 3 and last 3 characters of a credential of at
// least 12, "***" for a shorter one, "" for none.
func (s Secret) Preview() string {
	switch {
	case s.value == "":
		return ""
	case len(s.value) < 12:
		return "***"
	}
	return s.value[:3] + "..." + s.value[len(s.value)-3:]
}

// String implements fmt.Stringer: never the value.
func (s Secret) String() string { return "[redacted]" }

// GoString implements fmt.GoStringer (%#v): never the value.
func (s Secret) GoString() string { return "domain.Secret([redacted])" }

// Format makes every fmt verb print "[redacted]", %v of a struct holding a
// Secret included.
func (s Secret) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte("[redacted]")) }

// MarshalJSON writes the preview, never the value: a key read back through
// the admin API shows which credential is set without disclosing it.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(s.Preview()) }

// MarshalText writes the preview (YAML, logs that use encoding.TextMarshaler).
func (s Secret) MarshalText() ([]byte, error) { return []byte(s.Preview()), nil }
