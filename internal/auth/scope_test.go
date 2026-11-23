// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"strings"
	"testing"
)

// auth.yaml keys get the same scope checks as pushed keys: the router does
// not start on an invalid scope_max or a home_country outside it.
func TestStaticKeyScope(t *testing.T) {
	entry := func(max, home string) []APIKeyEntry {
		return []APIKeyEntry{{KeyHash: strings.Repeat("ab", 32), Metadata: KeyMetadata{Owner: "o", Name: "n"}, ScopeMax: max, HomeCountry: home}}
	}
	if _, err := NewStaticKeyProvider(entry("EU", "FR")); err != nil {
		t.Errorf("valid: %v", err)
	}
	for _, bad := range [][2]string{{"europe", ""}, {"EU", "US"}, {"", "France"}} {
		if _, err := NewStaticKeyProvider(entry(bad[0], bad[1])); err == nil {
			t.Errorf("scope_max %q home %q: accepted", bad[0], bad[1])
		}
	}
}

// A key's reservation must be a usable name, in auth.yaml as on push.
func TestStaticKeyReservation(t *testing.T) {
	entry := func(r string) []APIKeyEntry {
		return []APIKeyEntry{{KeyHash: strings.Repeat("ab", 32), Metadata: KeyMetadata{Owner: "o", Name: "n"}, Reservation: r}}
	}
	if _, err := NewStaticKeyProvider(entry("tenant-42")); err != nil {
		t.Errorf("valid: %v", err)
	}
	if _, err := NewStaticKeyProvider(entry("has space")); err == nil {
		t.Error("invalid reservation accepted")
	}
	reg := NewDynamicKeyProvider()
	bad := DynamicKeyEntry{ID: "k", KeyHash: strings.Repeat("ab", 32), Owner: "o", Name: "n", Enabled: true, Reservation: "a/b"}
	if err := reg.Upsert("1", bad); err == nil {
		t.Error("pushed key with an invalid reservation accepted")
	}
}
