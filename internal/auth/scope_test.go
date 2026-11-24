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

func TestExternalFallbackValidation(t *testing.T) {
	ok := &ExternalFallback{Provider: "openai", Model: "gpt", APIKey: NewSecret("sk-0123456789abc")}
	if err := validateExternalFallback("k", ok, ""); err != nil {
		t.Errorf("valid: %v", err)
	}
	if err := validateExternalFallback("k", ok, "all"); err != nil {
		t.Errorf("scope all: %v", err)
	}
	bad := []struct {
		fb    *ExternalFallback
		scope string
	}{
		{&ExternalFallback{Provider: "mistral", Model: "m", APIKey: NewSecret("x")}, ""},
		{&ExternalFallback{Provider: "openai", APIKey: NewSecret("x")}, ""},
		{&ExternalFallback{Provider: "openai", Model: "m"}, ""},
		{ok, "EU"},
		{ok, "country:FR"},
	}
	for _, b := range bad {
		if err := validateExternalFallback("k", b.fb, b.scope); err == nil {
			t.Errorf("%+v scope %q accepted", b.fb, b.scope)
		}
	}
}

// auth.yaml reads the client's provider key from the environment, never
// from the file.
func TestStaticExternalFallback(t *testing.T) {
	entry := func(env string) []APIKeyEntry {
		return []APIKeyEntry{{KeyHash: strings.Repeat("ab", 32), Metadata: KeyMetadata{Owner: "o", Name: "n"},
			ExternalFallback: &StaticExternalFallback{Provider: "openai", Model: "gpt", APIKeyEnv: env}}}
	}
	t.Setenv("ACME_OPENAI_KEY", "sk-acme-0123456789")
	if _, err := NewStaticKeyProvider(entry("ACME_OPENAI_KEY")); err != nil {
		t.Errorf("valid: %v", err)
	}
	if _, err := NewStaticKeyProvider(entry("UNSET_VARIABLE_FOR_TEST")); err == nil {
		t.Error("unset variable accepted")
	}
	if _, err := NewStaticKeyProvider(entry("")); err == nil {
		t.Error("missing api_key_env accepted")
	}
}
