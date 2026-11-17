// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"fmt"
	"strings"
)

// Scope bounds where a request may be served: "all", "EU" or "country:XX"
// (XX an ISO 3166-1 alpha-2 code). It applies to the agents a router picks
// (by the country they declare) and to the sites a request may be
// redirected to.
type Scope string

// ScopeAll is the widest scope: anywhere.
const ScopeAll Scope = "all"

// ScopeEU is every member state of the European Union.
const ScopeEU Scope = "EU"

// ParseScope validates a scope string.
func ParseScope(s string) (Scope, error) {
	switch {
	case s == string(ScopeAll), s == string(ScopeEU):
		return Scope(s), nil
	case strings.HasPrefix(s, "country:") && ValidCountry(strings.TrimPrefix(s, "country:")):
		return Scope(s), nil
	}
	return "", fmt.Errorf("invalid scope %q: use country:XX (ISO 3166-1 alpha-2), EU or all", s)
}

// CountryScope is the scope of one country.
func CountryScope(cc string) Scope { return Scope("country:" + cc) }

// Country returns the country of a country:XX scope, "" otherwise.
func (s Scope) Country() string {
	if cc, ok := strings.CutPrefix(string(s), "country:"); ok {
		return cc
	}
	return ""
}

// AllowsCountry reports whether a place in country cc ("" = undeclared) is
// inside the scope. An undeclared country is inside "all" only; an unknown
// scope allows nothing (fail-closed).
func (s Scope) AllowsCountry(cc string) bool {
	switch {
	case s == ScopeAll:
		return true
	case cc == "":
		return false
	case s == ScopeEU:
		return IsEUCountry(cc)
	default:
		return s.Country() != "" && s.Country() == cc
	}
}

// Within reports whether s is no wider than max: every place s allows, max
// allows too. A request may narrow its key's scope, never widen it.
func (s Scope) Within(max Scope) bool {
	switch max {
	case ScopeAll:
		return s == ScopeAll || s == ScopeEU || s.Country() != ""
	case ScopeEU:
		return s == ScopeEU || (s.Country() != "" && IsEUCountry(s.Country()))
	default:
		return max.Country() != "" && s == max
	}
}

// ValidCountry reports whether cc is two upper-case ASCII letters (the form
// of an ISO 3166-1 alpha-2 code).
func ValidCountry(cc string) bool {
	return len(cc) == 2 && cc[0] >= 'A' && cc[0] <= 'Z' && cc[1] >= 'A' && cc[1] <= 'Z'
}

// euCountries are the member states of the European Union (ISO 3166-1
// alpha-2; Greece is GR).
var euCountries = map[string]struct{}{
	"AT": {}, "BE": {}, "BG": {}, "HR": {}, "CY": {}, "CZ": {}, "DK": {}, "EE": {}, "FI": {},
	"FR": {}, "DE": {}, "GR": {}, "HU": {}, "IE": {}, "IT": {}, "LV": {}, "LT": {}, "LU": {},
	"MT": {}, "NL": {}, "PL": {}, "PT": {}, "RO": {}, "SK": {}, "SI": {}, "ES": {}, "SE": {},
}

// IsEUCountry reports whether cc is a member state of the European Union.
func IsEUCountry(cc string) bool {
	_, ok := euCountries[cc]
	return ok
}
