// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"fmt"

	"hivenet_router/internal/domain"
)

// validateKeyScope checks a key's scope_max ("" = all, else all, EU or
// country:XX) and home_country ("" or an ISO 3166-1 alpha-2 code), and that
// the home country lies inside scope_max: it is the scope of a request that
// asks for none, so it must be one the key may use.
func validateKeyScope(label, scopeMax, homeCountry string) error {
	max := domain.ScopeAll
	if scopeMax != "" {
		s, err := domain.ParseScope(scopeMax)
		if err != nil {
			return fmt.Errorf("auth: %s: scope_max: %w", label, err)
		}
		max = s
	}
	if homeCountry == "" {
		return nil
	}
	if !domain.ValidCountry(homeCountry) {
		return fmt.Errorf("auth: %s: home_country %q must be an upper-case ISO 3166-1 alpha-2 code such as FR", label, homeCountry)
	}
	if !domain.CountryScope(homeCountry).Within(max) {
		return fmt.Errorf("auth: %s: home_country %q is outside scope_max %q", label, homeCountry, max)
	}
	return nil
}
