// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package provider

import "fmt"

// IsSupported reports whether name is a known provider engine.
// To add a new provider: implement Provider, add one case to New, and add the name here.
func IsSupported(name string) bool {
	switch name {
	case "openai", "anthropic":
		return true
	default:
		return false
	}
}

// New constructs the Provider for cfg.Name.
// Returns an error if the name is unknown or the APIKey is empty.
// To add a new provider: implement Provider and add one case to the switch below.
func New(cfg Config) (Provider, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("provider %q: APIKey is required", cfg.Name)
	}
	switch cfg.Name {
	case "openai":
		return newOpenAI(cfg), nil
	case "anthropic":
		return newAnthropic(cfg), nil
	default:
		return nil, fmt.Errorf("provider %q: unknown provider — supported: openai, anthropic", cfg.Name)
	}
}
