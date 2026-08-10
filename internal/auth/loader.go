// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"hivenet_router/internal/config"

	"github.com/goccy/go-yaml"
)

// PerModelQuotaConfig is one entry under quota.per_model. Both fields carry
// matching `yaml:` and `json:` tags so the same struct deserialises from
// auth.yaml AND from the dynamic admin API JSON body — validation lives in
// one place regardless of source. Pointer types let the loader distinguish
// "absent" (nil) from "present and zero" (unlimited), so partial entries
// surface as a clear error instead of silently meaning "unlimited".
type PerModelQuotaConfig struct {
	RequestsPerMinutePerReplica *int `yaml:"requests_per_minute_per_replica" json:"requests_per_minute_per_replica,omitempty"`
	TokensPerDay                *int `yaml:"tokens_per_day"                  json:"tokens_per_day,omitempty"`
}

// QuotaConfig holds quota limits as declared in auth.yaml OR in the dynamic
// admin API JSON body. Tags cover both formats so the dynamic-key DTO can
// embed this struct directly and reuse Validate.
//
// Two shapes are supported and validation rejects mixing them on the same key:
//
//   - Legacy flat shape: requests_per_minute + tokens_per_day apply to every
//     model this key may call. per_model must be absent.
//   - Per-model shape:   per_model is authoritative. The flat fields must be
//     absent. Every entry must declare BOTH requests_per_minute_per_replica
//     AND tokens_per_day (partial entries are rejected at load — "forgot a
//     field" must never silently mean "unlimited").
type QuotaConfig struct {
	RequestsPerMinute int `yaml:"requests_per_minute,omitempty" json:"requests_per_minute,omitempty"`
	TokensPerDay      int `yaml:"tokens_per_day,omitempty"      json:"tokens_per_day,omitempty"`

	// Per-key input/output token-per-minute buckets. Like RequestsPerMinute and
	// TokensPerDay they are flat-shape fields and may not be combined with
	// per_model. Zero means unset. A key's input bucket must be able to hold one
	// maximum-size prompt (see ValidateITPMCoversMaxInput) or it silently caps
	// the usable context.
	InputTokensPerMinute  int `yaml:"input_tokens_per_minute,omitempty"  json:"input_tokens_per_minute,omitempty"`
	OutputTokensPerMinute int `yaml:"output_tokens_per_minute,omitempty" json:"output_tokens_per_minute,omitempty"`

	PerModel map[string]*PerModelQuotaConfig `yaml:"per_model,omitempty" json:"per_model,omitempty"`
}

// Validate checks the quota block for shape consistency and converts it into
// the runtime QuotaLimits. label is included in error messages so the operator
// can locate the offending key.
func (q QuotaConfig) Validate(label string) (QuotaLimits, error) {
	if q.InputTokensPerMinute < 0 || q.OutputTokensPerMinute < 0 {
		return QuotaLimits{}, fmt.Errorf(
			"auth: %s: input_tokens_per_minute and output_tokens_per_minute must be >= 0", label)
	}
	if q.PerModel == nil {
		return QuotaLimits{
			RequestsPerMinute:     q.RequestsPerMinute,
			TokensPerDay:          q.TokensPerDay,
			InputTokensPerMinute:  q.InputTokensPerMinute,
			OutputTokensPerMinute: q.OutputTokensPerMinute,
		}, nil
	}
	if q.RequestsPerMinute != 0 || q.TokensPerDay != 0 ||
		q.InputTokensPerMinute != 0 || q.OutputTokensPerMinute != 0 {
		return QuotaLimits{}, fmt.Errorf(
			"auth: %s: quota.per_model is set together with the flat fields (requests_per_minute / tokens_per_day / input_tokens_per_minute / output_tokens_per_minute) — use one shape, not both",
			label)
	}
	if len(q.PerModel) == 0 {
		return QuotaLimits{}, fmt.Errorf(
			"auth: %s: quota.per_model is present but empty — enumerate every model this key may call",
			label)
	}
	resolved := make(map[string]PerModelQuotaLimits, len(q.PerModel))
	for model, entry := range q.PerModel {
		validated, err := validatePerModelEntry(label, model, entry)
		if err != nil {
			return QuotaLimits{}, err
		}
		resolved[model] = validated
	}
	return QuotaLimits{PerModel: resolved}, nil
}

// validatePerModelEntry runs the six per-entry guards (non-empty model name,
// non-nil entry pointer, both knobs present, non-negative values) and returns
// the resolved runtime shape. Pulled out of Validate's loop so the cognitive
// complexity of Validate stays bounded as the guard list grows.
func validatePerModelEntry(label, model string, entry *PerModelQuotaConfig) (PerModelQuotaLimits, error) {
	if model == "" {
		return PerModelQuotaLimits{}, fmt.Errorf("auth: %s: quota.per_model contains an empty model name", label)
	}
	if entry == nil {
		return PerModelQuotaLimits{}, fmt.Errorf("auth: %s: quota.per_model[%q] is empty — declare both requests_per_minute_per_replica and tokens_per_day", label, model)
	}
	if entry.RequestsPerMinutePerReplica == nil {
		return PerModelQuotaLimits{}, fmt.Errorf("auth: %s: quota.per_model[%q] is missing requests_per_minute_per_replica (use 0 for unlimited)", label, model)
	}
	if entry.TokensPerDay == nil {
		return PerModelQuotaLimits{}, fmt.Errorf("auth: %s: quota.per_model[%q] is missing tokens_per_day (use 0 for unlimited)", label, model)
	}
	if *entry.RequestsPerMinutePerReplica < 0 {
		return PerModelQuotaLimits{}, fmt.Errorf("auth: %s: quota.per_model[%q].requests_per_minute_per_replica must be >= 0", label, model)
	}
	if *entry.TokensPerDay < 0 {
		return PerModelQuotaLimits{}, fmt.Errorf("auth: %s: quota.per_model[%q].tokens_per_day must be >= 0", label, model)
	}
	return PerModelQuotaLimits{
		RequestsPerMinutePerReplica: *entry.RequestsPerMinutePerReplica,
		TokensPerDay:                *entry.TokensPerDay,
	}, nil
}

// KeyMetadata holds human-readable identification fields for an API key entry.
// These are stored in auth.yaml alongside the hash — they are never used for
// any authentication decision.
type KeyMetadata struct {
	// Name is a human-readable label for this key (e.g. "Team A Production Key").
	// Required — used in logs and audit entries.
	Name string `yaml:"name"`

	// Owner is the tenant or user ID for billing and quota tracking.
	// This becomes AuthResult.TenantID. Required.
	// Can be an organisation name ("team-a"), a user ID ("user-42"), or any
	// opaque string that is meaningful to the operator's billing system.
	Owner string `yaml:"owner"`

	// Description is an optional free-text note about the key's purpose.
	Description string `yaml:"description,omitempty"`

	// CreatedAt is the date the key was generated (e.g. "18-03-2026").
	// Populated automatically by "hivenet-router keygen". Optional but recommended.
	CreatedAt string `yaml:"created_at,omitempty"`

	// ExpiresAt is an optional date after which the key is rejected.
	// Format: "DD-MM-YYYY". The key is valid through the entire named day (UTC)
	// and becomes invalid at midnight UTC at the start of the following day.
	// Example: "01-01-2027" → valid until 2027-01-02 00:00:00 UTC.
	// A missing or empty value means the key never expires.
	ExpiresAt string `yaml:"expires_at,omitempty"`
}

// APIKeyEntry is one entry under api.keys in auth.yaml.
// KeyHash must be the SHA-256 hex of the real key (produced by "hivenet-router keygen").
// The plaintext key is never stored — only the hash and a masked preview.
type APIKeyEntry struct {
	// KeyHash is the SHA-256 hex of the full raw key string.
	// This is what the router compares against at request time.
	KeyHash string `yaml:"key_hash"`

	// KeyPreview is a truncated display form of the key (e.g. "sk-...KJ4").
	// Populated by "hivenet-router keygen". Stored for human identification only —
	// never used in any auth decision.
	KeyPreview string `yaml:"key_preview,omitempty"`

	// Metadata holds identity, ownership, and lifecycle fields for this key.
	Metadata KeyMetadata `yaml:"metadata"`

	// Models is the list of model names this key is permitted to request.
	// An empty list (the default) grants access to all registered models.
	Models []string `yaml:"models,omitempty"`

	// Quota defines per-tenant rate and token limits. Zero means unlimited.
	Quota QuotaConfig `yaml:"quota,omitempty"`

	// MaxOccupancyShare is the fraction of a serverless replica's admit budget
	// this key may hold in flight at once. Valid range (0, 1]; 0 means unset. It
	// sits at the key level, not in quota, because it is measured against the
	// replica's admit_budget_tokens rather than a per-minute rate. Ignored on
	// reserved replicas.
	MaxOccupancyShare float64 `yaml:"max_occupancy_share,omitempty"`
}

// validateOccupancyShare bounds a key's max_occupancy_share to (0, 1]; 0 is
// accepted as "unset". A share above 1 would let one key reserve more than the
// whole replica, defeating the fairness the cap exists to provide.
func validateOccupancyShare(label string, share float64) error {
	if share < 0 || share > 1 {
		return fmt.Errorf("auth: %s: max_occupancy_share must be in (0, 1] (0 means unset), got %v", label, share)
	}
	return nil
}

// ValidateITPMCoversMaxInput checks that a serverless key's input token bucket
// can hold at least one maximum-size prompt. Input tokens are reserved up front,
// so a bucket below max_input_tokens would silently cap the usable context. A
// zero bucket (unset) or zero maxInputTokens is skipped. The check spans the
// policy and key configs, so the caller runs it once both are loaded.
func ValidateITPMCoversMaxInput(label string, inputTokensPerMinute, maxInputTokens int) error {
	if inputTokensPerMinute > 0 && maxInputTokens > 0 && inputTokensPerMinute < maxInputTokens {
		return fmt.Errorf(
			"auth: %s: input_tokens_per_minute (%d) is below the policy max_input_tokens (%d) — the bucket cannot hold one maximum-size prompt and would silently cap context",
			label, inputTokensPerMinute, maxInputTokens)
	}
	return nil
}

// AuthSectionConfig configures the /v1/* auth provider.
type AuthSectionConfig struct {
	Mode AuthMode      `yaml:"mode"`
	Keys []APIKeyEntry `yaml:"keys,omitempty"`
}

// AdminSectionConfig configures the /admin/* auth provider.
// Admin keys are NOT stored in this file — they come from HIVENET_ROUTER_ADMIN_API_KEYS.
type AdminSectionConfig struct {
	Mode AuthMode `yaml:"mode"`
}

// AuthConfig is the top-level structure of auth.yaml.
type AuthConfig struct {
	API   AuthSectionConfig  `yaml:"api"`
	Admin AdminSectionConfig `yaml:"admin"`
}

// DefaultAuthConfig returns a config with both sections defaulting to mode: none.
func DefaultAuthConfig() *AuthConfig {
	return &AuthConfig{
		API:   AuthSectionConfig{Mode: AuthModeNone},
		Admin: AdminSectionConfig{Mode: AuthModeNone},
	}
}

// LoadAuthConfig reads and parses an auth config YAML file.
func LoadAuthConfig(path string) (*AuthConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read config %q: %w", path, err)
	}
	var cfg AuthConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("auth: parse config %q: %w", path, err)
	}
	return &cfg, nil
}

// ParseExpiresAt parses an optional date string ("02-01-2006", i.e. DD-MM-YYYY)
// into a time.Time representing the start of the day FOLLOWING the named date (UTC).
// This means a key with expires_at "01-01-2027" is valid through the entire
// day 2027-01-01 and becomes invalid at 2027-01-02 00:00:00 UTC.
// Returns nil, nil for an empty string (key never expires).
// Returns an error if the string is non-empty but has an invalid format.
func ParseExpiresAt(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse("02-01-2006", s)
	if err != nil {
		return nil, fmt.Errorf("auth: expires_at %q: expected format DD-MM-YYYY: %w", s, err)
	}
	// Add 24 hours so the key is valid through the entire named day.
	// The check in Authenticate uses After(), so expiry fires at the start of
	// the next day UTC — matching the intuitive "expires at end of this date" semantics.
	t = t.UTC().Add(24 * time.Hour)
	return &t, nil
}

// ParseExpiresAtRFC3339 parses RFC3339 timestamps used by the dynamic admin API.
// Returns (nil, nil) for empty input.
func ParseExpiresAtRFC3339(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("auth: expires_at %q: expected RFC3339: %w", s, err)
	}
	u := t.UTC()
	return &u, nil
}

// ProvidersFromConfig builds the API and admin Provider implementations from the
// router config. It loads auth.yaml from cfg.AuthConfigFile if set; otherwise
// it uses cfg.AuthMode (from HIVENET_ROUTER_AUTH_MODE env var) to determine the mode.
//
// The third return value is the DynamicKeyProvider reference (non-nil only when
// mode is "dynamic"). The router stores it to wire admin key management endpoints
// and to guard against SIGHUP reload.
//
// For the admin section, raw keys are read from the HIVENET_ROUTER_ADMIN_API_KEYS
// environment variable (comma-separated). They are SHA-256-hashed at startup;
// the plaintext is never retained.
//
// Logs startup warnings for any section using mode=none.
// Returns an error (and causes startup to fail) for any misconfigured mode.
func ProvidersFromConfig(cfg *config.Config) (Provider, Provider, *DynamicKeyProvider, error) {
	var apiProvider, adminProvider Provider
	var dynProv *DynamicKeyProvider

	authCfg := DefaultAuthConfig()
	if cfg.AuthConfigFile != "" {
		var err error
		authCfg, err = LoadAuthConfig(cfg.AuthConfigFile)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// When no auth.yaml is provided, use the env-var override for the API mode.
	apiMode := authCfg.API.Mode
	if cfg.AuthConfigFile == "" && cfg.AuthMode != "" {
		apiMode = AuthMode(cfg.AuthMode)
	}

	// Build API provider (/v1/* endpoints).
	switch apiMode {
	case AuthModeNone, "":
		apiProvider = NewNoAuthProvider()
		log.Warn("Auth: API mode = none — /v1/* endpoints are publicly accessible without authentication")
	case AuthModeAPIKey:
		if len(authCfg.API.Keys) == 0 {
			return nil, nil, nil, fmt.Errorf("auth: api section mode=api-key but keys list is empty — add key entries to auth.yaml")
		}
		var err error
		apiProvider, err = NewStaticKeyProvider(authCfg.API.Keys)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("auth: build API key provider: %w", err)
		}
		log.Infof("Auth: API mode = api-key (%d key(s) configured)", len(authCfg.API.Keys))
	case "dynamic":
		dynProv = NewDynamicKeyProvider()
		apiProvider = dynProv
		log.Warn("Auth: dynamic key registry is empty, awaiting bootstrap from machines service")
	default:
		return nil, nil, nil, fmt.Errorf("auth: unknown API auth mode %q — supported: none, api-key, dynamic", apiMode)
	}

	// Build admin provider (/admin/* endpoints).
	adminProvider, err := buildAdminProvider(authCfg.Admin.Mode, dynProv != nil)
	if err != nil {
		return nil, nil, nil, err
	}

	return apiProvider, adminProvider, dynProv, nil
}

// buildAdminProvider creates the admin auth provider.
// Admin keys come from the HIVENET_ROUTER_ADMIN_API_KEYS env var (not auth.yaml).
// If hasDynamicAPI is true and mode is "" or "none", admin is elevated to api-key
// to protect the /admin/api-keys/* key-management surface.
func buildAdminProvider(mode AuthMode, hasDynamicAPI bool) (Provider, error) {
	if hasDynamicAPI && (mode == "" || mode == AuthModeNone) {
		mode = AuthModeAPIKey
		log.Info("Auth: dynamic API mode requires admin auth — elevating admin to api-key (set HIVENET_ROUTER_ADMIN_API_KEYS)")
	}
	switch mode {
	case AuthModeNone, "":
		// Fail closed: refuse to start with unauthenticated admin endpoints unless
		// the operator explicitly opts in. /admin/* can write policy and manage API
		// keys, so an accidental no-auth default is a serious exposure.
		if !insecureAdminAllowed() {
			return nil, fmt.Errorf("auth: admin mode = none exposes /admin/* (policy + API-key management) with no authentication; " +
				"set admin.mode: api-key in auth.yaml (HIVENET_ROUTER_AUTH_CONFIG / --auth-config-file) and provide HIVENET_ROUTER_ADMIN_API_KEYS, " +
				"or set HIVENET_ROUTER_ALLOW_INSECURE_ADMIN=true to run without it")
		}
		log.Warn("Auth: Admin mode = none — /admin/* endpoints are publicly accessible without authentication (HIVENET_ROUTER_ALLOW_INSECURE_ADMIN=true)")
		return NewNoAuthProvider(), nil
	case AuthModeAPIKey:
		rawKeys := os.Getenv("HIVENET_ROUTER_ADMIN_API_KEYS")
		if rawKeys == "" {
			return nil, fmt.Errorf("auth: admin section mode=api-key but HIVENET_ROUTER_ADMIN_API_KEYS env var is not set")
		}
		keys := splitTrimmed(rawKeys, ",")
		provider, err := NewStaticAdminKeyProvider(keys)
		if err != nil {
			return nil, fmt.Errorf("auth: build admin key provider: %w", err)
		}
		log.Infof("Auth: Admin mode = api-key (%d key(s) configured)", len(keys))
		return provider, nil
	default:
		return nil, fmt.Errorf("auth: unknown admin auth mode %q — supported: none, api-key", mode)
	}
}

// insecureAdminAllowed reports whether the operator explicitly opted into
// running /admin/* without authentication via HIVENET_ROUTER_ALLOW_INSECURE_ADMIN.
func insecureAdminAllowed() bool {
	v, err := strconv.ParseBool(os.Getenv("HIVENET_ROUTER_ALLOW_INSECURE_ADMIN"))
	return err == nil && v
}

// splitTrimmed splits s by sep, trims whitespace from each element,
// and drops empty strings.
func splitTrimmed(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
