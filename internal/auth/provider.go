// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package auth

import (
	"errors"
	"net/http"
	"strings"
	"sync/atomic"

	logging "github.com/ipfs/go-log/v2"
)

// AuthMode identifies which authentication mechanism is active for an endpoint group.
type AuthMode string

const (
	AuthModeNone   AuthMode = "none"
	AuthModeAPIKey AuthMode = "api-key"
)

// PerModelQuotaLimits is the resolved quota for one (key, model) pair. Zero
// means unlimited.
type PerModelQuotaLimits struct {
	// RequestsPerMinutePerReplica is the per-replica request rate. The effective
	// ceiling at admission is this value multiplied by the number of healthy
	// agents serving the model — so adding replicas grows every per-model
	// ceiling proportionally without any quota edit.
	RequestsPerMinutePerReplica int

	// TokensPerDay is the absolute daily token budget for this (key, model)
	// pair. Cost is not per-replica, so this is NOT scaled by replica count.
	TokensPerDay int
}

// QuotaLimits defines one key's rate limits. Zero means unlimited.
//
// Two shapes are supported and the loader rejects mixing them on the same key:
//
//   - Legacy flat shape: RequestsPerMinute + TokensPerDay apply to every model
//     this key may call. PerModel must be nil.
//   - Per-model shape:   PerModel is authoritative. The flat fields must be 0.
//     A request to a model is admitted only if PerModel contains an exact entry
//     for that model; otherwise the request is rejected with
//     ErrCodeRateLimitExceeded ("no quota declared for model X on this API
//     key"). There is no wildcard or catch-all — typo'd or unbudgeted models
//     must surface loudly, not silently fall through to a default ceiling.
type QuotaLimits struct {
	// Legacy flat per-key limits — used when PerModel is nil.
	RequestsPerMinute int
	TokensPerDay      int

	// Serverless per-key caps; zero means unset. They apply only when the
	// request lands on a serverless policy. InputTokensPerMinute and
	// OutputTokensPerMinute are token-per-minute buckets. MaxOccupancyShare is
	// the fraction of a replica's admit budget the key may hold in flight at once
	// (0 < share <= 1); it comes from the key-level max_occupancy_share field,
	// not the quota block.
	InputTokensPerMinute  int
	OutputTokensPerMinute int
	MaxOccupancyShare     float64

	// PerModel, when non-nil, replaces the flat fields above. Keys are model
	// names as they appear in request bodies. A nil map is the legacy shape;
	// an empty non-nil map is invalid and rejected at load.
	PerModel map[string]PerModelQuotaLimits
}

// ResolvePerModel returns the per-model quota entry that exactly matches the
// given model. If PerModel is nil (legacy flat shape) or the model is not
// enumerated, the second return value is false — callers in the legacy path
// branch into the umbrella RequestsPerMinute/TokensPerDay; callers in the
// per-model path reject with "quota not declared".
func (q QuotaLimits) ResolvePerModel(model string) (PerModelQuotaLimits, bool) {
	if q.PerModel == nil {
		return PerModelQuotaLimits{}, false
	}
	entry, ok := q.PerModel[model]
	return entry, ok
}

// addCeiling sums two per-key ceilings where 0 means unlimited: the sum is
// unlimited if either is.
func addCeiling(a, b int) int {
	if a == 0 || b == 0 {
		return 0
	}
	return a + b
}

// tenantCeilings sums the limits of one tenant's keys into the tenant's total
// ceiling, for the per-tenant limit gauges. Limits are enforced per key, so a
// tenant can use up to the sum of its keys' limits; a key with an unlimited
// (0) value makes that total unlimited. Flat-shape fields are summed over the
// flat-shape keys only, and each per-model entry over the keys that declare
// that model (a per-model key cannot call an undeclared model). With one key,
// the result is that key's limits unchanged.
func tenantCeilings(keys []QuotaLimits) QuotaLimits {
	var out QuotaLimits
	flat := 0
	for _, q := range keys {
		if q.PerModel != nil {
			if out.PerModel == nil {
				out.PerModel = make(map[string]PerModelQuotaLimits, len(q.PerModel))
			}
			for model, e := range q.PerModel {
				if prev, ok := out.PerModel[model]; ok {
					e.RequestsPerMinutePerReplica = addCeiling(prev.RequestsPerMinutePerReplica, e.RequestsPerMinutePerReplica)
					e.TokensPerDay = addCeiling(prev.TokensPerDay, e.TokensPerDay)
				}
				out.PerModel[model] = e
			}
		}
		if q.PerModel != nil && q.RequestsPerMinute == 0 && q.TokensPerDay == 0 {
			// A per-model key has no flat ceiling; it must not turn the
			// tenant's flat total into "unlimited".
			continue
		}
		if flat == 0 {
			out.RequestsPerMinute, out.TokensPerDay = q.RequestsPerMinute, q.TokensPerDay
			out.InputTokensPerMinute, out.OutputTokensPerMinute = q.InputTokensPerMinute, q.OutputTokensPerMinute
			out.MaxOccupancyShare = q.MaxOccupancyShare
		} else {
			out.RequestsPerMinute = addCeiling(out.RequestsPerMinute, q.RequestsPerMinute)
			out.TokensPerDay = addCeiling(out.TokensPerDay, q.TokensPerDay)
			out.InputTokensPerMinute = addCeiling(out.InputTokensPerMinute, q.InputTokensPerMinute)
			out.OutputTokensPerMinute = addCeiling(out.OutputTokensPerMinute, q.OutputTokensPerMinute)
			if out.MaxOccupancyShare == 0 || q.MaxOccupancyShare == 0 {
				out.MaxOccupancyShare = 0
			} else {
				out.MaxOccupancyShare = min(1, out.MaxOccupancyShare+q.MaxOccupancyShare)
			}
		}
		flat++
	}
	return out
}

// AuthResult is returned by a successful Provider.Authenticate call.
// It carries the resolved identity, quota limits, and access restrictions
// for downstream enforcement and audit logging.
type AuthResult struct {
	// TenantID is the billing/quota unit (from metadata.owner). Injected into the
	// Gin context as "tenant_id" and used by the quota enforcement layer (Ticket 5).
	TenantID string

	// KeyID identifies the key: the upstream key-sync service ID for dynamic
	// keys (e.g. "key-abc123"), "static-" + 16 hex of the key hash for
	// auth.yaml keys, "admin-" + 16 hex for admin keys. Injected into the Gin
	// context as "key_id" for per-key limits, metric labels and audit.
	// Empty only in auth mode none.
	KeyID string

	// KeyPreview is the truncated key (e.g. "sk-...KJ4") stored in
	// auth.yaml for human identification. Used in logs and audit entries — never
	// used for any auth decision.
	KeyPreview string

	// AllowedModels is the list of model names this key may request.
	// Empty means unrestricted access to all registered models.
	// Checked in the inference handlers after the request body is parsed.
	AllowedModels []string

	// QuotaLimits holds this key's rate/token limits, enforced per key by
	// QuotaMiddleware and the handlers. Zero values mean unlimited.
	QuotaLimits QuotaLimits

	// Redirect is set only for a request authenticated by a cross-site
	// redirect token (internal/redirect); nil otherwise.
	Redirect *RedirectInfo
}

// RedirectInfo is what a verified redirect token tells the API layer about a
// request repeated at a sibling policloud (contract 3 v2 §4).
type RedirectInfo struct {
	Hop       int    // redirects taken, including the one that brought it here
	BodySHA   string // hex SHA-256 the request body must match, checked before quota
	RequestID string // the first router's request ID, adopted here
	// ClientRequestID is the ID the client sent to the first router, if any,
	// adopted here too.
	ClientRequestID string
	Model           string // the concrete model the first router routed on, used as is
	Scope           string // the effective scope at the first router
	From            string // the first router's policloud ID, never redirected back to
	TokenID         string // the token's jti, marked used once the body check passes
	Exp             int64  // the token's expiry (Unix seconds), how long its jti is remembered
}

// Provider authenticates an incoming HTTP request and returns an AuthResult.
// All implementations must be safe for concurrent use.
type Provider interface {
	Authenticate(r *http.Request) (*AuthResult, error)
}

// Sentinel errors returned by Provider implementations.
var (
	// ErrMissingCredentials is returned when no credentials are present in the request.
	ErrMissingCredentials = errors.New("missing credentials")
	// ErrInvalidCredentials is returned when credentials are present but do not match.
	ErrInvalidCredentials = errors.New("invalid credentials")
)

// log is the package-level structured logger for the auth package.
var log = logging.Logger("auth")

// extractBearerToken extracts the bearer token from the Authorization header.
// Accepts "Bearer <token>" (standard) and bare "<token>" (convenience).
// Returns an empty string if the header is absent or blank.
func extractBearerToken(r *http.Request) string {
	v := strings.TrimSpace(r.Header.Get("Authorization"))
	if v == "" {
		return ""
	}
	if after, ok := strings.CutPrefix(v, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return v
}

// AtomicProvider wraps a Provider behind an atomic pointer for live hot-swapping
// on SIGHUP without interrupting in-flight requests.
// AtomicProvider itself implements Provider so it can be passed wherever a
// Provider is expected.
type AtomicProvider struct {
	p atomic.Pointer[Provider]
}

// NewAtomicProvider creates an AtomicProvider with the given initial provider.
func NewAtomicProvider(initial Provider) *AtomicProvider {
	a := &AtomicProvider{}
	a.Swap(initial)
	return a
}

// Swap atomically replaces the wrapped provider. Safe to call concurrently with Authenticate.
func (a *AtomicProvider) Swap(p Provider) {
	a.p.Store(&p)
}

// Load returns the currently active provider. Used for type assertions
// (e.g. to call optional methods like Tenants()) without going through the
// Provider interface.
func (a *AtomicProvider) Load() Provider {
	return *a.p.Load()
}

// Authenticate delegates to the currently active provider.
func (a *AtomicProvider) Authenticate(r *http.Request) (*AuthResult, error) {
	return (*a.p.Load()).Authenticate(r)
}
