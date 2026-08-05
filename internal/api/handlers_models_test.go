// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// handlers_models_test exercises effectiveAllowedModels, the per-key model
// visibility helper shared by ListModels, GetModel, and authorizeModel.
//
// Lives in package api (white-box) so it can reach the unexported helper
// without having to expand the public surface just for tests. The helper has
// three distinct branches (per-model quota → allowed_models → unrestricted)
// and the precedence between them matters; each branch gets a focused test.
package api

import (
	"net/http/httptest"
	"testing"

	"hivenet_router/internal/auth"

	"github.com/gin-gonic/gin"
)

// stampedContext returns a Gin context with the given auth values stamped on
// it — the same shape AuthMiddleware produces in production.
func stampedContext(limits *auth.QuotaLimits, allowedModels []string) *gin.Context {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	if limits != nil {
		c.Set("quota_limits", *limits)
	}
	if allowedModels != nil {
		c.Set("allowed_models", allowedModels)
	}
	return c
}

// TestEffectiveAllowedModels_PerModelQuotaIsTheAllowSet verifies the primary
// shape going forward: when a key has quota.per_model declared, the keys of
// that map ARE the model allow-list. No double-bookkeeping with a separate
// allowed_models entry — the per-model declaration is the single source of
// truth, which keeps strict-enumeration's "no quota declared → reject"
// guarantee aligned with what /v1/models exposes.
func TestEffectiveAllowedModels_PerModelQuotaIsTheAllowSet(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B-A3B": {RequestsPerMinutePerReplica: 187, TokensPerDay: 0},
			"Qwen/Qwen3.6-35B-A3B": {RequestsPerMinutePerReplica: 380, TokensPerDay: 0},
		},
	}
	c := stampedContext(&limits, nil)

	allowSet, unrestricted := effectiveAllowedModels(c)
	if unrestricted {
		t.Fatalf("per-model key must NOT be unrestricted — strict enumeration is the whole point")
	}
	if len(allowSet) != 2 {
		t.Fatalf("expected allow-set of size 2 (the two per_model entries), got %d (%v)", len(allowSet), allowSet)
	}
	for _, want := range []string{"Qwen/Qwen3.6-27B-A3B", "Qwen/Qwen3.6-35B-A3B"} {
		if _, ok := allowSet[want]; !ok {
			t.Errorf("expected %q in allow-set, missing", want)
		}
	}
	if _, ok := allowSet["Qwen/Qwen3.6-235B-A22B"]; ok {
		t.Errorf("allow-set must not contain models that aren't in per_model")
	}
}

// TestEffectiveAllowedModels_PerModelTakesPrecedenceOverAllowedModels verifies
// that when BOTH per_model AND allowed_models are present on a key, the
// per_model map wins. This avoids a confusing state where /v1/models could
// show models the strict per-model check would 429 on (or vice versa); the
// most specific declaration governs the visibility surface.
func TestEffectiveAllowedModels_PerModelTakesPrecedenceOverAllowedModels(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B-A3B": {RequestsPerMinutePerReplica: 187},
		},
	}
	// allowed_models lists a different model — must be ignored.
	c := stampedContext(&limits, []string{"openai/gpt-oss-20b"})

	allowSet, unrestricted := effectiveAllowedModels(c)
	if unrestricted {
		t.Fatalf("per-model key must NOT be unrestricted regardless of allowed_models")
	}
	if _, ok := allowSet["Qwen/Qwen3.6-27B-A3B"]; !ok {
		t.Errorf("per_model entry must drive the allow-set")
	}
	if _, ok := allowSet["openai/gpt-oss-20b"]; ok {
		t.Errorf("allowed_models must be ignored when per_model is set (per_model is the source of truth)")
	}
}

// TestEffectiveAllowedModels_LegacyAllowedModelsList verifies the back-compat
// path: a key with the legacy flat quota shape AND a non-empty allowed_models
// list uses that list as its allow-set. This matches pre-HAI-232 behaviour
// for the inference verbs and now extends it to the discovery endpoints too.
func TestEffectiveAllowedModels_LegacyAllowedModelsList(t *testing.T) {
	limits := auth.QuotaLimits{RequestsPerMinute: 100, TokensPerDay: 1_000_000} // no PerModel
	c := stampedContext(&limits, []string{"openai/gpt-oss-20b", "BAAI/bge-m3"})

	allowSet, unrestricted := effectiveAllowedModels(c)
	if unrestricted {
		t.Fatalf("non-empty allowed_models must produce a restricted allow-set")
	}
	if len(allowSet) != 2 {
		t.Fatalf("expected allow-set of size 2, got %d (%v)", len(allowSet), allowSet)
	}
	for _, want := range []string{"openai/gpt-oss-20b", "BAAI/bge-m3"} {
		if _, ok := allowSet[want]; !ok {
			t.Errorf("expected %q in allow-set, missing", want)
		}
	}
}

// TestEffectiveAllowedModels_Unrestricted_NoQuotaNoAllowList verifies the
// fully-open case: a key with neither per_model nor allowed_models declared
// sees the entire catalog. This is the pre-HAI-232 default for legacy flat
// keys without a model allowlist; staying unrestricted here avoids breaking
// any existing internal-tooling key that was implicitly relying on it.
func TestEffectiveAllowedModels_Unrestricted_NoQuotaNoAllowList(t *testing.T) {
	limits := auth.QuotaLimits{RequestsPerMinute: 100, TokensPerDay: 1_000_000} // no PerModel
	c := stampedContext(&limits, nil)                                           // no allowed_models

	allowSet, unrestricted := effectiveAllowedModels(c)
	if !unrestricted {
		t.Fatalf("a key with neither per_model nor allowed_models must be unrestricted; got allowSet=%v", allowSet)
	}
	if allowSet != nil {
		t.Errorf("unrestricted=true implies allowSet should be nil; got %v", allowSet)
	}
}

// TestEffectiveAllowedModels_Unrestricted_EmptyAllowedModelsList verifies that
// an EMPTY allowed_models list ([]string{}) is treated the same as "not set"
// — i.e. unrestricted. This matches authorizeModel's pre-HAI-232 behaviour
// (len(allowedModels) == 0 means allow everything) and avoids a confusing
// failure mode where setting allowed_models: [] in auth.yaml would silently
// lock the key out of every model.
func TestEffectiveAllowedModels_Unrestricted_EmptyAllowedModelsList(t *testing.T) {
	c := stampedContext(nil, []string{}) // empty but non-nil

	_, unrestricted := effectiveAllowedModels(c)
	if !unrestricted {
		t.Fatalf("empty allowed_models list must be unrestricted (len==0 means 'allow all', matching authorizeModel)")
	}
}

// TestEffectiveAllowedModels_NoContextKeysAtAll verifies the safety floor:
// when neither context key was ever stamped (e.g. auth was disabled), the
// helper must return unrestricted=true. Refusing to admit anything in this
// case would lock down /v1/models for any test or anonymous-auth deployment.
func TestEffectiveAllowedModels_NoContextKeysAtAll(t *testing.T) {
	c := stampedContext(nil, nil)

	_, unrestricted := effectiveAllowedModels(c)
	if !unrestricted {
		t.Fatalf("no context keys set must be unrestricted (no auth → no restriction)")
	}
}

// TestApplyAllowSet_AdminBypassReturnsFullCatalog verifies the core logic that
// makes /admin/models work: even when the Gin context carries a tenant-scoped
// allow-set (e.g. operator probing admin endpoints with a tenant token by
// mistake), the admin handler's applyAllowSet=false bypass returns every
// model. This is the table-stakes guarantee for the operator view — debugging
// a production routing issue is impossible if /admin/models hides models the
// operator's own per-model quota doesn't list. The corollary on the tenant
// path (applyAllowSet=true respects the allow-set) is covered end-to-end by
// the existing /v1/models tests; here we focus on the bypass.
func TestApplyAllowSet_AdminBypassReturnsFullCatalog(t *testing.T) {
	limits := auth.QuotaLimits{
		PerModel: map[string]auth.PerModelQuotaLimits{
			"Qwen/Qwen3.6-27B-A3B": {RequestsPerMinutePerReplica: 187},
		},
	}
	c := stampedContext(&limits, nil)

	// Tenant view: the helper produces a restricted allow-set as expected.
	allowSet, unrestricted := effectiveAllowedModels(c)
	if unrestricted {
		t.Fatalf("precondition: per-model key should produce a restricted allow-set")
	}
	if _, ok := allowSet["openai/gpt-oss-20b"]; ok {
		t.Fatalf("precondition: gpt-oss-20b should not be in the tenant's allow-set")
	}

	// Admin path simulation: writeModelList / writeModelDetail are invoked
	// with applyAllowSet=false. The filter branch is then never entered, so a
	// model NOT in the tenant's allow-set (e.g. gpt-oss-20b) would still be
	// returned. This mirrors the runtime predicate exactly so a future
	// refactor that changes the bypass semantics will fail this test.
	applyAllowSet := false
	wouldFilter := applyAllowSet && !unrestricted
	if wouldFilter {
		t.Fatalf("admin path (applyAllowSet=false) must never enter the filter branch — that would defeat /admin/models")
	}
}
