// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/policy"
	"hivenet_router/internal/storage"

	"github.com/gin-gonic/gin"
)

// errDynamicNotActive is returned when dynamic key registry endpoints are called
// but the registry is not configured (HIVENET_ROUTER_AUTH_MODE != dynamic).
const errDynamicNotActive = "dynamic key registry not active"

// ListRoutingTable handles GET /admin/routing-table.
// Returns a full snapshot of the routing table — live agent state merged with
// SRTT, success rate, and the latest engine metrics (KV cache, etc.).
// Intended for operational visibility without requiring a Grafana setup.
func (h *Handlers) ListRoutingTable(c *gin.Context) {
	agents := h.routingTable.GetRoutingTable()
	c.JSON(http.StatusOK, gin.H{
		"agents": agents,
		"total":  len(agents),
	})
}

// StorageInfo handles GET /admin/storage.
// Returns live key counts and disk usage for both BadgerDB instances.
func (h *Handlers) StorageInfo(c *gin.Context) {
	bs, ok := h.storage.(*storage.BadgerStorage)
	if !ok {
		c.JSON(http.StatusOK, gin.H{"error": "storage stats not available for this backend"})
		return
	}
	c.JSON(http.StatusOK, bs.Stats())
}

// GetPolicy handles GET /admin/policy.
// Returns the currently active routing policy as JSON.
func (h *Handlers) GetPolicy(c *gin.Context) {
	c.JSON(http.StatusOK, h.executor.GetPolicy())
}

// PutPolicy handles PUT /admin/policy.
// Accepts a YAML body, validates it, and atomically replaces the active policy.
// The change is ephemeral — on router restart the policy file on disk is loaded again.
// Returns 400 with a validation error message if the YAML is invalid or exceeds 1 MB.
func (h *Handlers) PutPolicy(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20) // 1 MB cap
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body (max 1 MB)"})
		return
	}
	p, err := policy.LoadBytes(body)
	if err != nil {
		if h.policyReloadObserver != nil {
			h.policyReloadObserver("api", "error")
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if h.providerValidator != nil {
		if err := h.providerValidator(p); err != nil {
			if h.policyReloadObserver != nil {
				h.policyReloadObserver("api", "error")
			}
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	h.executor.SetPolicy(p)
	if h.policyReloadObserver != nil {
		h.policyReloadObserver("api", "success")
	}
	log.Infof("Routing policy updated via PUT /admin/policy")
	c.JSON(http.StatusOK, gin.H{"status": "policy updated"})
}

// GetModelPolicies handles GET /admin/policy/models.
// Returns all named policy documents as JSON, keyed by document name.
// An empty object {} is returned when no named policies are active.
func (h *Handlers) GetModelPolicies(c *gin.Context) {
	c.JSON(http.StatusOK, h.executor.GetNamedPolicies())
}

// GetModelPolicy handles GET /admin/policy/models/*name.
// The URL segment identifies the policy document name (not a model name).
// Returns 404 if no document with that name exists.
func (h *Handlers) GetModelPolicy(c *gin.Context) {
	name := strings.TrimPrefix(c.Param("name"), "/")
	p, ok := h.executor.GetNamedPolicy(name)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "no policy named: " + name})
		return
	}
	c.JSON(http.StatusOK, p)
}

// PutModelPolicy handles PUT /admin/policy/models/*name.
// The URL segment is the policy document name (analogous to a filename).
// The body is a YAML policy document; the 'models:' field inside it declares
// which model names this policy applies to — exactly as in a .yaml file on disk.
// The change is ephemeral — on router restart the policy model dir on disk is loaded.
func (h *Handlers) PutModelPolicy(c *gin.Context) {
	name := strings.TrimPrefix(c.Param("name"), "/")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "policy name is required in URL"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20) // 1 MB cap
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body (max 1 MB)"})
		return
	}
	p, err := policy.LoadBytes(body)
	if err != nil {
		if h.policyReloadObserver != nil {
			h.policyReloadObserver("api", "error")
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(p.Models) == 0 {
		if h.policyReloadObserver != nil {
			h.policyReloadObserver("api", "error")
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "policy body must include a 'models:' field with at least one model name"})
		return
	}
	if h.providerValidator != nil {
		if err := h.providerValidator(p); err != nil {
			if h.policyReloadObserver != nil {
				h.policyReloadObserver("api", "error")
			}
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	if err := h.executor.SetNamedPolicy(name, p); err != nil {
		if h.policyReloadObserver != nil {
			h.policyReloadObserver("api", "error")
		}
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if h.policyReloadObserver != nil {
		h.policyReloadObserver("api", "success")
	}
	log.Infof("Named routing policy %q updated via PUT /admin/policy/models/%s (models: %v)", name, name, p.Models)
	c.JSON(http.StatusOK, gin.H{"status": "policy updated", "name": name, "models": p.Models})
}

// ResetMetrics handles POST /admin/metrics/reset. It clears all persisted lifetime
// per-agent counters (success/failure/tokens/disconnections/SRTT) from diskDB, the
// in-memory counter state, and the matching Prometheus series — so dashboards reflect
// behaviour since the reset rather than historical totals. Typical use: run it right
// after deploying a change to observe its effect against a clean baseline.
//
// Routing-level request counters live only in memory and already reset when the router
// process restarts (e.g. on deploy). Tenant/billing counters and agent metadata are
// never touched.
func (h *Handlers) ResetMetrics(c *gin.Context) {
	if h.resetMetrics == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metrics reset is not available (no counter store configured)"})
		return
	}
	if err := h.resetMetrics(); err != nil {
		log.Warnf("POST /admin/metrics/reset failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to reset metrics: " + err.Error()})
		return
	}
	log.Infof("Per-agent lifetime metrics reset via POST /admin/metrics/reset")
	c.JSON(http.StatusOK, gin.H{
		"status":  "metrics reset",
		"message": "per-agent lifetime counters cleared (disk, in-memory, and Prometheus series)",
	})
}

// DeleteModelPolicy handles DELETE /admin/policy/models/*name.
// Removes the named policy document; all models it served revert to the global
// policy. Returns 200 even if no document with that name existed (idempotent).
func (h *Handlers) DeleteModelPolicy(c *gin.Context) {
	name := strings.TrimPrefix(c.Param("name"), "/")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "policy name is required in URL"})
		return
	}
	h.executor.DeleteNamedPolicy(name)
	log.Infof("Named routing policy %q deleted via DELETE /admin/policy/models/%s", name, name)
	c.JSON(http.StatusOK, gin.H{"status": "policy deleted", "name": name})
}

// apiKeyQuota is the JSON shape for per-key quota limits accepted by the
// dynamic admin API. It aliases auth.QuotaConfig directly — the struct already
// carries matching `json:` tags, so the same type deserialises from YAML and
// JSON. The dual-tag setup keeps validation in one place (auth.QuotaConfig.
// Validate) and prevents the two formats from drifting apart silently.
type apiKeyQuota = auth.QuotaConfig

// apiKeyEntryRequest is the JSON shape for a single key entry, reused inside
// ReplaceAll. Field validation lives in auth.DynamicKeyProvider so this DTO
// only describes the wire format.
type apiKeyEntryRequest struct {
	ID            string      `json:"id"`
	KeyHash       string      `json:"key_hash"`
	KeyPreview    string      `json:"key_preview"`
	Owner         string      `json:"owner"`
	Name          string      `json:"name"`
	Enabled       bool        `json:"enabled"`
	ExpiresAt     string      `json:"expires_at"`
	AllowedModels []string    `json:"allowed_models"`
	Quota         apiKeyQuota `json:"quota"`

	// MaxOccupancyShare is the key-level serverless occupancy share, matching
	// the static auth.yaml field of the same name (it sits outside quota there
	// too, because it is a fraction of the replica pool's admit budget rather
	// than a per-minute rate). Valid range (0, 1]; 0 means unset.
	MaxOccupancyShare float64 `json:"max_occupancy_share"`
}

// upsertAPIKeyRequest is the JSON body for PUT /admin/api-keys/:id.
// Wraps a key entry plus the registry version. The :id URL parameter is the
// source of truth for the entry ID; any id field in the body is ignored.
type upsertAPIKeyRequest struct {
	Version           string      `json:"version"`
	KeyHash           string      `json:"key_hash"`
	KeyPreview        string      `json:"key_preview"`
	Owner             string      `json:"owner"`
	Name              string      `json:"name"`
	Enabled           bool        `json:"enabled"`
	ExpiresAt         string      `json:"expires_at"`
	AllowedModels     []string    `json:"allowed_models"`
	Quota             apiKeyQuota `json:"quota"`
	MaxOccupancyShare float64     `json:"max_occupancy_share"` // see apiKeyEntryRequest
}

// replaceAPIKeysRequest is the JSON body for POST /admin/api-keys/replace.
type replaceAPIKeysRequest struct {
	Version string               `json:"version"`
	Keys    []apiKeyEntryRequest `json:"keys"`
}

// toEntry converts the wire DTO into the domain struct, parsing expires_at and
// validating the quota block (mixed flat+per_model shapes are rejected here so
// the dynamic admin API enforces the same rules as auth.yaml — both formats
// share auth.QuotaConfig.Validate, so they cannot drift).
func (k apiKeyEntryRequest) toEntry() (auth.DynamicKeyEntry, error) {
	expiresAt, err := auth.ParseExpiresAtRFC3339(k.ExpiresAt)
	if err != nil {
		return auth.DynamicKeyEntry{}, fmt.Errorf("invalid expires_at: %w", err)
	}
	quota, err := k.Quota.Validate(fmt.Sprintf("key %q", k.ID))
	if err != nil {
		return auth.DynamicKeyEntry{}, err
	}
	// The share sits at the key level, like the static auth.yaml field; the
	// registry validates its (0,1] range on every mutation.
	quota.MaxOccupancyShare = k.MaxOccupancyShare
	return auth.DynamicKeyEntry{
		ID:            k.ID,
		KeyHash:       k.KeyHash,
		KeyPreview:    k.KeyPreview,
		Owner:         k.Owner,
		Name:          k.Name,
		Enabled:       k.Enabled,
		ExpiresAt:     expiresAt,
		AllowedModels: k.AllowedModels,
		Quota:         quota,
	}, nil
}

// validateKeyAdmission runs the cross-config admission invariants for one
// dynamic key against the policies currently in force — the same check static
// auth.yaml keys get at startup and on reload: on a serverless policy the key
// can reach, its ITPM bucket must hold at least one maximum-size prompt, or
// the cap silently shrinks the model's usable context for that key (the D16
// failure). Rejecting at upsert keeps the misconfiguration out of the
// registry, mirroring how the static loader refuses to start on it.
func (h *Handlers) validateKeyAdmission(e auth.DynamicKeyEntry) error {
	if h.executor == nil {
		return nil
	}
	check := func(p *policy.Policy) error {
		if p == nil || !p.IsServerless() || p.MaxInputTokens <= 0 || !p.GovernsAnyOf(e.AllowedModels) {
			return nil
		}
		label := fmt.Sprintf("key %q on serverless models %v", e.ID, p.Models)
		return auth.ValidateITPMCoversMaxInput(label, e.Quota.InputTokensPerMinute, p.MaxInputTokens)
	}
	if err := check(h.executor.GetPolicy()); err != nil {
		return err
	}
	for _, p := range h.executor.GetNamedPolicies() {
		if err := check(p); err != nil {
			return err
		}
	}
	return nil
}

// writeRegistryError translates a registry-level error into a 409 (stale
// version) or 400 (validation/format) response with a current_version hint
// where available.
func (h *Handlers) writeRegistryError(c *gin.Context, err error) {
	if errors.Is(err, auth.ErrStaleVersion) {
		ver, _ := h.keyRegistry.Version()
		c.JSON(http.StatusConflict, gin.H{"error": "stale version", "current_version": ver})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
}

// UpsertAPIKey handles PUT /admin/api-keys/:id.
// Adds or updates a single key entry. The :id URL parameter sets the entry ID;
// field validation is delegated to the registry.
func (h *Handlers) UpsertAPIKey(c *gin.Context) {
	if h.keyRegistry == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errDynamicNotActive})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	var req upsertAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if req.Version == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "version is required"})
		return
	}
	entry, err := apiKeyEntryRequest{
		ID:                c.Param("id"),
		KeyHash:           req.KeyHash,
		KeyPreview:        req.KeyPreview,
		Owner:             req.Owner,
		Name:              req.Name,
		Enabled:           req.Enabled,
		ExpiresAt:         req.ExpiresAt,
		AllowedModels:     req.AllowedModels,
		Quota:             req.Quota,
		MaxOccupancyShare: req.MaxOccupancyShare,
	}.toEntry()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.validateKeyAdmission(entry); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.keyRegistry.Upsert(req.Version, entry); err != nil {
		h.writeRegistryError(c, err)
		return
	}
	log.Infof("API key %q upserted via PUT /admin/api-keys/%s", entry.ID, entry.ID)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// DeleteAPIKey handles DELETE /admin/api-keys/:id.
// Revokes a key by ID. Idempotent — 200 even if key doesn't exist.
func (h *Handlers) DeleteAPIKey(c *gin.Context) {
	if h.keyRegistry == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errDynamicNotActive})
		return
	}
	version := c.Query("version")
	if version == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "version query parameter is required"})
		return
	}
	keyID := strings.TrimSpace(c.Param("id"))
	if keyID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key id must not be empty"})
		return
	}
	if err := h.keyRegistry.Delete(version, keyID); err != nil {
		h.writeRegistryError(c, err)
		return
	}
	log.Infof("API key %q deleted via DELETE /admin/api-keys/%s", keyID, keyID)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ReplaceAPIKeys handles POST /admin/api-keys/replace.
// Atomically replaces the entire key registry. Always accepted (no version check).
// Body is capped at 16 MB to allow fleet-wide snapshots (~50k keys at 300 B each).
func (h *Handlers) ReplaceAPIKeys(c *gin.Context) {
	if h.keyRegistry == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errDynamicNotActive})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<20)
	var req replaceAPIKeysRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if req.Version == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "version is required"})
		return
	}
	entries := make([]auth.DynamicKeyEntry, 0, len(req.Keys))
	for i, k := range req.Keys {
		entry, err := k.toEntry()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("key[%d]: %s", i, err)})
			return
		}
		if err := h.validateKeyAdmission(entry); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("key[%d]: %s", i, err)})
			return
		}
		entries = append(entries, entry)
	}
	if err := h.keyRegistry.ReplaceAll(req.Version, entries); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Infof("API key registry replaced via POST /admin/api-keys/replace (%d keys)", len(entries))
	c.JSON(http.StatusOK, gin.H{"ok": true, "count": len(entries)})
}

// ListAPIKeys handles GET /admin/api-keys.
// Returns all active key entries — key_hash is never exposed.
func (h *Handlers) ListAPIKeys(c *gin.Context) {
	if h.keyRegistry == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errDynamicNotActive})
		return
	}
	keys := h.keyRegistry.ListKeys()
	c.JSON(http.StatusOK, gin.H{"keys": keys, "count": len(keys)})
}

// GetAPIKey handles GET /admin/api-keys/:id.
// Returns a single key entry — key_hash is never exposed.
func (h *Handlers) GetAPIKey(c *gin.Context) {
	if h.keyRegistry == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errDynamicNotActive})
		return
	}
	keyID := strings.TrimSpace(c.Param("id"))
	if keyID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key id must not be empty"})
		return
	}
	entry, ok := h.keyRegistry.GetKey(keyID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "key not found: " + keyID})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": entry})
}

// GetAPIKeyVersion handles GET /admin/api-keys/version.
// Returns the current registry version and key count.
func (h *Handlers) GetAPIKeyVersion(c *gin.Context) {
	if h.keyRegistry == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": errDynamicNotActive})
		return
	}
	ver, count := h.keyRegistry.Version()
	c.JSON(http.StatusOK, gin.H{"version": ver, "count": count})
}
