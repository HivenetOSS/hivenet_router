// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package api_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/policy"

	"github.com/gin-gonic/gin"
)

// TestAdminKeys_RefusedPushIsCountedAndNamesTheKey: a key push with one
// entry the registry refuses (here a reserved owner, as an existing key-sync
// entry may have after an upgrade) answers 400 naming the entry, is counted
// for the rejection metric, and applies nothing: on a full replace the key it
// would have revoked keeps authenticating. A stale replace is not counted.
func TestAdminKeys_RefusedPushIsCountedAndNamesTheKey(t *testing.T) {
	h, reg := newAdminKeyHandlers(&policy.Policy{})
	var rejected []string
	h.SetKeyPushRejectedObserver(func(endpoint string) { rejected = append(rejected, endpoint) })
	hash := func(token string) string { s := sha256.Sum256([]byte(token)); return hex.EncodeToString(s[:]) }
	if err := reg.Upsert("v1", auth.DynamicKeyEntry{ID: "k1", KeyHash: hash("tok-k1"), Owner: "acme", Name: "n", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	call := func(method, path string, body map[string]any, id string) *httptest.ResponseRecorder {
		t.Helper()
		raw, _ := json.Marshal(body)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(method, path, bytes.NewReader(raw))
		c.Request.Header.Set("Content-Type", "application/json")
		if id != "" {
			c.Params = gin.Params{{Key: "id", Value: id}}
			h.UpsertAPIKey(c)
		} else {
			h.ReplaceAPIKeys(c)
		}
		return w
	}
	entry := func(id, owner string) map[string]any {
		return map[string]any{"id": id, "key_hash": hash("tok-" + id), "owner": owner, "name": "n", "enabled": true}
	}

	// A replace that drops k1 (a revocation) and adds a key with owner
	// "Default": refused, nothing applied, k1 still authenticates.
	w := call(http.MethodPost, "/admin/api-keys/replace", map[string]any{"version": "v2", "keys": []any{entry("k2", "Default")}}, "")
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), `\"k2\"`) {
		t.Fatalf("replace: %d %s, want 400 naming k2", w.Code, w.Body)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer tok-k1")
	if res, err := reg.Authenticate(req); err != nil || res.KeyID != "k1" {
		t.Errorf("k1 after the refused replace: %+v, %v; want it still in force", res, err)
	}
	// The same on a single upsert.
	body := entry("k3", "admin ")
	body["version"] = "v2"
	if w := call(http.MethodPut, "/admin/api-keys/k3", body, "k3"); w.Code != http.StatusBadRequest {
		t.Fatalf("upsert: %d %s, want 400", w.Code, w.Body)
	}
	// Two entries with one key_hash: each is valid alone, the set is not.
	dup := entry("k5", "acme")
	dup["key_hash"] = hash("tok-k4")
	if w := call(http.MethodPost, "/admin/api-keys/replace", map[string]any{"version": "v2", "keys": []any{entry("k4", "acme"), dup}}, ""); w.Code != http.StatusBadRequest {
		t.Fatalf("replace with a duplicate key_hash: %d %s, want 400", w.Code, w.Body)
	}
	// A stale version is a 409, not a refused push.
	if err := reg.ReplaceAll("v9", []auth.DynamicKeyEntry{{ID: "k1", KeyHash: hash("tok-k1"), Owner: "acme", Name: "n", Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	if w := call(http.MethodPost, "/admin/api-keys/replace", map[string]any{"version": "v3", "keys": []any{entry("k6", "acme")}}, ""); w.Code != http.StatusConflict {
		t.Fatalf("stale replace: %d %s, want 409", w.Code, w.Body)
	}
	if want := []string{"replace", "upsert", "replace"}; strings.Join(rejected, ",") != strings.Join(want, ",") {
		t.Errorf("rejections counted %v, want %v", rejected, want)
	}
}
