// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Black-box tests for the admin key API with a persistent key store: a
// mutation that could not be persisted answers 503 (retryable for the
// upstream key-sync service) instead of 200, and the version endpoint
// exposes the restored/confirmed/dirty state.
package api_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"hivenet_router/internal/auth"
	"hivenet_router/internal/policy"

	"github.com/gin-gonic/gin"
)

// failingStore fails every write while fail is set.
type failingStore struct {
	mu   sync.Mutex
	fail bool
}

func (s *failingStore) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("disk full")
	}
	return nil
}
func (s *failingStore) Load() (*auth.PersistedRegistry, error) { return nil, nil }
func (s *failingStore) Upsert(string, auth.DynamicKeyEntry, time.Time) error {
	return s.err()
}
func (s *failingStore) Delete(string, string, time.Time) error { return s.err() }
func (s *failingStore) ReplaceAll(string, []auth.DynamicKeyEntry, time.Time) error {
	return s.err()
}
func (s *failingStore) DeleteRecords([]string) error { return nil }
func (s *failingStore) Close() error                 { return nil }

func TestAdminKeys_PersistFailureAnswers503(t *testing.T) {
	h, reg := newAdminKeyHandlers(&policy.Policy{})
	reg.SetPersistRetryBackoff(time.Hour, time.Hour)
	fs := &failingStore{}
	reg.SetKeyStore(fs)
	t.Cleanup(func() { reg.CloseKeyStore() }) //nolint:errcheck

	if w := upsertKey(t, h, map[string]any{}, 0); w.Code != http.StatusOK {
		t.Fatalf("upsert with a healthy store: %d %s", w.Code, w.Body.String())
	}

	fs.mu.Lock()
	fs.fail = true
	fs.mu.Unlock()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodDelete, "/admin/api-keys/k1?version=v2", nil)
	c.Params = gin.Params{{Key: "id", Value: "k1"}}
	h.DeleteAPIKey(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("a revocation that was not persisted must answer 503, got %d %s", w.Code, w.Body.String())
	}
	if _, ok := reg.GetKey("k1"); ok {
		t.Fatal("the revocation must still be in effect in memory")
	}

	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/api-keys/replace", strings.NewReader(`{"version":"v3","keys":[]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.ReplaceAPIKeys(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("a replace that was not persisted must answer 503, got %d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/admin/api-keys/version", nil)
	h.GetAPIKeyVersion(c)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["version"] != "v3" || body["persistent"] != true || body["dirty"] != true || body["confirmed"] != true || body["restored"] != false {
		t.Fatalf("version endpoint = %v", body)
	}
}
