// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// A credential never comes out through fmt, JSON or text marshalling, also
// inside the struct that holds it: only Reveal returns it.
func TestSecretNeverPrints(t *testing.T) {
	const key = "sk-live-0123456789abcdef"
	fb := &ExternalFallback{Provider: "openai", Model: "m", APIKey: NewSecret(key)}
	outs := []string{
		fmt.Sprint(fb.APIKey), fmt.Sprintf("%v %+v %#v %s %q %x", fb.APIKey, fb.APIKey, fb.APIKey, fb.APIKey, fb.APIKey, fb.APIKey),
		fmt.Sprintf("%v %+v", fb, *fb), fmt.Sprintf("%#v", *fb),
	}
	j, _ := json.Marshal(fb)
	tx, _ := fb.APIKey.MarshalText()
	outs = append(outs, string(j), string(tx))
	for _, o := range outs {
		if strings.Contains(o, "0123456789") {
			t.Errorf("credential leaked: %s", o)
		}
	}
	if !strings.Contains(string(j), `"api_key":"sk-...def"`) {
		t.Errorf("JSON %s, want the preview sk-...def", j)
	}
	if fb.APIKey.Reveal() != key {
		t.Error("Reveal must return the credential")
	}
	if NewSecret("short").Preview() != "***" || NewSecret("").Preview() != "" {
		t.Error("short or empty credentials must not be previewed")
	}
}
