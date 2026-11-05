// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package jsonbody_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"hivenet_router/internal/jsonbody"
)

// TestSetTopLevel: the body ends up with exactly one top-level member of the
// key, with the given value, first; nested members of the same name are not
// touched; with nothing to remove the rest is byte-identical; the input is
// never modified.
func TestSetTopLevel(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		// Absent: inserted after "{", the rest exactly as sent.
		{"absent", `{"model":"m",  "messages":[]}`, `{"k":"v","model":"m",  "messages":[]}`},
		{"empty object", `{}`, `{"k":"v"}`},
		{"empty object with space", ` { } `, ` {"k":"v" } `},
		// Present once, last, duplicated, escaped spelling: all removed.
		{"present", `{"k":"client","model":"m"}`, `{"k":"v","model":"m"}`},
		{"present last", `{"model":"m","k":"client"}`, `{"k":"v","model":"m"}`},
		{"duplicated", `{"k":"a","model":"m","k":"b"}`, `{"k":"v","model":"m"}`},
		{"escaped spelling", `{"k":"client","model":"m"}`, `{"k":"v","model":"m"}`},
		{"only member", `{"k":{"nested":[1,2]}}`, `{"k":"v"}`},
		// Nested members of the same name are part of a value: kept.
		{"nested", `{"messages":[{"k":"x"}],"meta":{"k":1}}`, `{"k":"v","messages":[{"k":"x"}],"meta":{"k":1}}`},
		// Values are kept byte for byte, whitespace inside them included.
		{"value bytes kept", `{"k":1, "a": [ 1 , 2 ]}`, `{"k":"v","a": [ 1 , 2 ]}`},
		{"string with brace", `{"a":"}{\"k\":","k":0}`, `{"k":"v","a":"}{\"k\":"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := []byte(tc.in)
			orig := bytes.Clone(in)
			got, err := jsonbody.SetTopLevel(in, "k", []byte(`"v"`))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
			if !bytes.Equal(in, orig) {
				t.Error("input modified")
			}
			if !json.Valid(got) {
				t.Errorf("result is not valid JSON: %s", got)
			}
		})
	}

	for _, bad := range []string{``, `[]`, `"s"`, `42`, `null`, `{`, `{"a":1`, `{"a":1,}`, `{"a"}`, `{"a":}`, `{"a":"x}`, `{"a":1} x`, `{"a":1}{}`} {
		if _, err := jsonbody.SetTopLevel([]byte(bad), "k", []byte(`"v"`)); err == nil {
			t.Errorf("SetTopLevel(%q) accepted a body that is not one JSON object", bad)
		}
	}
	if _, err := jsonbody.SetTopLevel([]byte(`[1]`), "k", []byte(`1`)); !errors.Is(err, jsonbody.ErrNotObject) {
		t.Errorf("array body: err %v, want ErrNotObject", err)
	}
}

// TestRemoveTopLevel: every top-level member of the key is removed; with none
// present the body is returned as is.
func TestRemoveTopLevel(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"model":"m"}`, `{"model":"m"}`},
		{`{"k":1}`, `{}`},
		{`{"k":1,"model":"m","k":2}`, `{"model":"m"}`},
		{`{"model":"m","k":{"a":"}"}}`, `{"model":"m"}`},
		{`{"a":{"k":1}}`, `{"a":{"k":1}}`},
	} {
		got, err := jsonbody.RemoveTopLevel([]byte(tc.in), "k")
		if err != nil || string(got) != tc.want {
			t.Errorf("RemoveTopLevel(%s) = %s, %v; want %s", tc.in, got, err, tc.want)
		}
	}
	in := []byte(`{"model":"m"}`)
	if got, _ := jsonbody.RemoveTopLevel(in, "k"); &got[0] != &in[0] {
		t.Error("nothing to remove: want the input slice back")
	}
}

// TestReplaceTopLevel: the value of the last occurrence is replaced (the one
// encoding/json decodes); a missing key is ErrNoKey.
func TestReplaceTopLevel(t *testing.T) {
	got, err := jsonbody.ReplaceTopLevel([]byte(`{"model":"a", "x":1, "model":"b"}`), "model", []byte(`"c"`))
	if err != nil || string(got) != `{"model":"a", "x":1, "model":"c"}` {
		t.Errorf("got %s, %v", got, err)
	}
	if _, err := jsonbody.ReplaceTopLevel([]byte(`{"x":1}`), "model", []byte(`"c"`)); !errors.Is(err, jsonbody.ErrNoKey) {
		t.Errorf("missing key: err %v, want ErrNoKey", err)
	}
}

// FuzzSetTopLevel: for any JSON object, the result decodes to the input's
// members minus the key, plus the key with the new value; nothing else is
// added, lost or changed.
func FuzzSetTopLevel(f *testing.F) {
	for _, s := range []string{`{}`, `{"k":1}`, `{"a":"x","k":[1,{"k":2}],"b":null}`, `{"k":1,"k":2}`, `{"k":true}`, ` {"a" : 1 } `} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		var want map[string]any
		if json.Unmarshal(in, &want) != nil || want == nil {
			return // not a JSON object: an error is the right answer
		}
		got, err := jsonbody.SetTopLevel(in, "k", []byte(`"salt"`))
		if err != nil {
			// The scanner delimits scalars without validating them, so it
			// may only refuse what encoding/json also refuses.
			t.Fatalf("SetTopLevel refused a valid object %q: %v", in, err)
		}
		var out map[string]any
		if err := json.Unmarshal(got, &out); err != nil {
			t.Fatalf("result %q is not valid JSON: %v", got, err)
		}
		want["k"] = "salt"
		if !reflect.DeepEqual(out, want) {
			t.Fatalf("in %q\ngot  %v\nwant %v", in, out, want)
		}
		if n := bytes.Count(got, []byte(`"salt"`)); n < 1 {
			t.Fatalf("salt missing from %q", got)
		}
	})
}
