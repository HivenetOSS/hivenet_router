// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package jsonbody_test

import (
	"errors"
	"slices"
	"testing"

	"hivenet_router/internal/jsonbody"
)

// TestNames: top-level names come back decoded and in order, repeats
// included; nested names are not top-level.
func TestNames(t *testing.T) {
	got, err := jsonbody.Names([]byte(`{"a":1,"model":"m","n":{"model":"x"},"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "model", "n", "a"}; !slices.Equal(got, want) {
		t.Errorf("Names = %v, want %v", got, want)
	}
	for _, body := range []string{``, `[1]`, `{"a":1} {"b":2}`, `{"a"`} {
		if _, err := jsonbody.Names([]byte(body)); err == nil {
			t.Errorf("Names(%q) accepted", body)
		}
	}
}

// TestCheckNames: a body is refused when encoding/json and an exact-match
// reader could read different values: a repeated top-level name (of any
// field, once decoded), or a case-folded spelling of a field the caller
// reads, Unicode folds included. Case variants of fields the caller does
// not read, nested names, and exact names pass.
func TestCheckNames(t *testing.T) {
	fields := []string{"model", "max_tokens", "stream"}
	for _, tc := range []struct {
		name, body, field string // field: the NameError.Field wanted; "-" for a repeat
	}{
		{"model and Model", `{"model":"coder","Model":"chat"}`, "model"},
		{"Model alone", `{"Model":"chat"}`, "model"},
		{"MODEL", `{"MODEL":"chat"}`, "model"},
		{"Kelvin sign", `{"max_toKens":1}`, "max_tokens"},
		{"long s", `{"ſtream":true}`, "stream"},
		{"escaped variant", `{"Model":"chat"}`, "model"},
		{"model twice", `{"model":"a","model":"b"}`, "-"},
		{"escaped repeat", `{"model":"a","model":"b"}`, "-"},
		{"unread name twice", `{"temperature":1,"temperature":2}`, "-"},
	} {
		err := jsonbody.CheckNames([]byte(tc.body), fields)
		var ne *jsonbody.NameError
		if !errors.As(err, &ne) {
			t.Errorf("%s: err %v, want a NameError", tc.name, err)
			continue
		}
		if want := tc.field; (want == "-" && ne.Field != "") || (want != "-" && ne.Field != want) {
			t.Errorf("%s: NameError %+v, want field %q", tc.name, ne, want)
		}
	}
	for _, body := range []string{
		`{"model":"m","max_tokens":5,"stream":true}`,
		`{"model":"m","Temperature":1,"temperature":2}`,
		`{"model":"m","messages":[{"Model":"x","model":"y"}]}`,
		`{}`,
	} {
		if err := jsonbody.CheckNames([]byte(body), fields); err != nil {
			t.Errorf("CheckNames(%s) = %v, want nil", body, err)
		}
	}
	if err := jsonbody.CheckNames([]byte(`{"model":"m"} {"model":"n"}`), fields); err == nil || errors.As(err, new(*jsonbody.NameError)) {
		t.Errorf("trailing data: err %v, want a scan error", err)
	}
}

// TestFieldNames: the names encoding/json decodes into a struct: tag names
// (options dropped), Go names for untagged fields, embedded structs'
// fields promoted, "-" and unexported fields left out.
func TestFieldNames(t *testing.T) {
	type Inner struct {
		Deep string `json:"deep"`
	}
	type outer struct {
		Inner
		*Ptr
		Model  string `json:"model"`
		Max    int    `json:"max_tokens,omitempty"`
		Plain  bool
		Hidden string `json:"-"`
		secret string
	}
	got := jsonbody.FieldNames(outer{secret: "never decoded"})
	if want := []string{"deep", "p", "model", "max_tokens", "Plain"}; !slices.Equal(got, want) {
		t.Errorf("FieldNames = %v, want %v", got, want)
	}
}

type Ptr struct {
	P int `json:"p"`
}

// TestCheckShape: the name rule applies in every object the shape reaches,
// through arrays and nested objects, with the path of the offending object;
// objects outside the shape, and values of another kind than it describes,
// are not looked into.
func TestCheckShape(t *testing.T) {
	part := jsonbody.ObjectOf([]string{"type", "text"})
	part.Fields["content"] = &jsonbody.Shape{Items: part}
	msg := jsonbody.ObjectOf([]string{"role", "content"})
	msg.Fields["content"] = &jsonbody.Shape{Items: part}
	top := jsonbody.ObjectOf([]string{"model", "messages", "tools"})
	top.Fields["messages"] = &jsonbody.Shape{Items: msg}
	for _, tc := range []struct{ body, path string }{
		{`{"messages":[{"role":"user","content":"a","Content":"b"}]}`, "messages[0]"},
		{`{"messages":[{"role":"u"},{"role":"u","role":"v"}]}`, "messages[1]"},
		{`{"messages":[{"content":[{"type":"text","Text":"x"}]}]}`, "messages[0].content[0]"},
		{`{"messages":[{"content":[{"content":[{"TYPE":"x"}]}]}]}`, "messages[0].content[0].content[0]"},
	} {
		var ne *jsonbody.NameError
		if err := jsonbody.CheckShape([]byte(tc.body), top); !errors.As(err, &ne) || ne.Path != tc.path {
			t.Errorf("%s: err %v, want a NameError at %s", tc.body, err, tc.path)
		}
	}
	for _, body := range []string{
		`{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
		`{"messages":[{"content":"plain string, not parts"}]}`,
		`{"tools":[{"parameters":{"A":1,"a":2}}]}`,
		`{"messages":"not an array"}`,
		`{"messages":[]}`,
	} {
		if err := jsonbody.CheckShape([]byte(body), top); err != nil {
			t.Errorf("CheckShape(%s) = %v, want nil", body, err)
		}
	}
}
