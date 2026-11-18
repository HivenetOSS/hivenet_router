// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package jsonbody

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Names returns the names of the top-level members of body, in order and
// with repeats, decoded as encoding/json decodes them ("model" is
// "model").
func Names(body []byte) ([]string, error) {
	o, err := scan(body)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(o.members))
	for _, m := range o.members {
		name, err := decodeName(keyOf(body, m))
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// NameError is a name that two JSON readers could read differently.
type NameError struct {
	Name string // the name as sent
	// Field is the field Name is a variant of; empty when Name is repeated.
	Field string
	// Path is where the object holding Name is ("" for the top level,
	// "messages[2].content[0]" for a content part).
	Path string
}

func (e *NameError) Error() string {
	where := ""
	if e.Path != "" {
		where = " in " + e.Path
	}
	if e.Field == "" {
		return fmt.Sprintf("the field %q appears more than once%s", e.Name, where)
	}
	return fmt.Sprintf("the field %q%s is a differently cased spelling of %q", e.Name, where, e.Field)
}

// Shape describes what a reader decodes of a JSON value: for an object, the
// members it reads, each with the Shape of its value (nil when the reader
// does not look inside it); for an array, the Shape of its elements. A value
// of another kind than its Shape describes, or with no Shape, is not looked
// into: free-form subtrees (tool schemas, metadata) stay out of the check.
type Shape struct {
	Fields map[string]*Shape
	Items  *Shape
}

// ObjectOf is the Shape of an object whose members named fields are read,
// none looked into.
func ObjectOf(fields ...[]string) *Shape {
	s := &Shape{Fields: map[string]*Shape{}}
	for _, names := range fields {
		for _, n := range names {
			s.Fields[n] = nil
		}
	}
	return s
}

// CheckShape refuses a body that encoding/json and an exact-match reader
// (Python's json, pydantic, nlohmann::json: what inference backends use)
// would read differently anywhere shape says it is read: in every object the
// shape reaches, at the top level and nested, a name that appears twice
// (encoding/json keeps the last, some readers the first, some refuse), and a
// name that matches one of the object's read fields only under case folding.
// encoding/json matches names case-insensitively, with Unicode folding
// ("Model", "MODEL", "max_toKens" with a Kelvin sign), in nested objects
// too, while a backend sees only the exact spelling, so the router would act
// on another value than the backend: estimate a prompt from "Content":"hi"
// while the backend reads the "content" next to it. It returns a *NameError,
// or an error from scanning a body that is not one JSON object.
func CheckShape(body []byte, shape *Shape) error {
	if _, err := scan(body); err != nil {
		return err
	}
	return checkValue(body, shape, "")
}

// CheckNames is CheckShape for the top level only: the object's members
// named fields are read, none looked into.
func CheckNames(body []byte, fields []string) error {
	return CheckShape(body, ObjectOf(fields))
}

func checkValue(raw []byte, s *Shape, path string) error {
	if s == nil {
		return nil
	}
	i := skipWS(raw, 0)
	if i >= len(raw) {
		return nil
	}
	switch {
	case raw[i] == '{' && s.Fields != nil:
		o, err := scan(raw)
		if err != nil {
			return err
		}
		seen := make(map[string]bool, len(o.members))
		for _, m := range o.members {
			n, err := decodeName(keyOf(raw, m))
			if err != nil {
				return err
			}
			if seen[n] {
				return &NameError{Name: n, Path: path}
			}
			seen[n] = true
			for f := range s.Fields {
				if n != f && strings.EqualFold(n, f) {
					return &NameError{Name: n, Field: f, Path: path}
				}
			}
			if sub, ok := s.Fields[n]; ok && sub != nil {
				if err := checkValue(raw[m.valStart:m.end], sub, join(path, n)); err != nil {
					return err
				}
			}
		}
	case raw[i] == '[' && s.Items != nil:
		elems, err := elements(raw[i:])
		if err != nil {
			return err
		}
		for k, e := range elems {
			if err := checkValue(e, s.Items, fmt.Sprintf("%s[%d]", path, k)); err != nil {
				return err
			}
		}
	}
	return nil
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// decodeName decodes a quoted member name as encoding/json does.
func decodeName(quoted []byte) (string, error) {
	if !bytes.ContainsRune(quoted, '\\') {
		return string(quoted[1 : len(quoted)-1]), nil
	}
	var name string
	if json.Unmarshal(quoted, &name) != nil {
		return "", ErrMalformed
	}
	return name, nil
}

// elements returns the values of a JSON array starting at raw[0] == '['.
func elements(raw []byte) ([][]byte, error) {
	var out [][]byte
	i := skipWS(raw, 1)
	if i < len(raw) && raw[i] == ']' {
		return out, nil
	}
	for {
		i = skipWS(raw, i)
		end := skipValue(raw, i)
		if end < 0 {
			return nil, ErrMalformed
		}
		out = append(out, raw[i:end])
		i = skipWS(raw, end)
		if i < len(raw) && raw[i] == ',' {
			i++
			continue
		}
		if i < len(raw) && raw[i] == ']' {
			return out, nil
		}
		return nil, ErrMalformed
	}
}

// FieldNames returns the top-level JSON names encoding/json decodes into a
// value of v's struct type: each exported field's tag name, or its Go name
// when the tag has none, with the fields of embedded structs promoted as
// encoding/json promotes them. Fields tagged "-" are left out.
func FieldNames(v any) []string {
	var out []string
	var walk func(t reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return
		}
		for i := range t.NumField() {
			f := t.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, _, _ := strings.Cut(tag, ",")
			if f.Anonymous && name == "" {
				ft := f.Type
				for ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(ft)
					continue
				}
			}
			if !f.IsExported() {
				continue
			}
			if name == "" {
				name = f.Name
			}
			out = append(out, name)
		}
	}
	walk(reflect.TypeOf(v))
	return out
}
