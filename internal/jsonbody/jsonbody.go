// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

// Package jsonbody edits top-level members of a JSON request body in place,
// leaving every other byte as the client sent it. It is a byte scanner rather
// than a decode/re-encode: request bodies can be hundreds of KB, and the
// router forwards them verbatim apart from the member being changed.
package jsonbody

import (
	"bytes"
	"encoding/json"
	"errors"
)

var (
	// ErrNotObject: the body is not a JSON object.
	ErrNotObject = errors.New("request body must be a JSON object")
	// ErrMalformed: the body is not well-formed JSON.
	ErrMalformed = errors.New("malformed JSON object")
	// ErrNoKey: ReplaceTopLevel found no member with the key.
	ErrNoKey = errors.New("no such top-level member")
)

// member is one top-level "key": value pair, as offsets into the body.
type member struct {
	start    int // index of the key's opening quote
	valStart int // index of the value's first byte
	end      int // index just past the value
}

// object is a scanned top-level object.
type object struct {
	open, close int // indexes of '{' and '}'
	members     []member
}

// scan parses the top-level object of body. It rejects anything that is not a
// single object followed only by whitespace.
func scan(body []byte) (object, error) {
	var o object
	i := skipWS(body, 0)
	if i >= len(body) || body[i] != '{' {
		return o, ErrNotObject
	}
	o.open = i
	i++
	for {
		i = skipWS(body, i)
		if i < len(body) && body[i] == '}' && len(o.members) == 0 {
			break
		}
		if i >= len(body) || body[i] != '"' {
			return o, ErrMalformed
		}
		m := member{start: i}
		if i = skipString(body, i); i < 0 {
			return o, ErrMalformed
		}
		i = skipWS(body, i)
		if i >= len(body) || body[i] != ':' {
			return o, ErrMalformed
		}
		i = skipWS(body, i+1)
		m.valStart = i
		if i = skipValue(body, i); i < 0 {
			return o, ErrMalformed
		}
		m.end = i
		o.members = append(o.members, m)
		i = skipWS(body, i)
		if i < len(body) && body[i] == ',' {
			i++
			continue
		}
		if i < len(body) && body[i] == '}' {
			break
		}
		return o, ErrMalformed
	}
	o.close = i
	if skipWS(body, i+1) != len(body) {
		return o, ErrMalformed
	}
	return o, nil
}

// keyOf returns the quoted key of m.
func keyOf(body []byte, m member) []byte {
	return body[m.start:skipString(body, m.start)]
}

// isKey reports whether a quoted JSON key decodes to name (escaped spellings
// such as "model" included, as encoding/json would decode them).
func isKey(quoted []byte, name string) bool {
	if len(quoted) == len(name)+2 && string(quoted[1:len(quoted)-1]) == name {
		return true
	}
	if !bytes.ContainsRune(quoted, '\\') {
		return false
	}
	var k string
	return json.Unmarshal(quoted, &k) == nil && k == name
}

// ReplaceTopLevel replaces the value of the top-level member key with raw (an
// already-encoded JSON value). With duplicate keys it replaces the last one,
// the one encoding/json decodes. Every other byte is kept. Returns ErrNoKey
// when the body has no such member.
func ReplaceTopLevel(body []byte, key string, raw []byte) ([]byte, error) {
	o, err := scan(body)
	if err != nil {
		return nil, err
	}
	target := -1
	for i, m := range o.members {
		if isKey(keyOf(body, m), key) {
			target = i
		}
	}
	if target < 0 {
		return nil, ErrNoKey
	}
	m := o.members[target]
	out := make([]byte, 0, len(body)-(m.end-m.valStart)+len(raw))
	out = append(out, body[:m.valStart]...)
	out = append(out, raw...)
	return append(out, body[m.end:]...), nil
}

func skipWS(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// skipString returns the index just past the string starting at b[i] == '"',
// or -1 when it is not terminated.
func skipString(b []byte, i int) int {
	for i++; i < len(b); i++ {
		switch b[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return -1
}

// skipValue returns the index just past the JSON value starting at b[i], or
// -1. Scalars are delimited, not validated: the body has already been decoded
// by encoding/json before any edit.
func skipValue(b []byte, i int) int {
	if i >= len(b) {
		return -1
	}
	switch b[i] {
	case '"':
		return skipString(b, i)
	case '{', '[':
		depth := 0
		for ; i < len(b); i++ {
			switch b[i] {
			case '"':
				end := skipString(b, i)
				if end < 0 {
					return -1
				}
				i = end - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return -1
	default: // number, true, false, null
		start := i
		for i < len(b) && b[i] != ',' && b[i] != '}' && b[i] != ']' && b[i] != ' ' && b[i] != '\t' && b[i] != '\n' && b[i] != '\r' {
			i++
		}
		if i == start {
			return -1
		}
		return i
	}
}
