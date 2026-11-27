// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain_test

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"

	"hivenet_router/internal/domain"
)

func encode(t *testing.T, format string, w, h int) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	img.Set(0, 0, color.RGBA{1, 2, 3, 255})
	var b bytes.Buffer
	var err error
	switch format {
	case "png":
		err = png.Encode(&b, img)
	case "jpeg":
		err = jpeg.Encode(&b, img, nil)
	case "gif":
		err = gif.Encode(&b, img, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

// webpHeader is the start of an extended (VP8X) WebP file of w x h: enough
// for its size to be read.
func webpHeader(w, h int) string {
	var b bytes.Buffer
	b.WriteString("RIFF")
	_ = binary.Write(&b, binary.LittleEndian, uint32(4+8+10))
	b.WriteString("WEBPVP8X")
	_ = binary.Write(&b, binary.LittleEndian, uint32(10))
	b.Write([]byte{0, 0, 0, 0}) // flags and reserved
	put24 := func(v int) { b.Write([]byte{byte(v), byte(v >> 8), byte(v >> 16)}) }
	put24(w - 1)
	put24(h - 1)
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

func requestWith(t *testing.T, parts ...string) *domain.ChatRequest {
	t.Helper()
	body := `{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"what is it?"},` + strings.Join(parts, ",") + `]}]}`
	var req domain.ChatRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	return &req
}

func openAIImage(url string) string {
	return `{"type":"image_url","image_url":{"url":"` + url + `"}}`
}

// TestImageInfos_ReadsInlineImageHeaders: the size of an inline image is read
// from its header, in both dialects; an image given by URL, a non-base64 data
// URL and an unreadable image are unknown, and keep their place in order.
func TestImageInfos_ReadsInlineImageHeaders(t *testing.T) {
	req := requestWith(t,
		openAIImage("data:image/png;base64,"+encode(t, "png", 640, 480)),
		openAIImage("data:image/jpeg;base64,"+encode(t, "jpeg", 1920, 1080)),
		openAIImage("data:image/gif;base64,"+encode(t, "gif", 31, 17)),
		openAIImage("data:image/webp;base64,"+webpHeader(4000, 3000)),
		openAIImage("https://example.com/cat.png"),
		openAIImage("data:image/svg+xml,<svg/>"),
		openAIImage("data:image/png;base64,bm90IGFuIGltYWdl"),
		`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"`+encode(t, "png", 300, 1200)+`"}}`,
		`{"type":"image","source":{"type":"url","url":"https://example.com/dog.jpg"}}`,
	)
	sz := func(w, h int) domain.ImageInfo { return domain.ImageInfo{Width: w, Height: h} }
	want := []domain.ImageInfo{sz(640, 480), sz(1920, 1080), sz(31, 17), sz(4000, 3000), {}, {}, {}, sz(300, 1200), {}}
	got := req.ImageInfos()
	if len(got) != len(want) {
		t.Fatalf("%d images, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Width != want[i].Width || got[i].Height != want[i].Height {
			t.Errorf("image %d: %v, want %v", i, got[i], want[i])
		}
		// Every image here has something to identify it by but the
		// non-base64 data URL... which is hashed as a URL too.
		if got[i].ID == 0 {
			t.Errorf("image %d has no ID", i)
		}
	}
	if req.ImageInfos()[0] != got[0] {
		t.Error("a second call changed the result")
	}
	// The same image sent again, inline or by URL, has the same ID; another
	// image has another.
	again := requestWith(t, openAIImage("https://example.com/cat.png"), openAIImage("https://example.com/dog.png")).ImageInfos()
	if again[0].ID != got[4].ID || again[1].ID == got[4].ID {
		t.Errorf("URL IDs: %d %d vs %d", again[0].ID, again[1].ID, got[4].ID)
	}
}

// TestImageInfos_BoundedHeaderRead: a JPEG whose size marker sits past the
// bytes the router reads (a large metadata segment first) reads as unknown
// instead of being decoded further.
func TestImageInfos_BoundedHeaderRead(t *testing.T) {
	raw, _ := base64.StdEncoding.DecodeString(encode(t, "jpeg", 64, 64))
	// Insert APP1 segments (max 65533 bytes of data each) after SOI.
	var b bytes.Buffer
	b.Write(raw[:2])
	for i := 0; i < 2; i++ {
		b.Write([]byte{0xFF, 0xE1, 0xFF, 0xFF})
		b.Write(make([]byte, 0xFFFF-2))
	}
	b.Write(raw[2:])
	req := requestWith(t, openAIImage("data:image/jpeg;base64,"+base64.StdEncoding.EncodeToString(b.Bytes())))
	if got := req.ImageInfos(); len(got) != 1 || got[0].Known() {
		t.Errorf("%v, want one unknown image", got)
	}
}

// TestImageInfos_TextOnly: a request without images has none.
func TestImageInfos_TextOnly(t *testing.T) {
	var req domain.ChatRequest
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`), &req)
	if got := req.ImageInfos(); len(got) != 0 {
		t.Errorf("%v, want none", got)
	}
}
