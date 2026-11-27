// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package domain

import (
	"encoding/base64"
	"hash/fnv"
	"image"
	_ "image/gif"  // registers the GIF header decoder for image.DecodeConfig
	_ "image/jpeg" // registers the JPEG header decoder
	_ "image/png"  // registers the PNG header decoder
	"io"
	"strings"

	_ "golang.org/x/image/webp" // registers the WebP header decoder
)

// ImageInfo is the size of one image in a request: 0 x 0 when the router
// cannot tell, as for an image given by URL (the router never fetches one).
// ID identifies the image across requests (a chat resends its earlier images
// with every turn): a hash of its URL, or of an inline image's length and
// first and last bytes; 0 when there is nothing to identify it by.
type ImageInfo struct {
	Width, Height int
	ID            uint64
}

// Known reports whether the image's size was read.
func (i ImageInfo) Known() bool { return i.Width > 0 && i.Height > 0 }

// maxImageHeaderBytes bounds how much of an inline image is decoded to read
// its size: the header of a PNG, GIF or WebP is in its first bytes; a JPEG's
// size marker follows its metadata (EXIF), usually within 64 KiB. An image
// whose size is not found in that much reads as unknown.
const maxImageHeaderBytes = 64 << 10

// ImageSource is an Anthropic image block's source: inline base64 data, or a
// URL.
type ImageSource struct {
	Type      string `json:"type"` // "base64" or "url"
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

// ImageInfos returns the size of every image in the request's messages, in
// order: OpenAI "image_url" parts and Anthropic "image" blocks. An inline
// base64 image is sized from its header alone (PNG, JPEG, GIF, WebP), without
// decoding its pixels; an image given by URL, or one whose header cannot be
// read, is unknown. Computed once per request.
func (r *ChatRequest) ImageInfos() []ImageInfo {
	if r.imagesRead {
		return r.images
	}
	r.imagesRead = true
	for _, m := range r.Messages {
		parts, ok := m.Content.([]ContentPart)
		if !ok {
			continue
		}
		for _, p := range parts {
			switch p.Type {
			case "image_url":
				var info ImageInfo
				if p.ImageURL != nil {
					if payload := dataURLPayload(p.ImageURL.URL); payload != "" {
						info = inlineImageSize(payload)
						info.ID = inlineImageID(payload)
					} else {
						info.ID = hashString(p.ImageURL.URL)
					}
				}
				r.images = append(r.images, info)
			case "image":
				var info ImageInfo
				switch {
				case p.Source == nil:
				case p.Source.Type == "base64":
					info = inlineImageSize(p.Source.Data)
					info.ID = inlineImageID(p.Source.Data)
				case p.Source.URL != "":
					info.ID = hashString(p.Source.URL)
				}
				r.images = append(r.images, info)
			}
		}
	}
	return r.images
}

// dataURLPayload returns the base64 payload of a "data:<type>;base64,<data>"
// URL, or "" for any other URL.
func dataURLPayload(url string) string {
	if !strings.HasPrefix(url, "data:") {
		return ""
	}
	meta, payload, ok := strings.Cut(url, ",")
	if !ok || !strings.HasSuffix(meta, ";base64") {
		return ""
	}
	return payload
}

// inlineImageSize reads an image's size from the start of its base64
// encoding, decoding at most maxImageHeaderBytes.
func inlineImageSize(b64 string) ImageInfo {
	if b64 == "" {
		return ImageInfo{}
	}
	r := io.LimitReader(base64.NewDecoder(base64.StdEncoding, strings.NewReader(b64)), maxImageHeaderBytes)
	cfg, _, err := image.DecodeConfig(r)
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		return ImageInfo{}
	}
	return ImageInfo{Width: cfg.Width, Height: cfg.Height}
}

// imageIDEdge is how much of each end of an inline image's encoding its ID
// hashes, with its length: enough to tell images apart without hashing
// megabytes per request.
const imageIDEdge = 4 << 10

func inlineImageID(b64 string) uint64 {
	if b64 == "" {
		return 0
	}
	h := fnv.New64a()
	var n [8]byte
	for i, l := 0, uint64(len(b64)); i < 8; i++ {
		n[i] = byte(l >> (8 * i))
	}
	_, _ = h.Write(n[:])
	if len(b64) <= 2*imageIDEdge {
		_, _ = io.WriteString(h, b64)
	} else {
		_, _ = io.WriteString(h, b64[:imageIDEdge])
		_, _ = io.WriteString(h, b64[len(b64)-imageIDEdge:])
	}
	return h.Sum64()
}

func hashString(s string) uint64 {
	if s == "" {
		return 0
	}
	h := fnv.New64a()
	_, _ = io.WriteString(h, s)
	return h.Sum64()
}
