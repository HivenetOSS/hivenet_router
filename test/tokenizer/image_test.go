// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package tokenizer_test

import (
	"math"
	"testing"

	"hivenet_router/internal/domain"
	"hivenet_router/internal/tokenizer"
)

func img(w, h int) domain.ImageInfo { return domain.ImageInfo{Width: w, Height: h} }

// url is an image of unknown size with an identity, as given by URL.
func url(id uint64) domain.ImageInfo { return domain.ImageInfo{ID: id} }

// Image tokens measured on vLLM 0.31 (prompt_tokens with one image minus
// without), for three ways of turning an image into tokens.
var (
	qwen25VL = map[domain.ImageInfo]int{ // grows with pixels (~1 per 784)
		img(224, 224): 66, img(512, 512): 326, img(640, 480): 393, img(1024, 768): 1001,
		img(1280, 720): 1198, img(1920, 1080): 2693, img(3000, 2000): 7599, img(4000, 3000): 15303,
	}
	internVL3 = map[domain.ImageInfo]int{ // 448px tiles chosen by aspect ratio
		img(224, 224): 259, img(512, 512): 259, img(640, 480): 3331, img(1024, 768): 3331,
		img(1280, 720): 2307, img(1920, 1080): 2307, img(3000, 2000): 1795, img(4000, 3000): 3331,
	}
	llava15 = map[domain.ImageInfo]int{ // fixed
		img(224, 224): 579, img(512, 512): 579, img(640, 480): 579, img(1920, 1080): 579, img(4000, 3000): 579,
	}
)

// TestImageEstimator_Default: a model with nothing learned gets the default
// per image, for the point estimate and the upper bound alike.
func TestImageEstimator_Default(t *testing.T) {
	e := tokenizer.NewImageEstimator(1000)
	p, u := e.Estimate("m", "k", []domain.ImageInfo{img(640, 480), {}})
	if p != 2000 || u != 2000 {
		t.Errorf("(%d, %d), want (2000, 2000)", p, u)
	}
	if p, u := e.Estimate("m", "k", nil); p != 0 || u != 0 {
		t.Errorf("no images: (%d, %d), want (0, 0)", p, u)
	}
	if p, _ := tokenizer.NewImageEstimator(0).Estimate("m", "k", []domain.ImageInfo{{}}); p != tokenizer.DefaultImageTokens {
		t.Errorf("default %d, want %d", p, tokenizer.DefaultImageTokens)
	}
}

// TestImageEstimator_LearnsEachKindOfModel: after one request per size, every
// learned size is estimated exactly, whatever the model does with pixels.
func TestImageEstimator_LearnsEachKindOfModel(t *testing.T) {
	for name, truth := range map[string]map[domain.ImageInfo]int{"qwen2.5-vl": qwen25VL, "internvl3": internVL3, "llava-1.5": llava15} {
		e := tokenizer.NewImageEstimator(1024)
		for size, tokens := range truth {
			e.Observe(name, "k", []domain.ImageInfo{size}, tokens)
		}
		for size, tokens := range truth {
			if p, u := e.Estimate(name, "k", []domain.ImageInfo{size}); p != tokens || u != tokens {
				t.Errorf("%s %v: (%d, %d), want %d", name, size, p, u, tokens)
			}
		}
	}
}

// TestImageEstimator_UnseenSizeScalesByPixels: a size not seen yet is
// estimated from the learned tokens per pixel, within the fewest and most
// tokens seen for one image; its upper bound keeps 1.5x headroom, at most the
// most seen.
func TestImageEstimator_UnseenSizeScalesByPixels(t *testing.T) {
	e := tokenizer.NewImageEstimator(1024)
	e.Observe("qwen", "k", []domain.ImageInfo{img(1024, 768)}, 1001)
	e.Observe("qwen", "k", []domain.ImageInfo{img(1920, 1080)}, 2693)
	p, u := e.Estimate("qwen", "k", []domain.ImageInfo{img(1280, 720)}) // truth 1198
	if math.Abs(float64(p-1198))/1198 > 0.1 {
		t.Errorf("1280x720 estimated %d, want within 10%% of 1198", p)
	}
	if u != min(2693, (p*3+1)/2) && u != min(2693, p*3/2) {
		t.Errorf("upper %d, want 1.5x the estimate %d, at most the most seen 2693", u, p)
	}
	// Clamped: a tiny image is never estimated below the fewest seen.
	if p, _ := e.Estimate("qwen", "k", []domain.ImageInfo{img(10, 10)}); p != 1001 {
		t.Errorf("10x10 estimated %d, want the fewest seen, 1001", p)
	}
	// A fixed-cost model stays fixed at any size.
	f := tokenizer.NewImageEstimator(1024)
	f.Observe("llava", "k", []domain.ImageInfo{img(512, 512)}, 579)
	if p, _ := f.Estimate("llava", "k", []domain.ImageInfo{img(4000, 3000)}); p != 579 {
		t.Errorf("llava 4000x3000 estimated %d, want 579", p)
	}
}

// TestImageEstimator_UnknownSize: an image of unknown size is estimated at
// the model's typical image, with headroom for the occupancy budget (1.5x,
// at most the most seen); images sent only by URL teach it too.
func TestImageEstimator_UnknownSize(t *testing.T) {
	e := tokenizer.NewImageEstimator(1024)
	e.Observe("m", "k", []domain.ImageInfo{img(512, 512)}, 300)
	e.Observe("m", "k", []domain.ImageInfo{img(4000, 3000)}, 15000)
	p, u := e.Estimate("m", "k", []domain.ImageInfo{{}})
	if p <= 300 || p >= 15000 || u != min(15000, p*3/2) {
		t.Errorf("unknown size: (%d, %d), want 300 < point < 15000 and upper 1.5x point", p, u)
	}
	urls := tokenizer.NewImageEstimator(1024)
	for i := 0; i < 20; i++ {
		urls.Observe("m", "k", []domain.ImageInfo{url(uint64(i + 1))}, 579)
	}
	if p, u := urls.Estimate("m", "k", []domain.ImageInfo{{}}); p != 579 || u != 579 {
		t.Errorf("learned from URLs only: (%d, %d), want 579", p, u)
	}
}

// TestImageEstimator_RemembersEachImage: an image a chat sends again (by URL
// or inline) is estimated at exactly what it cost, and in a request that
// mixes it with new images the new ones get the rest.
func TestImageEstimator_RemembersEachImage(t *testing.T) {
	e := tokenizer.NewImageEstimator(1024)
	e.Observe("qwen", "k", []domain.ImageInfo{url(1)}, 1001)
	if p, u := e.Estimate("qwen", "k", []domain.ImageInfo{url(1)}); p != 1001 || u != 1001 {
		t.Errorf("same URL: (%d, %d), want 1001", p, u)
	}
	// Turn 2: the same image and a new one; 1001 + 15303.
	e.Observe("qwen", "k", []domain.ImageInfo{url(1), url(2)}, 1001+15303)
	if p, _ := e.Estimate("qwen", "k", []domain.ImageInfo{url(2)}); p != 15303 {
		t.Errorf("new image of turn 2: %d, want the rest, 15303", p)
	}
	if p, _ := e.Estimate("other-model", "k", []domain.ImageInfo{url(1)}); p != 1024 {
		t.Errorf("same URL on another model: %d, want the default", p)
	}
}

// TestImageEstimator_PerKeyTypicalImage: images of unknown size are
// estimated at the sending key's typical image once it has sent a few: a
// client sending thumbnails is not charged for another's photos.
func TestImageEstimator_PerKeyTypicalImage(t *testing.T) {
	e := tokenizer.NewImageEstimator(1024)
	for i := 0; i < 5; i++ {
		e.Observe("qwen", "photos", []domain.ImageInfo{url(uint64(100 + i))}, 15303)
		e.Observe("qwen", "thumbs", []domain.ImageInfo{url(uint64(200 + i))}, 66)
	}
	if p, _ := e.Estimate("qwen", "thumbs", []domain.ImageInfo{url(999)}); p != 66 {
		t.Errorf("thumbs key: %d, want 66", p)
	}
	if p, _ := e.Estimate("qwen", "photos", []domain.ImageInfo{url(999)}); p != 15303 {
		t.Errorf("photos key: %d, want 15303", p)
	}
	if p, _ := e.Estimate("qwen", "new-key", []domain.ImageInfo{url(999)}); p == 66 || p == 15303 {
		t.Errorf("a key with no samples: %d, want the model's typical image", p)
	}
}

// TestImageEstimator_SplitsInProportion: the tokens of a request with several
// images are shared in proportion to their estimates, so a known pair teaches
// each image its own cost.
func TestImageEstimator_SplitsInProportion(t *testing.T) {
	e := tokenizer.NewImageEstimator(1024)
	small, big := img(640, 480), img(1920, 1080)
	e.Observe("qwen", "k", []domain.ImageInfo{small}, 393)
	e.Observe("qwen", "k", []domain.ImageInfo{big}, 2693)
	for i := 0; i < 10; i++ {
		e.Observe("qwen", "k", []domain.ImageInfo{small, big}, 393+2693)
	}
	ps, _ := e.Estimate("qwen", "k", []domain.ImageInfo{small})
	pb, _ := e.Estimate("qwen", "k", []domain.ImageInfo{big})
	if ps != 393 || pb != 2693 {
		t.Errorf("after pairs: small %d big %d, want 393 and 2693", ps, pb)
	}
}

// TestImageEstimator_FollowsASettingChange: when a deployment starts sizing
// images down (Qwen max_pixels lowered), the estimate follows within a few
// requests, with no change to the router.
func TestImageEstimator_FollowsASettingChange(t *testing.T) {
	e := tokenizer.NewImageEstimator(1024)
	photo := img(4000, 3000)
	e.Observe("qwen", "k", []domain.ImageInfo{photo}, 15303)
	for i := 0; i < 10; i++ {
		e.Observe("qwen", "k", []domain.ImageInfo{photo}, 1232)
	}
	if p, _ := e.Estimate("qwen", "k", []domain.ImageInfo{photo}); math.Abs(float64(p-1232))/1232 > 0.5 {
		t.Errorf("after the change: %d, want close to 1232", p)
	}
}
