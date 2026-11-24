// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package tokenizer

import (
	"container/list"
	"math"
	"sync"

	"hivenet_router/internal/domain"
)

// DefaultImageTokens is the estimate for an image of a model that has not yet
// had a request with images answered: about what a 1-megapixel image costs on
// a model that sizes images by their pixels (Qwen-VL), and above a fixed-cost
// one (LLaVA 576, Gemma 3 256).
const DefaultImageTokens = 1024

// imageAlpha is the weight of a new sample in the learned values. Higher than
// the text ratio's: an image's cost is a deterministic function of its size,
// so a sample is not noise to average away, and a change of the deployment's
// image settings should be followed within a few requests.
const imageAlpha = 0.3

// Bounds on what the estimator remembers, so a flood of distinct model
// names, keys or images cannot grow it without limit.
const (
	maxImageModels  = 4096
	maxKeysPerModel = 10_000
	maxCachedImages = 100_000
	minKeySamples   = 3   // samples before a key's own mean is trusted
	unknownHeadroom = 1.5 // occupancy reserve for an image of unknown cost, × its estimate
)

// megapixelBounds are the upper bounds of the size buckets, in megapixels.
var megapixelBounds = []float64{0.06, 0.15, 0.3, 0.6, 1.2, 2.5, 5, 10}

// aspectBounds are the upper bounds of the shape buckets (long side / short
// side): square, 4:3, 3:2, 16:9, 2:1 and wider. Tiling models (InternVL,
// Llama 4) choose their tile grid from the aspect ratio, so two images of the
// same megapixels can cost very differently.
var aspectBounds = []float64{1.15, 1.42, 1.6, 1.9, 2.6}

// imageCell is a size bucket crossed with a shape bucket.
type imageCell struct{ size, shape uint8 }

func cellOf(img domain.ImageInfo) imageCell {
	mp := float64(img.Width) * float64(img.Height) / 1e6
	size := len(megapixelBounds)
	for i, b := range megapixelBounds {
		if mp <= b {
			size = i
			break
		}
	}
	long, short := img.Width, img.Height
	if short > long {
		long, short = short, long
	}
	ratio := float64(long) / float64(short)
	shape := len(aspectBounds)
	for i, b := range aspectBounds {
		if ratio <= b {
			shape = i
			break
		}
	}
	return imageCell{uint8(size), uint8(shape)}
}

// meanStat is a running mean of tokens per image.
type meanStat struct {
	mean float64
	n    int
}

func (s *meanStat) add(tokens float64) {
	if s.n == 0 {
		s.mean = tokens
	} else {
		s.mean = imageAlpha*tokens + (1-imageAlpha)*s.mean
	}
	s.n++
}

// imageModel is what the estimator has learned about one model's images.
type imageModel struct {
	cells    map[imageCell]float64 // tokens per image, by size and shape
	perPixel float64               // tokens per pixel, for a cell not learned yet
	min, max float64               // fewest and most tokens seen for one image
	all      meanStat              // tokens per image, of every image
	unknown  meanStat              // tokens per image of unknown size (URLs)
	keys     map[string]*meanStat  // the same, per API key: a client sends similar images
}

// cacheKey is one image on one model.
type cacheKey struct {
	model string
	id    uint64
}

type cacheEntry struct {
	key    cacheKey
	tokens float64
}

// ImageEstimator estimates the prompt tokens of a request's images before it
// is sent, for any engine and model: it learns each model's cost per image
// from the backend's exact prompt_tokens after each answered request, keyed
// by the image's size and shape, without knowing how the model turns pixels
// into tokens. It also remembers what each image cost, so an image a chat
// sends again with every turn is estimated exactly, even one given by URL;
// and for images of unknown size it keeps each API key's typical image.
// Safe for concurrent use.
type ImageEstimator struct {
	mu       sync.Mutex
	fallback float64
	models   map[string]*imageModel
	seen     map[cacheKey]*list.Element // of *cacheEntry, most recent first
	lru      *list.List
}

// NewImageEstimator returns an estimator that estimates defaultTokens per
// image for a model it has learned nothing about (DefaultImageTokens when
// defaultTokens is not positive).
func NewImageEstimator(defaultTokens int) *ImageEstimator {
	if defaultTokens <= 0 {
		defaultTokens = DefaultImageTokens
	}
	return &ImageEstimator{
		fallback: float64(defaultTokens),
		models:   make(map[string]*imageModel),
		seen:     make(map[cacheKey]*list.Element),
		lru:      list.New(),
	}
}

// cached returns what an image cost on model when it was last answered.
func (e *ImageEstimator) cached(model string, img domain.ImageInfo) (float64, bool) {
	if img.ID == 0 {
		return 0, false
	}
	el, ok := e.seen[cacheKey{model, img.ID}]
	if !ok {
		return 0, false
	}
	e.lru.MoveToFront(el)
	return el.Value.(*cacheEntry).tokens, true
}

func (e *ImageEstimator) remember(model string, img domain.ImageInfo, tokens float64) {
	if img.ID == 0 {
		return
	}
	k := cacheKey{model, img.ID}
	if el, ok := e.seen[k]; ok {
		el.Value.(*cacheEntry).tokens = tokens
		e.lru.MoveToFront(el)
		return
	}
	e.seen[k] = e.lru.PushFront(&cacheEntry{k, tokens})
	if e.lru.Len() > maxCachedImages {
		last := e.lru.Back()
		e.lru.Remove(last)
		delete(e.seen, last.Value.(*cacheEntry).key)
	}
}

// estimateOne is the point estimate and the occupancy reserve for one image.
func (e *ImageEstimator) estimateOne(model, key string, m *imageModel, img domain.ImageInfo) (point, upper float64) {
	if v, ok := e.cached(model, img); ok {
		return v, v
	}
	if m == nil {
		return e.fallback, e.fallback
	}
	headroom := func(v float64) float64 { return math.Max(v, math.Min(m.max, unknownHeadroom*v)) }
	if !img.Known() {
		// Size unknown (an image URL): the key's typical image once it has
		// sent a few, else the model's.
		v := m.unknown.mean
		if ks := m.keys[key]; ks != nil && ks.n >= minKeySamples {
			v = ks.mean
		}
		if m.unknown.n == 0 && (m.keys[key] == nil || m.keys[key].n < minKeySamples) {
			v = m.mean()
		}
		return v, headroom(v)
	}
	if v, ok := m.cells[cellOf(img)]; ok {
		return v, v
	}
	// A size not seen yet: scale by pixels, within what the model has
	// produced.
	v := m.perPixel * float64(img.Width) * float64(img.Height)
	v = math.Min(m.max, math.Max(m.min, v))
	return v, headroom(v)
}

// mean is the model's typical image when no image of unknown size has been
// answered yet: the mean of every image it has seen.
func (m *imageModel) mean() float64 { return m.all.mean }

// Estimate returns the estimated prompt tokens of images for model, sent with
// API key key: point for the token quotas, upper (never below point) for the
// occupancy budget, which keeps headroom for images whose cost is a guess.
func (e *ImageEstimator) Estimate(model, key string, images []domain.ImageInfo) (point, upper int) {
	if len(images) == 0 {
		return 0, 0
	}
	e.mu.Lock()
	m := e.models[model]
	var p, u float64
	for _, img := range images {
		pi, ui := e.estimateOne(model, key, m, img)
		p += pi
		u += math.Max(pi, ui)
	}
	e.mu.Unlock()
	return int(math.Ceil(p)), int(math.Ceil(u))
}

// Observe learns from an answered request sent with API key key: its images
// cost tokens prompt tokens in all (the backend's exact prompt_tokens less
// the text's estimate). Images whose cost is remembered take that cost; the
// rest share what remains in proportion to their estimates, since a
// request's image tokens add up per image.
func (e *ImageEstimator) Observe(model, key string, images []domain.ImageInfo, tokens int) {
	if len(images) == 0 || tokens <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.models[model]
	shares := make([]float64, len(images))
	known := make([]bool, len(images))
	var knownSum, guessSum float64
	for i, img := range images {
		if v, ok := e.cached(model, img); ok {
			shares[i], known[i] = v, true
			knownSum += v
			continue
		}
		shares[i], _ = e.estimateOne(model, key, m, img)
		guessSum += shares[i]
	}
	total := float64(tokens)
	if rest := total - knownSum; guessSum > 0 && rest > 0 {
		// The remembered images are exact: the others share the rest.
		for i := range shares {
			if !known[i] {
				shares[i] = rest * shares[i] / guessSum
			}
		}
	} else if sum := knownSum + guessSum; sum > 0 {
		// Nothing left over, or every image remembered (a deployment's
		// settings may have changed): everything in proportion.
		for i := range shares {
			shares[i] = total * shares[i] / sum
		}
	}
	if m == nil {
		if len(e.models) >= maxImageModels {
			return
		}
		m = &imageModel{cells: make(map[imageCell]float64), keys: make(map[string]*meanStat)}
		e.models[model] = m
	}
	for i, img := range images {
		m.learn(key, img, shares[i])
		e.remember(model, img, shares[i])
	}
}

// learn folds one image's share of tokens into the model.
func (m *imageModel) learn(key string, img domain.ImageInfo, tokens float64) {
	if m.max == 0 {
		m.min, m.max = tokens, tokens
	} else {
		m.min = math.Min(m.min, tokens)
		m.max = math.Max(m.max, tokens)
	}
	m.all.add(tokens)
	if !img.Known() {
		m.unknown.add(tokens)
		ks := m.keys[key]
		if ks == nil {
			if len(m.keys) >= maxKeysPerModel {
				return
			}
			ks = &meanStat{}
			m.keys[key] = ks
		}
		ks.add(tokens)
		return
	}
	c := cellOf(img)
	if v, ok := m.cells[c]; ok {
		m.cells[c] = imageAlpha*tokens + (1-imageAlpha)*v
	} else {
		m.cells[c] = tokens
	}
	perPixel := tokens / (float64(img.Width) * float64(img.Height))
	if m.perPixel == 0 {
		m.perPixel = perPixel
	} else {
		m.perPixel = imageAlpha*perPixel + (1-imageAlpha)*m.perPixel
	}
}
