// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"hivenet_router/internal/policy"
)

// Decision sources.
const (
	SourcePinned   = "pinned"   // task key hit a live pin whose model is still eligible
	SourceScored   = "scored"   // best route scored >= abstain_below
	SourceDefault  = "default"  // no route scored above abstain_below → default_route
	SourceFallback = "fallback" // first route had no eligible candidate → default_route or next best route
	SourceCount    = "count"    // /v1/messages/count_tokens → default_route, no pin
	SourceAffinity = "affinity" // conversation stayed on its previous model (affinity_ttl)
)

// ErrNoEligibleModel means no route of the alias has a candidate the request
// can run on; Decision.FilteredOut says why and Classify turns that into a
// Failure.
var ErrNoEligibleModel = errors.New("semantic: no eligible model for this request")

// Env is the per-request caller context the resolver needs from the router.
type Env struct {
	// Allowed reports whether the caller's API key may use model (allowlist,
	// including strict per-model quota entries). nil = unrestricted.
	Allowed func(model string) bool
	// Healthy reports whether model has at least one healthy agent. nil = assume healthy.
	Healthy func(model string) bool
	// EstimateTokens returns the prompt-token estimate for model (the router's
	// learned per-model ratio). nil disables the context-window filter.
	EstimateTokens func(model string) int
	// KeyID and TaskHeader feed the task key (see TaskKey).
	KeyID      string
	TaskHeader string
	// IsAlias reports whether a name is itself an alias; such candidates are
	// skipped (aliases never chain). nil = no other aliases.
	IsAlias func(name string) bool
	// CountOnly marks /v1/messages/count_tokens: resolve to the default route
	// without scoring or pinning.
	CountOnly bool
}

// Decision is the outcome of resolving one alias request.
type Decision struct {
	Alias        string
	Model        string
	Route        string
	Source       string
	RouteScores  map[string]float64
	RouteMatches map[string][]string
	Features     *Features
	FilteredOut  map[string]string // model → reason, for every candidate rejected
	TaskKey      string
	Latency      time.Duration
}

// Resolver turns an alias request into a concrete model. It is safe for
// concurrent use; configuration is passed per call so hot reloads apply to the
// next request without coordination.
type Resolver struct {
	signals []Signal
	pins    *PinStore
}

// NewResolver builds a resolver with the given pin store and signals
// (default: Structural only).
func NewResolver(pins *PinStore, signals ...Signal) *Resolver {
	if len(signals) == 0 {
		signals = []Signal{Structural{}}
	}
	if pins == nil {
		pins = NewPinStore(0, 0)
	}
	return &Resolver{signals: signals, pins: pins}
}

// Pins exposes the pin store (tests / diagnostics).
func (r *Resolver) Pins() *PinStore { return r.pins }

// Resolve picks the model for a request to alias. Order: a live pin for the
// task (if the alias pins, the pin is still valid for the current config and
// its model is still eligible), else the best-scoring route (or the default
// route when nothing reaches abstain_below), then the default route, then every
// other route by descending score, taking the first eligible candidate. When no
// route has one it returns ErrNoEligibleModel; Classify(d.FilteredOut) says why.
func (r *Resolver) Resolve(alias string, spec *policy.AliasSpec, profiles map[string]*policy.ModelProfile, v *RequestView, env Env) (d Decision, err error) {
	start := time.Now()
	d = Decision{Alias: alias, FilteredOut: map[string]string{}}
	defer func() { d.Latency = time.Since(start) }()

	f := &Features{}
	opts := ExtractOptions{RecentTurns: spec.RecentTurns}
	for _, s := range r.signals {
		s.Extract(v, opts, f)
	}
	d.Features = f

	est := estimateFor(env, spec)
	f.Set("estimated_input_tokens", float64(est.max()))
	req := requirementsOf(f)
	req.outputTokens = v.MaxOutputTokens
	// Eligibility does not depend on the route, so check each model once.
	memo := make(map[string]string, 4) // model → reason ("" = eligible)
	eligible := func(model string) (bool, string) {
		reason, done := memo[model]
		if !done {
			_, reason = checkEligible(model, profiles, spec, req, est, env)
			memo[model] = reason
		}
		return reason == "", reason
	}

	pinnable := !env.CountOnly && anyPinned(spec)
	affinity := !env.CountOnly && spec.AffinityDuration() > 0
	if pinnable || affinity {
		d.TaskKey = TaskKey(alias, env.TaskHeader, env.KeyID, v)
	}
	if pinnable {
		if model, route, ok := r.pins.Get(d.TaskKey); ok {
			reason := pinStale(spec, route, model)
			if reason == "" {
				var okE bool
				if okE, reason = eligible(model); okE {
					d.Model, d.Route, d.Source = model, route, SourcePinned
					return d, nil
				}
			}
			// The pin no longer fits the config or the model can no longer serve
			// this task: drop it, re-route and (if the new route pins) re-pin.
			d.FilteredOut[model] = "pinned_but_" + reason
			r.pins.Delete(d.TaskKey)
		}
	}

	first := indexOf(spec, spec.DefaultRoute)
	d.Source = SourceDefault
	if env.CountOnly {
		d.Source = SourceCount
	} else {
		d.RouteScores, d.RouteMatches = scoreRoutes(spec, f)
		if best, bestScore := argmax(spec, d.RouteScores); best >= 0 && bestScore > 0 && bestScore >= spec.AbstainBelow {
			first, d.Source = best, SourceScored
		}
	}

	if affinity {
		if model, route, ok := r.stickyChoice(spec, d.TaskKey, first, d.RouteScores, eligible); ok {
			d.Model, d.Route, d.Source = model, route, SourceAffinity
			return d, nil
		}
	}

	for i, idx := range routeOrder(spec, first, d.RouteScores) {
		route := &spec.Routes[idx]
		model := pickCandidate(route, profiles, eligible, d.FilteredOut)
		if model == "" {
			continue
		}
		d.Model, d.Route = model, route.Name
		if i > 0 {
			d.Source = SourceFallback
		}
		switch {
		case pinnable && route.PinTask:
			r.pins.Put(d.TaskKey, env.KeyID, model, route.Name, route.PinDuration(), spec.PinMaxAgeDuration())
		case affinity:
			r.pins.Put(affinityKey(d.TaskKey), env.KeyID, model, route.Name, spec.AffinityDuration(), spec.PinMaxAgeDuration())
		}
		return d, nil
	}
	d.Route = spec.Routes[first].Name
	return d, ErrNoEligibleModel
}

// affinityKey derives the conversation-affinity key from a task key, so
// affinity entries share the pin store (and its caps) without colliding with
// task pins.
func affinityKey(taskKey string) string { return "a" + taskKey }

// stickyChoice returns the model the conversation used last (affinity_ttl),
// unless the newly chosen route (index first) beats the sticky route's score by
// at least affinity_margin, the sticky entry no longer matches the config, or
// its model is no longer eligible. Stale entries are dropped.
func (r *Resolver) stickyChoice(spec *policy.AliasSpec, taskKey string, first int, scores map[string]float64, eligible func(string) (bool, string)) (model, route string, ok bool) {
	key := affinityKey(taskKey)
	model, route, found := r.pins.Get(key)
	if !found {
		return "", "", false
	}
	if routeHasCandidate(spec, route, model) != "" {
		r.pins.Delete(key)
		return "", "", false
	}
	chosen := spec.Routes[first].Name
	if chosen != route && scores[chosen] >= scores[route]+spec.AffinityMargin {
		return "", "", false // a clearly better route: switch (and re-stick below)
	}
	if okE, _ := eligible(model); !okE {
		r.pins.Delete(key)
		return "", "", false
	}
	return model, route, true
}

// routeOrder lists route indexes in the order Resolve tries them: first, then
// the default route, then the remaining routes by descending score (ties keep
// declaration order). Trying every route means an alias resolves whenever any
// of its candidates is usable, which is also when /v1/models lists it.
func routeOrder(spec *policy.AliasSpec, first int, scores map[string]float64) []int {
	order := make([]int, 0, len(spec.Routes))
	order = append(order, first)
	if def := indexOf(spec, spec.DefaultRoute); def != first {
		order = append(order, def)
	}
	rest := make([]int, 0, len(spec.Routes))
	for i := range spec.Routes {
		if i != order[0] && (len(order) < 2 || i != order[1]) {
			rest = append(rest, i)
		}
	}
	sort.SliceStable(rest, func(a, b int) bool {
		return scores[spec.Routes[rest[a]].Name] > scores[spec.Routes[rest[b]].Name]
	})
	return append(order, rest...)
}

// pinStale reports why a task pin no longer matches the alias config ("" =
// still valid): its route was removed or no longer pins, or its model is no
// longer a candidate of that route. Pins survive hot reloads, so this keeps a
// pinned task inside the current candidate set.
func pinStale(spec *policy.AliasSpec, routeName, model string) string {
	if reason := routeHasCandidate(spec, routeName, model); reason != "" {
		return reason
	}
	if !spec.Routes[routeIndex(spec, routeName)].PinTask {
		return "route_not_pinned"
	}
	return ""
}

// routeHasCandidate reports why model is not a current candidate of the named
// route ("" = it is).
func routeHasCandidate(spec *policy.AliasSpec, routeName, model string) string {
	i := routeIndex(spec, routeName)
	if i < 0 {
		return "route_removed"
	}
	for _, c := range spec.Routes[i].Candidates {
		if c.Model == model {
			return ""
		}
	}
	return "not_a_candidate"
}

// Eligibility reasons recorded in Decision.FilteredOut.
const (
	ReasonIsAlias        = "is_alias"
	ReasonNotAllowed     = "not_allowed"
	ReasonNoHealthyAgent = "no_healthy_agent"
	// ReasonContextWindow prefixes "context_window(est=..,out=..>limit=..)".
	ReasonContextWindow = "context_window"
	reasonPinnedPrefix  = "pinned_but_"
)

// capabilityReasons are the reasons a request needs a feature a model lacks.
var capabilityReasons = map[string]bool{
	"no_tool_calling": true, "no_vision": true, "no_structured_output": true, "no_reasoning": true,
}

// Failure classifies why no candidate was eligible, so the API can answer with
// a status the client can act on.
type Failure int

const (
	// FailureUnavailable: at least one allowed candidate is down (or the config
	// has no usable candidate). Retrying later can help → 503.
	FailureUnavailable Failure = iota
	// FailureForbidden: the key may use none of the candidates → 403.
	FailureForbidden
	// FailureTooLong: every allowed candidate's context window is too small for
	// prompt + output → 400 input_too_long, like the concrete-model cap.
	FailureTooLong
	// FailureUnsupported: every allowed candidate lacks a capability the request
	// needs (or is too small) → 400; retrying cannot help.
	FailureUnsupported
)

// Classify maps the reasons of a failed decision to a Failure. Reasons recorded
// for a dropped pin are ignored: they describe the old pin, not the candidates.
func Classify(filtered map[string]string) Failure {
	var allowed, tooLong, unsupported int
	for _, reason := range filtered {
		switch {
		case strings.HasPrefix(reason, reasonPinnedPrefix):
			continue
		case reason == ReasonNotAllowed:
			continue
		case strings.HasPrefix(reason, ReasonContextWindow):
			tooLong++
		case capabilityReasons[reason]:
			unsupported++
		}
		allowed++
	}
	switch {
	case allowed == 0 && len(filtered) > 0 && !onlyPinReasons(filtered):
		return FailureForbidden
	case allowed > 0 && tooLong == allowed:
		return FailureTooLong
	case allowed > 0 && tooLong+unsupported == allowed:
		return FailureUnsupported
	}
	return FailureUnavailable
}

func onlyPinReasons(filtered map[string]string) bool {
	for _, reason := range filtered {
		if !strings.HasPrefix(reason, reasonPinnedPrefix) {
			return false
		}
	}
	return true
}

// requirements are what a request needs from its model.
type requirements struct {
	tools, vision, structured, reasoning bool
	outputTokens                         int // reserved output (max_tokens); counts against the context window
}

func requirementsOf(f *Features) requirements {
	val := func(n string) float64 { v, _ := f.Value(n); return v }
	return requirements{
		// Tool definitions, or tool traffic already in the history, both need a
		// model that understands tool calls.
		tools:      val("has_tools") > 0 || val("tool_result_turns") > 0 || val("assistant_tool_call_turns") > 0,
		vision:     val("image_count") > 0,
		structured: val("needs_structured_output") > 0,
		reasoning:  val("needs_reasoning") > 0,
	}
}

// estimates caches per-model token estimates for one request.
type estimates map[string]int

func (e estimates) max() int {
	m := 0
	for _, v := range e {
		if v > m {
			m = v
		}
	}
	return m
}

func estimateFor(env Env, spec *policy.AliasSpec) estimates {
	e := estimates{}
	if env.EstimateTokens == nil {
		return e
	}
	for i := range spec.Routes {
		for _, c := range spec.Routes[i].Candidates {
			if _, done := e[c.Model]; !done {
				e[c.Model] = env.EstimateTokens(c.Model)
			}
		}
	}
	return e
}

// checkEligible applies, in order: alias check, key allowlist, capability
// requirements, context window (prompt estimate + reserved output), and health.
// The allowlist goes first so a model the key may not use is always reported as
// not_allowed, whatever else is wrong with it. Unknown profile data never
// excludes a model.
func checkEligible(model string, profiles map[string]*policy.ModelProfile, spec *policy.AliasSpec, req requirements, est estimates, env Env) (bool, string) {
	if env.IsAlias != nil && env.IsAlias(model) {
		return false, ReasonIsAlias
	}
	if env.Allowed != nil && !env.Allowed(model) {
		return false, ReasonNotAllowed
	}
	if p := profiles[model]; p != nil {
		c := p.Capabilities
		if req.tools && isFalse(c.ToolCalling) {
			return false, "no_tool_calling"
		}
		if req.vision && isFalse(c.Vision) {
			return false, "no_vision"
		}
		if req.structured && isFalse(c.StructuredOutput) {
			return false, "no_structured_output"
		}
		if req.reasoning && isFalse(c.Reasoning) {
			return false, "no_reasoning"
		}
		if p.ContextWindow > 0 {
			if n, ok := est[model]; ok {
				limit := int(float64(p.ContextWindow) * spec.ContextBuffer)
				if n+req.outputTokens > limit {
					return false, fmt.Sprintf("%s(est=%d,out=%d>limit=%d)", ReasonContextWindow, n, req.outputTokens, limit)
				}
			}
		}
	}
	if env.Healthy != nil && !env.Healthy(model) {
		return false, ReasonNoHealthyAgent
	}
	return true, ""
}

func isFalse(b *bool) bool { return b != nil && !*b }

// pickCandidate returns the first eligible candidate in the route's preferred
// order, recording why each rejected candidate was skipped.
func pickCandidate(route *policy.Route, profiles map[string]*policy.ModelProfile, eligible func(string) (bool, string), filtered map[string]string) string {
	order := make([]string, 0, len(route.Candidates))
	for _, c := range route.Candidates {
		order = append(order, c.Model)
	}
	switch route.Prefer {
	case "smallest", "largest":
		size := func(m string) float64 { return profiles[m].SizeB() }
		sort.SliceStable(order, func(i, j int) bool {
			si, sj := size(order[i]), size(order[j])
			if si == 0 || sj == 0 { // unknown sizes go last, keeping list order
				return si != 0 && sj == 0
			}
			if route.Prefer == "smallest" {
				return si < sj
			}
			return si > sj
		})
	}
	for _, m := range order {
		ok, reason := eligible(m)
		if ok {
			return m
		}
		filtered[m] = reason
	}
	return ""
}

// scoreRoutes computes Σ weight·match / Σ weight per route, and the signals
// that matched (Decision.RouteMatches, for diagnostics). The loader requires each route's
// weights to sum to 1, so the division only guards specs built in code.
// Routes without signals score 0.
func scoreRoutes(spec *policy.AliasSpec, f *Features) (map[string]float64, map[string][]string) {
	scores := make(map[string]float64, len(spec.Routes))
	matches := make(map[string][]string)
	window := f.Window()
	for i := range spec.Routes {
		rt := &spec.Routes[i]
		var total, got float64
		for _, s := range rt.Signals {
			total += s.Weight
			if hit, label := matchSignal(s, f, window); hit {
				got += s.Weight
				matches[rt.Name] = append(matches[rt.Name], label)
			}
		}
		if total > 0 {
			scores[rt.Name] = got / total
		} else {
			scores[rt.Name] = 0
		}
	}
	return scores, matches
}

func matchSignal(s policy.SignalRule, f *Features, window string) (bool, string) {
	if len(s.Keywords) > 0 {
		for _, kw := range s.Keywords {
			if containsKeyword(window, kw) {
				return true, "kw:" + kw
			}
		}
		return false, ""
	}
	v, _ := f.Value(s.Feature)
	if s.GTE == nil && s.LTE == nil {
		return v > 0, s.Feature
	}
	if s.GTE != nil && v < *s.GTE {
		return false, ""
	}
	if s.LTE != nil && v > *s.LTE {
		return false, ""
	}
	return true, s.Feature
}

// argmax returns the index of the highest-scoring route among those at or
// above their own min_score; ties go to the route declared first. -1 when no
// route qualifies.
func argmax(spec *policy.AliasSpec, scores map[string]float64) (int, float64) {
	best, bestScore := -1, -1.0
	for i := range spec.Routes {
		if s := scores[spec.Routes[i].Name]; s >= spec.Routes[i].MinScore && s > bestScore {
			best, bestScore = i, s
		}
	}
	return best, bestScore
}

// indexOf returns the index of the named route, or 0 when absent (only used
// for default_route, which the loader guarantees exists).
func indexOf(spec *policy.AliasSpec, name string) int {
	if i := routeIndex(spec, name); i >= 0 {
		return i
	}
	return 0
}

// routeIndex returns the index of the named route, or -1.
func routeIndex(spec *policy.AliasSpec, name string) int {
	for i := range spec.Routes {
		if spec.Routes[i].Name == name {
			return i
		}
	}
	return -1
}

func anyPinned(spec *policy.AliasSpec) bool {
	for i := range spec.Routes {
		if spec.Routes[i].PinTask {
			return true
		}
	}
	return false
}
