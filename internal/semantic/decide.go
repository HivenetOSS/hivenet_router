// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package semantic

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"hivenet_router/internal/policy"
)

// Decision sources.
const (
	SourcePinned   = "pinned"   // task key hit a live pin whose model is still eligible
	SourceScored   = "scored"   // best route scored >= abstain_below
	SourceDefault  = "default"  // no route scored above abstain_below → default_route
	SourceFallback = "fallback" // winning route had no eligible candidate → default_route
	SourceCount    = "count"    // /v1/messages/count_tokens → default_route, no pin
)

// ErrNoEligibleModel means neither the chosen route nor the default route has a
// candidate the request can run on; Decision.FilteredOut says why.
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
		pins = NewPinStore(0)
	}
	return &Resolver{signals: signals, pins: pins}
}

// Pins exposes the pin store (tests / diagnostics).
func (r *Resolver) Pins() *PinStore { return r.pins }

// Resolve picks the model for a request to alias.
func (r *Resolver) Resolve(alias string, spec *policy.AliasSpec, profiles map[string]*policy.ModelProfile, v *RequestView, env Env) (Decision, error) {
	start := time.Now()
	d := Decision{Alias: alias, FilteredOut: map[string]string{}}
	defer func() { d.Latency = time.Since(start) }()

	f := &Features{}
	opts := ExtractOptions{RecentTurns: spec.RecentTurns, StripMarkers: spec.StripMarkers}
	for _, s := range r.signals {
		s.Extract(v, opts, f)
	}
	d.Features = f

	est := estimateFor(env, spec)
	f.Set("estimated_input_tokens", float64(est.max()))
	req := requirementsOf(f)
	eligible := func(model string) (bool, string) {
		return checkEligible(model, profiles, spec, req, est, env)
	}

	pinnable := !env.CountOnly && anyPinned(spec)
	if pinnable {
		d.TaskKey = TaskKey(env.TaskHeader, env.KeyID, v)
		if model, route, ok := r.pins.Get(d.TaskKey); ok {
			okE, reason := eligible(model)
			if okE {
				d.Model, d.Route, d.Source = model, route, SourcePinned
				return d, nil
			}
			// The pinned model can no longer serve this task: re-route and re-pin.
			d.FilteredOut[model] = "pinned_but_" + reason
			r.pins.Delete(d.TaskKey)
		}
	}

	routeIdx := indexOf(spec, spec.DefaultRoute)
	d.Source = SourceDefault
	if env.CountOnly {
		d.Source = SourceCount
	} else {
		d.RouteScores, d.RouteMatches = scoreRoutes(spec, f)
		if best, bestScore := argmax(spec, d.RouteScores); best >= 0 && bestScore > 0 && bestScore >= spec.AbstainBelow {
			routeIdx, d.Source = best, SourceScored
		}
	}

	route := &spec.Routes[routeIdx]
	model := pickCandidate(route, profiles, eligible, d.FilteredOut)
	if model == "" && route.Name != spec.DefaultRoute {
		route = &spec.Routes[indexOf(spec, spec.DefaultRoute)]
		model = pickCandidate(route, profiles, eligible, d.FilteredOut)
		d.Source = SourceFallback
	}
	d.Route = route.Name
	if model == "" {
		return d, ErrNoEligibleModel
	}
	d.Model = model
	if pinnable && route.PinTask {
		r.pins.Put(d.TaskKey, model, route.Name, route.PinDuration())
	}
	return d, nil
}

// requirements are the capabilities a request needs from its model.
type requirements struct {
	tools, vision, structured, reasoning bool
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

// checkEligible applies, in order: alias check, capability requirements,
// context window, key allowlist, and health. Unknown profile data never excludes a model.
func checkEligible(model string, profiles map[string]*policy.ModelProfile, spec *policy.AliasSpec, req requirements, est estimates, env Env) (bool, string) {
	if env.IsAlias != nil && env.IsAlias(model) {
		return false, "is_alias"
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
				if n > limit {
					return false, fmt.Sprintf("context_window(est=%d>limit=%d)", n, limit)
				}
			}
		}
	}
	if env.Allowed != nil && !env.Allowed(model) {
		return false, "not_allowed"
	}
	if env.Healthy != nil && !env.Healthy(model) {
		return false, "no_healthy_agent"
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
// that matched (for the decision log). Routes without signals score 0.
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

// argmax returns the index of the highest-scoring route; ties go to the route
// declared first. -1 when there are no routes.
func argmax(spec *policy.AliasSpec, scores map[string]float64) (int, float64) {
	best, bestScore := -1, -1.0
	for i := range spec.Routes {
		if s := scores[spec.Routes[i].Name]; s > bestScore {
			best, bestScore = i, s
		}
	}
	return best, bestScore
}

func indexOf(spec *policy.AliasSpec, name string) int {
	for i := range spec.Routes {
		if spec.Routes[i].Name == name {
			return i
		}
	}
	return 0 // validated at load: default_route always exists
}

func anyPinned(spec *policy.AliasSpec) bool {
	for i := range spec.Routes {
		if spec.Routes[i].PinTask {
			return true
		}
	}
	return false
}
