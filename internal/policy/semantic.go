// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package policy

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Semantic routing configuration (R&D spike, rnd/semantic-routing/).
//
// Two optional blocks extend a per-model policy document:
//
//   - profile: operator-declared static facts about the model(s) the document
//     claims (context window, size, capabilities). Read by the semantic resolver
//     to filter candidates a request cannot run on.
//   - alias:   turns the names in the document's models: list into virtual model
//     aliases (e.g. "auto"). A request naming an alias is resolved to one
//     concrete model before quota, admission and replica routing run.
//
// A per-model document that carries only models: plus profile: or alias: may
// omit routing_policy; its models then keep the global routing policy (see
// InheritsRouting). The global policy itself must always declare routing.

// ModelProfile holds operator-declared static facts about a model.
// Zero / nil fields mean "unknown": an unknown capability or context window
// never excludes a model, mirroring how a nil metric passes exclude_if.
type ModelProfile struct {
	// ContextWindow is the served max_model_len in tokens (prompt + output).
	ContextWindow int `yaml:"context_window" json:"context_window,omitempty"`
	// ParamsB / ActiveParamsB are the total and per-token active parameter
	// counts in billions (equal for dense models). Used by prefer: smallest|largest.
	ParamsB       float64           `yaml:"params_b"        json:"params_b,omitempty"`
	ActiveParamsB float64           `yaml:"active_params_b" json:"active_params_b,omitempty"`
	Capabilities  ModelCapabilities `yaml:"capabilities"    json:"capabilities"`
}

// ModelCapabilities declares request features the model can serve.
// nil means unknown (treated as supported); false excludes the model from any
// request that needs the feature.
type ModelCapabilities struct {
	ToolCalling      *bool `yaml:"tool_calling"      json:"tool_calling,omitempty"`
	StructuredOutput *bool `yaml:"structured_output" json:"structured_output,omitempty"`
	Vision           *bool `yaml:"vision"            json:"vision,omitempty"`
	Reasoning        *bool `yaml:"reasoning"         json:"reasoning,omitempty"`
}

// SizeB returns the size used for prefer: smallest|largest — active parameters
// when declared, else total parameters; 0 when unknown.
func (p *ModelProfile) SizeB() float64 {
	if p == nil {
		return 0
	}
	if p.ActiveParamsB > 0 {
		return p.ActiveParamsB
	}
	return p.ParamsB
}

// AliasSpec configures how a virtual model alias is resolved to a concrete model.
type AliasSpec struct {
	// DefaultRoute is used when no route scores at or above AbstainBelow, and as
	// the fallback when the winning route has no eligible candidate.
	DefaultRoute string `yaml:"default_route" json:"default_route"`
	// AbstainBelow: the best route score must be >= this to be chosen. [0,1].
	AbstainBelow float64 `yaml:"abstain_below" json:"abstain_below"`
	// ContextBuffer is the fraction of a candidate's context window the estimated
	// prompt may fill. Default 0.95.
	ContextBuffer float64 `yaml:"context_buffer" json:"context_buffer,omitempty"`
	// RecentTurns is how many trailing user turns keyword rules scan. Default 3.
	RecentTurns int `yaml:"recent_turns" json:"recent_turns,omitempty"`
	// StripMarkers lists open/close tag pairs whose enclosed text is removed
	// before keyword matching (e.g. harness-injected <system-reminder> blocks).
	// Must have an even number of entries. Default: <system-reminder> pair.
	StripMarkers []string `yaml:"strip_markers" json:"strip_markers,omitempty"`
	Routes       []Route  `yaml:"routes"        json:"routes"`
}

// Route is one semantic category an alias can resolve to.
type Route struct {
	Name        string `yaml:"name"        json:"name"`
	Description string `yaml:"description" json:"description,omitempty"`
	// PinTask caches the resolved model under the request's task key for PinTTL,
	// so a multi-call task stays on one model.
	PinTask bool   `yaml:"pin_task" json:"pin_task,omitempty"`
	PinTTL  string `yaml:"pin_ttl"  json:"pin_ttl,omitempty"`
	// Prefer orders eligible candidates: "order" (default, as listed),
	// "smallest" or "largest" (by profile size; unknown sizes keep list order, last).
	Prefer     string       `yaml:"prefer"     json:"prefer,omitempty"`
	Signals    []SignalRule `yaml:"signals"    json:"signals,omitempty"`
	Candidates []Candidate  `yaml:"candidates" json:"candidates"`

	pinTTL time.Duration // parsed from PinTTL by validateAlias
}

// PinDuration returns the parsed pin TTL (default 30m when PinTask is set).
func (r *Route) PinDuration() time.Duration { return r.pinTTL }

// SignalRule contributes Weight × match to a route's score, where match is in
// [0,1]. Exactly one of Feature or Keywords is set.
//
//   - Feature with GTE and/or LTE: match is 1 when the value is within bounds.
//   - Feature with no bounds: match is 1 when the value is > 0 (boolean features
//     are 0/1).
//   - Keywords: match is 1 when any keyword occurs (case-insensitive, at word
//     boundaries) in the last RecentTurns user messages.
type SignalRule struct {
	Feature  string   `yaml:"feature"  json:"feature,omitempty"`
	Keywords []string `yaml:"keywords" json:"keywords,omitempty"`
	GTE      *float64 `yaml:"gte"      json:"gte,omitempty"`
	LTE      *float64 `yaml:"lte"      json:"lte,omitempty"`
	Weight   float64  `yaml:"weight"   json:"weight"`
}

// Candidate is one model a route may resolve to.
type Candidate struct {
	Model string `yaml:"model" json:"model"`
}

// knownSignalFeatures lists the request features a SignalRule may reference.
//
// Keep this in sync with the feature switch in internal/semantic (Features.Value);
// a name missing there would score as 0 forever. test/semantic asserts the two agree.
var knownSignalFeatures = map[string]struct{}{
	"has_tools":                 {},
	"tool_count":                {},
	"tool_result_turns":         {},
	"tool_error_count":          {},
	"assistant_tool_call_turns": {},
	"turn_count":                {},
	"message_count":             {},
	"system_prompt_bytes":       {},
	"request_bytes":             {},
	"estimated_input_tokens":    {},
	"image_count":               {},
	"needs_structured_output":   {},
	"needs_reasoning":           {},
	"code_presence":             {},
	"stack_trace":               {},
	"math_presence":             {},
}

// KnownSignalFeatures returns the sorted feature names accepted in signals.
func KnownSignalFeatures() []string {
	out := make([]string, 0, len(knownSignalFeatures))
	for k := range knownSignalFeatures {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

const defaultPinTTL = 30 * time.Minute

// InheritsRouting reports whether this per-model document omits routing
// entirely and only contributes a profile and/or alias. Its models then use
// the global routing policy. A document without profile/alias never inherits,
// so every pre-existing document (including ones built in code without a
// strategy) keeps claiming its models exactly as before.
func (p *Policy) InheritsRouting() bool {
	return (p.Profile != nil || p.Alias != nil) && p.RoutingPolicy.Strategy == "" && isZeroStep(p.RoutingPolicy)
}

func isZeroStep(s PolicyStep) bool {
	m := s.Match
	return m.Region == "" && m.Engine == "" && len(m.Tags) == 0 && m.Organization == "" &&
		m.Machine == "" && m.GPUModel == "" && len(s.ExcludeIf) == 0 && s.MaxTries == 0
}

// validateModelDoc validates a per-model document that inherits routing: it
// may carry only models:, profile: and alias:. Anything that configures routing
// or admission requires an explicit routing_policy, so a half-written routing
// block is never silently dropped.
func validateModelDoc(p *Policy) error {
	if len(p.FallbackChain) > 0 || p.FallbackProvider != nil || p.Mode != "" ||
		p.MaxInputTokens != 0 || p.ImagesMax != 0 || p.AdmitBudgetTokens != 0 ||
		p.MaxInflight != 0 || len(p.ShedIf) > 0 {
		return fmt.Errorf("policy: routing_policy.strategy is required when a document sets fallback, mode or admission fields")
	}
	return validateSemantic(p)
}

// validateSemantic checks the profile and alias blocks of any document.
func validateSemantic(p *Policy) error {
	if p.Alias != nil {
		if p.Profile != nil {
			return fmt.Errorf("policy: a document cannot declare both alias and profile (an alias is not a model)")
		}
		if !p.InheritsRouting() {
			return fmt.Errorf("policy: alias documents must not declare routing_policy (the resolved model's policy applies)")
		}
		if len(p.Models) == 0 {
			return fmt.Errorf("policy: alias documents must list the alias name(s) in the models key")
		}
		if err := validateAlias(p.Alias, p.Models); err != nil {
			return err
		}
	}
	if p.Profile != nil {
		if err := validateProfile(p.Profile); err != nil {
			return err
		}
	}
	return nil
}

func validateProfile(pr *ModelProfile) error {
	if pr.ContextWindow < 0 {
		return fmt.Errorf("policy: profile.context_window must be >= 0, got %d", pr.ContextWindow)
	}
	if pr.ParamsB < 0 || pr.ActiveParamsB < 0 {
		return fmt.Errorf("policy: profile.params_b and active_params_b must be >= 0")
	}
	if pr.ParamsB > 0 && pr.ActiveParamsB > pr.ParamsB {
		return fmt.Errorf("policy: profile.active_params_b (%g) exceeds params_b (%g)", pr.ActiveParamsB, pr.ParamsB)
	}
	return nil
}

func validateAlias(a *AliasSpec, aliasNames []string) error {
	if len(a.Routes) == 0 {
		return fmt.Errorf("policy: alias.routes must declare at least one route")
	}
	if a.AbstainBelow < 0 || a.AbstainBelow > 1 {
		return fmt.Errorf("policy: alias.abstain_below must be in [0,1], got %g", a.AbstainBelow)
	}
	if a.ContextBuffer == 0 {
		a.ContextBuffer = 0.95
	}
	if a.ContextBuffer < 0 || a.ContextBuffer > 1 {
		return fmt.Errorf("policy: alias.context_buffer must be in (0,1], got %g", a.ContextBuffer)
	}
	if a.RecentTurns == 0 {
		a.RecentTurns = 3
	}
	if a.RecentTurns < 0 {
		return fmt.Errorf("policy: alias.recent_turns must be >= 0, got %d", a.RecentTurns)
	}
	if a.StripMarkers == nil {
		a.StripMarkers = []string{"<system-reminder>", "</system-reminder>"}
	}
	if len(a.StripMarkers)%2 != 0 {
		return fmt.Errorf("policy: alias.strip_markers must list open/close pairs (even count), got %d", len(a.StripMarkers))
	}
	isAlias := make(map[string]struct{}, len(aliasNames))
	for _, n := range aliasNames {
		isAlias[n] = struct{}{}
	}
	seen := make(map[string]struct{}, len(a.Routes))
	for i := range a.Routes {
		r := &a.Routes[i]
		if r.Name == "" {
			return fmt.Errorf("policy: alias.routes[%d]: name is required", i)
		}
		if _, dup := seen[r.Name]; dup {
			return fmt.Errorf("policy: alias.routes: duplicate route name %q", r.Name)
		}
		seen[r.Name] = struct{}{}
		if len(r.Candidates) == 0 {
			return fmt.Errorf("policy: route %q: candidates must list at least one model", r.Name)
		}
		for j, c := range r.Candidates {
			if c.Model == "" {
				return fmt.Errorf("policy: route %q: candidates[%d].model is required", r.Name, j)
			}
			if _, self := isAlias[c.Model]; self {
				return fmt.Errorf("policy: route %q: candidate %q is an alias; candidates must be concrete models", r.Name, c.Model)
			}
		}
		switch r.Prefer {
		case "", "order", "smallest", "largest":
		default:
			return fmt.Errorf("policy: route %q: unknown prefer %q — supported: order, smallest, largest", r.Name, r.Prefer)
		}
		r.pinTTL = 0
		if r.PinTTL != "" {
			d, err := time.ParseDuration(r.PinTTL)
			if err != nil || d <= 0 {
				return fmt.Errorf("policy: route %q: invalid pin_ttl %q", r.Name, r.PinTTL)
			}
			r.pinTTL = d
		} else if r.PinTask {
			r.pinTTL = defaultPinTTL
		}
		for k, s := range r.Signals {
			if err := validateSignal(r.Name, k, s); err != nil {
				return err
			}
			// Keywords are matched case-insensitively; normalise once at load.
			for w := range s.Keywords {
				r.Signals[k].Keywords[w] = strings.ToLower(strings.TrimSpace(s.Keywords[w]))
			}
		}
	}
	if a.DefaultRoute == "" {
		return fmt.Errorf("policy: alias.default_route is required")
	}
	if _, ok := seen[a.DefaultRoute]; !ok {
		return fmt.Errorf("policy: alias.default_route %q is not one of the declared routes", a.DefaultRoute)
	}
	return nil
}

func validateSignal(route string, i int, s SignalRule) error {
	hasFeature, hasKeywords := s.Feature != "", len(s.Keywords) > 0
	if hasFeature == hasKeywords {
		return fmt.Errorf("policy: route %q: signals[%d]: set exactly one of feature or keywords", route, i)
	}
	if s.Weight < 0 {
		return fmt.Errorf("policy: route %q: signals[%d]: weight must be >= 0", route, i)
	}
	if hasKeywords {
		if s.GTE != nil || s.LTE != nil {
			return fmt.Errorf("policy: route %q: signals[%d]: gte/lte apply to features, not keywords", route, i)
		}
		for _, k := range s.Keywords {
			if strings.TrimSpace(k) == "" {
				return fmt.Errorf("policy: route %q: signals[%d]: empty keyword", route, i)
			}
		}
		return nil
	}
	if _, ok := knownSignalFeatures[s.Feature]; !ok {
		return fmt.Errorf("policy: route %q: signals[%d]: unknown feature %q — supported: %s",
			route, i, s.Feature, strings.Join(KnownSignalFeatures(), ", "))
	}
	if s.GTE != nil && s.LTE != nil && *s.GTE > *s.LTE {
		return fmt.Errorf("policy: route %q: signals[%d]: gte (%g) > lte (%g)", route, i, *s.GTE, *s.LTE)
	}
	return nil
}
