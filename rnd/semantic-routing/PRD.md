# PRD: Semantic Routing for Hivenet Router (R&D spike)

| Field | Value |
|---|---|
| Status | Draft v0.1 for review |
| Type | R&D spike (trial and experiment). Output is evidence and a recommendation, not shipped code. Prototype code is throwaway unless a later decision promotes it. |
| Date | 2026-09-17 |
| Branch | `rnd/semantic-routing-spike` |
| Artefacts | `rnd/semantic-routing/` (outside the Mintlify site in `docs/`, so `docs/AGENTS.md` rules do not apply) |

---

## 1. Goal description

### 1.1 What Hivenet Router does today
Hivenet Router exposes one OpenAI/Anthropic-compatible endpoint in front of a fleet of self-hosted
inference agents (vLLM, Ollama, SGLang, llama.cpp, Infinity). Given a request that names a concrete
model, it decides **which replica** serves it, using health, load, RFC 6298 latency, GPU/engine
pressure, YAML policies, fallback chains, and admission control. This is routing at the
**hardware and replica level**. The client must already know which model it wants.

### 1.2 What we want to add
A layer above that: given the **content** of a request (a single prompt, a multi-turn conversation,
or a turn from an agent harness), decide **which model** in the fleet is best suited to answer it,
then hand off to the existing replica-level routing. Three levels of decision, in increasing scope:

1. **Query level.** A single prompt about physics goes to the model that is strongest at science; a
   stack trace goes to the model that is strongest at software engineering.
2. **Conversation level.** The decision reads the conversation, not just the last message, so a
   follow-up question stays on the model that has been answering the thread.
3. **Agentic / task level.** A request that starts a long-running, tool-using task (deep research,
   autonomous coding) goes to the model best at agentic work, and the whole task stays on that model
   rather than being re-routed on every call.

The client asks for this behaviour explicitly by requesting a virtual model alias (for example
`model: "auto"`). A request that names a concrete model never enters the semantic path: it goes
straight to replica-level routing exactly as today, with no added latency, regardless of any header.

### 1.3 Why a spike, not a build
The 2026 routing literature is unsettled on what actually works: two large benchmarks show most
learned routers, commercial ones included, barely beat a nearest-neighbour baseline, and that the
candidate pool and the encoder matter more than the algorithm. We should therefore measure, on our
own categories and our own fleet, before committing to an architecture. The spike answers the
questions in section 4 with numbers and ends with a go/no-go recommendation.

### 1.4 Non-goals (for the spike)
- Production hardening, docs site pages, release packaging.
- Guardrails (PII, jailbreak, safety). Adjacent, same signal machinery, out of scope here.
- Semantic caching.
- Learning from RLHF-style pairwise votes or RLVR-style verifiable rewards; we do not have those signals.
- Changing replica-level routing or admission control.

---

## 2. Users and scenarios

| User | Scenario | What they need |
|---|---|---|
| Fleet operator | Runs 3–10 different models across a fleet; wants one endpoint that sends each request to the right one | Declare routes and per-model strengths in YAML; see why each request went where it went; add a model without retraining anything |
| API client (app developer) | Calls the router as if it were OpenAI/Anthropic | Send `model: "auto"` and get the best available model; or name a concrete model and get exactly that model |
| Agent harness (e.g. Claude Code, OpenCode, custom agents on `/v1/messages`) | Runs multi-step tasks with tools and a stable system prompt | Whole task pinned to one model; tool-capable models preferred when tools are present |
| Hivenet engineering | Needs to decide whether to invest in this layer | Evidence: route accuracy, latency, cost, and which signals earn their keep |

---

## 3. Requirements for the experimental prototype

### 3.1 Functional
- **F1 Alias trigger.** A request whose `model` names an operator-defined alias is routed
  semantically to one concrete model. Must work on `/v1/chat/completions` and `/v1/messages`.
- **F2 Concrete-model bypass.** A request whose `model` is a concrete model name skips the semantic
  path entirely and is handled by replica-level routing as today. No header can pull it into
  semantic routing. (Decided 2026-09-17; replaces an earlier opt-in-header proposal.) When an
  alias decision abstains (confidence below threshold), the alias's `default_route` is used.
- **F3 Route configuration in YAML.** Operator declares routes: name, natural-language description,
  example utterances, optional structural predicates, ordered candidate models with weights or
  benchmark scores, optional `pin_task` flag. Reuses the existing per-model policy document mechanism
  (`models:` key, `--policy-model-dir`, SIGHUP, admin PUT/GET).
- **F4 Candidate safety.** Chosen model must be in: route candidates ∩ the API key's allowed models ∩
  models with at least one healthy `llm` agent. Never route to a model the key could not have named.
- **F5 Signals (pluggable).** A `Signal` interface; the prototype implements at least:
  (a) structural features from the request (tools present, system prompt size, turn count, total
  bytes, images, keyword lists); (b) embedding similarity between the query and route
  descriptions/utterances, computed on a fleet embedding agent; (c) optionally a domain classifier
  on a fleet agent (vLLM `--task classify` with an Apache-2.0 mmBERT classifier).
- **F6 Task pinning.** Task key = `X-Hivenet-Task-ID` header if present, else a fingerprint of
  (API key id, system prompt, first user message). Routes with `pin_task: true` cache the resolved
  model for a TTL; re-route only when the pinned model has no healthy agent.
- **F7 Decision logging.** Every decision writes: requested model, resolved model, route, per-route
  scores, signals used, task key, source (alias/pinned/default), decision latency. Goes to the
  existing audit log and Prometheus. This is also the dataset for later learning.
- **F8 Models listing.** Aliases appear in `/v1/models` for keys whose allowlist intersects the
  candidates.

### 3.2 Non-functional
- **N1 Zero overhead on the concrete-model path.** Byte-identical behaviour and no added latency
  for requests that do not use an alias.
- **N2 Decision latency budget: ≤ 300 ms p99** for alias requests, measured end to end inside the
  router. Target for the structural-only path: ≤ 1 ms.
- **N3 No external calls.** Prompts never leave the cluster; all signal computation is in-process
  or on fleet agents.
- **N4 Router build unchanged.** The router binary stays `CGO_ENABLED=0`; anything needing cgo or a
  GPU runs on an agent.
- **N5 Observability.** Decision counters by alias/route/model/source and a latency histogram under
  the `hivenet_` prefix.
- **N6 Config hot-reload** consistent with existing policies (SIGHUP and admin API).

---

## 4. Hypotheses and experiments

Each hypothesis has a measurement and a pass threshold. Thresholds are proposals; adjust at review.

| ID | Hypothesis | Experiment | Metric | Pass |
|---|---|---|---|---|
| H1 | Training-free matching (structural signals + embedding similarity to operator-written route descriptions) is accurate enough for our categories | Build a labeled set of ~300 prompts over the v0 taxonomy (section 5). Score description-embedding matcher with 2–3 embedding models (bge-m3, MiniLM-class, Vela/mmBERT embed) | Route accuracy, macro-F1, abstain rate | ≥ 85% accuracy on single-turn; ≥ 75% on multi-turn where the intent is in an earlier turn |
| H2 | A trained domain classifier (mmBERT-32k intent, 14 MMLU-Pro domains) adds little over H1 for our taxonomy, and cannot express "agentic" | Same set; map its 14 labels onto our routes; compare with H1 alone and H1+classifier | Δ accuracy vs H1 | Keep classifier only if Δ ≥ 5 points |
| H3 | An LLM router (Arch-Router-1.5B or a Qwen-class 1.5B with route descriptions in the prompt) is more accurate on nuanced/multi-turn cases but costs too much latency for the gain | Same set on a GPU agent | Accuracy, p50/p99 latency, GPU seconds per 1k decisions | Adopt only if Δ accuracy ≥ 10 points *and* p99 ≤ 300 ms |
| H4 | Structural signals alone can detect "agentic / long-running task" requests reliably | Label agentic vs non-agentic on real harness traffic captured from a live Claude Code session against the router (set up manually by the team; requests recorded through the router's audit/decision log) | Precision/recall of the agentic route | ≥ 90% precision (false pins are costly) |
| H5 | Task pinning by fingerprint holds for real agent harnesses | Replay the captured Claude Code sessions; check fingerprint stability across a session and collision across sessions | Stability %, collision % | ≥ 95% stable, ≤ 1% collision |
| H6 | The implicit next-turn feedback signal is usable as a reward | Run the mmBERT feedback detector over the captured session logs; hand-label a 200-turn sample | Agreement with human labels; fraction of turns with any signal | ≥ 80% agreement; signal present on ≥ 30% of multi-turn conversations |
| H7 | In-process embedding without cgo is feasible | hugot pure-Go backend with a 22M MiniLM model at `CGO_ENABLED=0`, 512-token input | p50/p99 latency, RSS | If ≤ 10 ms p99 the fleet hop can be skipped for short prompts |
| H8 | vLLM can host the classifier models through the existing agent path | `vllm serve <mmbert classifier> --task classify` behind a `--capability classifier` agent | Works / does not; latency | Works with ≤ 20 ms p99 on GPU |

Comparators for all accuracy experiments: random, majority route, keyword-only.

---

## 5. Experimental taxonomy (v0)

### 5.1 Fleet models for the spike (decided 2026-09-17)
- GPT-OSS-20B
- Qwen-3.6-35B-A3B
- Qwen-3.8-27B

The alias YAML must use the model IDs exactly as the agents register them (what the backend reports
in `/v1/models`); confirm those IDs once the fleet is up.

### 5.2 Routes
Routes chosen from the kickoff examples. Every route lists all three models as candidates; the
**ordering and weights are an output of the spike, not an input**. They will be derived from two
sources and compared: (a) published benchmark scores for the three models on the closest public
benchmarks per route (coding, science/math, tool use), and (b) a judge-scored run of the labeled set
(section 5.3) on each of the three models on the fleet. If (a) and (b) disagree, (b) wins, since it
reflects our prompts and our serving setup.

| Route | Description (as the operator would write it) | Candidates (order TBD by experiment) |
|---|---|---|
| `software_engineering` | Writing, fixing, reviewing, or explaining code; build, tooling and dependency errors | GPT-OSS-20B, Qwen-3.6-35B-A3B, Qwen-3.8-27B |
| `science` | Physics, chemistry, biology, mathematics questions, derivations, and scientific reasoning | GPT-OSS-20B, Qwen-3.6-35B-A3B, Qwen-3.8-27B |
| `agentic_long_task` | Multi-step, tool-using, long-running tasks: deep research, autonomous coding, data pipelines | GPT-OSS-20B, Qwen-3.6-35B-A3B, Qwen-3.8-27B |
| `general` | Everything else; default and abstain target | GPT-OSS-20B, Qwen-3.6-35B-A3B, Qwen-3.8-27B |

### 5.3 Labeled set

Data sources for the labeled set: MMLU-Pro (science labels), SWE-bench / LiveCodeBench prompts
(software engineering), tau-bench / Terminal-Bench / our own harness logs (agentic), OpenAssistant /
Dolly (general). Multi-turn cases hand-constructed so intent lives in an earlier turn.

---

## 6. Success and exit criteria for the spike

The spike is done when we can answer, with numbers on our taxonomy and fleet:

1. Which signal set to ship first (structural only, +embedding, +classifier, +LLM router).
2. Where the embedding/classifier runs (in-process pure-Go vs fleet agent) and at what latency.
3. Whether task pinning by fingerprint works for the harnesses we care about.
4. Whether the implicit feedback signal is good enough to seed a bandit in a later phase.
5. A go/no-go recommendation and, if go, a scoped Phase 1 design.

**Go** if H1 passes and N2 is met on the chosen signal set. **No-go / rethink** if H1 fails on our
categories even with the best embedding model, since that means the operator-description approach
needs a trained router and a labeling effort before it is useful.

---

## 7. Deliverables

1. `rnd/semantic-routing/PRD.md` (this document).
2. `rnd/semantic-routing/dataset/` labeled prompts with a README on provenance and licence.
3. `rnd/semantic-routing/experiments/` runnable scripts and a results table per hypothesis.
4. A throwaway prototype on this branch implementing F1–F8 minimally, clearly labelled not-for-merge.
5. `rnd/semantic-routing/REPORT.md`: findings, go/no-go, and the Phase 1 design if go.

---

## 8. Constraints discovered in the codebase (inform the prototype)

- Router image is `CGO_ENABLED=0` on Alpine; only the agent links cgo (NVML). See N4.
- `QuotaMiddleware` reads the model name from the body before the handler runs, and strict per-model
  quotas reject models without an entry. Alias resolution must run as a middleware placed before it.
- Body is forwarded verbatim (`RawBytes`) and backends validate the model name, so the resolved model
  must be patched into the forwarded JSON.
- `ChatRequest` already carries `Tools`, `System`, `Messages`; it drops OpenAI `user` and Anthropic
  `metadata.user_id`, which would be better task keys when present.
- Reusable pieces: per-model policy documents and their loader/reload path; embedding agents via the
  processor with `Path: "/v1/embeddings"`; the tokenizer's "estimate, then true up with an EWMA"
  pattern as the template for later learning; `buildModelMap` for alias listing; the audit log.

---

## 9. Decisions

Resolved 2026-09-17:

- **Header override**: no. A concrete model name always bypasses semantic routing (see F2).
- **Fleet models**: GPT-OSS-20B, Qwen-3.6-35B-A3B, Qwen-3.8-27B (section 5.1).
- **Harness data for H4–H6**: a live Claude Code session against the router, set up manually by the
  team; the router's audit/decision log is the capture mechanism.

Still open:

1. **Quota bucket** for alias requests: the resolved model's per-model bucket (recommended; candidates
   without a quota entry are excluded from the candidate set), or the alias itself? Not blocking the
   experiments; blocks the prototype's quota integration.
2. **Location**: `rnd/` at repo root is in use for spike artefacts; say so if you want it elsewhere.

---

## 10. Risks

- **Category overlap.** "Agentic coding" is both software engineering and agentic; the route
  ordering and `pin_task` decide, and the dataset must contain such cases.
- **Embedding hop availability.** If no embedding agent is registered, the alias must degrade to
  structural-only routing, not fail.
- **Licence.** Arch-Router requires "Built with DigitalOcean" attribution; NVIDIA's task classifier is
  under a non-Apache licence. Only Apache-2.0 models (llm-semantic-router/*) are candidates for anything
  we might ship.
- **Feedback signal bias.** Users complain more than they praise; the reward needs normalisation
  before it drives a bandit.
- **Latency claims.** Vendor and paper latency numbers were measured on GPUs; we measure our own.

---

## Appendix A. Research summary (approach families)

Two 2026 benchmarks (LLMRouterBench, 400K instances / 33 models; "The Routing Plateau", 21 methods)
find that most learned routers cluster just above a kNN baseline; candidate pool and encoder quality
dominate. Families and how each handles a new model in the fleet:

| Family | Representative | Needs | New model | Latency |
|---|---|---|---|---|
| Structural / keyword rules | vLLM-SR heuristic signals; Switchyard | nothing | YAML edit | µs |
| Embedding vs route descriptions | aurelio semantic-router; semanticrouter-go; OpenCSGs/semantic-router | embedding model | YAML edit | 5–50 ms |
| Trained encoder classifier | vLLM-SR mmBERT-32k (14 MMLU-Pro domains, 80% acc, 321M, Apache-2.0, ONNX); NVIDIA task+complexity (DeBERTa) | labelled data per label set | retrain if labels change | 10–100 ms CPU |
| LLM as router | Arch-Router 1.5B; NVIDIA llm-router v2 (Qwen 1.7B) | GPU | edit descriptions | 50–300 ms |
| Preference-learned tables | RouteLLM (MF/BERT on Arena votes); P2L (per-prompt Bradley-Terry, 7B); RouterDC | pairwise votes | retrain (fixed index) | 10–100 ms |
| Clustering / kNN | UniRoute; Avengers-Pro; training-free online ANN routing | per-model eval on clusters | evaluate once | <5 ms + embed |
| Bandits / RL | TRACE-Router (task-level, delayed reward); GreenServ (LinUCB); dueling-feedback bandits; Router-R1 (RL, correctness + cost reward) | a reward | arms added online | ms |
| Cascade / trajectory | FrugalGPT; SWE-Router (route after a few cheap turns) | quality signal | none | extra generations |
| Calibrated decision models | Jev / RLCD (TypeSafe AI, Sep 2026; API-only, early access) | vendor | n/a | 70–500 ms |

Agentic routing findings: TRACE-Router (assign the whole task once; learn from the terminal reward),
SWE-Router (partial trajectory beats the prompt), Agent-as-a-Router (accumulate execution experience
per task type). All need a task identity and an outcome signal, which motivates F6 and F7.

Learning methods vs. the signals we have: RLVR needs verifiable outcomes (no); RLHF-style needs
pairwise votes (no); SFT on synthetic labels needs a judge pass (possible); contextual bandits need a
scalar delayed reward (yes, via the implicit next-turn detector); calibration à la RLCD is achievable
with temperature scaling on a held-out set for any classifier.

## Appendix B. Sources
- vLLM Semantic Router: https://vllm-sr.ai/docs/intro , https://github.com/vllm-project/semantic-router , https://arxiv.org/abs/2603.21354 , models https://hf.co/llm-semantic-router
- aurelio semantic-router https://github.com/aurelio-labs/semantic-router ; semanticrouter-go https://github.com/connerohnesorge/semanticrouter-go ; OpenCSGs/semantic-router https://github.com/OpenCSGs/semantic-router
- Arch-Router https://arxiv.org/abs/2506.16655 , https://hf.co/katanemo/Arch-Router-1.5B ; NVIDIA llm-router https://github.com/NVIDIA-AI-Blueprints/llm-router ; Switchyard https://github.com/NVIDIA-NeMo/Switchyard
- RouteLLM https://github.com/lm-sys/RouteLLM ; P2L https://github.com/lmarena/p2l ; hugot https://github.com/knights-analytics/hugot
- Survey https://arxiv.org/abs/2603.04445 ; Routing Plateau https://arxiv.org/abs/2606.07587 ; LLMRouterBench https://arxiv.org/abs/2601.07206 ; LLMRouter https://arxiv.org/abs/2608.06867
- TRACE-Router https://arxiv.org/abs/2607.22465 ; SWE-Router https://arxiv.org/abs/2607.00053 ; Agent-as-a-Router https://arxiv.org/abs/2606.22902 ; Router-R1 https://arxiv.org/abs/2506.09033 ; Dueling feedback https://arxiv.org/abs/2510.00841
- Jev / RLCD https://typesafe.ai/blog/introducing-system-one-models-and-jev , https://www.latent.space/p/ainews-jev-a-system-one-model-that
- vLLM classification serving https://docs.vllm.ai/en/stable/models/pooling_models/classify/
