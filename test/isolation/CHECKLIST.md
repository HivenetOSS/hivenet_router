# Tenant-isolation checklist

This is the checklist from `hivenet_ai_infra` doc 18 (Isolation and Security Model), turned into tests
(Epic G, ticket N8-3). It must pass before customers share one router pool.

Each line names the test that proves it. Run them all with:

```bash
go test ./test/... -run 'TestIsolation|TestChecklist' -count=1
```

`TestChecklist_LinesHaveTests` (in this directory) keeps this file honest:

- Every row has four cells and a status from the list below. Escape a `|` inside a cell as `\|`.
- A `done` line cites at least one test, and every backticked name in its Tests cell is a test that
  exists under `test/` and does not start with an unconditional `t.Skip`. File names do not count.
- Every `TestIsolation_*` function under `test/` appears in this file.

**Status values:**

- `done`: the tests exist and pass.
- `planned (PR-n)`: will land in that PR of the Epic G stack.
- `planned (#n)`: will land in that open pull request outside the Epic G stack.
- `Epic X`: owned by another epic.
- `infra`, `platform`: owned outside this repository.
- `out of scope`: decided not to do; the reason is in the note.

## 1. Key → customer on every request

| # | Line | Status | Tests |
|---|---|---|---|
| 1.1 | Tenant and key come only from the credential. Headers and body fields cannot override them, on every inference endpoint. | done | `TestIsolation_TenantFromKeyOnly` |
| 1.2 | Every `/v1` route requires a valid API key; the admin key is not one. No route sits outside `/v1`, `/admin` and the public list (`/health`). | done | `TestIsolation_V1RoutesRequireKey`, `TestIsolation_NoUnclassifiedRoutes` |
| 1.3 | Every static `auth.yaml` key has a stable, unique key ID; so does every admin key (doc-18 G8). | done | `TestStaticKeyProvider_DerivesStableKeyID`, `TestStaticKeyProvider_KeyHashNormalized`, `TestStaticAdminKeyProvider_KeyIDPerKey` |
| 1.4 | Key IDs and owners cannot contain characters that alias another key's counters or disguise a log line (control, invisible and line-separator characters, whitespace other than a plain space, a space at either end), and cannot take an identity the router assigns itself (`admin`, `default`, `anonymous`, `static-…`, `admin-…`). | done | `TestKeyIdentifiers_RejectControlChars`, `TestKeyIdentifiers_RejectReserved` |
| 1.5 | A redirect token resolves to its own key and tenant at the target. | done | `TestRedirect_AtTheTarget` |
| 1.6 | Concurrent responses are never delivered to the wrong request or tenant, streaming or not. | done | `TestIsolation_ConcurrentResponsesNotCrossWired` |
| 1.7 | Multi-tenant mode refuses API auth `none`, at startup and on SIGHUP (the previous keys stay in force). | planned (PR-8) | `TestValidateIsolationConfig`, `TestIsolation_SIGHUPCannotDropToNoAuth` |
| 1.8 | A static key's hash is checked like a dynamic key's (64 hex characters, either case): a hash that could never authenticate fails at load. | done | `TestStaticKeyProvider_KeyHashNormalized` |
| 1.9 | The router reads a request the way the backend does: a body with a repeated name, or with a differently cased spelling of a field the router reads (Unicode case folding included, e.g. `Model` next to `model`), at the top level or in an object it reads inside (each message, content part or block, `system` block), is refused with 400 before any allowlist, quota or routing decision, on every inference endpoint. | done | `TestIsolation_AmbiguousFieldsRefused`, `TestIsolation_ForbiddenModelCostsNothing`, `TestCheckNames`, `TestCheckShape` |

## 2. Per-customer counters, limits and scope

| # | Line | Status | Tests |
|---|---|---|---|
| 2.1 | RPM and daily-token counters are per key (persisted per key with the Badger backend), checked against that key's own limits. Another key cannot overwrite a key's ceiling. | done | `TestInMemoryLimiter_PerKeyBucketsWithinTenant`, `TestInMemoryLimiter_KeyCeilingNotOverwrittenByOtherKey`, `TestBadgerLimiter_PerKeyPersistence` |
| 2.2 | Input/output tokens-per-minute and occupancy share are per key, static keys included. | done | `TestIsolation_StaticKeysHaveDistinctB4State` |
| 2.3 | A key ID reassigned to another tenant starts with fresh counters: requests per minute, tokens per minute and tokens per day. | done | `TestInMemoryLimiter_SameKeyIDDifferentTenantIsolated`, `TestMinute_KeyReassignedToOtherTenantStartsFresh` |
| 2.4 | `X-RateLimit-*` headers describe the calling key only. | done | `TestIsolation_RateLimitHeadersArePerKey` |
| 2.5 | A key exhausting its request rate or daily tokens never causes a 429 for another key, of its own tenant or another (end to end, output tokens included). | done | `TestIsolation_TwoTenantsQuotaE2E` |
| 2.6 | With the model allowlist required, a key without one is refused on inference, discovery, aliases and redirects. The setting cannot be combined with auth mode `none`. | done | `TestIsolation_RequireModelAllowlist`, `TestValidateIsolationConfig`, `TestLoadFromEnv_IsolationSwitches` |
| 2.7 | Embeddings and rerank honour the same allowlist as chat (`per_model` keys included). | done | `TestEmbeddings_HonoursEffectiveAllowlist` |
| 2.8 | A redirect token's scope cannot be widened by the client. | done | `TestIsolation_RedirectTokenCannotWidenScope`, `TestRedirect_Scope` |
| 2.9 | A key's `scope_max` caps the scope a request may ask for. | Epic F | `TestIsolation_ScopeMaxCapsKeyScope` (skipped until Epic F) |
| 2.10 | A tenant-wide daily token budget: the lowest `tenant_tokens_per_day` among a tenant's keys caps all of them together, site-wide. A request refused by the key's or the tenant's budget charges neither, and its 429 names the budget that refused it, at admission and after the response; a refusal the budget does not explain (one router's share used up for the moment) carries `Retry-After`. | done | `TestInMemoryLimiter_TenantCap`, `TestTenantCapSharedAcrossRouters`, `TestIsolation_TenantCapE2E`, `TestIsolation_TenantCapOutputRefusalNamesOrganization` |
| 2.11 | Capacity is shared per model (B2/B3 budgets, queue, wait queue), so one customer's burst can slow another (noisy neighbour, vLLM ④). | out of scope | none: accepted residual; `max_occupancy_share` caps one key's share, and a dedicated replica (Tier 1, Epic N) removes it |
| 2.12 | A model outside the key's allowlist is refused with 403 before any quota is charged, any model is woken or any redirect is issued, on every inference endpoint. | done | `TestIsolation_ForbiddenModelCostsNothing` |
| 2.13 | A key with both `allowed_models` and `quota.per_model` may use only the models in both. | done | `TestAllowlist_IntersectsAllowedModelsAndPerModel` |
| 2.14 | Daily budgets (the key's and the tenant's) and the per-minute input rate count the prompt the backend reports, not only the router's estimate of it: the difference is charged after the response, and a non-streaming answer it does not fit is refused. | done | `TestIsolation_InputChargedFromBackendCount`, `TestB4_ITPMChargesBackendPromptTokens`, `TestInputShortfall`, `TestMinute_ChargeInputTokens` |

## 3. Per-customer prefix-cache salt (vLLM ①, CVE-2025-46570)

| # | Line | Status | Tests |
|---|---|---|---|
| 3.1 | vLLM agents get a per-tenant `cache_salt` on chat (streaming and not), `/v1/messages`, embeddings and rerank. Keys of one tenant share it; tenants never do. | done | `TestIsolation_CacheSaltPerTenant` |
| 3.2 | The salt is an HMAC of the tenant under a dedicated secret: 43 base64url characters, unguessable. | done | `TestSalt_FormatAndStability` |
| 3.3 | A `cache_salt` sent by the client is always removed (escaped spellings included); one sent twice is refused before routing (1.9). | done | `TestSetTopLevel`, `TestRemoveTopLevel`, `FuzzSetTopLevel`, `TestIsolation_CacheSaltPerTenant` |
| 3.4 | Every engine that can isolate its prefix cache does: vLLM gets the salt (chat isolated from v0.9.0, `/v1/messages` from v0.27.0, as the agent reports it, also after the engine restarts with another build), SGLang chat gets the salt (isolated from v0.5.11), llama.cpp skips its prompt cache (on `/v1/messages` through `extra_params`, from build b11795). Engines, versions and paths that cannot get no client salt, and each LLM agent left with a gap is logged and counted (`hivenet_cache_isolation_gaps`). | done | `TestApply_EngineCapabilities`, `TestCapabilities_Gaps`, `TestAtLeast`, `TestEngineVersion_ReadFromBackend`, `TestIsolation_CacheSaltPerTenant`, `TestEngineVersionFollowsBackendRestart` |
| 3.5 | The salt never reaches the OpenAI/Anthropic fallback providers. | done | `TestProviderFallback_NeverSendsCacheSalt` |
| 3.6 | Salting requires its own secret: at least 32 bytes, and none of the router's other secrets (JWT, key store, redirect, metrics token, Valkey password). | done | `TestValidateIsolationConfig`, `TestNew_RejectsShortSecret` |
| 3.7 | With salting off, bodies are forwarded byte-identical. | done | `TestSemanticAliasEndToEnd` |
| 3.8 | vLLM runs with the `sha256` prefix hash, at a patched version (≥ 0.11.1, vLLM ③). | infra | none: vLLM launch flags |
| 3.9 | An inference body must be exactly one JSON object. A body the router cannot prepare for the agent is refused before routing and never counts for or against an agent. | done | `TestIsolation_TrailingDataRejected`, `TestProcessor_UnsentRequestLeavesAgentCounters` |
| 3.10 | Prefix-aware routing keys its prefix index per tenant. | planned (#102–#104) | none: the anchors are scoped through `WithPrefixScope` once Epic G is on `main` |

## 4. Customer-scoped logs and metrics

| # | Line | Status | Tests |
|---|---|---|---|
| 4.1 | Every authenticated `/v1` audit and decision-log line carries the caller's `tenant_id` and a non-empty `key_id`. | done | `TestIsolation_LogsAttributable`, `TestIsolation_DecisionLogAttributable` |
| 4.2 | No log contains a prompt, response, backend error body, credential, salt secret or salt (doc-18 G7, as a regression test), on successful requests and on backend errors (4xx, 5xx, retried or not). | done | `TestIsolation_LogsContentFree`, `TestIsolation_BackendErrorsContentFree`, `TestDecodeAgentError` |
| 4.3 | The audit log file and the decision log are readable by their owner only (0600). | done | `TestIsolation_AuditFileMode`, `TestDecisionLog_FileMode` |
| 4.4 | A request moves only its own tenant's `hivenet_tenant_*` series, labelled with its own key ID. Tenant usage counts every key; the limit gauges sum the tenant's keys. | done | `TestIsolation_TenantMetricsAttribution`, `TestInMemoryLimiter_TenantUsageSumsKeysAndModels`, `TestTenants_AggregatesKeyLimits` |
| 4.5 | `/metrics` can require a bearer token and bind one interface (doc-18 G3). | planned (PR-7) | `TestMetricsHandler_BearerToken`, `TestIsPrivateBindAddr` |
| 4.6 | Multi-tenant mode refuses to start with `/metrics` open to anything but a loopback or private address without a token. | planned (PR-8) | `TestRouterNew_MultiTenantRefusesOpenMetrics`, `TestValidateIsolationConfig` |
| 4.7 | Splitting logs per customer. | out of scope | none: done in the log pipeline (for example a Loki label on `tenant_id`) |
| 4.8 | Every request's spans carry `tenant.id`, and no span status or event carries a prompt or a backend error body. | done | `TestIsolation_SpansContentFree`, `TestDecodeAgentError` |

## 5. No admin endpoint leaking across customers

| # | Line | Status | Tests |
|---|---|---|---|
| 5.1 | Every `/admin` route rejects missing credentials and every tenant's API key. | done | `TestIsolation_AdminRoutesRejectAPIKeys` |
| 5.2 | The admin key cannot be used as an API key. | done | `TestIsolation_AdminKeyIsNotAnAPIKey` |
| 5.3 | Admin auth fails closed unless insecure admin is explicitly allowed. | done | `TestProvidersFromConfig_AdminNoAuth_FailsClosed` |
| 5.4 | `/admin` responses carry no CORS headers; `/v1` CORS is unchanged (part of doc-18 G4). | done | `TestIsolation_CORSNotOnAdmin` |
| 5.5 | `/v1/models/:model` can hide agent identity (peer ID, organization, machine, version, last seen); the admin view keeps it. | done | `TestIsolation_ModelDetailHidesAgentIdentity` |
| 5.6 | Model discovery (`/v1/models`, `/v1/models/:model`) never names models outside the key's allowlist, from `models` or `quota.per_model`; a hidden model answers like an unknown one. | done | `TestIsolation_DiscoveryHidesModelsOutsideAllowlist` |
| 5.7 | A per-customer admin role. | out of scope | none: admin stays operator-only |
| 5.8 | Multi-tenant mode refuses insecure admin. | planned (PR-8) | `TestValidateIsolationConfig` |
| 5.9 | `/v1` answers CORS with `*`. This is safe: `/v1` accepts only bearer credentials, which a browser never attaches on its own, so a page on another origin gets nothing it could not fetch without the browser. | done | `TestCORSMiddleware_Preflight` |
| 5.10 | With agent identity hidden, only an allowlist of backend response headers reaches the client. | done | `TestIsolation_HiddenIdentityFiltersBackendHeaders` |

## 6. Multi-tenant mode

| # | Line | Status | Tests |
|---|---|---|---|
| 6.1 | `--multi-tenant` always turns on the model allowlist and hides agent identity; prefix-cache isolation is on unless explicitly switched off (6.3). It works with static keys and with dynamic keys pushed by key-sync, as in production. | planned (PR-8) | `TestIsolation_MultiTenantProductionProfile`, `TestApplyMultiTenant` |
| 6.2 | Multi-tenant mode always needs shared counters (`--sync-store=valkey`) with a password, however many routers the site runs, so per-key and tenant limits hold site-wide. A Valkey that is not on loopback and has no TLS gets a startup warning. | planned (PR-8) | `TestValidateIsolationConfig`, `TestIsolation_MultiTenantWarnsOnPlaintextValkey` |
| 6.3 | Prefix-cache isolation is on by default in multi-tenant mode and can be switched off only explicitly; the router then warns and counts every LLM agent as a gap. | planned (PR-8) | `TestApplyMultiTenant`, `TestIsolation_MultiTenantCacheSaltOptOut` |
| 6.4 | The settings are checked before anything irreversible: a refused start leaves the disk database untouched, even with `--reset-disk-db`. | planned (PR-8) | `TestRouterNew_RefusedStartKeepsDiskDB` |
| 6.5 | A multi-tenant router whose audit log path cannot be opened refuses to start instead of writing every tenant's audit lines to stdout. | planned (PR-8) | `TestRouterNew_MultiTenantRefusesUnopenableAuditLog` |

## 7. Doc-18 gaps owned elsewhere

| # | Line | Status | Tests |
|---|---|---|---|
| 7.1 | G1: a key reaches only its own dedicated replicas. | Epic N | none |
| 7.2 | G2: client credentials are stripped before the agent. | done | `TestBackendAPIKeyEndToEnd` |
| 7.3 | G4: TLS for `/v1` in-process. | out of scope | none: TLS terminates in front of the router |
| 7.4 | G5: one collision-free request ID end to end, a distinct one on each retry. | done | `TestRequestID_GeneratedIsV7`, `TestRequestID_RouterAlwaysMints`, `TestRequestID_PendingRequestUsesMiddlewareID`, `TestRetrySendsItsOwnRequestID` |
| 7.5 | G6: per-agent credentials; agent-reported metadata verified. | out of scope | none: agents run only on operator infrastructure; per-agent credentials are tracked separately |
| 7.6 | G7: a type that makes body logging impossible. | planned (#109) | none yet: 4.2 and 4.8 are the regression tests until #109 lands |
| 7.7 | vLLM ②: GPU memory residue between successive tenants. | platform | none: GPU reset at hand-off (doc 16) |

## 8. Isolation between the routers of a site

Every router of a site serves every agent and shares its counters through Valkey (`--sync-store=valkey`), so
the shared state is where one tenant could reach another's.

| # | Line | Status | Tests |
|---|---|---|---|
| 8.1 | Request, tokens-per-minute and daily counters are shared by every router of the site: a limit holds once per site, not once per router. | done | `TestRPMSharedAcrossRouters`, `TestRPMWithoutSharingMultiplies`, `TestTokensPerMinuteShared`, `TestDailySharedAcrossRouters` |
| 8.2 | The per-key buckets, tenant usage and tenant budgets of Epic G are the shared ones, so they too hold site-wide. | done | `TestPerKeyBudgetsSharedAcrossRouters`, `TestTenantUsageSiteWide`, `TestTenantCapSharedAcrossRouters`, `TestDailyShareRefusalIsRetryable` |
| 8.3 | While Valkey is unreachable each router enforces its share of every limit, and a key with a daily limit is refused until the site total is known. | done | `TestStoreDownEachRouterEnforcesItsShare`, `TestDailyFailsClosedWithoutTotals` |
| 8.4 | Occupancy budgets (B2, B4) are shared exactly: the routers together never hold more than the budget. | done | `TestOccupancySharedAcrossRouters`, `TestCreditsNeverOversubscribe` |
| 8.5 | A cross-site redirect token is accepted once by the whole site: a replay, or a token used by someone else, is refused. | done | `TestRedirect_SingleUseCannotBeStolen`, `TestReplayGuard`, `TestTokenAuth_SingleUseIsLast` |
| 8.6 | Every agent serves every router of the site and finds them through the routers themselves. | done | `TestAgentServesEveryRouterOfTheSite`, `TestAgentDiscoversEveryRouterOfTheSite` |
| 8.7 | Every router of the site uses the same cache-salt secret: a router that finds another secret's fingerprint in the shared store warns and counts it. | planned (PR-8) | `TestSaltFingerprintMismatchReported` |
