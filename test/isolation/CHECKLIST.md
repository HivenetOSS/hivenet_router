# Tenant-isolation checklist

This is the checklist from `hivenet_ai_infra` doc 18 (Isolation and Security Model), turned into tests
(Epic G, ticket N8-3). It must pass before customers share one router pool.

Each line names the test that proves it. Run them all with:

```bash
go test ./test/... -run 'TestIsolation|TestChecklist' -count=1
```

`TestChecklist_LinesHaveTests` (in this directory) keeps this file honest:

- Every test named on a `done` line must exist under `test/`.
- Every `TestIsolation_*` function under `test/` must appear in this file.

**Status values:**

- `done`: the test exists and passes.
- `planned (PR-n)`: will land in that PR of the Epic G stack.
- `Epic X`: owned by another epic.
- `infra`, `platform`: owned outside this repository.
- `out of scope`: decided not to do; the reason is in the note.

## 1. Key → customer on every request

| # | Line | Status | Tests |
|---|---|---|---|
| 1.1 | Tenant and key come only from the credential. Headers and body fields cannot override them, on every inference endpoint. | done | `TestIsolation_TenantFromKeyOnly` |
| 1.2 | Every `/v1` route requires a valid API key; the admin key is not one. | done | `TestIsolation_V1RoutesRequireKey` |
| 1.3 | Every static `auth.yaml` key has a stable, unique key ID (doc-18 G8). | planned (PR-1) | `TestStaticKeyProvider_DerivesStableKeyID` |
| 1.4 | Key IDs and owners cannot contain characters that alias another key's counters. | planned (PR-1) | `TestKeyIdentifiers_RejectControlChars` |
| 1.5 | A redirect token resolves to its own key and tenant at the target. | done | `TestRedirect_AtTheTarget` |
| 1.6 | Concurrent responses are never delivered to the wrong request or tenant, streaming or not. | done | `TestIsolation_ConcurrentResponsesNotCrossWired` |
| 1.7 | Multi-tenant mode refuses API auth `none`, at startup and on SIGHUP. | planned (PR-8) | `TestValidateIsolationConfig`, `TestIsolation_SIGHUPCannotDropToNoAuth` |

## 2. Per-customer counters, limits and scope

| # | Line | Status | Tests |
|---|---|---|---|
| 2.1 | RPM and daily-token counters are per key, checked against that key's own limits. Another key cannot overwrite a key's ceiling. | planned (PR-2) | `TestInMemoryLimiter_PerKeyBucketsWithinTenant`, `TestInMemoryLimiter_KeyCeilingNotOverwrittenByOtherKey` |
| 2.2 | Input/output tokens-per-minute and occupancy share are per key, static keys included. | planned (PR-1, PR-2) | `TestIsolation_StaticKeysHaveDistinctB4State`, `TestMinute_PerKeyWithinTenant` |
| 2.3 | A key ID reassigned to another tenant starts from zero. | planned (PR-2) | `TestInMemoryLimiter_SameKeyIDDifferentTenantIsolated` |
| 2.4 | `X-RateLimit-*` headers describe the calling key only. | planned (PR-2) | `TestIsolation_RateLimitHeadersArePerKey` |
| 2.5 | Tenant A exhausting its quota never causes a 429 for tenant B (end to end). | planned (PR-2) | `TestIsolation_TwoTenantsQuotaE2E` |
| 2.6 | With the model allowlist required, a key without one is refused on inference, discovery, aliases and redirects. | planned (PR-3) | `TestIsolation_RequireModelAllowlist` |
| 2.7 | Embeddings and rerank honour the same allowlist as chat (`per_model` keys included). | planned (PR-3) | `TestEmbeddings_HonoursEffectiveAllowlist` |
| 2.8 | A redirect token's scope cannot be widened by the client. | done | `TestIsolation_RedirectTokenCannotWidenScope`, `TestRedirect_Scope` |
| 2.9 | A key's `scope_max` caps the scope a request may ask for. | Epic F | `TestIsolation_ScopeMaxCapsKeyScope` (skipped until Epic F) |
| 2.10 | One quota across all of a customer's keys and policlouds. | Epic L | none |
| 2.11 | Capacity is shared per model (B2/B3 budgets, queue, wait queue), so one customer's burst can slow another (noisy neighbour, vLLM ④). | out of scope | none: accepted residual; `max_occupancy_share` caps one key's share, and a dedicated replica (Tier 1, Epic N) removes it |

## 3. Per-customer prefix-cache salt (vLLM ①, CVE-2025-46570)

| # | Line | Status | Tests |
|---|---|---|---|
| 3.1 | vLLM agents get a per-tenant `cache_salt` on chat (streaming and not), `/v1/messages`, embeddings and rerank. Keys of one tenant share it; tenants never do. | planned (PR-4) | `TestIsolation_CacheSaltPerTenant` |
| 3.2 | The salt is an HMAC of the tenant under a dedicated secret: 43 base64url characters, unguessable. | planned (PR-4) | `TestSalt_FormatAndStability` |
| 3.3 | A `cache_salt` sent by the client is always removed (duplicates and escaped spellings included). | planned (PR-4) | `TestSetTopLevel`, `FuzzSetTopLevel` |
| 3.4 | Engines without salt support get no salt, and no client salt either. | planned (PR-4) | `TestApply_EngineCapabilities` |
| 3.5 | The salt never reaches the OpenAI/Anthropic fallback providers. | planned (PR-4) | `TestProviderFallback_NeverSendsCacheSalt` |
| 3.6 | Salting requires its own secret (at least 32 bytes, not the agents' JWT secret). | planned (PR-4) | `TestValidateIsolationConfig` |
| 3.7 | With salting off, bodies are forwarded byte-identical. | done | `TestSemanticAliasEndToEnd` |
| 3.8 | vLLM runs with the `sha256` prefix hash, at a patched version (≥ 0.11.1, vLLM ③). | infra | none: vLLM launch flags |

## 4. Customer-scoped logs and metrics

| # | Line | Status | Tests |
|---|---|---|---|
| 4.1 | Every authenticated `/v1` audit and decision-log line carries the caller's `tenant_id` and a non-empty `key_id`. | planned (PR-5) | `TestIsolation_LogsAttributable` |
| 4.2 | No log contains a prompt, response, credential, salt secret or salt (doc-18 G7, as a regression test). | planned (PR-5) | `TestIsolation_LogsContentFree` |
| 4.3 | The audit log file is readable by its owner only (0600). | planned (PR-5) | `TestIsolation_AuditFileMode` |
| 4.4 | A request moves only its own tenant's `hivenet_tenant_*` series. | planned (PR-2) | `TestIsolation_TenantMetricsAttribution` |
| 4.5 | `/metrics` can require a bearer token (doc-18 G3). | planned (PR-7) | `TestMetricsHandler_BearerToken` |
| 4.6 | Multi-tenant mode refuses to start with an open `/metrics`. | planned (PR-8) | `TestRouterNew_MultiTenantRefusesOpenMetrics` |
| 4.7 | Splitting logs per customer. | out of scope | none: done in the log pipeline (for example a Loki label on `tenant_id`) |

## 5. No admin endpoint leaking across customers

| # | Line | Status | Tests |
|---|---|---|---|
| 5.1 | Every `/admin` route rejects missing credentials and every tenant's API key. | done | `TestIsolation_AdminRoutesRejectAPIKeys` |
| 5.2 | The admin key cannot be used as an API key. | done | `TestIsolation_AdminKeyIsNotAnAPIKey` |
| 5.3 | Admin auth fails closed unless insecure admin is explicitly allowed. | done | `TestProvidersFromConfig_AdminNoAuth_FailsClosed` |
| 5.4 | `/admin` responses carry no CORS headers; `/v1` CORS is unchanged (part of doc-18 G4). | planned (PR-6) | `TestIsolation_CORSNotOnAdmin` |
| 5.5 | `/v1/models/:model` can hide agent identity (peer ID, organization, machine). | planned (PR-6) | `TestIsolation_ModelDetailHidesAgentIdentity` |
| 5.6 | Model discovery never names models outside the key's allowlist. | done | `TestIsolation_TenantFromKeyOnly` (identity), `handlers_models_test.go` (filtering) |
| 5.7 | A per-customer admin role. | out of scope | none: admin stays operator-only |
| 5.8 | Multi-tenant mode refuses insecure admin. | planned (PR-8) | `TestValidateIsolationConfig` |

## 6. Doc-18 gaps owned elsewhere

| # | Line | Status | Tests |
|---|---|---|---|
| 6.1 | G1: a key reaches only its own dedicated replicas. | Epic N | none |
| 6.2 | G2: client credentials are stripped before the agent. | done | `TestBackendAPIKeyEndToEnd` |
| 6.3 | G4: TLS for `/v1` in-process. | out of scope | none: TLS terminates in front of the router |
| 6.4 | G5: one collision-free request ID end to end. | Epic A | none: branch `feat/single-request-id` |
| 6.5 | G6: per-agent credentials; agent-reported metadata verified. | out of scope | none |
| 6.6 | G7: a type that makes body logging impossible. | out of scope | none: 4.2 is the regression test |
| 6.7 | vLLM ②: GPU memory residue between successive tenants. | platform | none: GPU reset at hand-off (doc 16) |
