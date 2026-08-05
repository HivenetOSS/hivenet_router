# Using Hivenet Router from code

This guide shows how to call a Hivenet Router router from your own scripts and services using the standard OpenAI / Anthropic SDKs and common frameworks (LangChain, etc.). It targets app developers integrating the router into code (not interactive terminal users).

> **Snapshot date — last verified 2026-06-09.**
> The patterns below depend on stable SDK APIs (`base_url` / `baseURL` overrides) and the router's standard endpoints; no time-bound caveats apply today.

## What you get

- Standard, well-maintained SDKs — no custom HTTP client to write or maintain.
- Streaming, tool calls, embeddings, rerank — all the router's allowlisted endpoints reachable as one-line config changes from existing OpenAI / Anthropic code.
- Same auth (`Bearer`), same quotas, same audit logging as any other client.

## Prerequisites

- A reachable Hivenet Router router and an API key.
- One of: **Python ≥ 3.8** (for the Python SDKs) or **Node.js ≥ 18** (for the JS SDKs). Anything more recent works.
- For LangChain: Python ≥ 3.9.
- For Node.js SDK version guidance, see [Common prerequisites — Node.js](00-Overview.md#common-prerequisites--nodejs).

> **Tool-calling note (vLLM-side).** If your code uses tool/function calling, vLLM must be launched with parsing enabled:
> ```bash
> vllm serve <model> --enable-auto-tool-choice --tool-call-parser qwen3_xml
> ```
> Without this, the model will produce tool calls as **plain text** rather than structured `tool_calls` — your code will think no call was made.

---

## 30-second terminal test

The fastest possible "is this working?" check. One install, one inline command, replace two placeholders.

```bash
pip install openai

python3 -c "
from openai import OpenAI
client = OpenAI(
    base_url='https://<router-host>/v1',
    api_key='<api-key>',
)
resp = client.chat.completions.create(
    model='Qwen/Qwen3.6-27B',
    messages=[{'role':'user','content':'say hi in one short sentence'}],
    max_tokens=32,
)
print(resp.choices[0].message.content)
"
```

A line of generated text means the whole chain works (auth, base URL, model routing, response parsing). If you get an error, the [troubleshooting](#troubleshooting) section below resolves it without needing the SDK at all.

---

## Python — OpenAI SDK

The OpenAI Python SDK is the most universal option: stable, well-documented, supports streaming and tool calls. It's what you should reach for unless you have a specific reason not to.

```bash
pip install openai
```

### Non-streaming

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://<router-host>/v1",
    api_key="<api-key>",            # sent as: Authorization: Bearer <api-key>
)

response = client.chat.completions.create(
    model="Qwen/Qwen3.6-27B",
    messages=[
        {"role": "system", "content": "You are a concise assistant."},
        {"role": "user", "content": "Explain HTTP/2 in one sentence."},
    ],
    max_tokens=128,
)

print(response.choices[0].message.content)
print(f"Tokens: in={response.usage.prompt_tokens} out={response.usage.completion_tokens}")
```

### Streaming

```python
stream = client.chat.completions.create(
    model="Qwen/Qwen3.6-27B",
    messages=[{"role": "user", "content": "Count to ten, one per line."}],
    max_tokens=128,
    stream=True,
    stream_options={"include_usage": True},   # final chunk carries exact usage
)

for chunk in stream:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
print()
```

> `stream_options={"include_usage": True}` makes the backend emit a final chunk with `prompt_tokens` / `completion_tokens`. Without it, you only know the totals if you count text yourself.

### Async

```python
import asyncio
from openai import AsyncOpenAI

aclient = AsyncOpenAI(base_url="https://<router-host>/v1", api_key="<api-key>")

async def ask(prompt: str) -> str:
    r = await aclient.chat.completions.create(
        model="Qwen/Qwen3.6-27B",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=64,
    )
    return r.choices[0].message.content

print(asyncio.run(ask("Hello")))
```

### Tool calling

```python
response = client.chat.completions.create(
    model="Qwen/Qwen3.6-27B",
    messages=[{"role": "user", "content": "What's the weather in Paris?"}],
    max_tokens=128,
    tools=[{
        "type": "function",
        "function": {
            "name": "get_weather",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    }],
)

msg = response.choices[0].message
if msg.tool_calls:
    for call in msg.tool_calls:
        print(f"call: {call.function.name}({call.function.arguments})")
else:
    print(msg.content)
```

If `tool_calls` is empty but the model "talks about" calling a tool in `content`, the agent's vLLM is missing `--enable-auto-tool-choice` — fix on the backend, not in your code.

---

## Python — LangChain

LangChain users can point the standard `ChatOpenAI` at the router with two extra parameters. Nothing else in the LangChain code changes.

```bash
pip install langchain langchain-openai
```

```python
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    model="Qwen/Qwen3.6-27B",
    base_url="https://<router-host>/v1",
    api_key="<api-key>",
    max_tokens=128,
)

print(llm.invoke("Say hi in one short sentence.").content)
```

Everything else in LangChain (chains, agents, runnables, tools) works as documented upstream — the router is invisible to them. Same applies to LangGraph.

---

## Node.js — OpenAI SDK

```bash
npm install openai
# or: pnpm add openai / yarn add openai
```

### Non-streaming (ESM)

```js
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "https://<router-host>/v1",
  apiKey: process.env.HIVENET_ROUTER_API_KEY,    // never hard-code the key
});

const resp = await client.chat.completions.create({
  model: "Qwen/Qwen3.6-27B",
  messages: [{ role: "user", content: "Say hi in one short sentence." }],
  max_tokens: 128,
});

console.log(resp.choices[0].message.content);
```

Run with:
```bash
export HIVENET_ROUTER_API_KEY='<api-key>'
node script.mjs
```

### Streaming

```js
const stream = await client.chat.completions.create({
  model: "Qwen/Qwen3.6-27B",
  messages: [{ role: "user", content: "Count to ten." }],
  max_tokens: 128,
  stream: true,
  stream_options: { include_usage: true },
});

for await (const chunk of stream) {
  process.stdout.write(chunk.choices[0]?.delta?.content ?? "");
}
process.stdout.write("\n");
```

---

## Anthropic Python SDK (for `/v1/messages`)

If your code already speaks the Anthropic Messages dialect (e.g. you're porting from `api.anthropic.com`), the Anthropic Python SDK works against the router by overriding the base URL. Less common than the OpenAI SDK, but useful for Anthropic-native codebases.

```bash
pip install anthropic
```

```python
from anthropic import Anthropic

client = Anthropic(
    base_url="https://<router-host>",        # NO /v1 — the SDK appends /v1/messages
    api_key="<api-key>",
    default_headers={"Authorization": f"Bearer <api-key>"},  # see caveat below
)

response = client.messages.create(
    model="Qwen/Qwen3.6-27B",
    max_tokens=128,
    messages=[{"role": "user", "content": "Say hi in one short sentence."}],
)

print(response.content[0].text)
```

> **Caveat — Anthropic SDK sends `x-api-key`, not `Bearer`.** The official Anthropic SDK sets `x-api-key: <api-key>` from the `api_key` argument, which the router does *not* accept. The `default_headers` override above forces a `Bearer` header. The SDK will *also* still send `x-api-key`, but the router ignores it. If you want to be strict and suppress `x-api-key`, override the SDK's `_client` or write a small `httpx` wrapper. Most setups don't need this.
>
> The OpenAI SDK against `/v1/chat/completions` (above) has none of this friction — prefer it unless you specifically need Anthropic features (top-level `system` field, cache_control blocks, etc.).

---

## Other endpoints — embeddings, rerank

The router exposes the OpenAI-shaped embeddings and rerank endpoints too. The same `OpenAI(base_url=…)` client handles both:

```python
client = OpenAI(base_url="https://<router-host>/v1", api_key="<api-key>")

# Embeddings
emb = client.embeddings.create(
    model="BAAI/bge-m3",
    input=["short text to embed", "another piece of text"],
)
print(len(emb.data[0].embedding))   # vector length

# Rerank — not in the OpenAI SDK; use httpx directly
import httpx
r = httpx.post(
    "https://<router-host>/v1/rerank",
    headers={"Authorization": "Bearer <api-key>"},
    json={
        "model": "BAAI/bge-reranker-large",
        "query": "what is HTTP/2",
        "documents": ["HTTP/2 is a protocol...", "Unrelated text..."],
    },
    timeout=30,
)
print(r.json())
```

The model id must match what's registered (`curl /v1/models` to check).

---

## Verifying with `curl` first (the universal smoke test)

Before debugging an SDK-specific issue, prove the router path works with raw `curl`. If `curl` fails, the SDK isn't the problem:

```bash
# Auth + chat
curl -sS https://<router-host>/v1/chat/completions \
  -H "Authorization: Bearer <api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3.6-27B",
    "messages": [{"role":"user","content":"say hi"}],
    "max_tokens": 32
  }' | python3 -m json.tool

# Streaming
curl -sN https://<router-host>/v1/chat/completions \
  -H "Authorization: Bearer <api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3.6-27B",
    "messages": [{"role":"user","content":"count to 5"}],
    "stream": true,
    "max_tokens": 64,
    "stream_options": {"include_usage": true}
  }'

# Model discovery
curl -s https://<router-host>/v1/models -H "Authorization: Bearer <api-key>"
```

A 200 with valid JSON / SSE chunks means the chain works.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `AuthenticationError` / `401 Unauthorized` | Wrong `api_key`, or `apiKey` resolved to `undefined`/`None` from an unset env var | Verify with `echo $HIVENET_ROUTER_API_KEY`; for SDKs that auto-read `OPENAI_API_KEY` ensure no stale value is shadowing yours |
| `404` on `/v1/chat/completions` | `base_url` missing the `/v1`, or has it twice (`/v1/v1/...`) | OpenAI SDK: `base_url` ends in `/v1`. Anthropic SDK: `base_url` is the host root (no `/v1`) — the SDK appends `/v1/messages` itself |
| `400 model_not_found` | Model id doesn't match what an agent serves | `curl /v1/models -H "Authorization: Bearer <key>"` to see exact strings; case- and slash-sensitive |
| Streaming hangs at the start | SDK trying to JSON-decode the streamed response | Set `stream=True` (Python) / `stream: true` (Node) so the SDK uses its streaming code path |
| `tool_calls` is empty but the model "talks about" calling | vLLM launched without tool-call parsing | Restart the agent's vLLM with `--enable-auto-tool-choice --tool-call-parser qwen3_xml` (or `hermes` for Qwen2.5) |
| `stream_options.include_usage` is set but the final chunk has no usage | Backend doesn't emit the closing usage chunk | Drop `include_usage`; the router's own meter still records correct totals for billing and the audit log |
| `429 Too Many Requests` on a single call | Either RPM bucket exhausted or the request's worst-case `max_tokens` exceeds the remaining daily token budget | Check the `X-RateLimit-Remaining-Requests` and `X-RateLimit-Remaining-Tokens` response headers; lower `max_tokens` or wait |
| Anthropic SDK 401 despite valid key | Anthropic SDK sent `x-api-key` only; router needs `Bearer` | Pass `default_headers={"Authorization": "Bearer <key>"}` (see [Anthropic SDK section](#anthropic-python-sdk-for-v1messages)) |

---

## Known limitations

- **Anthropic SDK sends `x-api-key` automatically.** Workaround above. Use the OpenAI SDK against `/v1/chat/completions` to avoid this entirely.
- **Backend 4xx responses currently bench the agent** via the router's `prev_failures` gate (router behaviour, not SDK-specific). A misformatted request from your code can transiently take all agents serving a model offline. Durable fix is a router-side change to distinguish backend 4xx from agent failures.
- **No formal Python/JS Hivenet Router SDK** — you use the upstream OpenAI / Anthropic SDKs against the router's URL. This is intentional: it keeps you on the most stable, best-documented client libraries; nothing to maintain on the Hivenet Router side beyond the OpenAI-compatible endpoint contract.

---

## See also

- [Integrations Overview](00-Overview.md) — when to use code vs Claude Code / OpenCode / pi / Open WebUI.
- [Inference Endpoints](../API%20Reference/01-Chat-Completions.md) — `/v1/chat/completions` and `/v1/messages` reference and the allowlisted passthrough.
- [auth.yaml Reference](../Security%20%26%20Auth/03-auth.yaml-Reference.md) — API keys, quotas, token budget enforcement, rate-limit headers.
- [OpenAI Python SDK](https://github.com/openai/openai-python) — upstream reference for `OpenAI(base_url=...)`.
- [OpenAI Node SDK](https://github.com/openai/openai-node) — upstream reference for `new OpenAI({ baseURL })`.
- [LangChain `ChatOpenAI`](https://python.langchain.com/docs/integrations/chat/openai/) — upstream reference for `base_url` and `api_key` parameters.
- [Anthropic Python SDK](https://github.com/anthropics/anthropic-sdk-python) — upstream reference, including `base_url` and `default_headers`.
