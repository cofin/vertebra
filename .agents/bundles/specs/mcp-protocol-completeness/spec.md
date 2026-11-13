---
type: Spec
flow_id: mcp-protocol-completeness
title: MCP Protocol Completeness
description: "Complete MCP guard/confirmation/Mcp-Param-* correctness, dual-mode version negotiation with sessions and GET SSE, resource subscriptions, skills and MCP Apps UI extensions, IAP/OIDC auth, and CLI serve/bridge parity."
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-guards-params-and-alias-cleanup
  - ch02-version-negotiation-and-sessions
  - ch03-sse-streams-and-resource-subscriptions
  - ch04-skills-and-mcp-apps-core
  - ch05-framework-mounts-and-litestar-parity
  - ch06-stdio-serve-bridge-and-gcp-auth
  - ch07-docs-and-okf-sync
current_chapter: ch07-docs-and-okf-sync
current_task: "mcp-protocol-completeness:ch07:t01"
parent_prd: integration-remediation
depends_on_flows:
  - server-runtime-correctness
  - gcp-embeddings-and-identity
plan_revision: 3
plan_commit: 18baadcd
last_verified_checkpoint: 7607f4d8
last_reviewed_chapter: ch06-stdio-serve-bridge-and-gcp-auth
last_reviewed_commit: 7607f4d8
created_at: "2026-11-12T23:45:00Z"
updated_at: "2026-11-13T20:35:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T05:29:35Z"
    commit: 0166aa62
tags:
  - mcp
  - agent
  - protocol
  - sessions
  - sse
  - skills
sources:
  - id: agent-context
    resource: "src/py/vertebra/agent/core/context.py::AgentContext"
    title: Trusted AgentContext constructors and context-variable binding landed in 18baadcd
  - id: identity-py
    resource: "src/py/vertebra/agent/core/identity.py::IdentitySource"
    title: "IdentitySource protocol, MtlsIdentitySource, and CallableIdentitySource landed in 18baadcd"
  - id: task-store
    resource: "src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore"
    title: Owner-scoped MCP task store with isolated cancellation landed in 18baadcd
  - id: mcp-core
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore"
    title: Transport-agnostic MCP JSON-RPC and HTTP engine with Mcp-Param-* validation
  - id: mcp-models
    resource: "src/py/vertebra/agent/mcp/models.py::MCPConfig"
    title: "MCP configuration, protocol constants, and resource descriptors"
  - id: tool-schema
    resource: "src/py/vertebra/agent/core/schema.py::Tool"
    title: Single-pass Tool.authorize and Tool.call execution
  - id: conf-gate
    resource: "src/py/vertebra/agent/runtime.py::ConfirmationGate"
    title: Expiring single-use human confirmation ticket store
  - id: sse-broker
    resource: "src/py/vertebra/agent/core/sse.py::SseStreamBroker"
    title: SSE replay buffer and subscriber channel broker
  - id: skills-py
    resource: "src/py/vertebra/agent/mcp/skills.py::SkillCatalog"
    title: Skill catalog and filesystem skill source
  - id: cli-ai
    resource: "src/py/vertebra/cli/ai.py::serve"
    title: vtb ai mcp serve and bridge CLI commands
  - id: litestar-mcp
    resource: "src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin"
    title: LitestarMCP delegation factory and well-known handler
  - id: harness-okf
    resource: .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
    title: Agentic harness OKF chapter currently at 227 lines
---

# Specification: MCP Protocol Completeness

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Trusted `AgentContext` propagation (`from_headers`, `from_scope`, `from_wsgi_environ`), `MtlsIdentitySource`, `CallableIdentitySource`, `MCPConfig` opt-in proxy validation, forked task `CancellationToken`s, and owner-bound `MCPTaskStore` are live on `main` [^agent-context] [^identity-py] [^task-store].
- **Guards, `Mcp-Param-*` & Clean-Break Naming**: Guards run once via `Tool.authorize` (`-32004` / `-32005`), `ConfirmationGate` enforces pending-only exact-owner single-use expiring tickets, `MCPCore.handle` validates `Mcp-Param-*` headers (`-32020`), and the empty `MCPService(MCPCore)` alias subclass is deleted [^mcp-core] [^tool-schema] [^conf-gate].
- **Dual-Mode Protocol, Sessions, `GET` SSE & Subscriptions**: `SUPPORTED_PROTOCOL_VERSIONS = ('2026-07-28', '2025-11-25', '2025-06-18', '2025-03-26')` pairs stateless `2026-07-28` with legacy `MCPSessionStore` `initialize`, `GET /mcp` SSE streams, `DELETE /mcp`, `resources/subscribe`, and `2026-07-28` `subscriptions/listen` ([#109](https://github.com/cofin/vertebra/issues/109), [#112](https://github.com/cofin/vertebra/issues/112)) [^mcp-models] [^sse-broker].
- **Extensions, Mounts & GCP Auth**: Skills advertise `capabilities.extensions['io.modelcontextprotocol/skills']` ([#111](https://github.com/cofin/vertebra/issues/111)), MCP Apps advertise `capabilities.extensions['io.modelcontextprotocol/ui']` with validated `ui://` links ([#110](https://github.com/cofin/vertebra/issues/110)), `IapIdentitySource` verifies ES256 IAP JWTs, and `vtb ai mcp serve`/`bridge` discover real apps and mint OIDC `id_token`s via `AsyncGcpIdTokenCache` ([#114](https://github.com/cofin/vertebra/issues/114)) [^skills-py] [^cli-ai] [^litestar-mcp].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Merged Baseline (`18baadcd`), `ch01` Guards & Tickets (`54f39ff4`) & Remaining Problem Statement

Commit `18baadcd` ([PR #118](https://github.com/cofin/vertebra/pull/118)) landed trusted caller context propagation across ASGI, WSGI, Starlette, FastAPI, Flask, Django, Litestar fallback, native `App`, and `stdio`, alongside `MtlsIdentitySource`, `CallableIdentitySource`, `MCPConfig` proxy opt-in validation, forked task `CancellationToken`s, and owner-isolated `MCPTaskStore` records (documented in [mcp-and-a2a-protocol-harness.md](../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md)) [^agent-context] [^identity-py] [^task-store]. Chapter `ch01-guards-params-and-alias-cleanup` (`54f39ff4`) adds single-pass `Tool.authorize`/`Tool.call` guard execution (`-32004`/`-32005`), `Mcp-Param-*` header validation (`-32020`), expiring single-use `ConfirmationGate` tickets, and canonical `MCPCore` naming [^mcp-core] [^tool-schema] [^conf-gate].

Remaining protocol and harness gaps:
1. **Protocol Version Negotiation, Sessions, `GET` SSE & Subscriptions ([#109](https://github.com/cofin/vertebra/issues/109), [#112](https://github.com/cofin/vertebra/issues/112))**: `MCPCore.handle` rejects `2025-11-25`, `2025-06-18`, and `2025-03-26` clients without `data.supported`, echoes unvalidated `Mcp-Session-Id` values, returns `405` on `GET /mcp` and `DELETE /mcp`, and leaves `_resource_subscriptions` unpopulated [^mcp-core] [^sse-broker].
2. **Skills, MCP Apps UI, Litestar Parity, GCP Auth & CLI ([#110](https://github.com/cofin/vertebra/issues/110), [#111](https://github.com/cofin/vertebra/issues/111), [#114](https://github.com/cofin/vertebra/issues/114))**: Skills use non-standard `capabilities.skills` and are unwired on mounts; resources and tools lack SEP-1865 `title`/`icons`/`_meta` and `ui://` link validation; `litestar-mcp` is pinned below `0.20.0`; inbound IAP JWT verification (`IapIdentitySource`) is absent; and `vtb ai mcp serve`/`bridge` lack app discovery, dual-mode SSE relay, and `AsyncGcpIdTokenCache` `--audience` auth [^skills-py] [^cli-ai] [^litestar-mcp].

### 1.2 Non-Goals

- Adding legacy stateful sessions to `@vertebra/client` `McpClient` (stays stateless `2026-07-28`) or cross-process session persistence (`MCPSessionStore` is per-process in-memory, recorded in [mcp-limitations.md](../../knowledge/domains/agent/mcp-limitations.md)).
- Standalone `.agents/skills` bundle staging ([#115](https://github.com/cofin/vertebra/issues/115), owned by `server-runtime-correctness` `ch03:t04`) or dead Rust `McpStdioBridge` deletion (owned by `http3-and-http-hygiene` `t02`).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **`R1` Single-Pass Guards & `Mcp-Param-*` Validation**: Split `Tool.authorize` + `Tool.call` (`Tool.invoke` composing both), map `PermissionDeniedError` to `-32004` and `RateLimitExceededError`/`BudgetExceededError` to `-32005`, fail closed on 2-arg connection guards without a connection, and validate `Mcp-Param-<key>` request headers against `params` / `params['arguments']` (`400` / `-32020`).
- **`R2` Confirmation Lifecycle & `MCPService` Removal**: Enforce pending-only decisions, exact owner checks, single-use `ConfirmationGate.consume_call_approval`, `ttl_seconds=600.0`, and `max_tickets=10_000`; delete empty `MCPService(MCPCore)` and migrate callers to `MCPCore` / `ToolRegistry` (documenting extension `MCPRegistry` shims until host-extension cleanup).
- **`R3` Version Negotiation, Capabilities & Sessions ([#109](https://github.com/cofin/vertebra/issues/109))**: Negotiate `SUPPORTED_PROTOCOL_VERSIONS`, return `-32022` with `data.supported`/`supportedVersions`/`requested`, return HTTP `200` / `-32601` for unknown or modern-disallowed legacy methods (reserving HTTP `404` for unknown/expired `Mcp-Session-Id`), manage owner-scoped `MCPSessionStore` via `initialize` and `DELETE /mcp`, allow JSON-RPC batch arrays only on `2025-03-26`, and unify `_server_capabilities` + `logging/setLevel`.
- **`R4` `GET /mcp` SSE, Notifications & Subscriptions ([#109](https://github.com/cofin/vertebra/issues/109), [#112](https://github.com/cofin/vertebra/issues/112))**: Stream `McpHttpResponse.stream` across ASGI, Starlette, WSGI, Django, and native `App` `(status, headers, AsyncIterable[bytes])` writers with `Last-Event-ID` replay, heartbeats, and `max_streams_per_owner=8`; publish list-changed, level-gated `MCPCore.log`, and progress events; implement authorized `resources/subscribe`, `notify_resource_updated` fan-out, and `2026-07-28` `subscriptions/listen`.
- **`R5` Skills & MCP Apps Extensions ([#110](https://github.com/cofin/vertebra/issues/110), [#111](https://github.com/cofin/vertebra/issues/111))**: Complete `SkillCatalog` filtering/URI/resource/prompt projection under `capabilities.extensions['io.modelcontextprotocol/skills']`; add `McpIcon`, `title`/`icons`/`meta` on resources and tools, `ToolRegistry.validate_ui_links` / `MCPCore.validate()` under `capabilities.extensions['io.modelcontextprotocol/ui']`, app-only tool filtering, and wiring across `AgentPlugin`, Starlette, Flask, Django, and `litestar-mcp>=0.20.0`.
- **`R6` Stdio, `serve`/`bridge`, IAP & OIDC Auth ([#114](https://github.com/cofin/vertebra/issues/114))**: Run concurrent session-aware `run_stdio_server` against discovered app `MCPCore` in `vtb ai mcp serve`; support dual-mode HTTP + legacy SSE relay and `OidcTokenSource` (`AsyncGcpIdTokenCache`, `401` refresh retry) in `vtb ai mcp bridge --audience`; verify ES256 `x-goog-iap-jwt-assertion` in `IapIdentitySource`.
- **`R7` Docs & OKF Sync**: Update user/CLI/agent docs, changelog, `mcp-and-a2a-protocol-harness.md` (`227L -> <= 200L`), and `mcp-limitations.md`.

### 2.2 Key Public Signatures

```python
# src/py/vertebra/agent/mcp/models.py & sessions.py & service.py
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = ("2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26")
SKILLS_EXTENSION: str = "io.modelcontextprotocol/skills"
UI_EXTENSION: str = "io.modelcontextprotocol/ui"

class McpHttpResponse(FrozenBaseStruct, frozen=True, kw_only=True):
    status: int = 200
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    body: bytes = b""
    stream: AsyncIterator[bytes] | None = None

class MCPSessionStore:
    async def create(self, *, owner_id: str | None, protocol_version: str, client_capabilities: Mapping[str, Any], client_info: Mapping[str, Any], log_level: McpLogLevel) -> MCPSession: ...
    async def get(self, session_id: str, owner_id: str | None) -> MCPSession | None: ...
    async def update(self, session_id: str, owner_id: str | None, *, log_level: McpLogLevel | None = None, subscriptions: frozenset[str] | None = None) -> MCPSession | None: ...
    async def delete(self, session_id: str, owner_id: str | None) -> MCPSession | None: ...
    async def subscribed_session_ids(self, uri: str) -> tuple[str, ...]: ...

class IapIdentitySource:
    def __init__(self, audience: str, *, key_url: str = IAP_PUBLIC_KEY_URL, key_cache_seconds: float = 3600.0, jwks: Mapping[str, Any] | None = None) -> None: ...
    def resolve(self, request: IdentityRequest) -> CallerIdentity | None: ...
```

## 3. Documentation & OKF Knowledge Plan

`ch07-docs-and-okf-sync` updates `12` documentation and OKF files (`<= 200` lines per OKF chapter):
- **Public Docs (`t01`–`t02`)**: `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md`, `docs/reference/agent.md`, `docs/getting-started/cli.md`, `docs/reference/cli.md`, and `docs/changelog/index.md`.
- **OKF Knowledge (`t03`–`t04`)**: [mcp-and-a2a-protocol-harness.md](../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md) (`227L -> <= 200L`) [^harness-okf], [mcp-limitations.md](../../knowledge/domains/agent/mcp-limitations.md), [domains/index.md](../../knowledge/domains/index.md), [cli.md](../../knowledge/domains/framework/cli.md), [litestar.md](../../knowledge/domains/extensions/litestar.md), and [polyglot-test-matrix.md](../../knowledge/testing/polyglot-test-matrix.md).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Tasks | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R2` Guards, Params, Tickets, Alias | `ch01` (`t01`–`t04`) | `agent/core/{schema,guards}.py`, `agent/runtime.py`, `agent/mcp/{service,__init__}.py`, `cli/ai.py` | `pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py` |
| `R3` Versions & Sessions | `ch02` (`t01`–`t03`) | `agent/mcp/{models,sessions,service}.py` | `pytest src/py/tests/unit/agent/test_mcp_conformance.py` |
| `R4` `GET` SSE & Subscriptions | `ch03` (`t01`–`t04`) | `agent/core/sse.py`, `agent/mcp/{routes,registry,sessions,service}.py`, `plugin.py`, `django/mcp.py` | `pytest test_core_and_observability.py test_mcp_conformance.py` |
| `R5` Skills, UI & Mounts | `ch04` (`t01`–`t04`), `ch05` (`t01`–`t03`) | `agent/mcp/{skills,controllers,registry,webmcp,service}.py`, `extensions/{starlette,flask,django,litestar}/*` | `pytest test_mcp_conformance.py test_litestar_ecosystem_mcp.py` |
| `R6` Stdio, Bridge, IAP & OIDC | `ch06` (`t01`–`t03`) | `agent/mcp/{stdio,bridge,client}.py`, `agent/core/identity.py`, `cli/ai.py` | `pytest test_mcp_cli.py test_core_and_observability.py` |
| `R7` Docs & OKF | `ch07` (`t01`–`t04`) | `docs/**`, `.agents/bundles/knowledge/**` | `make docs && make review-check` |

## 5. Implementation Plan

- [x] [1f269a58] [ch01-guards-params-and-alias-cleanup](chapters/ch01-guards-params-and-alias-cleanup/spec.md) — Single-pass guards, `Mcp-Param-*` validation, single-use expiring confirmation tickets, and `MCPService` alias removal (`4` tasks; `< 15s` gate)
- [x] [d7839d5f] [ch02-version-negotiation-and-sessions](chapters/ch02-version-negotiation-and-sessions/spec.md) — `SUPPORTED_PROTOCOL_VERSIONS` negotiation, `MCPSessionStore`, legacy `initialize`, `2025-03-26` batching, and `DELETE /mcp` ([#109](https://github.com/cofin/vertebra/issues/109)) (`3` tasks; `< 12s` gate)
- [x] [353c0d0c] [ch03-sse-streams-and-resource-subscriptions](chapters/ch03-sse-streams-and-resource-subscriptions/spec.md) — `McpHttpResponse.stream`, `GET /mcp` SSE adapter writers, notifications, `resources/subscribe`, and `subscriptions/listen` ([#109](https://github.com/cofin/vertebra/issues/109), [#112](https://github.com/cofin/vertebra/issues/112)) (`4` tasks; `< 15s` gate)
- [x] [7ea0f382] [ch04-skills-and-mcp-apps-core](chapters/ch04-skills-and-mcp-apps-core/spec.md) — `SkillCatalog` disclosure, `io.modelcontextprotocol/skills`, and SEP-1865 `io.modelcontextprotocol/ui` models/projection ([#110](https://github.com/cofin/vertebra/issues/110), [#111](https://github.com/cofin/vertebra/issues/111)) (`4` tasks; `< 15s` gate)
- [x] [0166aa62] [ch05-framework-mounts-and-litestar-parity](chapters/ch05-framework-mounts-and-litestar-parity/spec.md) — Native/Starlette/Flask/Django skill + UI validation wiring and `litestar-mcp>=0.20.0` pin (`3` tasks; `< 15s` gate)
- [x] [7607f4d8] [ch06-stdio-serve-bridge-and-gcp-auth](chapters/ch06-stdio-serve-bridge-and-gcp-auth/spec.md) — Incremental stdio + `vtb ai mcp serve` discovery, dual-mode `bridge --audience` (`AsyncGcpIdTokenCache`), and `IapIdentitySource` ([#114](https://github.com/cofin/vertebra/issues/114)) (`3` tasks; `< 15s` gate)
- [~] [ch07-docs-and-okf-sync](chapters/ch07-docs-and-okf-sync/spec.md) — Agent/CLI docs, changelog, `mcp-and-a2a-protocol-harness.md` (`<= 200L`), and `mcp-limitations.md` sync (`4` tasks; `< 40s` gate)

## 6. Current State & Remaining Plan Snapshot

- **Execution Frontier**: `ch01` (`4` tasks), `ch02` (`3` tasks), `ch03` (`4` tasks), `ch04` (`4` tasks), `ch05` (`3` tasks), and `ch06` (`3` tasks) are completed (`4` open `compact` tasks remain in `ch07`).
- **Cross-Flow & External Gates**: `ch02` has zero upstream blockers; `ch03:t02` consumes progressive streaming from `server-runtime-correctness` `ch01`; `ch06:t02` consumes `AsyncGcpIdTokenCache` from `gcp-embeddings-and-identity` `ch02`; `ch05:t03` is gated on `litestar-mcp>=0.20.0` on PyPI without blocking `ch06`.
- **Full Flow Finish Gate (`/flow:finish`)**: `uv run --no-sync pytest src/py/tests/unit/agent src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py -q && make lint-all && make docs && make review-check`.

[^agent-context]: `src/py/vertebra/agent/core/context.py::AgentContext` — Trusted AgentContext constructors and context-variable binding landed in 18baadcd.
[^identity-py]: `src/py/vertebra/agent/core/identity.py::IdentitySource` — IdentitySource protocol, MtlsIdentitySource, and CallableIdentitySource landed in 18baadcd.
[^task-store]: `src/py/vertebra/agent/mcp/tasks.py::MCPTaskStore` — Owner-scoped MCP task store with isolated cancellation landed in 18baadcd.
[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore` — Transport-agnostic MCP JSON-RPC and HTTP engine with `Mcp-Param-*` validation.
[^mcp-models]: `src/py/vertebra/agent/mcp/models.py::MCPConfig` — MCP configuration, protocol constants, and resource descriptors.
[^tool-schema]: `src/py/vertebra/agent/core/schema.py::Tool` — Single-pass `Tool.authorize` and `Tool.call` execution.
[^conf-gate]: `src/py/vertebra/agent/runtime.py::ConfirmationGate` — Expiring single-use human confirmation ticket store.
[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker` — SSE replay buffer and subscriber channel broker.
[^skills-py]: `src/py/vertebra/agent/mcp/skills.py::SkillCatalog` — Skill catalog and filesystem skill source.
[^cli-ai]: `src/py/vertebra/cli/ai.py::serve` — vtb ai mcp serve and bridge CLI commands.
[^litestar-mcp]: `src/py/vertebra/extensions/litestar/mcp/plugin.py::create_mcp_plugin` — LitestarMCP delegation factory and well-known handler.
[^harness-okf]: `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` — Agentic harness OKF chapter currently at 227 lines.
