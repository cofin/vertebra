---
type: Spec
flow_id: mcp-webmcp-core
title: MCP 2026-07-28 Core and WebMCP Bridge
description: Build an agent-owned ToolSpec and ToolRegistry over the shared ToolProvider contract, a transport-agnostic MCPCore, framework adapters, and a WebMCP projection and TypeScript bridge.
state: active
parent_prd: integration-remediation
research:
- ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: "55a6002aabb2d4e2a8bdadd0f7014ea643217ffe"
state_revision: 2
current_task: t02
last_verified_checkpoint: null
created_at: '2026-11-03T03:30:00Z'
updated_at: '2026-11-06T21:39:00Z'
tags:
- mcp
- webmcp
- agent
- typescript
---

# MCP 2026-07-28 Core and WebMCP Bridge

## 1. Specification & Context

### 1.1 Upstream Execution Baseline

This flow executes on `feat/mcp-webmcp-core`, stacked on `feat/runtime-structure-and-throughput` (`c7b62a8c105f39c69f8411345c1a3ebb6722dfa7`) above `fix/post-merge-review-and-plan-sync` (`ebbb7fb07950ac7fc871f94ec6c9de66fa33e401`) and `main @ 80ff8a65e43a2bcef9e48a7763e89a7352118232`. The neutral `vertebra.typing.ToolProvider` callable-map protocol, adapter-owned `BigQueryToolProvider` / `SpannerToolProvider` (`vertebra.db.adapters.{bigquery,spanner}.tools`), generic `vertebra.agent.tools.build_tools` consumer, and argument-bound `ConfirmationGate` tokens were merged in [PR #77](https://github.com/cofin/vertebra/pull/77) (`80ff8a65`) and hardened in `7657e8b7`.

### 1.2 Code Analysis Summary

| Existing symbol | Verified consequence |
| --- | --- |
| `src/py/vertebra/agent/core/schema.py::{SchemaBuilder,Tool}` | Walks `dataclasses` / `TypedDict` manually instead of `msgspec.defstruct` + `msgspec.json.schema_components`, calls `eval(...)` on string annotations in `Tool._convert_argument`, and lacks an immutable `ToolSpec` struct with explicit `exposure` sets and output schemas. |
| `src/py/vertebra/agent/mcp/registry.py::MCPRegistry` & `src/py/vertebra/agent/tools/__init__.py::build_tools` | Separate registries discover `@tool`, `SkillController`, routes, and `ToolProvider` callables without a single `ToolRegistry` preserving original typed signatures, per-call confirmation reasons, and lazy binding. |
| `src/py/vertebra/agent/mcp/service.py::MCPService` | Mixes JSON-RPC dispatch with legacy `2024-11-05` methods (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`) instead of a transport-agnostic `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` enforcing MCP `2026-07-28` HTTP rules (`_meta` version negotiation, same-origin `Origin` `403`, `GET` `405`, notification `202`, batch `400`, `server/discover`, `ETag`). |
| `src/py/vertebra/extensions/{flask,django,starlette,fastapi,litestar}/mcp` & `src/py/vertebra/agent/plugin.py` | Flask and Django duplicate `_dispatch_jsonrpc` and local `MCPConfig` classes and spin per-request `asyncio.run` loops; Litestar `opt["mcp_tool"]` risks double registration alongside `litestar-mcp`; `/.well-known/mcp.json` schemas diverge. |
| `src/py/vertebra/agent/mcp/webmcp.py` & `src/ts/vertebra-client/src/{webmcp.ts,agent/index.ts}` | `webmcp.py` reads `_mcp_tool_meta` dicts instead of `ToolSpec.exposure` (`"webmcp" in exposure`), omits `title`, `outputSchema`, and `consequentialHint`; `webmcp.ts` checks only `navigator.modelContext`, keeps legacy `provideContext`, and lacks `AbortSignal` disposal and HMR idempotency; `build/_import_map.py::_CLIENT_SUBPATHS` hardcodes subpaths instead of reading `package.json` `exports`. |

### 1.3 Relevant Patterns and Glossary

Read [MCP and A2A protocol harness](../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [Litestar extension](../../knowledge/domains/extensions/litestar.md), [client and adapter subpaths](../../knowledge/patterns/typescript/client-and-adapter-subpaths.md), [module and class layout](../../knowledge/patterns/python/module-and-class-layout.md), and the `python-framework`, `litestar-mcp`, and `typescript-client` skills.

`vertebra.typing.ToolProvider` remains engine-neutral (`operations() -> Mapping[str, Callable[..., Awaitable[Any]]]` and `confirmation_reason(operation_name, arguments) -> str | None`); `BigQueryToolProvider` and `SpannerToolProvider` retain zero upward imports from `vertebra.agent`, and `vertebra.agent.tools` contains zero vendor-specific dispatch.

## 2. Requirements

### 2.1 Functional Requirements and Architecture

- **R1.** Define `ToolSpec(FrozenBaseStruct, frozen=True, gc=False)` and `ToolAnnotations(FrozenBaseStruct, frozen=True, gc=False)` in `src/py/vertebra/agent/core/schema.py` carrying `name`, `title`, `description`, `input_schema`, `output_schema`, `annotations` (`read_only`, `destructive`, `idempotent`, `open_world`, `consequential`), `exposure: frozenset[Literal["mcp", "webmcp", "a2a"]]`, `requires_confirmation`, and `guards`.
- **R2.** Derive input and output JSON Schemas once via `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components` and coerce tool call arguments via `msgspec.convert`, removing all `eval()` calls and string-annotation heuristics from `SchemaBuilder` and `Tool`.
- **R3.** Implement `ToolRegistry` consuming `vertebra.typing.ToolProvider`, `@tool` callables, `SkillController`s, and opted-in routes while preserving per-call confirmation reasons, canonical argument/owner binding, native policy-error boundaries, and zero upward imports in database adapters.
- **R4.** Implement `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` enforcing MCP `2026-07-28` HTTP semantics (`_meta` protocol version negotiation, header validation `400`, default same-origin `Origin` check `403`, unknown method `-32601`, `GET` `405`, notification `202`, batch rejection `400`, `server/discover`, `resultType`, `ttlMs`/`cacheScope`, deterministic `ETag`, `authorize(ctx, spec)` guards, `ConfirmationGate` `_meta["vertebra/confirmationToken"]` binding, referenced background tasks, and removal of deprecated `2024-11-05` methods).
- **R5.** Replace duplicated Flask/Django/Starlette/FastAPI MCP JSON-RPC routers and local `MCPConfig` classes with one shared ASGI MCP app and one shared WSGI MCP app backed by a persistent `anyio` blocking portal.
- **R6.** Route native `AgentPlugin` through `MCPCore`, delegate Litestar applications cleanly to `litestar-mcp` without double-registering `opt["mcp_tool"]` handlers, and unify `/.well-known/mcp.json` on one canonical discovery schema.
- **R7.** Project `/.well-known/webmcp.json` strictly from `ToolSpec` entries with `"webmcp" in exposure` (default excluded), mapping `requires_confirmation` to `consequentialHint` and emitting `title` and `outputSchema`.
- **R8.** Update `src/ts/vertebra-client/src/webmcp.ts` (`document.modelContext ?? navigator.modelContext`, `registerTool(tool, { signal })`, dispose handle, HMR/SPA idempotency, per-tool error isolation, removal of `provideContext`), update `src/ts/vertebra-client/src/agent/index.ts` (`Accept: application/json, text/event-stream`, `_meta`, SSE parsing, `encodeMcpHeaderValue`), derive `build/_import_map.py::_CLIENT_SUBPATHS` from `package.json` `exports`, and sync docs and OKF chapters.

### 2.2 Non-Functional and Safety Constraints

- Never call `eval()` on type annotations or user input.
- Default `ToolSpec.exposure` excludes `"webmcp"`; browser tool advertisement requires explicit opt-in (`webmcp=True` or `"webmcp"` in `exposure`).
- Confirmation tokens remain cryptographically bound to `(tool_name, canonical_arguments_digest, principal_id)` in `ConfirmationGate`.
- WSGI MCP dispatch reuses a persistent `anyio` blocking portal rather than calling `asyncio.run` per request.

### 2.3 Acceptance Scenarios

| Given | When | Then |
| --- | --- | --- |
| Module using `from __future__ import annotations`, `Annotated[..., msgspec.Meta(...)]`, and `AgentContext \| None` | `SchemaBuilder` builds `ToolSpec` and `Tool.invoke` runs | Input and output schemas derive via `msgspec.defstruct`/`schema_components` and `msgspec.convert` validates arguments with zero `eval()` calls. |
| Neutral `ToolProvider` plus `BigQueryToolProvider` and `SpannerToolProvider` | Registered in `ToolRegistry` and invoked via `MCPCore` | Typed schemas, per-call confirmation binding, and native policy errors work identically without vendor branches in `vertebra.agent`. |
| Cross-origin HTTP POST, HTTP GET, JSON-RPC batch, notification, or deprecated `initialize`/`ping` | Sent to `MCPCore.handle` | Returns `403`, `405`, `400`, `202`, and `-32601` respectively; `server/discover` and `tools/list` emit deterministic `ETag` and `2026-07-28` `_meta`. |
| Starlette, FastAPI, Flask, Django, Litestar, and native `App` mounts | Serving `/mcp` and `/.well-known/mcp.json` | ASGI/WSGI apps share `MCPCore` (with persistent WSGI portal), Litestar delegates to `litestar-mcp` without duplicate tools, and `/.well-known/mcp.json` matches one schema. |
| Browser environment with `document.modelContext` and HMR reloads | `registerWebMCPTools` runs with `AbortSignal` | Only `"webmcp" in exposure` tools appear in `/.well-known/webmcp.json`, per-tool errors are isolated, repeated HMR calls stay idempotent, and aborting disposes tools. |

### 2.4 Alternatives Considered

| Decision | Chosen seam and alternative |
| --- | --- |
| Schema generation | `typing.get_type_hints` + `msgspec.defstruct` + `msgspec.json.schema_components` delegates all JSON Schema edge cases to `msgspec`'s C implementation instead of maintaining a parallel AST/type walker. |
| Litestar integration | Direct delegation to `litestar-mcp` avoids maintaining a competing MCP stack inside Litestar while sharing canonical `/.well-known/mcp.json` discovery. |
| WSGI execution | A persistent `anyio` blocking portal avoids per-request event-loop creation overhead in Flask and Django. |

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning tasks | Verification strategy | Canonical public seam / gate |
| --- | --- | --- | --- |
| R1, R2 | [t01](tasks/t01-toolspec-and-msgspec-schema.md) | behavior_tdd | `src/py/tests/unit/agent/test_core_and_observability.py`, `src/py/tests/unit/agent/test_mcp_enterprise_tools.py` |
| R3 | [t02](tasks/t02-tool-registry-and-providers.md) | behavior_tdd | `src/py/tests/unit/agent/test_mcp_enterprise_tools.py`, `src/py/tests/unit/db/test_bigquery.py`, `src/py/tests/unit/db/test_spanner.py` |
| R4 | [t03](tasks/t03-mcp-core-protocol-engine.md) | behavior_tdd | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R5 | [t04](tasks/t04-shared-asgi-wsgi-mcp-adapters.md) | behavior_tdd | `src/py/tests/unit/extensions/test_starlette_plugin.py`, `src/py/tests/unit/extensions/test_fastapi_plugin.py`, `src/py/tests/unit/extensions/test_flask_plugin.py`, `src/py/tests/unit/extensions/test_django_plugin.py` |
| R6 | [t05](tasks/t05-agent-plugin-and-litestar-mcp.md) | behavior_tdd | `src/py/tests/unit/extensions/test_litestar_ecosystem_mcp.py`, `src/py/tests/unit/agent/test_mcp_conformance.py`, `src/py/tests/unit/agent/test_a2a_protocol.py` |
| R7 | [t06](tasks/t06-webmcp-manifest-projection.md) | behavior_tdd | `src/py/tests/unit/agent/test_mcp_conformance.py` |
| R8 | [t07](tasks/t07-typescript-webmcp-bridge.md), [t08](tasks/t08-mcp-client-import-map-and-docs.md) | behavior_tdd | `src/ts/vertebra-client/tests/agent.test.ts`, `src/py/tests/unit/framework/test_import_map.py` |

## 4. Implementation Plan

### Phase 1: `ToolSpec`, `msgspec` Schema Pipeline, `ToolRegistry`, and `MCPCore`

- [x] [6b819b6] t01 — Implement ToolSpec and msgspec defstruct/schema_components builder without eval (tasks/t01-toolspec-and-msgspec-schema.md)
- [ ] t02 — Build ToolRegistry over neutral ToolProvider, SkillControllers, and route discovery (tasks/t02-tool-registry-and-providers.md)
- [ ] t03 — Implement transport-agnostic MCPCore 2026-07-28 HTTP protocol engine (tasks/t03-mcp-core-protocol-engine.md)

**Phase 1 Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent -q` and `mypy` / `ruff` on `src/py/vertebra/agent` exit 0.

### Phase 2: Framework Adapters, `litestar-mcp` Delegation, WebMCP Projection, and `@vertebra/client` Bridge

- [ ] t04 — Unify Starlette, FastAPI, Flask, and Django mounts onto shared ASGI and WSGI MCP apps (tasks/t04-shared-asgi-wsgi-mcp-adapters.md)
- [ ] t05 — Wire native AgentPlugin to MCPCore, delegate Litestar to litestar-mcp, and unify well-known discovery (tasks/t05-agent-plugin-and-litestar-mcp.md)
- [ ] t06 — Project WebMCP manifests strictly from ToolSpec exposure metadata (tasks/t06-webmcp-manifest-projection.md)
- [ ] t07 — Upgrade TypeScript webmcp.ts bridge to document.modelContext.registerTool with signal disposal (tasks/t07-typescript-webmcp-bridge.md)
- [ ] t08 — Update TypeScript McpClient SSE/_meta handling, derive _CLIENT_SUBPATHS from package.json, and sync docs/OKF (tasks/t08-mcp-client-import-map-and-docs.md)

**Phase 2 Recovery Checkpoint:** `make lint-all`, `make py-test`, `make ts-test`, and `make docs` exit 0, followed by independent `@code-reviewer` and `@quality-reviewer` passes before archiving on `feat/mcp-webmcp-core`.

## 5. Continuity Snapshot

- State is `planned`; all 8 worksheets (`t01`–`t08`) are `pending` at `plan_revision: 2`, anchored at `plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"`.
- Neutral `vertebra.typing.ToolProvider` and argument-bound `ConfirmationGate` tokens from `80ff8a65` and `7657e8b7` remain preserved and consumed directly.
- Next action on `feat/mcp-webmcp-core`: `/flow:implement mcp-webmcp-core` starting at `t01`.
