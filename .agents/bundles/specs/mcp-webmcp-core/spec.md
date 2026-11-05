---
type: Spec
flow_id: mcp-webmcp-core
title: MCP 2026-07-28 Core and WebMCP Bridge
description: Build an agent-owned ToolSpec and ToolRegistry over the shared ToolProvider contract, a transport-agnostic MCPCore, framework adapters, and a WebMCP projection and TypeScript bridge.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: "2026-11-05T06:20:28Z"
tags: [mcp, webmcp, agent, typescript]
---

# Flow Charter: MCP 2026-07-28 Core and WebMCP Bridge

## Charter

Every tool is defined once and exposed to MCP `2026-07-28` clients, A2A skills, and opt-in browser WebMCP agents through one `ToolSpec`, one `msgspec` schema pipeline, and one transport-agnostic `MCPCore`. Litestar applications delegate directly to `litestar-mcp`.

## Scope

1. **`ToolSpec`, existing neutral `ToolProvider`, and `ToolRegistry`.**
   - Add `ToolSpec(FrozenBaseStruct, frozen=True, gc=False)` in `vertebra/agent/core/schema.py` carrying `name`, `title`, `description`, `input_schema`, `output_schema`, `annotations` (`read_only`, `destructive`, `idempotent`, `open_world`, `consequential`), `exposure: frozenset[Literal["mcp", "webmcp", "a2a"]]`, `requires_confirmation`, and `guards`.
   - Build `ToolRegistry` from the existing `vertebra.typing.ToolProvider` callable-map seam established in `db-engine-primitives:t05`, adapting `@tool` functions, `SkillController`s, route discovery, and explicit native adapter provider instances. `BigQueryToolProvider` and `SpannerToolProvider` remain in `vertebra.db.adapters.{bigquery,spanner}.tools`; `agent.tools` supplies generic `build_tools` consumption. Consolidate discovery without a second vendor-aware provider protocol, agent-owned engine operations, or native descriptor schemas. Derive richer agent-owned `ToolSpec` values from original typed callables, preserving per-call confirmation reasons, canonical argument/owner binding, native policy-error boundaries, and lazy construction.
2. **`msgspec` schema derivation & argument conversion.**
   - Derive input/output JSON Schemas once via `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components` and coerce tool call arguments with `msgspec.convert`, removing string-annotation heuristics and `eval()` in `SchemaBuilder` so `from __future__ import annotations` modules resolve cleanly and signatures are cached. Preserve original provider method schemas and validation; native providers remain independent of ToolSpec, Tool, MCP, and WebMCP.
3. **`MCPCore.handle(McpHttpRequest) -> McpHttpResponse`.**
   - Enforce MCP `2026-07-28` HTTP semantics: `_meta` protocol version negotiation, header validation (`400`), default same-origin `Origin` validation (`403`), unknown method `-32601` (`404` where specified), `GET` `405`, notification `202`, batch rejection (`400`), `server/discover`, `resultType`, `ttlMs`/`cacheScope`, deterministic `ETag`, typed per-method params, `authorize(ctx, spec)` guard execution, `ConfirmationGate` `input_required` responses (preserving Chapter 1 `_meta["vertebra/confirmationToken"]` binding), referenced background tasks, and removal of deprecated `2024-11-05` methods (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`, and `tasks/list` outside the tasks extension).
4. **Thin framework adapters & `litestar-mcp` delegation.**
   - Replace duplicated Flask/Django `_dispatch_jsonrpc` routers and local `MCPConfig` classes with one shared ASGI MCP app and one WSGI MCP app (using a persistent `anyio` blocking portal instead of per-request `asyncio.run`).
   - Mount `/mcp` cleanly on Starlette/FastAPI and route native `AgentPlugin` through `MCPCore`.
   - Delegate Litestar apps to `litestar-mcp` without double-registering `opt["mcp_tool"]` routes, and unify `/.well-known/mcp.json` on one canonical schema.
5. **WebMCP projection & `@vertebra/client` bridge.**
   - Project `/.well-known/webmcp.json` (or configured manifest route) strictly from `ToolSpec`s with `"webmcp" in exposure` (default excluded per Chapter 1), mapping `requires_confirmation` to `consequentialHint` and including `title` and `outputSchema`.
   - Update `src/ts/vertebra-client/src/webmcp.ts` to feature-detect `document.modelContext ?? navigator.modelContext`, call `registerTool(tool, { signal })` returning a dispose handle, isolate per-tool registration errors, remain idempotent across HMR/SPA navigation, honor `execute(input, { signal })`, and drop legacy `provideContext`.
   - Update `src/ts/vertebra-client/src/agent/` to send `Accept: application/json, text/event-stream` and `_meta`, parse SSE responses, and fix `encodeMcpHeaderValue` edge cases; derive `build/_import_map.py::_CLIENT_SUBPATHS` directly from `src/ts/vertebra-client/package.json` `exports`.
6. **Docs & OKF.** Update `docs/usage/agent-protocols.md`, `docs/usage/a2a-and-workflows.md`, `docs/reference/agent.md`, and `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md`.

## Out of Scope

- A2A `1.0` wire changes beyond consuming `ToolSpec`.

## Public Seams

`ToolSpec`, `ToolRegistry`, `vertebra.typing.ToolProvider` (consumed unchanged), `MCPCore`, ASGI/WSGI MCP apps, `registerWebMCPTools`, and `McpClient`.

## Dependencies

`critical-correctness-fixes` (completed), `refactor/sync-async-names` (`08ac03a`), and `db-engine-primitives` (adapter-owned providers, neutral ToolProvider, and generic agent.tools consumer).

## Acceptance Boundary

- MCP/WebMCP consumes an engine-free provider and both explicit native providers through the same neutral contract, preserving typed validation and per-call confirmation binding. Native DB providers retain zero upward imports and the generic consumer contains no vendor dispatch.

- `litestar-mcp` MCP `2026-07-28` conformance checks pass against Vertebra Starlette, Flask, and native `App` mounts.
- Vitest covers `document.modelContext` registration, `AbortSignal` disposal, HMR idempotency, and per-tool error isolation.
- `make lint-all`, `make py-test`, `make ts-test`, and `make docs` pass.

## Draft Task Outline

- t01 `ToolSpec` + `msgspec.defstruct`/`schema_components` schema builder and `msgspec.convert` invocation.
- t02 Consume the existing neutral `ToolProvider` in `ToolRegistry` and consolidate route/controller/generic-agent-tool discovery without relocating adapter operations or duplicating the provider contract.
- t03 Transport-agnostic `MCPCore` `2026-07-28` protocol engine.
- t04 Shared ASGI and WSGI MCP adapters replacing Flask/Django JSON-RPC copies.
- t05 Native `AgentPlugin` adapter + `litestar-mcp` delegation + unified `/.well-known/mcp.json`.
- t06 WebMCP manifest projection from `ToolSpec`.
- t07 TypeScript `webmcp.ts` `document.modelContext.registerTool` bridge.
- t08 TypeScript `McpClient` SSE/`_meta` updates, `package.json`-derived import map, and docs/OKF.
