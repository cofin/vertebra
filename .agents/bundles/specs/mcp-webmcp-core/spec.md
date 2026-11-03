---
type: Spec
flow_id: mcp-webmcp-core
title: MCP 2026-07-28 Core and WebMCP Bridge
description: Introduce one ToolSpec/ToolRegistry with msgspec-derived schemas, a transport-agnostic MCPCore with ASGI/WSGI/native adapters, litestar-mcp delegation for Litestar apps, a WebMCP manifest projection, and a current document.modelContext TypeScript bridge.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T03:30:00Z
tags: [mcp, webmcp, agent, typescript]
---

# Flow Charter: MCP 2026-07-28 Core and WebMCP Bridge

## Charter

Each tool is defined once and reaches standard MCP clients and browser agents through one registry, one schema derivation, and one protocol core. Exposure and confirmation are explicit per tool. Litestar apps use litestar-mcp.

## Scope

1. **`ToolSpec` and registry.**
   - `agent/core/schema.py` gains `ToolSpec` (frozen msgspec Struct) with: name, title, description, input/output schema, annotations (`read_only`, `destructive`, `idempotent`, `open_world`, `consequential`), `exposure: frozenset[Literal["mcp", "webmcp", "a2a"]]`, `requires_confirmation`, and guards.
   - A `ToolRegistry` is fed by `ToolProvider` implementations: controllers, route discovery, and the BigQuery/Spanner DB tool providers relocated by Chapter 3.
   - The four discovery paths are replaced: `MCPRegistry.discover_app_routes`, `AgentPlugin._inspect_item`, `SkillController`, and `db/mcp_tools.py`.
2. **Schemas.** Built once with `get_type_hints(include_extras=True)` → `msgspec.defstruct` → `msgspec.json.schema_components`. Invocation uses `msgspec.convert`. This replaces the `SchemaBuilder` string-annotation path and the `eval` in `_convert_argument`, so PEP 563 modules produce correct schemas. Signatures are cached.
3. **`MCPCore.handle(McpHttpRequest) -> McpHttpResponse`.**
   - Covers 2026-07-28: `_meta` protocol version, header validation (400), Origin same-origin by default (403), unknown method 404/-32601, GET 405, notification 202, no batching, `server/discover`, `resultType`, `ttlMs`/`cacheScope`, a correct ETag, typed per-method params, and the removed methods dropped (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`, `tasks/list` outside the tasks extension).
   - Includes an `authorize(ctx, spec)` hook and confirmation surfaced as an `input_required` result.
   - Background tasks are kept referenced.
4. **Adapters.**
   - One ASGI app and one WSGI app (anyio portal, no `asyncio.run`) replace the Flask/Django `_dispatch_jsonrpc` copies and their local `MCPConfig`.
   - Starlette/FastAPI mount `/mcp`.
   - The native vertebra adapter routes `AgentPlugin` through the core.
   - Litestar apps delegate to litestar-mcp, without double registration of `opt["mcp_tool"]`.
   - `/.well-known/mcp.json` has one shape.
5. **WebMCP.**
   - The manifest is a projection of `ToolSpec`s with `"webmcp" in exposure`. `requires_confirmation` maps to `consequentialHint`, and title and outputSchema are included.
   - `src/ts/vertebra-client/src/webmcp.ts`:
     - Feature-detects `document.modelContext ?? navigator.modelContext`.
     - Uses `registerTool(tool, {signal})` and returns a dispose handle.
     - Isolates per-tool registration failures.
     - Is idempotent across HMR/SPA navigation.
     - Honors `execute(input, {signal})`.
     - No `provideContext`.
   - `src/ts/vertebra-client/src/agent/` sends `Accept: application/json, text/event-stream` and `_meta`, parses SSE, and fixes `encodeMcpHeaderValue` edge cases.
   - `build/_import_map.py::_CLIENT_SUBPATHS` derives from `package.json` exports.
6. **Docs.** Rewrite `domains/agent/mcp-and-a2a-protocol-harness.md` and the MCP usage docs.

## Out of Scope

- A2A protocol changes beyond sharing `ToolSpec`.

## Public Seams

`ToolSpec`, `ToolRegistry`, `ToolProvider`, `MCPCore`, the ASGI/WSGI MCP apps, `registerWebMCPTools`, and `McpClient`.

## Dependencies

Chapter 1 (security fixes land first) and Chapter 3 (DB tool relocation).

## Acceptance Boundary

- litestar-mcp's `tools/ci/run_mcp_conformance.py` passes against a vertebra Starlette app and a Flask app.
- Vitest covers a fake `document.modelContext` that rejects duplicates.
- `make lint-all`, `make py-test`, and `make ts-test` pass.

## Draft Task Outline

- t01 `ToolSpec` + msgspec schema derivation.
- t02 `ToolRegistry`/`ToolProvider` + discovery consolidation.
- t03 `MCPCore` protocol handling.
- t04 ASGI/WSGI adapters replacing the Flask/Django dispatchers.
- t05 Native adapter + litestar-mcp delegation.
- t06 WebMCP manifest projection.
- t07 TS WebMCP bridge.
- t08 TS MCP client + import map + docs.
