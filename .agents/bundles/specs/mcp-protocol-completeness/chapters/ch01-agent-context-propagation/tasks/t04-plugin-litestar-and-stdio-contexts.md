---
type: Task
id: "mcp-protocol-completeness:ch01:t04"
chapter_id: ch01-agent-context-propagation
title: Pass contexts from native, Litestar fallback, and stdio entry points
description: Build AgentContext values in AgentPlugin native endpoints and the Litestar fallback endpoint, make the fallback return litestar.Response with core status and headers, mount GET and DELETE alongside POST, and give run_stdio_server one session context.
status: draft
state: in_progress
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "mcp-protocol-completeness:ch01:t03"
read_set:
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/stdio.py
  - src/py/vertebra/agent/core/context.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/cli/test_mcp_cli.py
files:
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/agent/mcp/stdio.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
  - src/py/tests/unit/cli/test_mcp_cli.py
estimated_loc: 150
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T03:51:23Z"
last_verified_commit: null
verification_evidence: "Red: five native/Litestar/stdio regressions and anonymous task-owner bypass; Green: 55 conformance/CLI tests, Ruff and Mypy, independent full chapter suite 86 passed."
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: agent-plugin
    resource: src/py/vertebra/agent/plugin.py::AgentPlugin
    title: Native and Litestar MCP route mounting
  - id: stdio
    resource: src/py/vertebra/agent/mcp/stdio.py::run_stdio_server
    title: Stdio transport runner
tags:
  - mcp
  - context
  - litestar
  - stdio
---

# Task t04: Pass Contexts from Native, Litestar Fallback, and Stdio Entry Points

## Objective & Scope Boundary

Close the remaining context gaps: native `App` endpoints, the Litestar fallback used when no `LitestarMCP` plugin is installed, and the stdio runner [^agent-plugin] [^stdio]. The fallback currently drops request headers and returns a `dict`, so every status code (`202`, `304`, `400`, `403`) becomes `200`. Out of scope: incremental stdin reading and stdio notifications (`ch09:t01`), streaming `GET` bodies (`ch04:t02`).

## Bounded Read Manifest

- `src/py/vertebra/agent/plugin.py` — `AgentPlugin.mount_native` (`_mcp_endpoint`, `_mcp_get_endpoint`), `_mount_litestar_routes` (`_litestar_mcp_endpoint`).
- `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server`.
- `src/py/vertebra/agent/core/context.py` — `from_headers`, `from_scope`.
- `src/py/tests/unit/agent/test_mcp_conformance.py::test_native_app_agent_plugin_routes_through_mcp_core_and_unifies_well_known`.
- `src/py/tests/unit/cli/test_mcp_cli.py` — Stdio test patterns.

## Symbol & Signature Contracts

- Native endpoints keep the injectable signature `(body: bytes = b"", headers: Any = None)`; headers normalize through a private static `AgentPlugin._header_dict(headers: Any) -> dict[str, str]` (`as_dict()` when present, else `dict(...)`, else `{}`); `context = AgentContext.from_headers(header_map, identity_source=srv.config.identity_source)`.
- `mount_native` registers `POST`, `GET`, and `DELETE` on `srv.config.path`, each forwarding the HTTP method into `McpHttpRequest`.
- Litestar fallback handler: `async def _litestar_mcp_endpoint(request: litestar.Request[Any, Any, Any]) -> litestar.Response[bytes]` mounted with `litestar.route(srv.config.path, http_method=["POST", "GET", "DELETE"], include_in_schema=False)`; builds `AgentContext.from_scope(request.scope, identity_source=srv.config.identity_source)`, forwards `request.method`, `dict(request.headers)`, and `await request.body()`, and returns `litestar.Response(content=resp.body, status_code=resp.status, headers=resp.headers, media_type=resp.headers.get("Content-Type", "application/json"))`.
- `run_stdio_server(service, *, stdin=None, stdout=None, max_message_bytes=4_194_304, context: AgentContext | None = None) -> int`: `session_context = context or AgentContext(session_id=f"stdio-{uuid.uuid4().hex}")`; every `service.handle_payload(line, session_context)` call receives it.

## Edge Cases & Error Semantics

- Native header objects without `as_dict` and plain dicts both normalize.
- Litestar fallback `202` responses carry an empty body with `status_code=202`.
- An explicit `context` passed to `run_stdio_server` is used unchanged.

## Public-Seam Test Blueprint

- **Files**: `src/py/tests/unit/agent/test_mcp_conformance.py` (native and Litestar via `litestar.testing.create_test_client`), `src/py/tests/unit/cli/test_mcp_cli.py` (stdio).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Native headers ignored by default | `AgentPluginConfig(mcp=MCPConfig())`; native `POST /mcp` handler with `headers={"x-goog-authenticated-user-email": "ana@example.com", "MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/call", "Mcp-Name": "whoami"}` and `tools/call whoami` | Status `200`; result text `"None"` |
| Native custom callable source | `AgentPluginConfig(mcp=MCPConfig(trust_proxy_identity=True, identity_source=CallableIdentitySource(resolver=lambda r: CallerIdentity(email=r.headers.get("x-test-email")))))`; native `POST /mcp` handler with `headers={"x-test-email": "ana@example.com", "MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "tools/call", "Mcp-Name": "whoami"}` and `tools/call whoami` | Status `200`; result text `"ana@example.com"` |
| Native DELETE route | `route_map[("DELETE", "/mcp")]` exists | Handler present |
| Litestar fallback status | `create_test_client(route_handlers=[], plugins=[AgentPlugin()])`; `POST /mcp` body `{"jsonrpc": "2.0", "method": "notifications/initialized"}` | HTTP `202`, empty body |
| Litestar fallback headers | Same client; `POST /mcp` with `Origin: https://evil.example.com`, `Host: testserver` | HTTP `403` |
| Litestar fallback version header | `POST /mcp` with `tools/list` and `MCP-Protocol-Version: 2026-07-28` | HTTP `200`, response header `mcp-protocol-version == "2026-07-28"` |
| Stdio context | `run_stdio_server(service, stdin=<async iter of one tools/call whoami line>, stdout=<collector>, context=AgentContext(identity=CallerIdentity(user_id="cli")))` | Collected response text `"cli"` |
| Stdio default session | Same without `context`; tool returns `AgentContext.current().session_id` | Result text starts with `"stdio-"` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/cli/test_mcp_cli.py -q -k "native or litestar_fallback or stdio_context"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/cli/test_mcp_cli.py -q && uv run --no-sync ruff check src/py/vertebra/agent/plugin.py src/py/vertebra/agent/mcp/stdio.py && uv run --no-sync mypy src/py/vertebra/agent/plugin.py src/py/vertebra/agent/mcp/stdio.py`.

## Acceptance Criteria

- [x] R3.2: native, Litestar fallback, and stdio entry points pass contexts.
- [x] The Litestar fallback preserves core status codes and headers and mounts `POST`, `GET`, and `DELETE`.
- [x] Existing native route test keeps passing with the added `DELETE` route.

## Notes & Discoveries

- Correctness review reproduces anonymous access to owner-bound tasks. An internal lookup sentinel distinguishes omitted trusted lookups from explicit anonymous owner_id=None; public get/update/cancel/list now preserve owner isolation.

- [2026-11-12 00:30] Native `App` exposes `get`, `post`, `put`, `delete`, and `patch` decorators but no `options`; `OPTIONS /mcp` on native apps is answered by the ingress layer, not by this mount.

[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin` — Native and Litestar MCP route mounting.
[^stdio]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Stdio transport runner.
