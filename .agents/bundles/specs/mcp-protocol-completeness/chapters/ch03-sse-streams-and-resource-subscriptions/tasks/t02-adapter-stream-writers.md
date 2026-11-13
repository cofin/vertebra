---
type: Task
id: "mcp-protocol-completeness:ch03:t02"
flow_id: mcp-protocol-completeness
chapter_id: ch03-sse-streams-and-resource-subscriptions
title: Write Incremental MCP SSE Streams Across All Framework Adapters
description: "Wire McpHttpResponse.stream through AsgiMcpApp, Starlette, WsgiMcpApp, native AgentPlugin, Litestar fallback, and Django."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "mcp-protocol-completeness:ch03:t01"
files:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/extensions/django/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/extensions/django/mcp.py
  - src/py/vertebra/extensions/flask/mcp.py
  - src/py/vertebra/server/_dispatch.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 190
tags:
  - mcp
  - sse
  - adapters
  - streaming
sources:
  - id: mcp-routes
    resource: "src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp"
    title: "ASGI, Starlette, and WSGI MCP adapters"
  - id: agent-plugin
    resource: "src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native"
    title: Native App and Litestar fallback MCP mounts
  - id: django-mcp
    resource: "src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request"
    title: Django MCP request adapter
---

# Task `t02`: Write Incremental MCP SSE Streams Across All Framework Adapters

## Objective & Scope Boundary

Write `McpHttpResponse.stream` incrementally across all HTTP/WSGI/ASGI/native mounts per [`../spec.md` §2](../spec.md) [^mcp-routes] [^agent-plugin] [^django-mcp].

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/routes.py`, `src/py/vertebra/agent/plugin.py`, & `src/py/vertebra/extensions/django/mcp.py`.
- `src/py/vertebra/server/_dispatch.py` — native `(status, headers, AsyncIterable[bytes])` contract from `server-runtime-correctness:ch01:t04`.

## Contracts & Edge Cases

- `AsgiMcpApp.__call__` sends `more_body=True` chunks and closes `resp.stream` immediately on `http.disconnect`; `handle_starlette_request` returns `StreamingResponse`.
- `WsgiMcpApp` yields chunks from `PortalProvider.default()` with a `close()` method closing the async iterator; `dispatch_mcp_request` returns Django `StreamingHttpResponse` when `resp.stream` is present.
- `AgentPlugin.mount_native` registers `GET`/`POST`/`DELETE`/`OPTIONS` on `config.path` returning `(resp.status, resp.headers, resp.stream)` when streaming, and Litestar fallback returns `litestar.response.Stream`.

## Test Matrix

- Parameterized adapter tests in `test_mcp_conformance.py` verifying incremental chunk delivery before stream completion and generator `aclose()` on disconnect across ASGI, Starlette, WSGI/Flask, Django, native `App`, and Litestar fallback.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/plugin.py src/py/vertebra/extensions/django/mcp.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/plugin.py src/py/vertebra/extensions/django/mcp.py && uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [ ] Every MCP HTTP adapter yields SSE chunks progressively and closes `resp.stream` on client disconnect.
- [ ] `GET`, `POST`, `DELETE`, and `OPTIONS` are routed on every mount.

[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp` — ASGI, Starlette, and WSGI MCP adapters.
[^agent-plugin]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Native App and Litestar fallback MCP mounts.
[^django-mcp]: `src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request` — Django MCP request adapter.
