---
type: Task
id: mcp-webmcp-core:t04
title: Unify Starlette, FastAPI, Flask, and Django mounts onto shared ASGI and WSGI MCP apps
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t03
files:
- src/py/vertebra/agent/mcp/routes.py
- src/py/vertebra/extensions/starlette/mcp.py
- src/py/vertebra/extensions/fastapi/mcp.py
- src/py/vertebra/extensions/flask/mcp.py
- src/py/vertebra/extensions/django/mcp.py
tests:
- src/py/tests/unit/extensions/test_starlette_plugin.py
- src/py/tests/unit/extensions/test_fastapi_plugin.py
- src/py/tests/unit/extensions/test_flask_plugin.py
- src/py/tests/unit/extensions/test_django_plugin.py
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T06:36:00Z'
commit: null
tags:
- mcp
- asgi
- wsgi
- extensions
requirements:
- R5
---

# Unify Starlette, FastAPI, Flask, and Django mounts onto shared ASGI and WSGI MCP apps

## Objective

Replace duplicated per-framework `_dispatch_jsonrpc` handlers and local `MCPConfig` classes in Starlette, FastAPI, Flask, and Django with one shared ASGI MCP app and one shared WSGI MCP app (backed by a persistent `anyio` blocking portal) in `src/py/vertebra/agent/mcp/routes.py`.

## Context

Read `../spec.md`, `t03-mcp-core-protocol-engine.md`, and `src/py/vertebra/utils/portal.py`. Never call `asyncio.run(...)` per WSGI request; reuse a managed `anyio` blocking portal lifecycle.

## Steps

1. Write failing tests in `test_starlette_plugin.py`, `test_fastapi_plugin.py`, `test_flask_plugin.py`, and `test_django_plugin.py` verifying `2026-07-28` `server/discover`, `tools/list`, `tools/call`, `Origin` `403`, `GET` `405`, notification `202`, and persistent portal reuse across consecutive WSGI requests.
2. Implement `AsgiMcpApp` and `WsgiMcpApp` in `src/py/vertebra/agent/mcp/routes.py` translating ASGI scopes and WSGI `environ` dicts to `McpHttpRequest` / `McpHttpResponse` over `MCPCore`.
3. Slim `src/py/vertebra/extensions/{starlette,fastapi,flask,django}/mcp.py` to thin framework mount wrappers importing canonical `MCPConfig` and `MCPCore` without duplicate JSON-RPC dispatchers.
4. Run verification and commit `refactor(mcp): share AsgiMcpApp and WsgiMcpApp across Starlette, FastAPI, Flask, and Django`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/extensions/test_django_plugin.py -q
uv run --no-sync ruff check src/py/vertebra/agent/mcp/routes.py src/py/vertebra/extensions
uv run --no-sync mypy src/py/vertebra/agent/mcp/routes.py src/py/vertebra/extensions
```

## Acceptance Criteria

- [ ] R5: Flask, Django, Starlette, and FastAPI delegate MCP HTTP traffic to shared `AsgiMcpApp` / `WsgiMcpApp` over `MCPCore` with zero duplicated JSON-RPC routers or per-request `asyncio.run` loops.

## Notes & Discoveries

None.
