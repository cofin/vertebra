---
type: Spec
flow_id: mcp-protocol-completeness
chapter_id: ch03-sse-streams-and-resource-subscriptions
title: "Chapter 03: GET SSE Streams, Notifications, and Resource Subscriptions"
description: "Add McpHttpResponse.stream, GET /mcp SSE streams across all adapters, list/log/progress notifications, authorized resources/subscribe fan-out, and 2026-07-28 subscriptions/listen."
status: stable
state: completed
planning_status: empirical_dependent
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch02-version-negotiation-and-sessions
replan_trigger: "Confirm server-runtime-correctness ch01 progressive ASGI and native App (status, headers, AsyncIterable[bytes]) streaming before executing t02"
plan_commit: 18baadcd
last_reviewed_commit: 353c0d0c
updated_at: "2026-11-13T05:03:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T05:03:00Z"
    commit: 353c0d0c
tags:
  - mcp
  - sse
  - streaming
  - subscriptions
  - resources
sources:
  - id: sse-broker
    resource: "src/py/vertebra/agent/core/sse.py::SseStreamBroker"
    title: SSE replay buffers and subscriber channels
  - id: mcp-routes
    resource: "src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp"
    title: "ASGI, Starlette, and WSGI MCP HTTP adapters"
  - id: registry-py
    resource: "src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener"
    title: Registry change listener hook
  - id: notify-py
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated"
    title: Resource update notification entry point
---

# Chapter 03: GET SSE Streams, Notifications, and Resource Subscriptions

<!-- truth: start -->
- `McpHttpResponse.stream: AsyncIterator[bytes] | None` carries incremental SSE frames; legacy `GET /mcp` with `Accept: text/event-stream` opens `SseStreamBroker.open_stream` with `Last-Event-ID` replay, `: keepalive` heartbeats (`15.0s`), and `max_streams_per_owner=8` (`503` / `-32000`), while `2026-07-28` `GET` returns `405` [^sse-broker].
- `AsgiMcpApp`, Starlette, `WsgiMcpApp`, Django, and native `AgentPlugin` (`(status, headers, AsyncIterable[bytes])`) write streams incrementally and close generators on disconnect [^mcp-routes].
- Registry mutations broadcast `notifications/{tools,resources,prompts}/list_changed`, `MCPCore.log` publishes level-gated `notifications/message`, and progress tokens stream over POST SSE or session streams [^registry-py].
- `resources/subscribe` and `resources/unsubscribe` validate URIs and `subscription_authorizer` (`-32004` fail-closed), `await MCPCore.notify_resource_updated(uri)` fans out to subscribed sessions and `2026-07-28` `subscriptions/listen` streams, and `_resource_subscriptions` dead state is replaced by `MCPSessionStore` + `_listen_registrations` ([#112](https://github.com/cofin/vertebra/issues/112)) [^notify-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`MCPCore.handle` rejects all `GET` requests with `405`, adapters buffer `bytes` bodies only, `ToolRegistry.add_listener` has zero callers, and `MCPCore.notify_resource_updated` reads an unpopulated `_resource_subscriptions` map ([#109](https://github.com/cofin/vertebra/issues/109), [#112](https://github.com/cofin/vertebra/issues/112)) [^sse-broker] [^mcp-routes] [^registry-py] [^notify-py]. `t02` consumes the progressive streaming contract from `server-runtime-correctness` `ch01`.

## 2. Requirements, Concrete Contracts & Architecture

- **`R4a` (`t01`)**: Add `stream: AsyncIterator[bytes] | None = None` on `McpHttpResponse` and `enable_get_sse: bool = True`, `sse_heartbeat_seconds: float = 15.0`, `max_streams_per_owner: int = 8` on `MCPConfig`; add `StreamLimitExceededError` and `SseStreamBroker.{open_stream,close_session}` in `sse.py`; handle legacy `GET /mcp` (`405` in `2026-07-28`, when `enable_get_sse=False`, or on missing/`*/*` `Accept`; `406` on incompatible `Accept`; `404` on unknown session; `503` on stream limit).
- **`R4b` (`t02`)**: Stream `McpHttpResponse.stream` incrementally in `AsgiMcpApp.__call__` (monitoring `http.disconnect`), `handle_starlette_request` (`StreamingResponse`), `WsgiMcpApp` (portal chunk iterator + `close()`), `AgentPlugin.mount_native` (`(resp.status, resp.headers, resp.stream)` + Litestar `Stream`), and `extensions/django/mcp.py` (`StreamingHttpResponse`).
- **`R4c` (`t03`)**: Pass changed kind `Literal["tools", "resources", "prompts"]` to `ToolRegistry` listeners, broadcast `list_changed` notifications, add `await MCPCore.log(level, data, *, logger=None) -> int`, and wire `ProgressReporter` to POST SSE responses (when `Accept` includes `text/event-stream`) or caller session streams.
- **`R4d` (`t04`)**: Add `resource_subscriptions: bool = True`, `max_subscriptions_per_session: int = 256`, and `subscription_authorizer` on `MCPConfig`; implement `resources/subscribe` and `resources/unsubscribe` backed by `MCPSessionStore` reverse URI index, convert `notify_resource_updated(uri) -> int` to `async` removing dead `_resource_subscriptions`, and add stateless `2026-07-28` `subscriptions/listen` stamping `_meta['io.modelcontextprotocol/subscriptionId']`.

## 3. Documentation & OKF Knowledge Plan

SSE streaming, progress notifications, and resource subscriptions are documented in `ch07` (`docs/usage/agent-protocols.md`, `mcp-and-a2a-protocol-harness.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R4a` | `t01` | `agent/mcp/models.py`, `agent/core/sse.py`, `agent/mcp/service.py` | `pytest test_core_and_observability.py test_mcp_conformance.py` |
| `R4b` | `t02` | `agent/mcp/routes.py`, `agent/plugin.py`, `extensions/django/mcp.py` | `pytest test_mcp_conformance.py` |
| `R4c` | `t03` | `agent/mcp/registry.py`, `agent/mcp/service.py` | `pytest test_mcp_conformance.py` |
| `R4d` | `t04` | `agent/mcp/{sessions,models,service}.py` | `pytest test_mcp_conformance.py` |

## 5. Implementation Plan

- [x] [2454dbef] [t01-streaming-envelope-and-legacy-get-sse.md](tasks/t01-streaming-envelope-and-legacy-get-sse.md) — Add `McpHttpResponse.stream`, `SseStreamBroker.open_stream`, and legacy `GET /mcp` SSE negotiation
- [x] [2454dbef] [t02-adapter-stream-writers.md](tasks/t02-adapter-stream-writers.md) — Stream `McpHttpResponse.stream` incrementally across ASGI, Starlette, WSGI, native `App`, Litestar fallback, and Django
- [x] [2454dbef] [t03-list-changed-logging-and-progress-notifications.md](tasks/t03-list-changed-logging-and-progress-notifications.md) — Broadcast `list_changed`, level-gated `MCPCore.log`, and POST/session progress notifications
- [x] [2454dbef] [t04-resource-subscriptions-fanout-and-stateless-listen.md](tasks/t04-resource-subscriptions-fanout-and-stateless-listen.md) — Implement `resources/subscribe`, async `notify_resource_updated` fan-out, and `2026-07-28` `subscriptions/listen`

## 6. Current State & Remaining Plan Snapshot

- **Status**: `ch03-sse-streams-and-resource-subscriptions` is completed at `2454dbef` (`sync` `353c0d0c`); all `4` chapter tasks passed `ruff`, `mypy` across 41 files, and `251` unit tests. Next frontier is [`ch04-skills-and-mcp-apps-core`](../ch04-skills-and-mcp-apps-core/spec.md).
- **Fast Chapter Gate (`< 15s`)**: `uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions/django/mcp.py && uv run --no-sync mypy src/py/vertebra/agent src/py/vertebra/extensions/django/mcp.py && uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_conformance.py -q`.

[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker` — SSE replay buffers and subscriber channels.
[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp` — ASGI, Starlette, and WSGI MCP HTTP adapters.
[^registry-py]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener` — Registry change listener hook.
[^notify-py]: `src/py/vertebra/agent/mcp/service.py::MCPCore.notify_resource_updated` — Resource update notification entry point.
