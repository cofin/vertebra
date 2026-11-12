---
type: Spec
parent_flow: mcp-protocol-completeness
chapter_id: ch04-get-sse-streams
title: "Chapter 4: GET SSE Streams"
description: Add the streaming response envelope, legacy GET SSE streams with replay, heartbeats, and limits, incremental adapter writers, list-changed and logging notifications, and progress delivery over session streams and POST SSE responses.
status: draft
state: planned
planning_status: empirical_dependent
depends_on_chapters:
  - ch03-version-negotiation-and-sessions
replan_trigger: "server-runtime-correctness lands incremental ASGI response streaming (#81); confirm the native App handler streaming return contract and the native Response streaming form used by WsgiMcpApp.handle_vertebra_request before executing t02"
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: sse-broker
    resource: src/py/vertebra/agent/core/sse.py::SseStreamBroker
    title: Replay buffers and subscriber channels
  - id: mcp-core
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle
    title: HTTP method dispatch
  - id: mcp-routes
    resource: src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp
    title: Adapter response writers
  - id: registry
    resource: src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener
    title: Registry mutation listeners (no callers at c8722c76)
  - id: progress
    resource: src/py/vertebra/agent/core/context.py::ProgressReporter
    title: Progress callback carrier
tags:
  - mcp
  - sse
  - streaming
  - notifications
---

# Chapter 4: GET SSE Streams

<!-- truth: start -->
- **Envelope**: `McpHttpResponse.stream: AsyncIterator[bytes] | None` carries incremental bodies; `body` stays empty when `stream` is set [^mcp-core].
- **GET rule**: legacy `GET /mcp` with `Accept: text/event-stream` opens a stream keyed by the session id (or `anon-<hex>`), replays from `Last-Event-ID`, sends `: keepalive` comments every `sse_heartbeat_seconds=15.0`, and enforces `max_streams_per_owner=8` (`503` / `-32000`); `2026-07-28` `GET` and `enable_get_sse=False` return `405`; missing or wildcard `Accept` returns `405`; incompatible `Accept` returns `406` [^sse-broker].
- **Writers**: `AsgiMcpApp` writes `more_body` chunks and stops on `http.disconnect`; Starlette uses `StreamingResponse`; WSGI iterates the stream through the portal; native endpoints return `(status, headers, AsyncIterable[bytes])` [^mcp-routes].
- **Notifications**: registry listeners receive the changed kind and `MCPCore` broadcasts `notifications/{tools,resources,prompts}/list_changed`; `MCPCore.log` publishes level-gated `notifications/message`; progress tokens publish `notifications/progress` to a POST SSE response when the request accepts `text/event-stream`, else to the caller's session stream [^registry] [^progress].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`MCPCore.handle` answers every `GET` with `405`, `McpHttpResponse` can only hold a complete `bytes` body, and every adapter buffers. `SseStreamBroker` exists but nothing opens streams from HTTP, `ToolRegistry.add_listener` has no callers, and `ProgressReporter` is never connected to MCP requests ([#109](https://github.com/cofin/vertebra/issues/109)). Vertebra's own ASGI mount buffers responses until `server-runtime-correctness` delivers incremental streaming ([#81](https://github.com/cofin/vertebra/issues/81)), so the native writer task waits on that flow.

### 1.2 Upstream Execution Baseline

- Depends on `ch03` (sessions, negotiated protocol, `logging/setLevel`).
- Depends on flow `server-runtime-correctness` for `t02` (native and ASGI-mount streaming). `t01`, `t03`, and `t04` exercise `MCPCore` directly and do not need the upstream flow.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Seam |
| --- | --- | --- |
| Broker | `src/py/vertebra/agent/core/sse.py::SseStreamBroker.subscribe`, `publish`, `broadcast`, `disconnect`, `SseFrame.encode` [^sse-broker] | In-process anyio memory streams. |
| Core | `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` [^mcp-core] | In-process. |
| Adapters | `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp.__call__`, `handle_starlette_request`, `WsgiMcpApp` [^mcp-routes] | External servers. |
| Registry | `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener`, `_notify` [^registry] | In-process. |
| Progress | `src/py/vertebra/agent/core/context.py::ProgressReporter` [^progress] | In-process callback. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Agent harness](../../../../knowledge/domains/agent/mcp-and-a2a-protocol-harness.md), [`http-ingress`](../../../../../skills/http-ingress/SKILL.md) (streaming and backpressure context).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R9.1**: Streaming envelope and legacy `GET` stream negotiation, replay, heartbeat, and limits.
- **R9.2**: Every adapter writes streams incrementally and stops on client disconnect.
- **R10.1**: List-changed broadcasts and level-gated logging notifications.
- **R10.2**: Progress notifications on POST SSE responses or session streams.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# models.py
class McpHttpResponse:  # added
    stream: AsyncIterator[bytes] | None = None
class MCPConfig:  # added
    enable_get_sse: bool = True
    sse_heartbeat_seconds: float = 15.0
    max_streams_per_owner: int = 8

# sse.py
class StreamLimitExceededError(VertebraError, RuntimeError): ...
class SseStreamBroker:
    def open_stream(self, session_id: str, *, owner_id: str | None, last_event_id: str | None = None, heartbeat_seconds: float = 15.0, max_streams_per_owner: int = 8) -> AsyncIterator[bytes]: ...
    def close_session(self, session_id: str) -> None: ...

# registry.py
class ToolRegistry:
    def add_listener(self, listener: Callable[[Literal["tools", "resources", "prompts"]], None]) -> None: ...

# service.py
class MCPCore:
    async def log(self, level: McpLogLevel, data: Any, *, logger: str | None = None) -> int: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `MCPCore.handle` (`GET`) -> `SseStreamBroker.open_stream` -> `McpHttpResponse(stream=...)` -> adapter writer.
- `ToolRegistry._notify(kind)` -> `MCPCore._on_registry_change(kind)` -> `SseStreamBroker.broadcast`.
- Tool code -> `MCPCore.log` / `AgentContext.progress_reporter.report` -> request sink or `SseStreamBroker.publish(session_id, ...)`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Modern GET | `MCP-Protocol-Version: 2026-07-28` | `405`, `Allow: POST, DELETE, OPTIONS` | Stateless profile |
| Wildcard accept | `Accept: */*` | `405` | Explicit SSE negotiation |
| JSON accept | `Accept: application/json` | `406`, `-32600` | Content negotiation |
| Unknown session | `Mcp-Session-Id: nope` | `404`, `-32602` | Owner-scoped sessions |
| Stream limit | 9th concurrent stream for one owner | `503`, `-32000` | Bounded streams |
| Replay | `Last-Event-ID: 2` after frames `1..4` | Frames `3`, `4` first | Resumability |
| Disconnect | ASGI `http.disconnect` | Generator closed; subscriber removed; owner count decremented | No leaked subscribers |
| Anonymous stream close | Stream without session ends | Buffer removed via `close_session` | No unbounded buffers |
| Log below level | Session level `warning`, `log("info", ...)` | Returns `0`, nothing published | Level gating |

### 2.5 Acceptance Scenarios

- Given a legacy session with an open `GET` stream, when a tool registers, then the stream yields a `notifications/tools/list_changed` frame.
- Given a POST `tools/call` with `_meta.progressToken = "p1"` and `Accept: application/json, text/event-stream`, when the tool reports progress twice, then the response stream yields two progress frames and then the result frame.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected & Why |
| --- | --- | --- |
| Envelope | `stream` field on `McpHttpResponse` | A separate response type makes each adapter branch on type and duplicates headers handling. |
| Heartbeat | SSE comment frames | Empty `data:` events reach client handlers and look like messages. |
| Progress in modern mode | POST SSE response | Dropping progress silently loses tool feedback for `2026-07-28` clients. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch04]` — `GET` streams, notifications, and progress are documented in `ch11:t01`; buffering behind the native ASGI mount before the upstream flow lands is recorded in the limitations chapter (`ch11:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R9.1 | [t01](tasks/t01-streaming-envelope-and-get.md) | `behavior_tdd` | `test_mcp_conformance.py`, `test_core_and_observability.py` |
| R9.2 | [t02](tasks/t02-adapter-stream-writers.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R10.1 | [t03](tasks/t03-list-changed-and-logging-notifications.md) | `behavior_tdd` | `test_mcp_conformance.py` |
| R10.2 | [t04](tasks/t04-progress-delivery.md) | `behavior_tdd` | `test_mcp_conformance.py` |

## 5. Implementation Plan

- [ ] mcp-protocol-completeness:ch04:t01 — Add the streaming envelope and legacy GET streams (tasks/t01-streaming-envelope-and-get.md)
- [ ] mcp-protocol-completeness:ch04:t02 — Write streams incrementally from every adapter (tasks/t02-adapter-stream-writers.md)
- [ ] mcp-protocol-completeness:ch04:t03 — Broadcast list changes and level-gated log messages (tasks/t03-list-changed-and-logging-notifications.md)
- [ ] mcp-protocol-completeness:ch04:t04 — Deliver progress over POST SSE responses and session streams (tasks/t04-progress-delivery.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py src/py/tests/unit/agent/test_core_and_observability.py -q && uv run --no-sync ruff check src/py/vertebra/agent src/py/vertebra/extensions/django/mcp.py && uv run --no-sync mypy src/py/vertebra/agent` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/4` tasks completed; `planning_status: empirical_dependent` until `server-runtime-correctness` lands incremental ASGI streaming.
- **Downstream Dependents**: `ch05:t02`/`t03` publish to these streams; `ch09:t01` and `ch10:t01` consume them over stdio and the bridge.

[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker` — Replay buffers and subscriber channels.
[^mcp-core]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP method dispatch.
[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp` — Adapter response writers.
[^registry]: `src/py/vertebra/agent/mcp/registry.py::ToolRegistry.add_listener` — Registry mutation listeners (no callers at `c8722c76`).
[^progress]: `src/py/vertebra/agent/core/context.py::ProgressReporter` — Progress callback carrier.
