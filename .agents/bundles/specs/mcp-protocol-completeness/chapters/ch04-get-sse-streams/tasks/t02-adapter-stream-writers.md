---
type: Task
id: "mcp-protocol-completeness:ch04:t02"
chapter_id: ch04-get-sse-streams
title: Write streams incrementally from every adapter
description: Make AsgiMcpApp, the Starlette handler, WsgiMcpApp, native AgentPlugin endpoints, and the Django dispatch path write McpHttpResponse.stream incrementally and close the stream on client disconnect.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch04:t01"
read_set:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/extensions/django/mcp.py
  - src/py/vertebra/agent/controller.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/routes.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/extensions/django/mcp.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 200
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:30:00Z"
verified: []
sources:
  - id: mcp-routes
    resource: src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp.__call__
    title: Buffered ASGI writer
  - id: chat-stream
    resource: src/py/vertebra/agent/controller.py::AgentChatController.handle_request
    title: Native streaming return precedent (status, headers, AsyncIterable)
  - id: django-dispatch
    resource: src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request
    title: Django native Request dispatch
tags:
  - sse
  - adapters
---

# Task t02: Write Streams Incrementally from Every Adapter

## Objective & Scope Boundary

Each adapter writes `McpHttpResponse.stream` as it is produced and releases the broker subscription when the client goes away [^mcp-routes]. The native return form follows the existing chat precedent `(status, headers, AsyncIterable[bytes])` [^chat-stream]. Out of scope: the core stream itself (`t01`). This task waits on `server-runtime-correctness` for native and ASGI-mount delivery (chapter `replan_trigger`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/routes.py` — `AsgiMcpApp.__call__`, `handle_starlette_request`, `WsgiMcpApp.handle_vertebra_request`, `handle_flask_view`, `__call__`.
- `src/py/vertebra/agent/plugin.py` — native `GET` endpoint and Litestar fallback from `ch01:t04`.
- `src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request`.
- `src/py/vertebra/agent/controller.py::AgentChatController.handle_request` — Streaming tuple precedent.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `AsgiMcpApp.__call__`: when `resp.stream` is set, sends `http.response.start`, then one `http.response.body` event per chunk with `more_body=True`, then a final empty body with `more_body=False`; an anyio task group runs a watcher that awaits `receive()` until `{"type": "http.disconnect"}` and cancels the writer; `resp.stream.aclose()` runs in `finally`. `send_raw_response` is used only for buffered bodies.
- `AsgiMcpApp.handle_starlette_request`: streams return `starlette.responses.StreamingResponse(resp.stream, status_code=resp.status, headers=resp.headers, media_type="text/event-stream")`.
- `WsgiMcpApp`: private `_iterate_stream(self, stream: AsyncIterator[bytes]) -> Iterator[bytes]` yields `portal.call(stream.__anext__)` results until `StopAsyncIteration` and calls `portal.call(stream.aclose)` in `finally`; `__call__` returns that iterator for streams; `handle_flask_view` returns `(iterator, status, headers)`; `handle_vertebra_request` returns `tuple[int, list[tuple[str, str]], AsyncIterator[bytes]]` for streams and `Response` otherwise (return annotation `Response | tuple[int, list[tuple[str, str]], AsyncIterator[bytes]]`).
- `dispatch_mcp_request` in `django/mcp.py` widens its return annotation to the same union.
- Native `AgentPlugin` `GET` endpoint returns `(resp.status, resp.headers, resp.stream)` when streaming; the Litestar fallback returns `litestar.response.Stream(resp.stream, status_code=..., headers=..., media_type="text/event-stream")`.

## Edge Cases & Error Semantics

- Exceptions raised while iterating propagate after `aclose()`; no partial-success masking.
- A disconnect before the first chunk still closes the generator.
- WSGI iterators closed early by the server (`GeneratorExit`) close the async stream through the portal.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_adapters_stream_incrementally`).
- Fixture: legacy session plus a `GET` stream; publish two frames via `core.stream_broker.publish(sid, SseFrame(data="x"))` before reading; heartbeat `0.05` s.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| ASGI chunks | `AsgiMcpApp(core)` with GET scope; `receive` yields `http.disconnect` after the collector sees two body events | First body event `more_body is True` with `b"id: 1\ndata: x\n\n"`; last event `more_body is False`; broker subscriber count for `sid` returns to `0` |
| Starlette | `handle_starlette_request(Request(get_scope, receive))` | Returns `StreamingResponse`; `media_type == "text/event-stream"` |
| WSGI | `WsgiMcpApp(core)(get_environ, start_response)`; take two items then `close()` | Items `b"id: 1\ndata: x\n\n"`, `b"id: 2\ndata: x\n\n"`; subscriber count `0` after close |
| Native GET | Native handler with `headers={"accept": "text/event-stream", "mcp-protocol-version": "2025-06-18", "mcp-session-id": sid}` | Returns 3-tuple; third element has `__anext__` |
| Django | `dispatch_mcp_request(registry, <native GET Request>)` | Returns 3-tuple for streams |
| Buffered unchanged | ASGI `POST tools/list` | Single body event with `more_body is False` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "stream_incrementally"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/plugin.py src/py/vertebra/extensions/django/mcp.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/routes.py src/py/vertebra/agent/plugin.py src/py/vertebra/extensions/django/mcp.py`.

## Acceptance Criteria

- [ ] R9.2: every adapter streams incrementally and releases subscriptions on disconnect.
- [ ] Buffered responses keep their current single-event behaviour.

## Notes & Discoveries

- [2026-11-12 00:30] Before `server-runtime-correctness` lands, Vertebra's own ASGI mount buffers the stream until completion; external ASGI servers stream correctly. Recorded in the limitations chapter.

[^mcp-routes]: `src/py/vertebra/agent/mcp/routes.py::AsgiMcpApp.__call__` — Buffered ASGI writer.
[^chat-stream]: `src/py/vertebra/agent/controller.py::AgentChatController.handle_request` — Native streaming return precedent (status, headers, AsyncIterable).
[^django-dispatch]: `src/py/vertebra/extensions/django/mcp.py::dispatch_mcp_request` — Django native Request dispatch.
