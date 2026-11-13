---
type: Task
id: "mcp-protocol-completeness:ch03:t01"
flow_id: mcp-protocol-completeness
chapter_id: ch03-sse-streams-and-resource-subscriptions
title: Add McpHttpResponse.stream and Legacy GET /mcp SSE Streams
description: "Extend McpHttpResponse with stream, add SseStreamBroker.open_stream with replay/heartbeat/owner limits, and handle legacy GET /mcp in MCPCore."
status: stable
state: completed
commit: 2454dbef
last_verified_commit: 2454dbef
last_verified_at: "2026-11-13T05:02:00Z"
verification_evidence: "ruff + mypy (41 files) + pytest (251 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/tests/unit/agent/test_core_and_observability.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 230
tags:
  - mcp
  - sse
  - streaming
  - agent
sources:
  - id: sse-broker
    resource: "src/py/vertebra/agent/core/sse.py::SseStreamBroker"
    title: SSE replay and subscriber broker
  - id: mcp-handle
    resource: "src/py/vertebra/agent/mcp/service.py::MCPCore.handle"
    title: HTTP verb dispatch
---

# Task `t01`: Add McpHttpResponse.stream and Legacy GET /mcp SSE Streams

## Objective & Scope Boundary

Implement `McpHttpResponse.stream`, `SseStreamBroker.{open_stream,close_session}`, and legacy `GET /mcp` SSE negotiation ([#109](https://github.com/cofin/vertebra/issues/109)) per [`../spec.md` §2](../spec.md) [^sse-broker] [^mcp-handle].

## Bounded Read Manifest

- `src/py/vertebra/agent/core/sse.py` & `src/py/vertebra/agent/mcp/{models,service}.py`.

## Contracts & Edge Cases

- `SseStreamBroker.open_stream(session_id, *, owner_id, last_event_id=None, heartbeat_seconds=15.0, max_streams_per_owner=8)` replays buffered frames after `last_event_id`, emits `: keepalive\n\n` comments on idle timeout, decrements owner counts on close, cleans up `anon-*` buffers, and raises `StreamLimitExceededError` (`503` / `-32000`) past `max_streams_per_owner`.
- `GET /mcp` returns `405` in `2026-07-28` mode, when `enable_get_sse=False`, or when `Accept` is missing/`*/*`; `406` when `Accept` excludes `text/event-stream`; `404` on unknown `Mcp-Session-Id`; and `200` `text/event-stream` otherwise.

## Test Matrix

- Parameterized tests in `test_core_and_observability.py` and `test_mcp_conformance.py` covering `2026-07-28` `405`, `*/*` `405`, `application/json` `406`, unknown session `404`, `9th` stream `503`, `Last-Event-ID` replay, keepalive comments, and disconnect cleanup.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/agent/core/sse.py src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent && uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_conformance.py -q
```

## Acceptance Criteria

- [x] Legacy `GET /mcp` opens bounded resumable SSE streams while `2026-07-28` `GET` returns `405`.
- [x] Closing or cancelling a stream releases subscriber and owner-quota state.

[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker` — SSE replay and subscriber broker.
[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP verb dispatch.
