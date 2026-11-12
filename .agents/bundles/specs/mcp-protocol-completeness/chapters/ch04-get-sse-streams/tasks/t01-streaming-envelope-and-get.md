---
type: Task
id: "mcp-protocol-completeness:ch04:t01"
chapter_id: ch04-get-sse-streams
title: Add the streaming envelope and legacy GET streams
description: Add McpHttpResponse.stream, SseStreamBroker.open_stream with per-session event ids, replay, heartbeats, owner stream limits, and close_session, and implement legacy GET /mcp negotiation in MCPCore.handle.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch03:t04"
read_set:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/tests/unit/agent/test_core_and_observability.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/models.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_core_and_observability.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 240
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
  - id: sse-broker
    resource: src/py/vertebra/agent/core/sse.py::SseStreamBroker
    title: Broker subscribe, publish, and replay
  - id: mcp-handle
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle
    title: HTTP method dispatch
tags:
  - sse
  - streaming
---

# Task t01: Add the Streaming Envelope and Legacy GET Streams

## Objective & Scope Boundary

`MCPCore.handle` returns an incremental SSE stream for legacy `GET /mcp` [^mcp-handle], backed by new broker primitives [^sse-broker]. Out of scope: adapter writers (`t02`) and what gets published (`t03`, `t04`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/models.py` — `McpHttpResponse`, `MCPConfig`.
- `src/py/vertebra/agent/core/sse.py` — `SseFrame`, `SseReplayBuffer.replay_since`, `SseStreamBroker`.
- `src/py/vertebra/agent/mcp/service.py` — `handle` method branches, `_resolve_session` from `ch03:t03`.
- `src/py/tests/unit/agent/test_core_and_observability.py::test_sse_replay_buffer_and_stream_broker_resumption`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- `models.py`: `McpHttpResponse.stream: AsyncIterator[bytes] | None = None`; `MCPConfig.enable_get_sse: bool = True`, `sse_heartbeat_seconds: float = 15.0`, `max_streams_per_owner: int = 8`.
- `sse.py`:
  - `class StreamLimitExceededError(VertebraError, RuntimeError)` in `__all__`.
  - `SseStreamBroker.publish` stamps `id=str(next_seq)` per session (counter in `self._sequences: dict[str, int]`) when `frame.id == ""`.
  - `SseStreamBroker.open_stream(self, session_id: str, *, owner_id: str | None, last_event_id: str | None = None, heartbeat_seconds: float = 15.0, max_streams_per_owner: int = 8) -> AsyncIterator[bytes]`: checks `self._owner_counts[owner_id or "anonymous"] >= max_streams_per_owner` and raises `StreamLimitExceededError` before returning; registers a subscriber via `subscribe`; returns an async generator that yields replayed frames, then `frame.encode()` for each received frame, `b": keepalive\n\n"` after each `heartbeat_seconds` of silence (`anyio.move_on_after`), and in `finally` removes only its own send stream and decrements the owner count.
  - `SseStreamBroker.close_session(self, session_id: str) -> None`: closes subscribers and drops the buffer and sequence counter.
- `service.py` `GET` branch: `enable_get_sse` false or header `2026-07-28` -> `405` (`Allow: POST, DELETE, OPTIONS`); unsupported header -> `400` / `-32022`; `Accept` missing or only `*/*` -> `405`; `Accept` without `text/event-stream` -> `406` / `-32600` `"Not Acceptable: Accept header must include text/event-stream"`; session header resolved through `_resolve_session` (unknown -> `404`); stream key is the session id or `f"anon-{secrets.token_hex(8)}"` (anonymous generators call `close_session(key)` on exit); `StreamLimitExceededError` -> `503` / `-32000` `"Stream limit exceeded"`; success -> `200` with `Content-Type: text/event-stream`, `Cache-Control: no-cache, no-transform`, `X-Accel-Buffering: no`, `MCP-Protocol-Version`, and `Mcp-Session-Id` when present.
- `405` responses for unsupported verbs now carry `Allow: GET, POST, DELETE, OPTIONS`.

## Edge Cases & Error Semantics

- `Last-Event-ID` that is not in the buffer replays the whole buffer (existing `replay_since` behaviour).
- Closing the generator before the first frame still decrements the owner count.
- Session `DELETE` (`ch03:t03`) now calls `close_session` instead of `disconnect`.

## Public-Seam Test Blueprint

- **Files**: `src/py/tests/unit/agent/test_core_and_observability.py` (broker cases), `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_get_stream_negotiation`).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Event ids | `broker.publish("s1", SseFrame(data="a"))`, then `"b"` | Buffered ids `["1", "2"]` |
| Replay | Publish `a,b,c,d`; `open_stream("s1", owner_id="o", last_event_id="2")`; read two chunks | `b"id: 3\ndata: c\n\n"`, `b"id: 4\ndata: d\n\n"` |
| Heartbeat | `open_stream("s2", owner_id="o", heartbeat_seconds=0.01)`; read one chunk | `b": keepalive\n\n"` |
| Owner limit | `max_streams_per_owner=1`; open twice for `"o"` | Second raises `StreamLimitExceededError` |
| Release on close | Open, `aclose()` the generator, open again with limit `1` | Second open succeeds |
| GET legacy | `initialize` (`2025-06-18`), then `GET` with `Accept: text/event-stream`, `Mcp-Session-Id: sid`, `MCP-Protocol-Version: 2025-06-18` | `200`; `Content-Type == "text/event-stream"`; `resp.stream is not None`; `resp.body == b""` |
| GET modern | `GET` with header `2026-07-28` | `405`; `Allow == "POST, DELETE, OPTIONS"` |
| GET wildcard | `GET` with `Accept: */*`, header `2025-06-18` | `405` |
| GET JSON accept | `Accept: application/json` | `406`; `error.code == -32600` |
| GET unknown session | `Mcp-Session-Id: nope` | `404`; `-32602` |
| GET disabled | `MCPConfig(enable_get_sse=False)` | `405` |
| GET limit | `MCPConfig(max_streams_per_owner=1)`; two GETs for one owner | Second `503`; `error.code == -32000` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_core_and_observability.py src/py/tests/unit/agent/test_mcp_conformance.py -q -k "stream"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent -q && uv run --no-sync ruff check src/py/vertebra/agent && uv run --no-sync mypy src/py/vertebra/agent/core/sse.py src/py/vertebra/agent/mcp`.

## Acceptance Criteria

- [ ] R9.1: envelope, negotiation, replay, heartbeat, and limits match the matrix.
- [ ] Anonymous stream buffers are removed when the stream ends.

## Notes & Discoveries

- [2026-11-12 00:30] `SseStreamBroker.publish` at `c8722c76` never assigns ids, so `Last-Event-ID` replay cannot locate a position.

[^sse-broker]: `src/py/vertebra/agent/core/sse.py::SseStreamBroker` — Broker subscribe, publish, and replay.
[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — HTTP method dispatch.
