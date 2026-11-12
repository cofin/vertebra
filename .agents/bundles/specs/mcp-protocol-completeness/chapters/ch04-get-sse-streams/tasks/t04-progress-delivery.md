---
type: Task
id: "mcp-protocol-completeness:ch04:t04"
chapter_id: ch04-get-sse-streams
title: Deliver progress over POST SSE responses and session streams
description: Wire _meta.progressToken into AgentContext.progress_reporter; answer POST requests that accept text/event-stream with an SSE stream of progress and log frames followed by the final response, and route progress for JSON-only legacy session requests to the session stream.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch04:t03"
read_set:
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/context.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/tests/unit/agent/test_mcp_conformance.py
files:
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/agent/test_mcp_conformance.py
estimated_loc: 180
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
  - id: progress
    resource: src/py/vertebra/agent/core/context.py::ProgressReporter
    title: Progress reporter callback
  - id: mcp-handle
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle
    title: POST dispatch
tags:
  - progress
  - sse
---

# Task t04: Deliver Progress over POST SSE Responses and Session Streams

## Objective & Scope Boundary

`notifications/progress` reaches the client that asked for it [^progress] [^mcp-handle]. A POST whose params carry `_meta.progressToken` and whose `Accept` includes `text/event-stream` receives an SSE response in either protocol mode; legacy JSON-only requests with a session route progress to the session stream. `MCPCore.log` uses the same request sink first. Out of scope: task (`io.modelcontextprotocol/task`) progress, which stays on task status polling.

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/service.py` — `handle`, `_handle_tools_call`, `log`, `_publish_to_session`.
- `src/py/vertebra/agent/core/context.py` — `ProgressReporter`, `AgentContext.progress_reporter`.
- `src/py/vertebra/agent/core/sse.py` — `SseFrame.encode`.
- `src/py/tests/unit/agent/test_mcp_conformance.py`.

## Symbol & Signature Contracts

- Private `MCPCore._progress_token(params: Mapping[str, Any]) -> str | int | None` reads `params["_meta"]["progressToken"]` (`str` or `int` only).
- POST SSE path in `handle`: when a token exists, the frame is a single request, and `Accept` contains `text/event-stream`, `handle` creates an `anyio` memory stream (capacity `64`), sets `ctx.metadata["mcp.notification_sink"]` to an async callable that sends encoded `SseFrame` bytes, sets `ctx.progress_reporter = ProgressReporter(callback=<sink wrapper emitting {"jsonrpc": "2.0", "method": "notifications/progress", "params": payload}>, progress_token=token)`, runs dispatch in a task under the bound context, and returns `200` with `Content-Type: text/event-stream` and a `stream` that yields notification frames followed by one frame holding the JSON-RPC response, then ends.
- Session path: token present, `Accept` without `text/event-stream`, legacy session bound -> `ProgressReporter` callback publishes the progress notification through `_publish_to_session(session_id, ...)`; the HTTP response stays JSON.
- `MCPCore.log` prefers `ctx.metadata["mcp.notification_sink"]` when present (level gating then uses `config.default_log_level` for sessionless modern requests and the session level otherwise), else the session stream.
- `initialize` and notifications never take the SSE path.

## Edge Cases & Error Semantics

- Handler exceptions still produce a JSON-RPC error frame as the final SSE event.
- Tokens of other types (`float`, `dict`) are ignored (no progress wiring).
- Client disconnect cancels the dispatch task through the stream close.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/agent/test_mcp_conformance.py` (new parametrized `test_mcp_progress_delivery`).
- Tool: `async def slow(context: AgentContext) -> str` calling `await context.progress_reporter.report(1, 2, "half")`, `await context.progress_reporter.report(2, 2)`, then returning `"done"`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Modern POST SSE | `tools/call slow` with `_meta={"progressToken": "p1"}`, headers `MCP-Protocol-Version: 2026-07-28`, `Mcp-Method: tools/call`, `Mcp-Name: slow`, `Accept: application/json, text/event-stream` | `200`; `Content-Type == "text/event-stream"`; decoded frames: progress `{"progressToken": "p1", "progress": 1, "total": 2, "message": "half"}`, progress `{"progressToken": "p1", "progress": 2, "total": 2}`, final result text `"done"` |
| Legacy POST SSE | Same with header `2025-06-18` and session | Same three frames |
| Legacy session routing | Header `2025-06-18`, session, `Accept: application/json` | JSON `200` result `"done"`; session stream yields two progress frames |
| No token | `tools/call slow` without `_meta.progressToken`, SSE accept | JSON `200` response (no stream) |
| Error frame | Tool raising inside SSE path after one report | Frames: one progress, then final frame with `result.isError is True` |
| Log through sink | Tool calls `await core.log("info", "x")` in the SSE path with `MCPConfig(logging=True)` | A `notifications/message` frame precedes the result frame |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q -k "progress_delivery"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp/service.py && uv run --no-sync mypy src/py/vertebra/agent/mcp/service.py`.

## Acceptance Criteria

- [ ] R10.2: progress reaches POST SSE responses and legacy session streams.
- [ ] `MCPCore.log` prefers the request sink.

## Notes & Discoveries

- [2026-11-12 00:30] `ProgressReporter` exists in `vertebra.agent.core.context` but no MCP path constructs it with a callback at `c8722c76`.

[^progress]: `src/py/vertebra/agent/core/context.py::ProgressReporter` — Progress reporter callback.
[^mcp-handle]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle` — POST dispatch.
