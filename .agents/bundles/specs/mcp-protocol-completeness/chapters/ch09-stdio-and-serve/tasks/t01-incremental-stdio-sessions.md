---
type: Task
id: "mcp-protocol-completeness:ch09:t01"
chapter_id: ch09-stdio-and-serve
title: Make stdio incremental, concurrent, and session-aware
description: Rewrite run_stdio_server to read lines incrementally, dispatch requests concurrently with serialized writes, honour notifications/cancelled, bind legacy initialize sessions, relay server notifications to stdout, write progress lines, and apply header-free negotiation in MCPCore.handle_payload.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "mcp-protocol-completeness:ch08:t01"
read_set:
  - src/py/vertebra/agent/mcp/stdio.py
  - src/py/vertebra/agent/mcp/service.py
  - src/py/vertebra/agent/core/sse.py
  - src/py/vertebra/agent/core/context.py
  - src/py/tests/unit/cli/test_mcp_cli.py
files:
  - src/py/vertebra/agent/mcp/stdio.py
  - src/py/vertebra/agent/mcp/service.py
tests:
  - src/py/tests/unit/cli/test_mcp_cli.py
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
  - id: stdio
    resource: src/py/vertebra/agent/mcp/stdio.py::run_stdio_server
    title: Current stdio runner
  - id: handle-payload
    resource: src/py/vertebra/agent/mcp/service.py::MCPCore.handle_payload
    title: Header-free dispatch
tags:
  - stdio
  - sessions
---

# Task t01: Make Stdio Incremental, Concurrent, and Session-Aware

## Objective & Scope Boundary

Stdio becomes a full MCP transport for both protocol modes [^stdio] [^handle-payload]. Out of scope: CLI app discovery (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server`.
- `src/py/vertebra/agent/mcp/service.py` — `handle_payload`, `_negotiate_protocol`, `sessions`, `stream_broker`, notification sink handling from `ch04:t04`.
- `src/py/vertebra/agent/core/sse.py::SseStreamBroker.subscribe`, `close_session`.
- `src/py/vertebra/agent/core/context.py` — `AgentContext.fork`, `ProgressReporter`.
- `src/py/tests/unit/cli/test_mcp_cli.py`.

## Symbol & Signature Contracts

- `service.py::MCPCore.handle_payload(self, raw_bytes: bytes, context: AgentContext | None = None) -> bytes | None`: before dispatch, negotiates from `params._meta["io.modelcontextprotocol/protocolVersion"]` and `context.metadata.get("mcp.session")` with the same mode rule as HTTP (no headers); unsupported `_meta` versions return a `-32022` error frame with the `data` payload from `ch03:t01`; legacy-only methods in modern mode return a `-32601` frame; writes `"mcp.protocol"` on the context.
- `stdio.py::run_stdio_server(service: MCPCore, *, stdin: AsyncIterable[bytes] | None = None, stdout: Callable[[bytes], Awaitable[None]] | None = None, max_message_bytes: int = 4_194_304, context: AgentContext | None = None) -> int`:
  - Default stdin iterates `anyio.wrap_file(sys.stdin.buffer)`; default stdout writes `data + b"\n"` and flushes through `anyio.wrap_file(sys.stdout.buffer)`; all writes go through one `anyio.Lock`.
  - `base = context or AgentContext(session_id=f"stdio-{uuid.uuid4().hex}")`.
  - A relay task subscribes to `f"anon-{base.session_id}"` at start; after a successful `initialize` it switches to the created session id (`request_ctx.metadata["mcp.created_session_id"]`), binding `base.metadata["mcp.session"]` and `base.session_id`.
  - Each line runs in an `anyio` task group task with `request_ctx = base.fork()`; requests with `_meta.progressToken` set `request_ctx.progress_reporter` to write `notifications/progress` lines and set `"mcp.notification_sink"` to the same writer.
  - `in_flight: dict[str | int, anyio.CancelScope]`; `notifications/cancelled` with `params.requestId` cancels the matching scope; cancelled requests write nothing.
  - Oversized lines write `{"jsonrpc": "2.0", "id": null, "error": {"code": -32600, "message": "Message exceeded max size of <n> bytes"}}`.
  - At EOF: waits for in-flight tasks, deletes the bound session (`service.sessions.delete`), closes relay subscriptions (`close_session`), returns `0`.

## Edge Cases & Error Semantics

- Blank lines are skipped.
- Relay frames are written as their `data` payload (one JSON object per line), never SSE framing.
- Exceptions inside one request produce that request's JSON-RPC error frame without stopping other requests.

## Public-Seam Test Blueprint

- **File**: `src/py/tests/unit/cli/test_mcp_cli.py` (new parametrized `test_stdio_server_protocol`; stdin is an `anyio` memory-stream-backed async iterator so the test controls when EOF happens).
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Incremental | Send `tools/list` (id `1`) without closing stdin; wait for one write | Write has `id == 1` before EOF |
| Concurrency | Tool `slow` awaits an event; send `slow` (id `2`) then `tools/list` (id `3`) | Response `3` arrives before `2`; setting the event yields `2` |
| Cancellation | `slow` (id `5`), then `{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 5}}`, then EOF | No write with `id == 5` |
| Legacy session relay | `initialize {"protocolVersion": "2025-06-18"}` (id `1`), `resources/subscribe {"uri": "config://app"}` (id `2`), then `await service.notify_resource_updated("config://app")` | A write with `method == "notifications/resources/updated"` and `params.uri == "config://app"` |
| Modern list change | No initialize; `service.registry.register_tool(<echo>)` | A write with `method == "notifications/tools/list_changed"` |
| Progress | `tools/call` with `_meta.progressToken = "p"` on a tool reporting `report(1, 1)` | Progress line `params == {"progressToken": "p", "progress": 1, "total": 1}` before the result |
| Modern ping | `ping` with `_meta["io.modelcontextprotocol/protocolVersion"] = "2026-07-28"` | Error frame `code == -32601` |
| Oversize | Line of `max_message_bytes + 1` bytes with `max_message_bytes=16` | Error frame `code == -32600` |
| EOF cleanup | `initialize` then EOF | Returns `0`; `await service.sessions.active_session_ids() == ()` |

## Verification

- **Red**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py -q -k "stdio_server_protocol"` fails.
- **Green**: `uv run --no-sync pytest src/py/tests/unit/cli/test_mcp_cli.py src/py/tests/unit/agent/test_mcp_conformance.py -q && uv run --no-sync ruff check src/py/vertebra/agent/mcp && uv run --no-sync mypy src/py/vertebra/agent/mcp/stdio.py src/py/vertebra/agent/mcp/service.py`.

## Acceptance Criteria

- [ ] R15.1: incremental, concurrent, cancellable stdio with sessions, relay, and progress matches the matrix.

## Notes & Discoveries

- [2026-11-12 00:30] The default stdio path at `c8722c76` reads all of `sys.stdin.buffer` in a worker thread before dispatching any line.

[^stdio]: `src/py/vertebra/agent/mcp/stdio.py::run_stdio_server` — Current stdio runner.
[^handle-payload]: `src/py/vertebra/agent/mcp/service.py::MCPCore.handle_payload` — Header-free dispatch.
