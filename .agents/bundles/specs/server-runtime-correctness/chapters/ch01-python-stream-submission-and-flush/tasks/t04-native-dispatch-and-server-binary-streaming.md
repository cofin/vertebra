---
type: Task
id: "server-runtime-correctness:ch01:t04"
flow_id: server-runtime-correctness
chapter_id: ch01-python-stream-submission-and-flush
title: Wire Native App Streaming in _dispatch.py and End-to-End Server Acceptance
description: "Stream (status, headers, AsyncIterable[bytes] | Iterable[bytes]) returns from native App handlers in server/_dispatch.py and prove embedded + SHM streaming through vertebra-server."
status: stable
state: completed
commit: 6a23bd18
last_verified_at: "2026-11-13T02:35:00Z"
last_verified_commit: 6a23bd18
verification_evidence: "cargo build --bin vertebra-server + ruff + mypy + pytest test_asgi_sse.py test_vertebra_server.py (120 passed across suite)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch01:t02"
  - "server-runtime-correctness:ch01:t03"
files:
  - src/py/vertebra/server/_dispatch.py
tests:
  - src/py/tests/unit/server/test_asgi_sse.py
  - src/py/tests/integration/server/__init__.py
  - src/py/tests/integration/server/conftest.py
  - src/py/tests/integration/server/test_vertebra_server.py
read_set:
  - src/py/vertebra/server/_dispatch.py
  - src/py/vertebra/agent/plugin.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/server/test_asgi_sse.py
estimated_loc: 235
tags:
  - server
  - streaming
  - sse
  - integration
sources:
  - id: dispatch-py
    resource: "src/py/vertebra/server/_dispatch.py::_submit_coerced"
    title: Native App response coercion and submission
  - id: agent-plugin-py
    resource: "src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native"
    title: Native App MCP/Agent SSE endpoint caller
---

# Task `t04`: Wire Native App Streaming in _dispatch.py and End-to-End Server Acceptance

## Objective & Scope Boundary

Support progressive `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` streaming in `src/py/vertebra/server/_dispatch.py` so native `App` SSE endpoints never `500`, and add live `vertebra-server` streaming acceptance tests in `src/py/tests/integration/server/` per [`../spec.md` §2](../spec.md) [^dispatch-py] [^agent-plugin-py].

## Bounded Read Manifest

- `src/py/vertebra/server/_dispatch.py` — `_coerce_response`, `_submit_coerced`, `_await_async_item`, and `BatchDispatchPlan`.
- `src/py/vertebra/agent/plugin.py` — native `App` route return shapes consumed by `mcp-protocol-completeness`.

## Contracts & Edge Cases

- Detect `(status: int, headers: Mapping[str, str] | Sequence[tuple[str | bytes, str | bytes]], body: AsyncIterable[bytes] | Iterable[bytes])` in `_dispatch.py` before JSON serialization.
- Stream `Iterable[bytes]` and `AsyncIterable[bytes]` chunks via `_vertebra.submit_stream_frame` (`STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT` on start, continuation chunks, final `STREAM_FLAG_FINAL`, `STREAM_FLAG_ERROR` on iterator failure, and clean `aclose()` on `ConnectionResetError`).
- Create `src/py/tests/integration/server/{__init__,conftest,test_vertebra_server}.py` exercising embedded and SHM `vertebra-server` over raw sockets to assert first-chunk delivery while an async event gate remains open across ASGI SSE, RSGI stream, and native `App` SSE routes.

## Test Matrix

- Parameterized unit tests in `test_asgi_sse.py` for sync/async iterable `(status, headers, stream)` tuples, empty streams, mid-stream exceptions, and client disconnect `aclose()`, plus embedded/SHM binary tests in `test_vertebra_server.py`.

## Verification

```bash
cargo build --manifest-path src/rs/Cargo.toml -p vertebra-http --bin vertebra-server && uv run --no-sync ruff check src/py/vertebra/server/_dispatch.py src/py/tests/integration/server && uv run --no-sync mypy src/py/vertebra/server/_dispatch.py && uv run --no-sync pytest src/py/tests/unit/server/test_asgi_sse.py src/py/tests/integration/server/test_vertebra_server.py -q
```

## Acceptance Criteria

- [x] Native `App` handlers returning `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` stream progressively with zero `TypeError` / `500` fallthrough.
- [x] `vertebra-server` passes embedded and SHM progressive streaming integration assertions.

[^dispatch-py]: `src/py/vertebra/server/_dispatch.py::_submit_coerced` — Native App response coercion and submission.
[^agent-plugin-py]: `src/py/vertebra/agent/plugin.py::AgentPlugin.mount_native` — Native App MCP/Agent SSE endpoint caller.
