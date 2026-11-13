---
type: Task
id: "server-runtime-correctness:ch01:t03"
flow_id: server-runtime-correctness
chapter_id: ch01-python-stream-submission-and-flush
title: Stream Python AsgiSend Fallback Chunks via Injected Submitter
description: "Add stream_submitter, streamed, and finish_stream to AsgiSend and wire AsgiRunner and _AsgiMountRunner submit paths to _vertebra.submit_stream_frame."
status: stable
state: completed
commit: 6a23bd18
last_verified_at: "2026-11-13T02:35:00Z"
last_verified_commit: 6a23bd18
verification_evidence: "ruff check + mypy src/py/vertebra/server clean; pytest test_asgi.py passed"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch01:t01"
files:
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/server/worker.py
tests:
  - src/py/tests/unit/server/test_asgi.py
read_set:
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/server/test_asgi.py
estimated_loc: 185
tags:
  - python
  - asgi
  - streaming
  - server
sources:
  - id: asgi-send-py
    resource: "src/py/vertebra/server/_asgi/_queues.py::AsgiSend"
    title: Python fallback ASGI send collector
  - id: asgi-runner-py
    resource: "src/py/vertebra/server/_asgi/_runner.py::AsgiRunner"
    title: Python ASGI runner spawn and submit paths
---

# Task `t03`: Stream Python AsgiSend Fallback Chunks via Injected Submitter

## Objective & Scope Boundary

Enable progressive streaming on the Python `AsgiSend` fallback path (`spawn_single`, `_execute_and_submit`) per [`../spec.md` §2](../spec.md) [^asgi-send-py] [^asgi-runner-py]. Out of scope: buffered `AsgiRunner.__call__` (remains collecting by contract).

## Bounded Read Manifest

- `src/py/vertebra/server/_asgi/_queues.py` — `AsgiSend` state machine.
- `src/py/vertebra/server/_asgi/_runner.py` & `src/py/vertebra/server/worker.py` — `AsgiRunner` and `_AsgiMountRunner` submit paths.

## Contracts & Edge Cases

- `AsgiSend(request_id=0, on_chunk=None, stream_submitter=None)` exposes `streamed: bool` and `finish_stream(*, error: bool = False) -> None`.
- When `stream_submitter` is provided and `more_body=True` arrives, `AsgiSend` emits `STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT` with encoded byte headers followed by continuation/trailer/final frames; single-chunk responses stay unary (`streamed == False`).
- `_execute_and_submit` calls `send.finish_stream(error=True)` on mid-stream exceptions and skips unary `submit_response` whenever `send.streamed` is true.

## Test Matrix

- Parameterized unit tests in `test_asgi.py` covering multi-chunk streaming, empty `more_body=True` start flush, trailers, mid-stream exception `STREAM_FLAG_ERROR`, and `ConnectionResetError` propagation.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server && uv run --no-sync mypy src/py/vertebra/server && uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py -q
```

## Acceptance Criteria

- [x] `AsgiSend` streams multi-chunk responses progressively without buffering when `stream_submitter` is injected.
- [x] Buffered `AsgiRunner.__call__` continues returning complete `AsgiResponse` values.

[^asgi-send-py]: `src/py/vertebra/server/_asgi/_queues.py::AsgiSend` — Python fallback ASGI send collector.
[^asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py::AsgiRunner` — Python ASGI runner spawn and submit paths.
