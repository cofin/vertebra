---
type: Task
id: "server-runtime-correctness:ch03:t03"
chapter_id: ch03-asgi-rsgi-incremental-flush
title: Stream the Python AsgiSend fallback through an injected stream submitter
description: Add a stream_submitter to AsgiSend so non-final body, zero-copy, and SSE chunks are submitted as stream frames, wire _vertebra.submit_stream_frame into AsgiRunner._execute_and_submit and _AsgiMountRunner._execute_and_submit, and submit unary responses only when nothing was streamed.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch02:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch03-asgi-rsgi-incremental-flush/spec.md
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/server/test_asgi.py
files:
  - src/py/vertebra/server/_asgi/_queues.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/server/worker.py
tests:
  - src/py/tests/unit/server/test_asgi.py
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: asgi-send-py
    resource: src/py/vertebra/server/_asgi/_queues.py::AsgiSend
    title: AsgiSend message handling, on_chunk callback, and response property
  - id: asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py::AsgiRunner
    title: run_single, spawn_single, _execute_and_submit, and _to_native_response
  - id: mount-runner-py
    resource: src/py/vertebra/server/worker.py::_AsgiMountRunner
    title: Sharded mount runner _execute_and_submit
tags:
  - asgi
  - python
  - streaming
---

# Task t03: Stream the Python AsgiSend Fallback through an Injected Stream Submitter

## Objective & Scope Boundary

Give `AsgiSend` an optional `stream_submitter` so the Python submit paths stream like the Rust protocol. `AsgiRunner.run_single` gains a `stream_submitter` keyword that it passes to `AsgiSend`; `AsgiRunner._execute_and_submit` and `_AsgiMountRunner._execute_and_submit` pass `submit_stream_frame` and skip the unary `submit_response` when `send.streamed`. The buffered batch path (`AsgiRunner.__call__`, `run_batch`, `_AsgiMountRunner.__call__`) passes no submitter and keeps collecting. Out of scope: Rust protocols and `AsgiStreamSend`/`AsgiStreamQueuePair`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/server/_asgi/_queues.py::{AsgiSend,AsgiSend.__call__,AsgiSend.response,AsgiSend.send_raw_response,_encode_sse_message,ChunkCallback}` [^asgi-send-py].
- `src/py/vertebra/server/_asgi/_runner.py::{AsgiRunner.run_single,AsgiRunner.spawn_single,AsgiRunner._execute_and_submit,AsgiRunner._to_native_response}` [^asgi-runner-py].
- `src/py/vertebra/server/worker.py::_AsgiMountRunner._execute_and_submit` [^mount-runner-py].
- `src/py/vertebra/_vertebra.pyi::{submit_stream_frame,STREAM_FLAG_STREAMING,STREAM_FLAG_HEADERS_SENT,STREAM_FLAG_FINAL,STREAM_FLAG_ERROR,STREAM_FLAG_TRAILERS}`.
- `src/py/tests/unit/server/test_asgi.py` — existing `AsgiSend`/`AsgiRunner` tests.

## Symbol & Signature Contracts

- `src/py/vertebra/server/_asgi/_queues.py`:
  - `type StreamSubmitter = Callable[[int, int, int, list[tuple[bytes, bytes]], bytes], None]` exported in `__all__`.
  - `AsgiSend.__init__(self, request_id: int = 0, on_chunk: ChunkCallback | None = None, stream_submitter: StreamSubmitter | None = None) -> None`; new slots `_stream_submitter`, `_streamed`, `_stream_closed`.
  - `@property def streamed(self) -> bool`.
  - `__call__`: with a submitter, `http.response.body`/`http.response.zerocopysend`/`http.response.sse` with `more_body=True` submit `(request_id, status, flags, raw_headers_or_empty, chunk)` with `STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT` on the first frame; when already streaming, the final chunk submits `STREAM_FLAG_FINAL` (deferred to `http.response.trailers` when `trailers=True`), and `http.response.trailers` submits `STREAM_FLAG_FINAL | STREAM_FLAG_TRAILERS` with the trailer pairs; chunks are not appended to `_body_chunks` once streaming.
  - `def finish_stream(self, *, error: bool = False) -> None` submits one empty `STREAM_FLAG_FINAL` or `STREAM_FLAG_ERROR` frame when streaming and not closed.
  - Without a submitter, behaviour is unchanged.
- `src/py/vertebra/server/_asgi/_runner.py`:
  - `async def run_single(self, request: Any, *, stream_submitter: StreamSubmitter | None = None) -> AsgiResponse`; on exception after `send.streamed`, call `send.finish_stream(error=True)` and return the collected response; on normal return after streaming, call `send.finish_stream()`.
  - `_execute_and_submit` calls `run_single(request, stream_submitter=submit_stream_frame)` and calls `_native_submit_response` only when `not resp.streamed`.
- `src/py/vertebra/server/worker.py::_AsgiMountRunner._execute_and_submit` mirrors the same logic through `self._runner.run_single(request, stream_submitter=submit_stream_frame)`.
- `src/py/vertebra/server/_asgi/_queues.py::AsgiResponse` gains `streamed: bool = False`, populated by `AsgiSend.response` from `_streamed`.

## Edge Cases & Error Semantics

- `ConnectionResetError` raised by the submitter propagates out of `send` to the application, then `run_single` logs at `debug` (client gone) instead of `exception`, and no unary response is submitted.
- Fastpath responses (`self._fastpath.dispatch` returns a tuple) never stream and keep the unary path.
- `on_chunk` callbacks still fire for every chunk when both callbacks are set.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/server/test_asgi.py`, dependency category `unit`; tests inject a list-recording submitter.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Body streaming | App sends start `200` `[(b"content-type", b"text/plain")]`, body `b"a"` `more_body=True`, body `b"b"` `more_body=False`; `run_single(req, stream_submitter=rec)` | `rec == [(1, 200, 0b10001, [(b"content-type", b"text/plain")], b"a"), (1, 200, 0b11, [], b"b")]`; returned `AsgiResponse.streamed is True` |
| SSE streaming | `http.response.sse` `{data: "x", more_body: True}` then `{more_body: False}` | First frame body `b"data: x\n\n"`, last frame flags include `STREAM_FLAG_FINAL` |
| No submitter unchanged | Same body app with `run_single(req)` | `resp.body == b"ab"`, `resp.streamed is False` |
| Error after streaming | App raises `RuntimeError("boom")` after one streamed chunk | Last recorded flags `STREAM_FLAG_STREAMING \| STREAM_FLAG_ERROR`; no exception escapes `run_single` |
| Closed client | Submitter raises `ConnectionResetError` on the second frame | App sees `ConnectionResetError`; `run_single` returns with `streamed is True`; no unary submit |
| Trailers | Start `trailers=True`; body `b"x"` `more_body=True`; body `b""`; trailers `[(b"grpc-status", b"0")]` | Final frame flags `STREAM_FLAG_STREAMING \| STREAM_FLAG_FINAL \| STREAM_FLAG_TRAILERS` with `[(b"grpc-status", b"0")]` |
| Mount runner skip | `_AsgiMountRunner._execute_and_submit` with a streaming app and patched `submit_stream_frame`/`submit_response` | `submit_response` never called; stream frames recorded |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py -q -k stream`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_runner.py src/py/tests/unit/server/test_e2e.py -q
  uv run --no-sync ruff check src/py/vertebra/server
  uv run --no-sync mypy src/py/vertebra/server
  ```
- Planned local commit: `feat(server): stream Python ASGI fallback responses through submit_stream_frame`

## Acceptance Criteria

- [ ] `AsgiSend` streams through `stream_submitter` with the stated flag semantics and stays unchanged without one (R7).
- [ ] `AsgiRunner._execute_and_submit` and `_AsgiMountRunner._execute_and_submit` stream and suppress the unary submit when streamed (R7).
- [ ] The buffered batch path still returns complete responses (R7).

## Notes & Discoveries

[^asgi-send-py]: `src/py/vertebra/server/_asgi/_queues.py::AsgiSend` — `AsgiSend` message handling, `on_chunk` callback, and `response` property.
[^asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py::AsgiRunner` — `run_single`, `spawn_single`, `_execute_and_submit`, and `_to_native_response`.
[^mount-runner-py]: `src/py/vertebra/server/worker.py::_AsgiMountRunner` — Sharded mount runner `_execute_and_submit`.
