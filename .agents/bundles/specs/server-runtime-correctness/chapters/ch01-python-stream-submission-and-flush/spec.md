---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch01-python-stream-submission-and-flush
title: "Chapter 01: Python Stream Submission and Progressive Protocol Flush"
description: "Expose PyO3 submit_stream_frame and max_stream_frame_payload, flush mounted ASGI and RSGI streams progressively, and wire native App streaming responses in server/_dispatch.py."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-13T02:38:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:executor
    at: "2026-11-13T02:38:00Z"
    commit: 6a23bd18
tags:
  - pyo3
  - streaming
  - asgi
  - rsgi
  - server
sources:
  - id: transport-py-rs
    resource: "src/rs/vertebra-py/src/transport.rs::ResponseSubmitter"
    title: PyO3 unary response submission and SHM ring transport binding
  - id: asgi-rs
    resource: "src/rs/vertebra-py/src/asgi.rs::AsgiProtocol"
    title: Rust ASGI protocol state machine
  - id: rsgi-http-rs
    resource: "src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol"
    title: Rust RSGI HTTP stream protocol
  - id: asgi-queues-py
    resource: "src/py/vertebra/server/_asgi/_queues.py::AsgiSend"
    title: Python fallback ASGI send queue
  - id: dispatch-py
    resource: "src/py/vertebra/server/_dispatch.py::_submit_coerced"
    title: Native App handler response coercion and submission
---

# Chapter 01: Python Stream Submission and Progressive Protocol Flush

<!-- truth: start -->
- `_vertebra.submit_stream_frame(request_id, status, flags, headers, body)` and `_vertebra.max_stream_frame_payload()` bind `BatchBridge::submit_stream_frame` and `Transport::send_stream_frame` with detached-GIL backpressure and `ConnectionResetError` on closed clients [^transport-py-rs].
- `AsgiProtocol` and `RsgiHttpProtocol` flush headers and every non-final body, zero-copy, SSE, and `response_stream` chunk immediately in direct-submit mode while keeping single-chunk unary fast paths untouched [^asgi-rs] [^rsgi-http-rs].
- Python `AsgiSend` streams through an injected `stream_submitter`, and native `App` `server/_dispatch.py` streams `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` and streaming `Response` bodies without falling through to `encode_json_bytes` [^asgi-queues-py] [^dispatch-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`vertebra-py/src/transport.rs` binds `submit_stream_frame`, `max_stream_frame_payload`, and `STREAM_FLAG_*` over both embedded `BatchBridge` and SHM `VRSF` rings [^transport-py-rs]; `AsgiProtocol` and `RsgiHttpProtocol` flush headers and continuation chunks progressively in direct-submit mode while preserving zero-copy unary responses [^asgi-rs] [^rsgi-http-rs]; Python `AsgiSend` streams fallback runs via `stream_submitter` [^asgi-queues-py]; and `server/_dispatch.py` streams `(status, headers, AsyncIterable[bytes] | Iterable[bytes])` and streaming `Response` bodies across embedded and SHM `vertebra-server` topologies [^dispatch-py]. Buffered `AsgiRunner.__call__` calls remain collecting by contract.

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` (`t01`)**: Add `ResponseSubmitter::submit_stream_frame_inner`, `#[pyfunction] submit_stream_frame`, `#[pyfunction] max_stream_frame_payload`, and `STREAM_FLAG_{STREAMING,FINAL,ERROR,HEADERS_SENT,TRAILERS}` in `vertebra-py` + `_vertebra.pyi`, chunking SHM payloads above `max_stream_frame_payload()` and rerouting `submit_raw_response(..., flags=FLAG_STREAMING)`.
- **`R2` (`t02`)**: Add `streaming: bool` and `stream_closed: bool` to `AsgiProtocolState` (`flush_stream_chunk`) and `stream_started: bool` to `RsgiHttpProtocol` (`RsgiTransport::send_bytes`/`send_str`), emitting `FLAG_HEADERS_SENT`, continuation `FLAG_STREAMING`, `FLAG_FINAL | FLAG_TRAILERS`, and `FLAG_ERROR` frames, raising `ConnectionResetError` on disconnect.
- **`R3` (`t03`)**: Add `stream_submitter: StreamSubmitter | None = None`, `streamed: bool`, and `finish_stream(*, error: bool = False)` on `AsgiSend` and wire `AsgiRunner._execute_and_submit` and `_AsgiMountRunner._execute_and_submit`.
- **`R4` (`t04`)**: Support `(status, headers, body_iter)` (`AsyncIterable[bytes] | Iterable[bytes]`) and streaming bodies in `server/_dispatch.py` (`_submit_coerced`, `_await_async_item`, `_coerce_response`), flushing progressive `STREAM_FLAG_*` frames via `_vertebra.submit_stream_frame`, and verify embedded + SHM `vertebra-server` streaming in `src/py/tests/integration/server/test_vertebra_server.py`.

```python
# src/py/vertebra/_vertebra.pyi
def max_stream_frame_payload() -> int: ...
def submit_stream_frame(
    request_id: int,
    status: int,
    flags: int,
    headers: Sequence[tuple[bytes | str, bytes | str]] | None,
    body: Buffer,
) -> None: ...

# src/py/vertebra/server/_asgi/_queues.py
type StreamSubmitter = Callable[[int, int, int, list[tuple[bytes, bytes]], bytes], None]
```

## 3. Documentation & OKF Knowledge Plan

Public streaming documentation (`docs/usage/server-and-workers.md`) and architecture chapters (`pyo3-ffi-and-batch-dispatch.md`, `asgi-and-wsgi-runners.md`) are updated in `ch07`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R1` | `t01` | `src/rs/vertebra-py/src/{transport,lib}.rs`, `src/py/vertebra/_vertebra.pyi` | `cargo test -p vertebra-py --lib transport` + `stubtest` + `test_ffi.py` |
| `R2` | `t02` | `src/rs/vertebra-py/src/asgi.rs`, `src/rs/vertebra-py/src/rsgi/http.rs` | `cargo test -p vertebra-py --lib` + `test_asgi.py` + `test_rsgi_protocol.py` |
| `R3` | `t03` | `src/py/vertebra/server/_asgi/{_queues,_runner}.py`, `worker.py` | `pytest src/py/tests/unit/server/test_asgi.py` |
| `R4` | `t04` | `src/py/vertebra/server/_dispatch.py` | `pytest src/py/tests/unit/server/test_asgi_sse.py src/py/tests/integration/server/test_vertebra_server.py` |

## 5. Implementation Plan

- [x] [6a23bd18] [t01-submit-stream-frame-binding.md](tasks/t01-submit-stream-frame-binding.md) — Expose `submit_stream_frame`, `max_stream_frame_payload`, and `STREAM_FLAG_*` across both transports
- [x] [6a23bd18] [t02-rust-asgi-and-rsgi-progressive-flush.md](tasks/t02-rust-asgi-and-rsgi-progressive-flush.md) — Flush ASGI `more_body=True` chunks and RSGI `response_stream` sends progressively
- [x] [6a23bd18] [t03-python-asgi-send-streaming.md](tasks/t03-python-asgi-send-streaming.md) — Wire Python `AsgiSend` streaming fallback through `_vertebra.submit_stream_frame`
- [x] [6a23bd18] [t04-native-dispatch-and-server-binary-streaming.md](tasks/t04-native-dispatch-and-server-binary-streaming.md) — Stream native `App` `(status, headers, AsyncIterable | Iterable)` responses in `_dispatch.py` and verify `vertebra-server` binary streaming

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`6a23bd18`)**: All 4 tasks in `ch01-python-stream-submission-and-flush` pass `cargo test -p vertebra-py --lib` (`13 passed`), `cargo clippy --all-targets -- -D warnings`, `mypy.stubtest vertebra._vertebra`, `ruff`, `mypy`, and `120 passed` across unit and live `embedded`/`shm` `vertebra-server` streaming acceptance tests.
- **Fast Chapter Gate (`< 45s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib && cargo build --manifest-path src/rs/Cargo.toml -p vertebra-http --bin vertebra-server && UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync maturin develop --uv --skip-install && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync ruff check src/py/vertebra/server src/py/tests/integration/server && uv run --no-sync pytest src/py/tests/unit/ffi/test_ffi.py src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_asgi_sse.py src/py/tests/unit/server/test_rsgi_protocol.py src/py/tests/integration/server/test_vertebra_server.py -q`.

[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 unary response submission and SHM ring transport binding.
[^asgi-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — Rust ASGI protocol state machine.
[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — Rust RSGI HTTP stream protocol.
[^asgi-queues-py]: `src/py/vertebra/server/_asgi/_queues.py::AsgiSend` — Python fallback ASGI send queue.
[^dispatch-py]: `src/py/vertebra/server/_dispatch.py::_submit_coerced` — Native App handler response coercion and submission.
