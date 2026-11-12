---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch03-asgi-rsgi-incremental-flush
title: "Chapter 03: Progressive ASGI, RSGI, and Python Fallback Flush with Server-Binary Streaming Acceptance"
description: Flush ASGI body, zero-copy, and SSE chunks and RSGI stream sends as stream frames in direct-submit mode, stream the Python AsgiSend fallback through an injected submitter, and prove incremental delivery end to end through vertebra-server in embedded and SHM topologies.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-python-stream-submission
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: asgi-protocol-rs
    resource: src/rs/vertebra-py/src/asgi.rs::AsgiProtocol
    title: AsgiProtocolState, send, take_completed_for_batch, finalize_async_return, finalize_async_error, and drive_asgi_batch
  - id: rsgi-http-rs
    resource: src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol
    title: RSGI HTTP protocol stream_chunks accumulation and RsgiTransport
  - id: asgi-send-py
    resource: src/py/vertebra/server/_asgi/_queues.py::AsgiSend
    title: Python ASGI send collector with optional on_chunk callback
  - id: asgi-runner-py
    resource: src/py/vertebra/server/_asgi/_runner.py::AsgiRunner
    title: Python ASGI runner spawn and submit fallback paths
tags:
  - asgi
  - rsgi
  - streaming
  - sse
---

# Chapter 03: Progressive ASGI, RSGI, and Python Fallback Flush with Server-Binary Streaming Acceptance

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`AsgiProtocol::send` appends non-final chunks to `body_buf` and submits once on the final chunk; `RsgiHttpProtocol` appends `send_bytes`/`send_str` data to `stream_chunks` and submits once in `finalize_async_return`; the Python `AsgiSend` fallback collects into `_body_chunks` and `AsgiRunner._execute_and_submit` submits a unary `Response` [^asgi-protocol-rs] [^rsgi-http-rs] [^asgi-send-py] [^asgi-runner-py]. This chapter changes all three submit paths to flush stream frames through `ch02`'s seam, and adds the binary-level acceptance suite that closes [#81](https://github.com/cofin/vertebra/issues/81).

### 1.2 Non-Goals

- The buffered batch path (`AsgiRunner.__call__`, `_AsgiMountRunner.__call__`) keeps returning complete `AsgiResponse` objects.
- WebSocket and the Rust SSE hub (`vertebra-http/src/sse.rs`) are unchanged.

### 1.3 Upstream Baseline & Code Analysis

`drive_asgi_batch` builds protocols with `AsgiProtocol::from_batch(..., direct_submit=true, immediate_submit)`; inline-completed coroutines are collected by `take_completed_for_batch`, suspended ones submit on completion (`immediate_submit`). `finalize_async_return` submits a `500` when the app returned without `http.response.start`; `finalize_async_error` handles raised exceptions. `ch02` supplies `ResponseSubmitter::submit_stream_frame_inner` and `_vertebra.submit_stream_frame`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R5** (`t01`): ASGI progressive flush in direct-submit mode.
- **R6** (`t02`): RSGI progressive flush in direct-submit mode.
- **R7** (`t03`): Python `AsgiSend` streaming via `stream_submitter`.
- **R8** (`t04`): Server-binary acceptance tests in embedded and SHM topologies.

### 2.2 Concrete Type & Signature Contracts

```rust
// src/rs/vertebra-py/src/asgi.rs — AsgiProtocolState additions
streaming: bool,        // a FLAG_HEADERS_SENT frame was submitted
stream_closed: bool,    // a terminal frame was submitted or the client is gone
// AsgiProtocol private helpers
fn flush_stream_chunk(&self, py: Python<'_>, st: &mut AsgiProtocolState, chunk: &[u8], terminal_flags: u16) -> PyResult<()>;

// src/rs/vertebra-py/src/rsgi/http.rs — HttpProtocolState addition
stream_started: bool,
```

```python
# src/py/vertebra/server/_asgi/_queues.py
type StreamSubmitter = Callable[[int, int, int, list[tuple[bytes, bytes]], bytes], None]

class AsgiSend:
    def __init__(
        self,
        request_id: int = 0,
        on_chunk: ChunkCallback | None = None,
        stream_submitter: StreamSubmitter | None = None,
    ) -> None: ...
    @property
    def streamed(self) -> bool: ...
    def finish_stream(self, *, error: bool = False) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| ASGI app under `drive_asgi_batch` | `AsgiProtocol::send` → `flush_stream_chunk` | `ResponseSubmitter::submit_stream_frame_inner` |
| ASGI app returns or raises after streaming began | `finalize_async_return` / `finalize_async_error` | final empty frame / error frame |
| RSGI app `transport.send_bytes` | `RsgiTransport::send_bytes` | `submit_stream_frame_inner` |
| `AsgiRunner._execute_and_submit`, `_AsgiMountRunner._execute_and_submit` | `AsgiSend(stream_submitter=submit_stream_frame)` | `_vertebra.submit_stream_frame`; unary `submit_response` only when `not send.streamed` |
| `test_vertebra_server.py` | `vertebra-server` subprocess | raw socket reads asserting first chunk before app release |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| Start + single final body | Unary path (zero-copy, `single_body_py`, content-length) unchanged. |
| Non-final chunk with empty body | Headers frame once; no empty continuation. |
| `trailers=True` | Body completion submits nothing terminal; `http.response.trailers` submits `FLAG_FINAL \| FLAG_TRAILERS` with trailer headers. |
| App raises after streaming began | One `FLAG_ERROR` frame; exception logged; no `500` unary. |
| App returns after streaming without final chunk | One empty `FLAG_FINAL` frame. |
| `send` after `ConnectionResetError` or `notify_disconnect` | Raises `ConnectionResetError`; `receive` yields `http.disconnect`. |
| Inline batch collection after streaming began | `take_completed_for_batch` returns `None`. |
| `direct_submit=false` (`AsgiProtocol` constructed from Python, `take_response`) | Buffers as today. |
| Content-Length header present with streaming | Headers forwarded unchanged; hyper enforces length. |

### 2.5 Alternatives Considered

Flushing only when `immediate_submit` is set was rejected: an inline coroutine that streams then suspends would mix a collected unary response with later frames. Wrapping ASGI apps in a Python streaming adapter was rejected because `drive_asgi_batch` drives coroutines in Rust and the Python layer never sees the messages.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch03]`; streaming behaviour is documented in `docs/usage/server-and-workers.md` (`ch10:t01`) and `asgi-and-wsgi-runners.md` (`ch10:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R5 | [t01](tasks/t01-asgi-protocol-progressive-flush.md) | `behavior_tdd` | `src/rs/vertebra-py/src/asgi.rs` `mod tests`; `src/py/tests/unit/server/test_asgi.py` |
| R6 | [t02](tasks/t02-rsgi-response-stream-flush.md) | `behavior_tdd` | `src/rs/vertebra-py/src/rsgi/http.rs` `mod tests`; `src/py/tests/unit/server/test_rsgi_protocol.py` |
| R7 | [t03](tasks/t03-python-asgi-send-streaming.md) | `behavior_tdd` | `src/py/tests/unit/server/test_asgi.py` |
| R8 | [t04](tasks/t04-server-binary-streaming-acceptance.md) | `integration` | `src/py/tests/integration/server/test_vertebra_server.py` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch03:t01 — Flush ASGI body, zero-copy, and SSE chunks progressively in AsgiProtocol (tasks/t01-asgi-protocol-progressive-flush.md)
- [ ] server-runtime-correctness:ch03:t02 — Flush RSGI response_stream sends progressively (tasks/t02-rsgi-response-stream-flush.md)
- [ ] server-runtime-correctness:ch03:t03 — Stream the Python AsgiSend fallback through an injected stream submitter (tasks/t03-python-asgi-send-streaming.md)
- [ ] server-runtime-correctness:ch03:t04 — Prove incremental streaming through vertebra-server in embedded and SHM topologies (tasks/t04-server-binary-streaming-acceptance.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync pytest src/py/tests/unit/server src/py/tests/integration/server -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t04` (`0/4`).
- **Downstream Dependents**: `mcp-protocol-completeness` (streamable HTTP over mounted ASGI); `ch07:t02` reuses the streaming probe in the smoke test.

[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — `AsgiProtocolState`, `send`, `take_completed_for_batch`, `finalize_async_return`, `finalize_async_error`, and `drive_asgi_batch`.
[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — RSGI HTTP protocol `stream_chunks` accumulation and `RsgiTransport`.
[^asgi-send-py]: `src/py/vertebra/server/_asgi/_queues.py::AsgiSend` — Python ASGI send collector with optional `on_chunk` callback.
[^asgi-runner-py]: `src/py/vertebra/server/_asgi/_runner.py::AsgiRunner` — Python ASGI runner spawn and submit fallback paths.
