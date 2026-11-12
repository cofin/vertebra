---
type: Task
id: "server-runtime-correctness:ch03:t01"
chapter_id: ch03-asgi-rsgi-incremental-flush
title: Flush ASGI body, zero-copy, and SSE chunks progressively in AsgiProtocol
description: In direct-submit mode, submit the response start with the first non-final http.response.body, http.response.zerocopysend, or http.response.sse chunk and every later chunk as stream frames, finalize with trailers, an empty final frame, or an error frame, keep single-chunk responses unary, and raise ConnectionResetError on send after disconnect.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch02:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch03-asgi-rsgi-incremental-flush/spec.md
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-core/src/bridge.rs
  - src/py/tests/unit/server/test_asgi.py
files:
  - src/rs/vertebra-py/src/asgi.rs
tests:
  - src/rs/vertebra-py/src/asgi.rs
  - src/py/tests/unit/server/test_asgi.py
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: asgi-protocol-rs
    resource: src/rs/vertebra-py/src/asgi.rs::AsgiProtocol
    title: AsgiProtocolState, send, take_completed_for_batch, finalize_async_return, finalize_async_error, notify_disconnect
  - id: transport-py-rs
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: submit_stream_frame_inner from ch02
tags:
  - asgi
  - rust
  - streaming
---

# Task t01: Flush ASGI Body, Zero-Copy, and SSE Chunks Progressively in AsgiProtocol

## Objective & Scope Boundary

Change `AsgiProtocol` in `src/rs/vertebra-py/src/asgi.rs` so that, when `self.direct_submit` is true, the first non-final body-bearing message submits a `FLAG_STREAMING | FLAG_HEADERS_SENT` frame with status, `headers_raw`, and the chunk, later chunks submit `FLAG_STREAMING` frames, and completion submits exactly one terminal frame. Out of scope: RSGI (`t02`), Python fallback (`t03`), and the `direct_submit=false` path used by `take_response`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts and edge cases.
- `src/rs/vertebra-py/src/asgi.rs::{AsgiProtocolState,AsgiProtocol,AsgiProtocol::send,AsgiProtocol::materialize_single_into_buf,AsgiProtocol::extract_prepared_from_guard,AsgiProtocol::take_completed_for_batch,AsgiProtocol::take_error_for_batch,AsgiProtocol::finalize_async_return,AsgiProtocol::finalize_async_error,AsgiProtocol::notify_disconnect,encode_sse_message_into}` [^asgi-protocol-rs].
- `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter::submit_stream_frame_inner` (`ch02:t01`) [^transport-py-rs].
- `src/rs/vertebra-core/src/bridge.rs::{BatchBridge::init,BatchBridge::get,BatchBridge::set_stream_sink}` — test harness.
- `src/py/tests/unit/server/test_asgi.py::test_asgi_protocol_unary_multi_chunk_and_disconnect`.

## Symbol & Signature Contracts

- `AsgiProtocolState` gains `streaming: bool` and `stream_closed: bool` (default `false`).
- `fn flush_stream_chunk(&self, py: Python<'_>, st: &mut AsgiProtocolState, chunk: &[u8], terminal_flags: u16) -> PyResult<()>` computes `flags = FLAG_STREAMING | terminal_flags | if !st.streaming { FLAG_HEADERS_SENT } else { 0 }`, takes `headers_raw` only for the first frame, sets `st.streaming = true`, `st.submitted = true`, and `st.stream_closed = terminal_flags != 0`, then releases the state lock before calling `ResponseSubmitter::submit_stream_frame_inner`. A `ConnectionResetError` from the submitter sets `stream_closed`, stores `disconnected = true`, resolves the pending disconnect future, and re-raises.
- `send` rules in direct-submit mode for `http.response.body`, `http.response.zerocopysend`, `http.response.sse`:
  - Not yet streaming and `!more_body` → existing unary logic.
  - `more_body` → materialise any buffered single body, append the chunk (SSE encoded via `encode_sse_message_into`) and flush it as one frame (skip continuation frames whose chunk is empty after the first frame).
  - Already streaming and `!more_body` → flush the chunk with `FLAG_FINAL` unless `expects_trailers && !trailers_done`, in which case flush data only and wait.
- `http.response.trailers` while streaming → encode trailer headers with `encode_py_headers_into` and submit a `FLAG_FINAL | FLAG_TRAILERS` frame carrying them.
- `http.response.pathsend` while streaming → `RuntimeError("http.response.pathsend after streaming began")`.
- `take_completed_for_batch` returns `None` when `st.streaming`.
- `finalize_async_return`: streaming and not `stream_closed` → submit an empty `FLAG_FINAL` frame.
- `finalize_async_error`: streaming and not `stream_closed` → submit one empty `FLAG_ERROR` frame instead of a `500`.
- `send` when `disconnected` or `stream_closed` after a reset → `PyConnectionResetError(format!("client disconnected (request_id={})", self.request_id))`.

## Edge Cases & Error Semantics

- First message `more_body=True` with empty body: headers frame is sent once with an empty body.
- Mixed `http.response.body` and `http.response.sse` chunks share one stream.
- `content-length` supplied by the app is forwarded unchanged; no automatic content-length is added for streams.
- The state lock is never held across `submit_stream_frame_inner`, which may block with the GIL detached.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-py/src/asgi.rs` (new `#[cfg(test)] mod tests`; installs a capturing `StreamSink` on `BatchBridge::init(..)`'s global bridge and drives `AsgiProtocol` with `direct_submit=true` under `pyo3::Python::attach`), `src/py/tests/unit/server/test_asgi.py` (Python surface); dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Body streaming | `send(start 200, [content-type: text/plain])`, `send(body b"part-a\|", more_body=True)`, `send(body b"part-b", more_body=False)` | Captured frames: `(FLAG_STREAMING\|FLAG_HEADERS_SENT, b"part-a\|", headers present)`, `(FLAG_STREAMING\|FLAG_FINAL, b"part-b", no headers)` |
| SSE streaming | start `text/event-stream`; `http.response.sse {data: "1", more_body: True}`; `{data: "2", more_body: True}`; `{data: "", more_body: False}` | Frames bodies `b"data: 1\n\n"`, `b"data: 2\n\n"`, terminal `FLAG_FINAL` frame |
| Unary unchanged | start + `body b"ok"` (`more_body` absent) | No stream frame captured; one unary submission through `submit_raw_inner` |
| Trailers | start with `trailers=True`; body `b"x"` `more_body=True`; body `b""` `more_body=False`; `http.response.trailers [(b"grpc-status", b"0")]` | Frames: headers frame `b"x"`; final frame flags `FLAG_STREAMING\|FLAG_FINAL\|FLAG_TRAILERS` with trailer header `grpc-status: 0` |
| Error after streaming | Stream one chunk, then call `finalize_async_error` | Exactly one extra frame with `FLAG_STREAMING\|FLAG_ERROR`; no `500` unary |
| Return without final | Stream one chunk, then `finalize_async_return` | One empty `FLAG_STREAMING\|FLAG_FINAL` frame |
| Closed client | Sink returns `StreamDelivery::Closed` for the second frame | Second `send` raises `ConnectionResetError`; third `send` raises `ConnectionResetError` without reaching the sink |
| Batch collection | Stream a chunk then `take_completed_for_batch(false)` | `None` |
| Python flush path engaged | `init_transport()`; `AsgiProtocol(b"", request_id=3, direct_submit=True)`; await start; await body `b"a"` `more_body=True` | `RuntimeError` matching `"response stream sink not registered"` |
| Python buffered path | Existing `test_asgi_protocol_unary_multi_chunk_and_disconnect` (`direct_submit=False`) | Still passes with `take_response().body == b"part-a\|part-b"` |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib asgi::tests`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib asgi::tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
  make dev
  uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_asgi_sse.py src/py/tests/unit/server/test_stage1_hotpath_and_314t.py -q
  ```
- Planned local commit: `feat(asgi): flush more_body chunks progressively in AsgiProtocol`

## Acceptance Criteria

- [ ] Direct-submit ASGI responses with `more_body=True` emit ordered stream frames for body, zero-copy, and SSE messages (R5).
- [ ] Trailers, implicit completion, errors after streaming, and disconnects follow the stated frame semantics (R5).
- [ ] Single-chunk responses and `direct_submit=false` protocols are unchanged (R5).

## Notes & Discoveries

[^asgi-protocol-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — `AsgiProtocolState`, `send`, `take_completed_for_batch`, `finalize_async_return`, `finalize_async_error`, `notify_disconnect`.
[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — `submit_stream_frame_inner` from `ch02`.
