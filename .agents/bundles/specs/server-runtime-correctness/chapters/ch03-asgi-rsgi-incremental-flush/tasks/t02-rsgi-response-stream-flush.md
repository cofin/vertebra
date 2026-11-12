---
type: Task
id: "server-runtime-correctness:ch03:t02"
chapter_id: ch03-asgi-rsgi-incremental-flush
title: Flush RSGI response_stream sends progressively
description: In direct-submit mode, make RsgiTransport.send_bytes and send_str submit stream frames immediately (headers with the first), finish streams with a final or error frame from finalize_async_return and finalize_async_error, and keep stream_chunks accumulation only for the non-direct take_response path.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch02:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch03-asgi-rsgi-incremental-flush/spec.md
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-core/src/bridge.rs
  - src/py/tests/unit/server/test_rsgi_protocol.py
files:
  - src/rs/vertebra-py/src/rsgi/http.rs
tests:
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/py/tests/unit/server/test_rsgi_protocol.py
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: rsgi-http-rs
    resource: src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol
    title: HttpProtocolState, RsgiTransport, response_stream, extract_prepared, finalize_async_return, finalize_async_error
tags:
  - rsgi
  - rust
  - streaming
---

# Task t02: Flush RSGI response_stream Sends Progressively

## Objective & Scope Boundary

Make RSGI streaming responses incremental: when `RsgiHttpProtocol.direct_submit` is true, `response_stream(status, headers)` records status and headers, and each `RsgiTransport::send_bytes`/`send_str` call submits a stream frame through `ResponseSubmitter::submit_stream_frame_inner` (the first carrying `FLAG_HEADERS_SENT` and headers). Completion submits one terminal frame. Out of scope: unary RSGI responses (`response_bytes`, `response_str`, `response_file`, `response_file_range`) and the non-direct path.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-py/src/rsgi/http.rs::{HttpProtocolState,RsgiHttpProtocol,RsgiHttpProtocol::response_stream,RsgiTransport,RsgiTransport::send_bytes,RsgiTransport::send_str,RsgiHttpProtocol::extract_prepared,RsgiHttpProtocol::take_completed_for_batch,RsgiHttpProtocol::finalize_async_return,RsgiHttpProtocol::finalize_async_error}` [^rsgi-http-rs].
- `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter::submit_stream_frame_inner` (`ch02:t01`).
- `src/rs/vertebra-core/src/bridge.rs::{BatchBridge::init,BatchBridge::set_stream_sink}` — test harness.
- `src/py/tests/unit/server/test_rsgi_protocol.py` — existing RSGI protocol tests.

## Symbol & Signature Contracts

- `RsgiTransport` gains a handle to the owning protocol's shared stream context: `Arc<RsgiStreamContext>` where `struct RsgiStreamContext { request_id: u64, direct_submit: bool, state: Mutex<RsgiStreamState> }` and `struct RsgiStreamState { status: u16, headers_raw: Vec<u8>, started: bool, closed: bool, buffered: Vec<u8> }`; `stream_chunks: Arc<Mutex<Vec<u8>>>` is replaced by this context.
- `RsgiTransport::send_bytes(&self, py, data: &[u8])`: `direct_submit` → submit `FLAG_STREAMING | (FLAG_HEADERS_SENT if !started)` with headers on the first frame, then return the ready awaitable; closed → `PyConnectionResetError`; non-direct → append to `buffered` as today.
- `send_str` delegates to `send_bytes` with UTF-8 bytes.
- `RsgiHttpProtocol::response_stream` stores status and headers into the context and sets `HttpProtocolState.streaming = true` and `stream_started = false`.
- `finalize_async_return`: streaming + direct + not closed → if no frame was sent yet, submit one `FLAG_STREAMING | FLAG_HEADERS_SENT | FLAG_FINAL` frame with empty body; otherwise one empty `FLAG_STREAMING | FLAG_FINAL` frame. Non-streaming responses keep `extract_prepared`.
- `finalize_async_error`: streaming + started → one empty `FLAG_STREAMING | FLAG_ERROR` frame; streaming not started → existing `500`.
- `take_completed_for_batch(false)` returns `None` while a direct stream is open (already true via `st.streaming`).

## Edge Cases & Error Semantics

- `send_bytes(b"")` before any data still submits the headers frame once; later empty sends are no-ops.
- Sends after `finalize_*` raise `RuntimeError("RSGI stream already finished")`.
- The context mutex is never held across `submit_stream_frame_inner`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-py/src/rsgi/http.rs` (new `#[cfg(test)] mod tests`, builds `RsgiHttpProtocol` with `direct_submit: true` and a capturing `StreamSink` on the global bridge), `src/py/tests/unit/server/test_rsgi_protocol.py` (non-direct regression); dependency category `unit`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Progressive sends | `response_stream(200, [("content-type","text/plain")])`; `send_bytes(b"a")`; `send_str("b")`; `finalize_async_return` | Frames: `(FLAG_STREAMING\|FLAG_HEADERS_SENT, b"a", headers)`, `(FLAG_STREAMING, b"b")`, `(FLAG_STREAMING\|FLAG_FINAL, b"")` |
| No data | `response_stream(204, [])`; `finalize_async_return` | One frame `FLAG_STREAMING\|FLAG_HEADERS_SENT\|FLAG_FINAL`, status `204` |
| Error after data | `send_bytes(b"a")`; `finalize_async_error` | Second frame `FLAG_STREAMING\|FLAG_ERROR` |
| Closed client | Sink returns `Closed` on the second send | Second `send_bytes` raises `ConnectionResetError` |
| Non-direct regression | Existing Python test using `RsgiHttpProtocol` without `direct_submit` | `take_response().body` equals the concatenated sends |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib rsgi::http::tests`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib rsgi::http::tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
  make dev
  uv run --no-sync pytest src/py/tests/unit/server/test_rsgi_protocol.py -q
  ```
- Planned local commit: `feat(rsgi): flush response_stream sends progressively`

## Acceptance Criteria

- [ ] Direct-submit RSGI streams emit one frame per send with headers on the first and one terminal frame (R6).
- [ ] Error and closed-client behaviour follows the stated contract (R6).
- [ ] Unary RSGI responses and the non-direct path are unchanged (R6).

## Notes & Discoveries

[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — `HttpProtocolState`, `RsgiTransport`, `response_stream`, `extract_prepared`, `finalize_async_return`, `finalize_async_error`.
