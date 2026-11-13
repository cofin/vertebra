---
type: Task
id: "server-runtime-correctness:ch01:t02"
flow_id: server-runtime-correctness
chapter_id: ch01-python-stream-submission-and-flush
title: Flush ASGI and RSGI Streaming Responses Progressively in Rust
description: "Update AsgiProtocol and RsgiHttpProtocol in vertebra-py to flush response headers and non-final chunks immediately via ResponseSubmitter::submit_stream_frame_inner."
status: stable
state: completed
commit: 6a23bd18
last_verified_at: "2026-11-13T02:35:00Z"
last_verified_commit: 6a23bd18
verification_evidence: "cargo test -p vertebra-py --lib (13 passed); pytest test_asgi.py test_rsgi_protocol.py passed"
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
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/rsgi/http.rs
tests:
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/py/tests/unit/server/test_asgi.py
  - src/py/tests/unit/server/test_rsgi_protocol.py
read_set:
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/rs/vertebra-py/src/transport.rs
  - src/py/tests/unit/server/test_asgi.py
  - src/py/tests/unit/server/test_rsgi_protocol.py
estimated_loc: 240
tags:
  - rust
  - asgi
  - rsgi
  - streaming
  - sse
sources:
  - id: asgi-rs
    resource: "src/rs/vertebra-py/src/asgi.rs::AsgiProtocol"
    title: ASGI send and batch completion state machine
  - id: rsgi-http-rs
    resource: "src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol"
    title: RSGI response_stream transport
---

# Task `t02`: Flush ASGI and RSGI Streaming Responses Progressively in Rust

## Objective & Scope Boundary

Implement progressive chunk flushing in `AsgiProtocol` and `RsgiHttpProtocol` for [#81](https://github.com/cofin/vertebra/issues/81) per [`../spec.md` §2](../spec.md) [^asgi-rs] [^rsgi-http-rs]. Out of scope: Python `AsgiSend` fallback (`t03`) and native `App` `_dispatch.py` (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-py/src/asgi.rs` — `AsgiProtocol::send`, `take_completed_for_batch`, `finalize_async_return`, and `finalize_async_error`.
- `src/rs/vertebra-py/src/rsgi/http.rs` — `RsgiHttpProtocol` and `RsgiTransport::{send_bytes,send_str}`.
- `src/rs/vertebra-py/src/transport.rs` — `ResponseSubmitter::submit_stream_frame_inner` from `t01`.

## Contracts & Edge Cases

- Single-chunk (`more_body=False` on first body) responses keep the existing zero-copy unary path.
- In `direct_submit` mode, `more_body=True` flushes `FLAG_STREAMING | FLAG_HEADERS_SENT` once (omitting empty continuation frames), flushes `http.response.body`, `http.response.zerocopysend`, and `http.response.sse` chunks in order, defers `FLAG_FINAL` until `http.response.trailers` when `trailers=True`, emits `FLAG_ERROR` if the coroutine raises mid-stream, and raises `ConnectionResetError` + sets `http.disconnect` when closed.
- `RsgiTransport::send_bytes` and `send_str` flush progressive frames in direct-submit mode and finalize cleanly on return.

## Test Matrix

- Parameterized tests in `test_asgi.py` and `test_rsgi_protocol.py` verifying first-chunk arrival before generator release, SSE framing, HTTP trailers, mid-stream error frame emission, client disconnect `ConnectionResetError`, and unchanged unary responses.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib && UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync maturin develop --uv && uv run --no-sync pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_rsgi_protocol.py -q
```

## Acceptance Criteria

- [x] ASGI `more_body=True` and RSGI `response_stream` flush chunks before handler return.
- [x] Single-chunk unary responses and `direct_submit=False` buffered callers remain unchanged.

[^asgi-rs]: `src/rs/vertebra-py/src/asgi.rs::AsgiProtocol` — ASGI send and batch completion state machine.
[^rsgi-http-rs]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — RSGI response_stream transport.
