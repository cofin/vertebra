---
type: Task
id: "server-runtime-correctness:ch01:t01"
flow_id: server-runtime-correctness
chapter_id: ch01-python-stream-submission-and-flush
title: Expose PyO3 submit_stream_frame and max_stream_frame_payload
description: "Bind ResponseSubmitter::submit_stream_frame_inner, submit_stream_frame, max_stream_frame_payload, and STREAM_FLAG_* constants in vertebra-py and _vertebra.pyi."
status: stable
state: completed
commit: 6a23bd18
last_verified_at: "2026-11-13T02:35:00Z"
last_verified_commit: 6a23bd18
verification_evidence: "cargo test -p vertebra-py --lib (13 passed); mypy.stubtest vertebra._vertebra (Success); pytest test_ffi.py passed"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-py/src/transport.rs
  - src/py/tests/unit/ffi/test_ffi.py
read_set:
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
  - src/rs/vertebra-core/src/bridge.rs
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/py/tests/unit/ffi/test_ffi.py
estimated_loc: 210
tags:
  - rust
  - pyo3
  - streaming
  - ffi
sources:
  - id: transport-py-rs
    resource: "src/rs/vertebra-py/src/transport.rs::ResponseSubmitter"
    title: Existing unary ResponseSubmitter and SHM ring helpers
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: Typed Python stub for vertebra._vertebra
---

# Task `t01`: Expose PyO3 submit_stream_frame and max_stream_frame_payload

## Objective & Scope Boundary

Implement `ResponseSubmitter::submit_stream_frame_inner`, `_vertebra.submit_stream_frame`, `_vertebra.max_stream_frame_payload`, and `STREAM_FLAG_*` constants per [`../spec.md` §2](../spec.md) [^transport-py-rs] [^vertebra-pyi]. Out of scope: ASGI/RSGI protocol state machines (`t02`).

## Bounded Read Manifest

- `src/rs/vertebra-py/src/transport.rs` — `ResponseSubmitter` and `submit_raw_response` streaming branch.
- `src/rs/vertebra-py/src/lib.rs` — `_vertebra` module registration.
- `src/py/vertebra/_vertebra.pyi` — stub declarations checked by `mypy.stubtest`.
- `src/rs/vertebra-core/src/bridge.rs` & `transport/ring_pair.rs` — `BatchBridge::submit_stream_frame` and `RingPair::max_stream_frame_payload`.

## Contracts & Edge Cases

- Register `STREAM_FLAG_STREAMING`, `STREAM_FLAG_FINAL`, `STREAM_FLAG_ERROR`, `STREAM_FLAG_HEADERS_SENT`, `STREAM_FLAG_TRAILERS`, `max_stream_frame_payload() -> int`, and 5-arg `submit_stream_frame(request_id, status, flags, headers, body) -> None`.
- Reject missing `STREAM_FLAG_STREAMING` with `ValueError`; uninitialized transport or missing sink raises `RuntimeError`; `StreamDelivery::Closed` raises `ConnectionResetError`.
- Detach the GIL during bridge `deliver_blocking` and SHM `RingFull` waits; split bodies exceeding `max_stream_frame_payload()` across consecutive `VRSF` frames keeping `FLAG_HEADERS_SENT` only on the first chunk and terminal flags only on the last.

## Test Matrix

- Rust unit tests in `transport.rs` + parameterized pytest cases in `test_ffi.py` covering embedded bridge delivery, closed-client `ConnectionResetError`, SHM multi-chunk splitting above `max_stream_frame_payload()`, and invalid flags.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib transport && UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync maturin develop --uv && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync pytest src/py/tests/unit/ffi/test_ffi.py -q
```

## Acceptance Criteria

- [x] `submit_stream_frame`, `max_stream_frame_payload`, and `STREAM_FLAG_*` pass `mypy.stubtest vertebra._vertebra`.
- [x] Embedded and SHM stream frame submission pass unit tests with zero regressions.

[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — Existing unary ResponseSubmitter and SHM ring helpers.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Typed Python stub for vertebra._vertebra.
