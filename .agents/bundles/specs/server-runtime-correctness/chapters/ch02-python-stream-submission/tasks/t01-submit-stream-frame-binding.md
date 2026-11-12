---
type: Task
id: "server-runtime-correctness:ch02:t01"
chapter_id: ch02-python-stream-submission
title: Expose submit_stream_frame with detached-GIL backpressure for both transports
description: Add ResponseSubmitter::submit_stream_frame_inner and the _vertebra.submit_stream_frame function with STREAM_FLAG_* constants, route frames to the SHM rings or the in-process bridge with detached-GIL blocking and ConnectionResetError on closed embedded streams, reroute the streaming branch of submit_raw_response, and update the typed stub.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch01:t03"
  - "server-runtime-correctness:ch01:t04"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch02-python-stream-submission/spec.md
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/rs/vertebra-py/src/asgi.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/ffi/test_ffi.py
files:
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-py/src/transport.rs
  - src/py/tests/unit/ffi/test_ffi.py
estimated_loc: 230
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
  - id: transport-py-rs
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: Unary submitter, SHM inline-or-detached sender, notification flush, and submit_raw_response
  - id: asgi-headers-rs
    resource: src/rs/vertebra-py/src/asgi.rs::encode_py_headers_into
    title: Python header sequence to compact raw header encoder
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: Typed stub checked by mypy.stubtest
tags:
  - rust
  - pyo3
  - streaming
---

# Task t01: Expose submit_stream_frame with Detached-GIL Backpressure for Both Transports

## Objective & Scope Boundary

Add one Rust seam (`ResponseSubmitter::submit_stream_frame_inner`) and one Python function (`_vertebra.submit_stream_frame`) that submit a stream frame on the initialised transport, export `STREAM_FLAG_*` constants, reroute `submit_raw_response`'s streaming branch through the seam, and update `src/py/vertebra/_vertebra.pyi`. Out of scope: protocol state machines (`ch03`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-py/src/transport.rs::{ResponseSubmitter::submit_raw_inner,send_shm_response_inline_or_detached,flush_shm_response_notification,map_shm_submit_err,with_buffer_slice,submit_raw_response,SHM_TRANSPORT}` and `mod tests::shm_submit_notifies_and_recovers_from_mid_batch_ring_full` [^transport-py-rs].
- `src/rs/vertebra-py/src/lib.rs` — `m.add_function(wrap_pyfunction!(transport::submit_raw_response, m)?)?` registration block.
- `src/rs/vertebra-py/src/asgi.rs::encode_py_headers_into` (`pub(crate)`) [^asgi-headers-rs].
- `src/py/vertebra/_vertebra.pyi::{submit_raw_response,submit_response_raw}` [^vertebra-pyi].
- `src/py/tests/unit/ffi/test_ffi.py` — FFI test conventions.

## Symbol & Signature Contracts

- `src/rs/vertebra-py/src/transport.rs`:
  - `pub fn submit_stream_frame_inner(py: Python<'_>, request_id: u64, status: u16, flags: u16, headers_raw: &[u8], body: &[u8]) -> PyResult<()>`:
    - `flags & FLAG_STREAMING == 0` → `PyValueError("stream frames require STREAM_FLAG_STREAMING")`.
    - SHM (`SHM_TRANSPORT.get()`): split `body` into chunks of `transport.max_stream_frame_payload().expect(..) - headers_raw.len()` for the first chunk and `max_stream_frame_payload()` after; first chunk keeps `FLAG_HEADERS_SENT` and `headers_raw`, last chunk keeps terminal flags; each chunk tries `send_stream_frame`, on `TransportError::RingFull` detaches the GIL, calls `notify_response`, then `send_stream_frame_blocking`; finishes with `flush_shm_response_notification`; errors map through `map_shm_submit_err`.
    - Embedded (`BatchBridge::get()`): build `ResponseStreamFrame` with `Bytes::copy_from_slice`, call `py.detach(|| bridge.submit_stream_frame(frame))`; `Ok(Accepted)` → `Ok(())`; `Ok(Closed)` → `PyConnectionResetError(format!("client disconnected (request_id={request_id})"))`; `Err(TransportError::Disconnected)` → `PyRuntimeError("response stream sink not registered")`.
    - Neither → `PyRuntimeError("transport not initialized; call init_transport() first")`.
  - `#[pyfunction] #[pyo3(name = "submit_stream_frame", signature = (request_id, status, flags, headers, body))] pub fn submit_stream_frame(py, request_id: u64, status: u16, flags: u16, headers: Option<&Bound<'_, PyAny>>, body: &Bound<'_, PyAny>) -> PyResult<()>` encodes headers with `encode_py_headers_into` and the body with `with_buffer_slice`.
  - `submit_raw_response`: when `flags & FLAG_STREAMING != 0`, compute the same encoded `ResponseSlot` frames for the return value but submit each chunk through `submit_stream_frame_inner` (headers encoded with `encode_headers_compact`) instead of `send_shm_response_detached`; the unary branch is unchanged.
- `src/rs/vertebra-py/src/lib.rs`: register `transport::submit_stream_frame` and add module constants `STREAM_FLAG_STREAMING`, `STREAM_FLAG_FINAL`, `STREAM_FLAG_ERROR`, `STREAM_FLAG_HEADERS_SENT`, `STREAM_FLAG_TRAILERS` from `vertebra_core::ipc`.
- `src/py/vertebra/_vertebra.pyi`: add the five `Final[int]` constants and `def submit_stream_frame(request_id: int, status: int, flags: int, headers: Sequence[tuple[bytes | str, bytes | str]] | None, body: Buffer) -> None: ...`.

## Edge Cases & Error Semantics

- Empty body with `FLAG_HEADERS_SENT` sends exactly one frame carrying headers.
- SHM chunking never produces an empty middle frame; a terminal frame with an empty body is still sent when the body length is an exact multiple of the chunk size.
- `headers` longer than `u16::MAX` bytes encoded → `PyValueError("response headers exceed 65535 bytes")` before any frame is sent.
- `ConnectionResetError` is a subclass of `OSError`, satisfying the ASGI rule that `send` on a closed connection raises an `OSError` subclass.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-py/src/transport.rs` (`mod tests`, `pyo3::Python::attach`, SHM `RingPair` reader thread and a local `BatchBridge`-style sink harness), `src/py/tests/unit/ffi/test_ffi.py` (Python surface); dependency category `unit`/`integration`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| SHM chunking | Transport over `RingPair` with xlarge slot `4096`; `submit_stream_frame_inner(py, 21, 200, FLAG_STREAMING\|FLAG_HEADERS_SENT\|FLAG_FINAL, hdrs, &[b'x'; 10_000])` | Reader drains `3` `VRSF` frames for id `21`: first has `FLAG_HEADERS_SENT` and headers, last has `FLAG_FINAL`, bodies concatenate to `10_000` `b'x'` |
| SHM ring full | `small.slot_count = 2`, reader drains slowly | All frames delivered in order; no error |
| Missing streaming flag | `flags = FLAG_FINAL` | `ValueError("stream frames require STREAM_FLAG_STREAMING")` |
| Raw response reroute | `submit_raw_response(5, 200, [("x-a","1")], b"abc", FLAG_STREAMING)` on SHM | Returns encoded `ResponseSlot` frames as today; reader drains `VRSF` frames (not `VRES`) for id `5` |
| Python constants | `from vertebra._vertebra import STREAM_FLAG_STREAMING, STREAM_FLAG_FINAL` | `(STREAM_FLAG_STREAMING, STREAM_FLAG_FINAL) == (1, 2)` |
| Python no sink | `init_transport()` (in-process, no HTTP dispatcher) then `submit_stream_frame(1, 200, STREAM_FLAG_STREAMING \| STREAM_FLAG_HEADERS_SENT, [(b"content-type", b"text/plain")], b"a")` | `RuntimeError` matching `"response stream sink not registered"` |
| Python bad flags | `submit_stream_frame(1, 200, 0, None, b"")` | `ValueError` |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib transport`
- Final Green + lint:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib transport
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._vertebra
  uv run --no-sync pytest src/py/tests/unit/ffi/test_ffi.py -q
  ```
- Planned local commit: `feat(py): add submit_stream_frame for embedded and SHM transports`

## Acceptance Criteria

- [ ] `submit_stream_frame_inner` and `_vertebra.submit_stream_frame` route frames on both transports with the stated chunking, backpressure, and error mapping (R4).
- [ ] `submit_raw_response` streaming chunks reach the rings as `VRSF` frames (R3, R4).
- [ ] Stub and constants pass `mypy.stubtest vertebra._vertebra` (R4).

## Notes & Discoveries

[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — Unary submitter, SHM inline-or-detached sender, notification flush, and `submit_raw_response`.
[^asgi-headers-rs]: `src/rs/vertebra-py/src/asgi.rs::encode_py_headers_into` — Python header sequence to compact raw header encoder.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Typed stub checked by `mypy.stubtest`.
