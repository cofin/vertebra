---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch02-python-stream-submission
title: "Chapter 02: Python-Callable Stream Frame Submission for Both Transports"
description: Expose ResponseSubmitter::submit_stream_frame_inner and the _vertebra.submit_stream_frame function with flag constants, routing to BatchBridge::submit_stream_frame or Transport::send_stream_frame with detached-GIL backpressure and ConnectionResetError on closed embedded streams, and reroute the streaming branch of submit_raw_response through it.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-bridge-stream-frames
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
  - id: transport-py-rs
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: ResponseSubmitter::submit_raw_inner, send_shm_response_inline_or_detached, and submit_raw_response
  - id: vertebra-py-lib-rs
    resource: src/rs/vertebra-py/src/lib.rs::pymodule_init
    title: _vertebra function and constant registration
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: Typed stub for the _vertebra extension
tags:
  - rust
  - pyo3
  - streaming
---

# Chapter 02: Python-Callable Stream Frame Submission for Both Transports

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Protocol code in `vertebra-py` and the Python fallback runner submit responses only through unary paths (`ResponseSubmitter::submit_raw_inner`, `submit_response`, `submit_response_raw`). `submit_raw_response(..., flags=FLAG_STREAMING)` encodes 16-byte `ResponseSlot` frames for its return value but writes each chunk to SHM as a unary `VRES` frame through `send_shm_response_detached`, and has no embedded path at all [^transport-py-rs]. This chapter provides one Rust seam and one Python function that submit a stream frame on whichever transport is initialised.

### 1.2 Non-Goals

- Protocol state machines (`ch03`).
- Disconnect detection for SHM workers: the SHM worker has no back-channel for client closes, so SHM submissions succeed and the reader discards frames for closed streams; ASGI disconnect still arrives through `notify_disconnect` (recorded in `ch10:t04`).

### 1.3 Upstream Baseline & Code Analysis

`send_shm_response_inline_or_detached` sends inline under `INLINE_SUBMIT_THRESHOLD` and on `TransportError::RingFull` detaches the GIL, notifies, and calls `send_response_blocking`; `flush_shm_response_notification` wakes the reader. `SHM_TRANSPORT` and `BatchBridge::get()` select the transport. `ch01` supplies `BatchBridge::submit_stream_frame` and `Transport::{send_stream_frame,send_stream_frame_blocking,max_stream_frame_payload}`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R4** (`t01`): `ResponseSubmitter::submit_stream_frame_inner`, `_vertebra.submit_stream_frame`, `STREAM_FLAG_*` constants, typed stub, and `submit_raw_response` streaming reroute.

### 2.2 Concrete Type & Signature Contracts

```rust
// src/rs/vertebra-py/src/transport.rs
impl ResponseSubmitter {
    pub fn submit_stream_frame_inner(py: Python<'_>, request_id: u64, status: u16, flags: u16,
        headers_raw: &[u8], body: &[u8]) -> PyResult<()>;
}
#[pyfunction]
#[pyo3(name = "submit_stream_frame", signature = (request_id, status, flags, headers, body))]
pub fn submit_stream_frame(py: Python<'_>, request_id: u64, status: u16, flags: u16,
    headers: Option<&Bound<'_, PyAny>>, body: &Bound<'_, PyAny>) -> PyResult<()>;
```

```python
# src/py/vertebra/_vertebra.pyi
STREAM_FLAG_STREAMING: Final[int]
STREAM_FLAG_FINAL: Final[int]
STREAM_FLAG_ERROR: Final[int]
STREAM_FLAG_HEADERS_SENT: Final[int]
STREAM_FLAG_TRAILERS: Final[int]
def submit_stream_frame(
    request_id: int,
    status: int,
    flags: int,
    headers: Sequence[tuple[bytes | str, bytes | str]] | None,
    body: Buffer,
) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `AsgiProtocol::send`, `RsgiTransport::send_bytes` (`ch03`) | `ResponseSubmitter::submit_stream_frame_inner` | SHM: `Transport::send_stream_frame[_blocking]` + `flush_shm_response_notification`; embedded: `py.detach(|| BatchBridge::submit_stream_frame)` |
| Python `AsgiSend` fallback (`ch03:t03`) | `_vertebra.submit_stream_frame` | `submit_stream_frame_inner` |
| `AsgiRunner._emit_trailers`, other `submit_raw_response(flags=FLAG_STREAMING)` callers | `submit_raw_response` | `submit_stream_frame_inner` per chunk |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| No transport initialised | `RuntimeError("transport not initialized; call init_transport() first")`. |
| Embedded, no stream sink | `RuntimeError("response stream sink not registered")`. |
| Embedded, `StreamDelivery::Closed` | `ConnectionResetError("client disconnected (request_id=<id>)")`. |
| SHM body larger than `max_stream_frame_payload` | Split into consecutive frames; only the first keeps `FLAG_HEADERS_SENT` and headers, only the last keeps `FLAG_FINAL`/`FLAG_TRAILERS`/`FLAG_ERROR`. |
| SHM ring full | GIL detached, notify, blocking send. |
| `flags` without `FLAG_STREAMING` | `ValueError("stream frames require STREAM_FLAG_STREAMING")`. |
| Non-contiguous buffer body | Copied once via `with_buffer_slice`. |

### 2.5 Alternatives Considered

Keeping `submit_raw_response` as the streaming entry point was rejected: it returns encoded frames for tests, accepts `str` headers only, and its slot-capacity chunking belongs to the transport. A Python-only implementation was rejected because the Rust protocols need the same seam without crossing back into Python.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch02]`; `submit_stream_frame` is documented in `docs/usage/server-and-workers.md` (`ch10:t01`) and `asgi-and-wsgi-runners.md` (`ch10:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R4 | [t01](tasks/t01-submit-stream-frame-binding.md) | `behavior_tdd` | `src/rs/vertebra-py/src/transport.rs` `mod tests`; `src/py/tests/unit/ffi/test_ffi.py`; `mypy.stubtest vertebra._vertebra` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch02:t01 — Expose submit_stream_frame with detached-GIL backpressure for both transports (tasks/t01-submit-stream-frame-binding.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py --lib transport && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync pytest src/py/tests/unit/ffi/test_ffi.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01` (`0/1`).
- **Downstream Dependents**: `ch03:t01`–`ch03:t03`.

[^transport-py-rs]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — `ResponseSubmitter::submit_raw_inner`, `send_shm_response_inline_or_detached`, and `submit_raw_response`.
[^vertebra-py-lib-rs]: `src/rs/vertebra-py/src/lib.rs::pymodule_init` — `_vertebra` function and constant registration.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Typed stub for the `_vertebra` extension.
