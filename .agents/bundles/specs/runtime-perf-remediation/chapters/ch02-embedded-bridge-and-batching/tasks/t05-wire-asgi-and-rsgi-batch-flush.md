---
type: Task
id: "runtime-perf-remediation:ch02:t05"
chapter_id: ch02-embedded-bridge-and-batching
title: Wire drive_asgi_batch, drive_native_batch, and drive_rsgi_batch to submit_batch_flushed
description: Switch shared flush_and_gather_inline (driving drive_asgi_batch and drive_native_batch in asgi.rs) and drive_rsgi_batch (rsgi/http.rs) to ResponseSubmitter::submit_batch_flushed while leaving scope and protocol struct layouts untouched for ch03.
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch02:t04"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/rsgi/http.rs
files:
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/rsgi/http.rs
tests:
  - src/rs/vertebra-py/src/asgi.rs
  - src/rs/vertebra-py/src/rsgi/http.rs
estimated_loc: 45
plan_revision: 2
plan_commit: "86527cf1"
commit: "3024aee2"
sources:
  - id: src-py-transport
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: PyO3 response batch submission across SHM and BatchBridge
  - id: src-py-asgi
    resource: src/rs/vertebra-py/src/asgi.rs::drive_asgi_batch
    title: Native Rust ASGI 3.0 batch driver
  - id: src-py-rsgi
    resource: src/rs/vertebra-py/src/rsgi/http.rs::drive_rsgi_batch
    title: Native Rust RSGI 1.6 batch driver
tags:
  - vertebra-py
  - asgi
  - rsgi
  - zero-copy
---

# Wire drive_asgi_batch, drive_native_batch, and drive_rsgi_batch to submit_batch_flushed

## Objective & Scope Boundary

Route `flush_and_gather_inline` (shared by `drive_asgi_batch` and `drive_native_batch` in `asgi.rs`) and `drive_rsgi_batch` (`rsgi/http.rs`) through `ResponseSubmitter::submit_batch_flushed` so flushed ASGI, native async, and RSGI responses transfer owned buffers into `BatchBridge` without a second copy [^src-py-transport] [^src-py-asgi] [^src-py-rsgi].

Touch only the batch response flush call sites in `asgi.rs` (`flush_and_gather_inline`) and `rsgi/http.rs`. Do not alter `AsgiScope`, `RsgiScope`, or `RsgiHttpProtocol` fields (owned by `ch03-ingress-and-worker-hotpaths`).

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md` (Section 2.2 contracts)
- `src/rs/vertebra-py/src/transport.rs` (`ResponseSubmitter::submit_batch_flushed`)
- `src/rs/vertebra-py/src/asgi.rs` (`drive_asgi_batch` flush site)
- `src/rs/vertebra-py/src/rsgi/http.rs` (`drive_rsgi_batch` flush site)

## Contracts & Edge Cases

- Replace borrowed slice assembly + `ResponseSubmitter::submit_batch_raw_inner(py, ...)` in `flush_and_gather_inline` (driving `drive_asgi_batch` and `drive_native_batch` in `asgi.rs`) and `drive_rsgi_batch` (`rsgi/http.rs`) with `ResponseSubmitter::submit_batch_flushed(py, flushed)`.
- Preserve error propagation, streaming bypasses (already-streamed requests skipped from unary batch flush), and empty-batch short-circuit behavior.
- Remove any temporary slice-borrow `Vec` that becomes unused after switching to `submit_batch_flushed`.

## Test Matrix

| Case | Scenario | Expected Result |
| :--- | :--- | :--- |
| ASGI batch flush | Execute `drive_asgi_batch` on Embedded `BatchBridge` with `BatchResponseSink` | All non-streaming responses arrive in one batch callback with exact status, headers, and body |
| Native batch flush | Execute `drive_native_batch` on Embedded `BatchBridge` with `BatchResponseSink` | Batch completes in one callback with exact status, headers, and body |
| RSGI batch flush | Execute `drive_rsgi_batch` on Embedded `BatchBridge` with `BatchResponseSink` | All unary responses arrive in one batch callback with exact status, headers, and body |
| Error / exception item in batch | Handler raises exception mid-batch | `500` error response flushed cleanly without dropping sibling responses |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py
```

## Acceptance Criteria

- `drive_asgi_batch` and `drive_native_batch` (via `flush_and_gather_inline`) submit owned flushed responses via `ResponseSubmitter::submit_batch_flushed`.
- `drive_rsgi_batch` submits owned flushed responses via `ResponseSubmitter::submit_batch_flushed`.
- No `AsgiScope` or `RsgiHttpProtocol` struct layouts are modified.
- `cargo test -p vertebra-py` passes with zero Clippy warnings.

[^src-py-transport]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 response batch submission across SHM and BatchBridge.
[^src-py-asgi]: `src/rs/vertebra-py/src/asgi.rs::drive_asgi_batch` — Native Rust ASGI 3.0 batch driver.
[^src-py-rsgi]: `src/rs/vertebra-py/src/rsgi/http.rs::drive_rsgi_batch` — Native Rust RSGI 1.6 batch driver.
