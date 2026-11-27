---
type: Task
id: "runtime-perf-remediation:ch02:t04"
chapter_id: ch02-embedded-bridge-and-batching
title: Zero-copy owned-buffer ResponseSubmitter::submit_batch_flushed in vertebra-py
description: Add ResponseSubmitter::submit_batch_flushed converting owned headers_raw and PreparedRawBody::Owned Vec<u8> buffers via Bytes::from without a second copy on Embedded mode, and eliminate redundant copies in submit_results and submit_response.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch02:t02"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md
  - src/rs/vertebra-py/src/transport.rs
files:
  - src/rs/vertebra-py/src/transport.rs
tests:
  - src/rs/vertebra-py/src/transport.rs
estimated_loc: 130
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-py-transport
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: PyO3 response batch submission across SHM and BatchBridge
tags:
  - vertebra-py
  - zero-copy
  - transport
  - pyo3
---

# Zero-copy owned-buffer ResponseSubmitter::submit_batch_flushed in vertebra-py

## Objective & Scope Boundary

Eliminate the second `Bytes::copy_from_slice` heap copy when protocol runners flush owned response headers and bodies on Embedded mode [^src-py-transport].

Stay strictly inside `src/rs/vertebra-py/src/transport.rs`. Keep `submit_batch_raw_inner` intact for compatibility until callers migrate in `t05` and `t06`.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md` (Section 2.2 contracts)
- `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter`, `submit_batch_raw_inner`, `submit_results`, `submit_response`, and `PreparedRawBody`
- `src/rs/vertebra-py/src/asgi.rs::FlushedProtocolResponse` (verify exact struct definition and visibility)

## Contracts & Edge Cases

Implement `ResponseSubmitter::submit_batch_flushed` as declared in `chapters/ch02-embedded-bridge-and-batching/spec.md`:

- Early-return `Ok(())` when `items.is_empty()`.
- On `SHM_TRANSPORT`: borrow `item.headers_raw.as_slice()` and the body slice of `item.body`, call `send_shm_response_inline_or_detached(...)` per item, and finish with one `flush_shm_response_notification(...)`.
- On `BatchBridge` (Embedded):
  - Convert owned `item.headers_raw: Vec<u8>` via `if item.headers_raw.is_empty() { Bytes::new() } else { Bytes::from(item.headers_raw) }` (zero memcpy).
  - Convert `item.body`: move `PreparedRawBody::Owned(vec)` via `Bytes::from(vec)` (zero memcpy; `Bytes::new()` when empty), and handle any `PyBytes` / borrowed / static variants cleanly without holding invalid borrows across `py.detach`.
  - Track `total_bytes`, submit once via `bridge.submit_responses(bridge_responses)` inline when `total_bytes <= INLINE_SUBMIT_THRESHOLD`, or under `py.detach(...)` when larger.
- Also update `submit_results` and `submit_response` in `transport.rs` to move owned `encode_headers_compact(...)` `Vec<u8>` and cloned `Bytes` body directly into `BridgeResponse` on Embedded mode instead of re-borrowing via `submit_batch_raw_inner` / `submit_raw_inner`.

## Test Matrix

| Case | Scenario | Expected Result |
| :--- | :--- | :--- |
| Zero-copy pointer preservation | Submit `FlushedProtocolResponse` with non-empty owned `headers_raw` and `PreparedRawBody::Owned(body)` | Delivered `BridgeResponse` `headers_raw.as_ptr()` and `body.as_ptr()` equal original `Vec` pointers |
| Empty batch / empty fields | Empty `Vec` and item with empty headers/body | Empty batch is no-op; empty fields produce empty `Bytes` without allocation |
| Large payload detach boundary | Batch exceeding `INLINE_SUBMIT_THRESHOLD` (64 KiB) | Completes via `py.detach` and delivers full payload intact to `BatchResponseSink` |
| Non-owned body variant | Submit `PyBytes` or non-`Owned` `PreparedRawBody` variant | Exact payload delivered cleanly with GIL discipline preserved |
| `submit_results` / `submit_response` parity | Submit `Response` objects on Embedded bridge | Exact status, compact headers, and body delivered without extra copy |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py transport
```

## Acceptance Criteria

- `ResponseSubmitter::submit_batch_flushed` converts owned `headers_raw` and `PreparedRawBody::Owned` via `Bytes::from` with zero extra copy on Embedded mode.
- SHM path in `submit_batch_flushed` preserves Dekker response notification flushing and mid-batch `RingFull` recovery.
- `submit_results` and `submit_response` avoid re-copying owned header/body buffers on Embedded mode.
- `cargo test -p vertebra-py transport` passes.

[^src-py-transport]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 response batch submission across SHM and BatchBridge.
