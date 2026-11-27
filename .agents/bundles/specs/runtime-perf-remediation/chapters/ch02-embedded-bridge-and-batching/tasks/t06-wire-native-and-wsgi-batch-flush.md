---
type: Task
id: "runtime-perf-remediation:ch02:t06"
chapter_id: ch02-embedded-bridge-and-batching
title: Wire drive_wsgi_batch inline flush to submit_batch_flushed
description: Switch the inline WSGI batch flush site in wsgi/pool.rs and wsgi/runner.rs to ResponseSubmitter::submit_batch_flushed while preserving ch01 concurrent WsgiExecutor immediate per-request submission.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch02:t04"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md
  - src/rs/vertebra-py/src/transport.rs
  - src/rs/vertebra-py/src/wsgi/pool.rs
  - src/rs/vertebra-py/src/wsgi/runner.rs
files:
  - src/rs/vertebra-py/src/wsgi/pool.rs
  - src/rs/vertebra-py/src/wsgi/runner.rs
tests:
  - src/rs/vertebra-py/src/wsgi/runner.rs
estimated_loc: 35
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-py-transport
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: PyO3 response batch submission across SHM and BatchBridge
  - id: src-wsgi-runner
    resource: src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch
    title: Native Rust WSGI batch runner
tags:
  - vertebra-py
  - wsgi
  - zero-copy
---

# Wire drive_wsgi_batch inline flush to submit_batch_flushed

## Objective & Scope Boundary

Route the inline batch flush path of `drive_wsgi_batch` (`wsgi/pool.rs` / `wsgi/runner.rs`) through `ResponseSubmitter::submit_batch_flushed` so owned headers and bodies move zero-copy into `BatchBridge` [^src-py-transport] [^src-wsgi-runner].

Coordinate cleanly with `ch01-wsgi-adaptive-pool` (which lands first): modify only the final inline batch flush call (do not alter `WsgiExecutor` stall detection, `WsgiServerConfig`, or concurrent immediate `submit_raw_inner` submission). (`drive_native_batch` shares `flush_and_gather_inline` in `asgi.rs` and is wired in `ch02:t05`.)

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md` (Section 2.2 contracts)
- `src/rs/vertebra-py/src/transport.rs` (`ResponseSubmitter::submit_batch_flushed`)
- `src/rs/vertebra-py/src/wsgi/pool.rs` (inline batch flush site)
- `src/rs/vertebra-py/src/wsgi/runner.rs` (`drive_wsgi_batch`)

## Contracts & Edge Cases

- Replace borrowed slice assembly + `ResponseSubmitter::submit_batch_raw_inner(py, ...)` with `ResponseSubmitter::submit_batch_flushed(py, flushed)` in the WSGI inline batch flush path.
- Leave concurrent `WsgiExecutor` per-request immediate submission (`submit_raw_inner`) untouched so concurrent WSGI responses still flush immediately without waiting for batch tail completion.
- Preserve error responses (`500`), empty-batch handling, and unused-variable cleanliness.

## Test Matrix

| Case | Scenario | Expected Result |
| :--- | :--- | :--- |
| WSGI inline batch flush | Execute `drive_wsgi_batch` (inline mode) on Embedded `BatchBridge` | Batch completes in one callback via `submit_batch_flushed` with exact status, headers, and body |
| WSGI concurrent compatibility | Verify concurrent mode (from `ch01`) still submits immediately | Per-request submission unaffected |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py
```

## Acceptance Criteria

- `drive_wsgi_batch` inline batch path submits owned flushed responses via `ResponseSubmitter::submit_batch_flushed` without disturbing `ch01` pool behavior.
- `cargo test -p vertebra-py` passes and `make lint-rs` is clean.

[^src-py-transport]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 response batch submission across SHM and BatchBridge.
[^src-wsgi-runner]: `src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch` — Native Rust WSGI batch runner.
