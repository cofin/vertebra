---
type: Task
id: "runtime-perf-remediation:ch01:t03"
chapter_id: ch01-wsgi-adaptive-pool
title: Wire drive_wsgi_batch and export shutdown hook
description: Connect drive_wsgi_batch to shared WsgiServerConfig and WsgiExecutor, and register shutdown_wsgi_executor in _vertebra.
status: stable
state: completed
priority: P0
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch01:t02"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
  - src/rs/vertebra-py/src/wsgi/runner.rs
  - src/rs/vertebra-py/src/wsgi/pool.rs
  - src/rs/vertebra-py/src/lib.rs
files:
  - src/rs/vertebra-py/src/wsgi/runner.rs
  - src/rs/vertebra-py/src/lib.rs
tests:
  - src/rs/vertebra-py/src/wsgi/runner.rs
estimated_loc: 110
plan_revision: 2
plan_commit: "86527cf1"
commit: "b110db7d"
sources:
  - id: ch01-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
    title: Chapter 01 adaptive WSGI pool specification
  - id: src-wsgi-runner
    resource: src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch
    title: Native Rust WSGI batch runner
  - id: src-py-lib
    resource: src/rs/vertebra-py/src/lib.rs::pymodule_init
    title: PyO3 _vertebra native module registration
tags:
  - wsgi
  - rust
  - pyo3
---

# Wire drive_wsgi_batch and export shutdown hook

## Objective & Scope Boundary

Wire `drive_wsgi_batch` in `src/rs/vertebra-py/src/wsgi/runner.rs` to allocate one `Arc<WsgiServerConfig>` per batch and dispatch through `global_wsgi_executor()`, and expose `shutdown_wsgi_executor` from `src/rs/vertebra-py/src/lib.rs` [^ch01-spec] [^src-wsgi-runner] [^src-py-lib]. Do not edit Python files in this task.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` (Sections 2.2–2.4)
- `src/rs/vertebra-py/src/wsgi/runner.rs`
- `src/rs/vertebra-py/src/wsgi/pool.rs`
- `src/rs/vertebra-py/src/lib.rs`

## Contracts & Edge Cases

- Extract `pub(crate) fn execute_single_wsgi_item` in `runner.rs` matching `spec.md` Section 2.2 [^ch01-spec]: builds `WsgiEnviron::with_config(snapshot, index, config)` and `WsgiStartResponse::default()`, runs `execute_wsgi_into_raw`, and on `Err(err)` calls `log_app_exception(py, "WSGI", &err)` and returns an HTTP `500` `FlushedProtocolResponse` with `ERROR_500_HEADERS_RAW` and `ERROR_500_BODY`.
- Build `WsgiBatchItems` from `collect_batch_targets(batch, indices)?`: detect contiguous single-snapshot batches (`0..len` sharing `Arc::ptr_eq`) with zero `Vec` allocation; fall back to `WsgiBatchItems::Indexed` for heterogeneous lists or explicit `indices`.
- Allocate one `Arc::new(WsgiServerConfig::new(root_path, server_name, server_port, url_scheme))` per batch and call `global_wsgi_executor().dispatch_batch(py, app, items, config, execute_single_wsgi_item)`.
- Keep `drive_wsgi_batch` Python signature and empty-batch return (`Ok(0)`) unchanged.
- In `src/rs/vertebra-py/src/lib.rs`, re-export `shutdown_wsgi_executor` in `pub use wsgi::{...}` and register `wrap_pyfunction!(wsgi::shutdown_wsgi_executor, m)?` in `pymodule_init`.

## Test Matrix

| Test Case | Assertion |
| :--- | :--- |
| Contiguous vs indexed `WsgiBatchItems` extraction | Single-snapshot contiguous targets yield `WsgiBatchItems::Contiguous`; non-contiguous indices or mixed snapshots yield `WsgiBatchItems::Indexed` preserving exact `(snapshot, idx)` order. |
| Empty batch early exit | `drive_wsgi_batch` returns `Ok(0)` without initializing the executor or submitting responses. |
| Crate compilation and lint | `vertebra-py` builds cleanly and exports `shutdown_wsgi_executor`. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
```

## Acceptance Criteria

- `drive_wsgi_batch` creates one `Arc<WsgiServerConfig>` per batch and delegates to `WsgiExecutor::dispatch_batch`.
- Exceptions in WSGI callables produce HTTP `500` responses identically across inline and concurrent paths.
- `shutdown_wsgi_executor` is registered on `_vertebra` and `cargo test -p vertebra-py wsgi` plus `clippy` pass.

[^ch01-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` — Chapter 01 adaptive WSGI pool specification.
[^src-wsgi-runner]: `src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch` — Native Rust WSGI batch runner.
[^src-py-lib]: `src/rs/vertebra-py/src/lib.rs::pymodule_init` — PyO3 _vertebra native module registration.
