---
type: Task
id: "runtime-perf-remediation:ch01:t02"
chapter_id: ch01-wsgi-adaptive-pool
title: Implement native Rust WsgiExecutor pool
description: Build WsgiExecutor with adaptive inline execution, zero-idle stall watchdog, 2s hysteresis, OS thread pool, and GIL-safe shutdown.
status: stable
state: completed
priority: P0
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch01:t01"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
  - src/rs/vertebra-py/src/wsgi/mod.rs
  - src/rs/vertebra-py/src/wsgi/environ.rs
  - src/rs/vertebra-py/src/transport.rs
files:
  - src/rs/vertebra-py/src/wsgi/pool.rs
  - src/rs/vertebra-py/src/wsgi/mod.rs
tests:
  - src/rs/vertebra-py/src/wsgi/pool.rs
estimated_loc: 245
plan_revision: 2
plan_commit: "86527cf1"
commit: "b110db7d"
sources:
  - id: ch01-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md
    title: Chapter 01 adaptive WSGI pool specification
  - id: src-wsgi-mod
    resource: src/rs/vertebra-py/src/wsgi/mod.rs
    title: WSGI Rust module exports
tags:
  - wsgi
  - rust
  - concurrency
  - pyo3
---

# Implement native Rust WsgiExecutor pool

## Objective & Scope Boundary

Create `src/rs/vertebra-py/src/wsgi/pool.rs` implementing `WsgiExecutionMode`, `WsgiPoolConfig`, `WsgiBatchItems`, `WsgiExecutor`, `global_wsgi_executor`, and `shutdown_wsgi_executor`, and export them plus `WsgiServerConfig` from `src/rs/vertebra-py/src/wsgi/mod.rs` [^ch01-spec] [^src-wsgi-mod]. Do not modify `runner.rs`, `lib.rs`, or Python files in this task.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` (Sections 2.1–2.4)
- `src/rs/vertebra-py/src/wsgi/mod.rs`
- `src/rs/vertebra-py/src/wsgi/environ.rs`
- `src/rs/vertebra-py/src/transport.rs` (verify exact `ResponseSubmitter::submit_raw_inner` and `submit_batch_raw_inner` signatures)

## Contracts & Edge Cases

- Implement `WsgiExecutionMode`, `WsgiPoolConfig`, `WsgiBatchItems`, `WsgiItemExecutor`, `WsgiExecutor`, `global_wsgi_executor`, and `shutdown_wsgi_executor` matching `spec.md` Section 2.2 [^ch01-spec].
- Environment parsing (`WsgiPoolConfig::from_env`):
  - `VERTEBRA_WSGI_MODE`: `"auto"`, `"concurrent"`, `"inline"` (case-insensitive); default `Auto` on GIL builds and `Concurrent` under `#[cfg(Py_GIL_DISABLED)]`.
  - `VERTEBRA_WSGI_THREADS`: positive `usize` clamped to `1..=512`; default `available_parallelism().map_or(4, |n| n.get()).saturating_mul(4).clamp(32, 128)`.
  - `VERTEBRA_WSGI_STALL_US`: positive `u64`; default `250`. Hysteresis fixed at `2_000_000` µs (`2 s`).
- Inline fastpath (`Inline` mode, or `Auto` outside hysteresis): execute items inline and flush via `ResponseSubmitter::submit_batch_raw_inner`.
- Adaptive handoff (`Auto` mode with `total > 1`): follow `spec.md` Section 2.3 using `AtomicUsize::fetch_add(1, AcqRel)` on the caller and `AtomicBool::swap(true, AcqRel)` + `AtomicUsize::swap(total, AcqRel)` in the watchdog so every index `0..total` executes strictly once without acquiring the GIL on the watchdog thread. Wrap `Py<PyAny>` in `GilSafePyObject` (whose `Drop` calls `Python::attach` if ever dropped off-GIL) and unpublish the active handoff before dropping the caller's on-GIL `Arc`, guaranteeing a non-GIL thread (such as the watchdog) can never decrement a `Py<PyAny>` refcount off-GIL.
- Pool workers: spawn lazily on first concurrent enqueue (never spawned on pure inline traffic), acquire GIL per task via `Python::attach`, call `execute_item`, submit immediately via `ResponseSubmitter::submit_raw_inner`, and drop `FlushedProtocolResponse` and task handles inside `Python::attach` before releasing the GIL.
- Watchdog thread: spawn lazily on first `Auto` batch with `total > 1`; park indefinitely on `Condvar::wait` when idle (`0%` idle CPU) after a `2 ms` active grace window.
- Global registry & shutdown: store `RwLock<Option<Arc<WsgiExecutor>>>`; `shutdown_wsgi_executor(py)` removes the `Option`, releases the lock, and runs `py.detach(|| exec.shutdown())` to stop the watchdog, drain all queued tasks, and join every thread.
- Structure pure-Rust scheduling/handoff logic (`WsgiPoolConfig` parser, atomic handoff state machine, and generic/testable worker queue) so `cargo test` validates stall handoff and shutdown without needing an embedded Python interpreter.

## Test Matrix

| Test Case | Assertion |
| :--- | :--- |
| Config parsing & defaults | `auto`/`concurrent`/`inline` (mixed case), invalid fallback, `Py_GIL_DISABLED` default, thread clamping (`32..=128` default, `1..=512` override), and stall threshold fallback. |
| Atomic handoff race freedom | Concurrent inline loop + watchdog handoff across 1,000 iterations executes every index `0..N` exactly once with zero duplicates and zero drops. |
| Watchdog stall trigger & hysteresis | Blocking item `0` (`15 ms`) triggers handoff after `250 µs`; remaining items `1..16` finish in parallel (`< 80 ms`) and hysteresis marks subsequent batches concurrent. |
| Clean shutdown & zero-task drain | Calling `shutdown()` while tasks are queued drains every task and joins all threads without hanging; shutting down an unused executor is an instant no-op. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi::pool
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
```

## Acceptance Criteria

- `WsgiExecutor` and `shutdown_wsgi_executor` compile cleanly on both GIL and `Py_GIL_DISABLED` targets (`#[cfg(Py_GIL_DISABLED)]` branch checked).
- Pure-Rust unit tests in `pool.rs` verify config parsing, stall handoff, hysteresis, and clean shutdown.
- `cargo clippy -p vertebra-py --all-targets -- -D warnings` passes with zero warnings.

[^ch01-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch01-wsgi-adaptive-pool/spec.md` — Chapter 01 adaptive WSGI pool specification.
[^src-wsgi-mod]: `src/rs/vertebra-py/src/wsgi/mod.rs` — WSGI Rust module exports.
