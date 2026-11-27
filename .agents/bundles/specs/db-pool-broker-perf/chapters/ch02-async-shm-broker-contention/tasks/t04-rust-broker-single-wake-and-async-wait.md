---
type: Task
id: t04
chapter_id: ch02-async-shm-broker-contention
title: Add single-waiter SHM futex wake and LeaseBroker.wait_for_wake_async
description: Replace wake_all thundering herds on single-slot SHM release/dispose with wake_one_waiter_seqcst, expose ShmLeaseTable::{wake_seq,partition_count,wait_for_wake}, and bridge LeaseBroker::{wake_seq,partition_count,wait_for_wake_async} with stub updates.
status: stable
state: pending
priority: high
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/rs/vertebra-py/src/async_bridge.rs
  - src/py/vertebra/_vertebra.pyi
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
estimated_loc: 140
plan_revision: 2
plan_commit: "16ccb56e"
commit: null
sources:
  - id: src-core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: ShmLeaseTable wake_waiters_seqcst and acquire_internal
  - id: src-py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: PyO3 LeaseBroker bindings
  - id: src-vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi::LeaseBroker
    title: Native extension type stub for LeaseBroker
tags:
  - broker
  - shm
  - rust
  - pyo3
---

# t04: Add single-waiter SHM futex wake and LeaseBroker.wait_for_wake_async

## Objective & Scope Boundary

Eliminate kernel futex thundering herds on single-slot releases and provide an awaitable cross-process SHM wake primitive for `AsyncSHMBrokerTransport` without altering `ShmLeaseHeader` or `ShmPartitionEntry` ABI sizes.

In scope:
- `src/rs/vertebra-core/src/broker.rs`:
  - Add `wake_one_waiter_seqcst(&self)`: increment `hdr.wake_seq`, check `hdr.waiters.0.load(Ordering::SeqCst) > 0`, call `hdr.wake_seq.0.wake_one()` if `hdr.partition_count.0.load(Ordering::Acquire) <= 1` else `hdr.wake_seq.0.wake_all()`.
  - Call `wake_one_waiter_seqcst()` in `ShmLeaseTable::release()` and `ShmLeaseTable::dispose_quarantined()`; keep `wake_waiters_seqcst()` (`wake_all()`) for `reap_expired()` and `reclaim_worker()`.
  - Add `pub fn wake_seq(&self) -> u32`, `pub fn partition_count(&self) -> u32`, and `pub fn wait_for_wake(&self, expected_seq: u32, timeout: Duration) -> u32` using `WaitGuard` on `hdr.waiters.0`, checking `hdr.wake_seq.0.load_seqcst() != expected_seq` after registering waiter, sleeping via `hdr.wake_seq.0.wait_timeout(expected_seq, timeout)`, and returning the current `wake_seq`.
- `src/rs/vertebra-py/src/broker.rs`:
  - Add `pub fn wake_seq(&self) -> u32`, `pub fn partition_count(&self) -> u32`, and `pub fn wait_for_wake_async(&self, py: Python<'_>, expected_seq: u32, timeout_ms: u64) -> PyResult<crate::async_bridge::RustDbTaskAwaitable>` using `spawn_rust_db_task` + `tokio::task::spawn_blocking`.
- `src/py/vertebra/_vertebra.pyi`: add `wake_seq(self) -> int`, `partition_count(self) -> int`, and `wait_for_wake_async(self, expected_seq: int, timeout_ms: int = 25) -> Awaitable[int]` on `LeaseBroker`.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` (lines 250-305, 1270-1415, 1515-1620, 1720-1750, 1930-1955) [^src-core-broker]
- `src/rs/vertebra-py/src/broker.rs` (lines 865-1010) [^src-py-broker]
- `src/py/vertebra/_vertebra.pyi` (lines 950-1060) [^src-vertebra-pyi]

## Contracts & Edge Cases

- Implement exact Rust and `.pyi` signatures from `../../spec.md` Section 2.2.
- Dekker ordering invariant in `wait_for_wake`: `hdr.waiters.0.fetch_add(1, Ordering::SeqCst)` MUST precede `hdr.wake_seq.0.load_seqcst()`, matching `wake_one_waiter_seqcst`'s `wake_seq.fetch_add_seqcst(1)` before `waiters.load(Ordering::SeqCst)` so no wakeup is lost.
- Zero-duration `timeout` returns `hdr.wake_seq.0.load_seqcst()` immediately without blocking.
- Compile-time `size_of::<ShmLeaseHeader>()` and `size_of::<ShmPartitionEntry>()` assertions remain untouched.

## Test Matrix

- Rust test: `wait_for_wake` returns immediately when `wake_seq` already differs from `expected_seq`.
- Rust test: thread blocked in `wait_for_wake` wakes promptly when another thread calls `release()` (unpartitioned and 1-partition `wake_one`, and 2-partition `wake_all`).
- Rust test: `wait_for_wake` decrements `waiters` back to `0` on both wake and timeout.

## Verification

```bash
make dev-ext EXT=core && cargo test -p vertebra-core broker && cargo test -p vertebra-py broker && uv run --no-sync python -m mypy.stubtest vertebra._vertebra
```

## Acceptance Criteria

- All `vertebra-core` and `vertebra-py` broker tests pass.
- `mypy.stubtest vertebra._vertebra` exits `0`.
- `cargo clippy -p vertebra-core -p vertebra-py -- -D warnings` passes cleanly.

[^src-core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — ShmLeaseTable wake_waiters_seqcst and acquire_internal.
[^src-py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker bindings.
[^src-vertebra-pyi]: `src/py/vertebra/_vertebra.pyi::LeaseBroker` — Native extension type stub for LeaseBroker.
