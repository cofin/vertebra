---
type: Task
id: "runtime-perf-remediation:ch03:t02"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Zero-Arc AdmissionPermit, fence-free Dekker drop, and coarse circuit clock
description: Borrow BackpressureController inside AdmissionPermit, replace AcqRel+mfence with SeqCst RMW+load, and amortize clock_gettime on closed-circuit record_success.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/rs/vertebra-http/src/backpressure.rs
files:
  - src/rs/vertebra-http/src/backpressure.rs
tests:
  - src/rs/vertebra-http/src/backpressure.rs
estimated_loc: 120
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-http-backpressure
    resource: src/rs/vertebra-http/src/backpressure.rs::BackpressureController
    title: AdmissionPermit, BackpressureController, and sliding-window circuit breaker
tags:
  - backpressure
  - atomics
  - performance
---

# Zero-Arc AdmissionPermit, Fence-Free Dekker Drop, and Coarse Circuit Clock

## Objective & Scope Boundary

Remove per-request `Arc::clone`/`Arc::drop`, redundant `mfence`, and per-request `clock_gettime` overhead from `BackpressureController` while preserving Dekker waiter wakeups and deterministic circuit-breaker transitions [^src-http-backpressure].

Out of scope: `response.rs` dispatch flow, `StreamGuard` ownership, and `BackpressureConfig` schema.

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source file: `src/rs/vertebra-http/src/backpressure.rs`

## Contracts & Edge Cases

Follow `AdmissionPermit<'a>` and `BackpressureController::{try_acquire, try_acquire_for_path}` contracts in `spec.md` Section 2.2:

- **Borrowed `AdmissionPermit<'a>`**: Store `controller: &'a BackpressureController` and `is_priority: bool` directly. Unwrap `in_flight`, `priority_in_flight`, and `wake_state` inside `BackpressureController` from `Arc<...>` to inline fields (`CacheLine<AtomicUsize>` and `QueueWakeState`), eliminating all per-request refcount atomics on acquire and drop.
- **Fence-free Dekker release**:
  - In `Drop for AdmissionPermit<'_>`: for priority permits, run `priority_in_flight.0.fetch_sub(1, Ordering::Release)`. For unary permits, run `in_flight.0.fetch_sub(1, Ordering::SeqCst)` followed by `if wake_state.queue_depth.0.load(Ordering::SeqCst) > 0 { wake_state.slot_notify.notify_one(); }`. Remove the standalone `std::sync::atomic::fence(Ordering::SeqCst)` from `Drop`.
  - On the queued waiter path in `try_acquire`, use `Ordering::SeqCst` when incrementing `wake_state.queue_depth` alongside the existing `notified.enable()` + `fence(Ordering::SeqCst)` re-check so zero waiters can miss a wakeup.
- **Amortized circuit clock**:
  - Add `cached_elapsed_ms: AtomicU64` (initialized to `0`, storing raw `clock_origin.elapsed().as_millis() as u64` without `clock_offset_ms`) and a thread-local `CLOCK_SAMPLE_TICK: Cell<u32>` counter.
  - Add `#[inline] fn coarse_elapsed_ms(&self) -> u64`: refresh `cached_elapsed_ms` from `clock_origin.elapsed()` every 64 calls per thread (`(tick & 63) == 0`) or when `cached == 0`, then `saturating_add(self.clock_offset_ms.load(Ordering::Acquire))`.
  - In `record_success(&self)`: load `circuit_word` first; when `word == 0` (closed fast path), call `self.record_bucket_sample(self.coarse_elapsed_ms(), false)` and return immediately. When `word != 0` (`Open` or `HalfOpen`), read exact `self.elapsed_ms()` so state transitions and `advance_circuit_clock_for_test` remain exact.
  - Keep exact `self.elapsed_ms()` in `record_failure()`, `check_circuit_state()`, and `try_admit_circuit()`.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| Existing admission, saturation, queue timeout, and circuit-breaker tests | All tests in `backpressure::tests` pass unchanged. |
| Concurrent waiter wakeup stress (Dekker check) | Saturate `max_in_flight = 1` with multiple queued waiters across Tokio tasks, drop permits rapidly, and assert every waiter acquires without timing out. |
| Coarse clock + `advance_circuit_clock_for_test` | Calling `record_success()` > 128 times updates buckets across `advance_circuit_clock_for_test` and still trips/recovers the circuit accurately on failures and half-open probes. |
| Priority route bypass | Priority routes acquire `is_priority = true` permits under unary saturation without waking unary waiters or mutating `in_flight`. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http backpressure::tests
```

## Acceptance Criteria

- `AdmissionPermit<'a>` holds zero `Arc` fields and `Drop` contains zero standalone `fence(Ordering::SeqCst)`.
- Closed-circuit `record_success()` calls `clock_origin.elapsed()` at most once per 64 calls per thread.
- Concurrent queued-waiter stress test passes with zero missed wakeups or timeouts.
- Full `cargo test -p vertebra-http` compiles cleanly with `AdmissionPermit<'_>`.

[^src-http-backpressure]: `src/rs/vertebra-http/src/backpressure.rs::BackpressureController` — AdmissionPermit, BackpressureController, and sliding-window circuit breaker.
