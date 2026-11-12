---
type: Task
id: "driver-conformance-ci:ch03:t04"
chapter_id: ch03-engine-live-suites
title: Honor the Oracle AQ listener wait budget
description: Make run_live_aq_worker use the configured wait_timeout_seconds as the overall idle dequeue wait budget, sliced into 1-second DBMS_AQ.DEQUEUE waits, with a session liveness ping when the budget elapses, and prove the configured value changes behavior.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-oracle/src/notify.rs
  - src/rs/vertebra-oracle/src/client.rs
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/vertebra/_oracle.pyi
  - src/py/tests/integration/db/test_oracle_native.py
files:
  - src/rs/vertebra-oracle/src/notify.rs
  - src/py/vertebra/db/adapters/oracle/notify.py
tests:
  - src/rs/vertebra-oracle/src/notify.rs
estimated_loc: 140
plan_revision: 2
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
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: worker
    resource: src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker
    title: AQ dequeue loop that discards wait_timeout_seconds
  - id: spawn
    resource: src/rs/vertebra-oracle/src/notify.rs::AsyncOracleListener::spawn
    title: Listener thread spawn passing wait_timeout_seconds
  - id: notify-config
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotifyConfig
    title: Python wait_timeout_seconds field, validation, and docstring
  - id: ping
    resource: src/rs/vertebra-oracle/src/client.rs
    title: Existing oracledb Connection::ping usage
tags:
  - oracle
  - notify
  - rust
---

# Task t04: Honor the Oracle AQ Listener Wait Budget

## Objective & Scope Boundary

`run_live_aq_worker` receives `wait_timeout_seconds` and discards it (`let _ = wait_timeout_seconds.max(1);`), always dequeuing with a fixed `wait_sec = 1` in an unbounded loop [^worker]. `OracleNotifyConfig.wait_timeout_seconds` is validated (`>= 1`) and forwarded through `open_listener`, so the setting currently has no effect [^notify-config] [^spawn].

1. In `src/rs/vertebra-oracle/src/notify.rs`, add a private `DequeueWaitBudget` value type that owns the configured budget and the start instant of the current idle cycle and yields per-call `DBMS_AQ.DEQUEUE` wait slices of at most `1` second (whole seconds, never `0`) until the budget elapses.
2. Rewrite the `run_live_aq_worker` loop to bind `wait_sec` from `DequeueWaitBudget::next_slice` on every iteration instead of the fixed `1_i64`. A delivered message resets the budget. When the budget is exhausted with no message, the worker calls `conn.ping()` [^ping]; success resets the budget and continues; failure records the classified `OracleError` through `state.record_fatal_error` and exits the loop (the same path as any non-idle dequeue error). Remove the `let _ = wait_timeout_seconds.max(1);` statement and rewrite the slicing comment block above the loop to describe the budget.
3. In `src/py/vertebra/db/adapters/oracle/notify.py`, rewrite the `OracleNotifyConfig.wait_timeout_seconds` docstring to: "Idle wait budget in seconds before the dedicated worker pings its session; dequeues run in 1-second slices so close() stays responsive."

Out of scope: Python poll multiplexing (`poll_timeout`), reconnect backoff fields, the `.pyi` signature (unchanged: `wait_timeout_seconds: int = 5`), and user docs (`ch05 / t02`).

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/notify.rs` — `run_live_aq_worker`, `AsyncOracleListener::spawn`, `ListenerSharedState`, `is_idle_dequeue_timeout`, `mod tests` [^worker] [^spawn].
- `src/rs/vertebra-oracle/src/client.rs` — existing `conn.ping()` call and its error mapping through `OracleError::from_oracledb` [^ping].
- `src/py/vertebra/db/adapters/oracle/notify.py` — `OracleNotifyConfig` [^notify-config].
- `src/py/vertebra/_oracle.pyi` — `AsyncOracleClient.open_listener` signature.
- `src/py/tests/integration/db/test_oracle_native.py` — live listener usage with `wait_timeout_seconds=30`.

## Symbol & Signature Contracts

```rust
struct DequeueWaitBudget {
    budget: Duration,
    cycle_start: Instant,
}

impl DequeueWaitBudget {
    fn new(wait_timeout_seconds: u32, now: Instant) -> Self;
    fn next_slice(&self, now: Instant) -> Option<i64>;
    fn reset(&mut self, now: Instant);
}
```

- `new` clamps `wait_timeout_seconds` to at least `1` second (the Python config already rejects `< 1`; the clamp protects direct `_oracle` callers).
- `next_slice` returns `Some(1)` while `now - cycle_start < budget` and `None` once the elapsed time reaches the budget.
- `reset` sets `cycle_start = now`.
- The worker passes `Instant::now()` at each call site; tests pass synthetic instants.

## Edge Cases & Error Semantics

- `wait_timeout_seconds = 0` from a direct `_oracle` caller: budget clamps to `1` second.
- Message arrives mid-cycle: deliver the frame, then `reset`.
- `ORA-25228` idle timeout inside a slice: continue the loop; the budget keeps counting.
- Budget exhausted and `ping()` fails: the classified error (for example `ORA-03113`, sqlstate `08006`, transient) is recorded through `record_fatal_error`, so the Python side observes it on the next `next_notification`.
- `close()` during a slice: the slice ends within `1` second and the loop exits on `state.is_closed()`; budget length never delays close.
- `ping()` itself is not cancellable through `OracleBreakHandle`; it runs between dequeues, so close latency stays bounded by one ping round trip.

## Public-Seam Test Blueprint

- **Canonical file**: `src/rs/vertebra-oracle/src/notify.rs` `mod tests` (add `DequeueWaitBudget` to the `use super::{...}` list).

| Case | Concrete Input | Expected Output |
| --- | --- | --- |
| Configured budget of 3 | `new(3, t0)`; `next_slice` at `t0`, `t0+1s`, `t0+2s`, `t0+3s` | `Some(1)`, `Some(1)`, `Some(1)`, `None` |
| Configured budget of 1 | `new(1, t0)`; `next_slice` at `t0`, `t0+1s` | `Some(1)`, `None` |
| Configured value changes behavior | `new(1, t0).next_slice(t0+2s)` vs `new(5, t0).next_slice(t0+2s)` | `None` vs `Some(1)` |
| Zero clamps | `new(0, t0)`; `next_slice` at `t0`, `t0+1s` | `Some(1)`, `None` |
| Reset restarts cycle | `new(2, t0)`; `reset(t0+2s)`; `next_slice(t0+3s)` | `Some(1)` |
| Slice never exceeds one second | `new(30, t0).next_slice(t0+500ms)` | `Some(1)` |

Use `#[test]` functions with an array of `(budget, offsets_ms, expected)` tuples iterated by a single assertion helper; Rust has no parametrize, so one table-driven test per row group is the house pattern in this module.

## Verification

- `cd src/rs && cargo test -p vertebra-oracle --locked notify::tests`
- `cd src/rs && cargo clippy -p vertebra-oracle --all-targets -- -D warnings`
- `! git grep -n "let _ = wait_timeout_seconds" -- src/rs/vertebra-oracle`
- `make oracle-test`
- Live (Docker, Oracle lane): `make db-conformance ENGINE=oracle` keeps the `test_oracle_native.py` listener tests green.

## Acceptance Criteria

- [ ] `run_live_aq_worker` derives every `wait_sec` from `DequeueWaitBudget` and no longer discards `wait_timeout_seconds` (R16).
- [ ] Budget exhaustion pings the session and surfaces ping failures as fatal listener errors (R16).
- [ ] Unit tests prove that different configured values produce different slice sequences (R16).
- [ ] `OracleNotifyConfig.wait_timeout_seconds` docstring describes the budget (R16).

## Notes & Discoveries

- [2026-11-12 01:30] The fixed 1-second slice exists because cancelling `SYS.DBMS_AQ.DEQUEUE` through a break triggers an upstream `todo!()` in `oracledb` `26.0.0-beta.5`; the budget keeps that slicing and adds no break-based cancellation.
- [2026-11-12 01:30] Removing the parameter instead was rejected: the field is public on `OracleNotifyConfig` and the `.pyi`, and an idle liveness ping gives it a concrete purpose.

[^worker]: `src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker` — AQ dequeue loop that discards `wait_timeout_seconds`.
[^spawn]: `src/rs/vertebra-oracle/src/notify.rs::AsyncOracleListener::spawn` — Listener thread spawn.
[^notify-config]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotifyConfig` — Python field, validation, and docstring.
[^ping]: `src/rs/vertebra-oracle/src/client.rs` — Existing `oracledb` `Connection::ping` usage.
