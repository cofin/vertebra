---
type: Task
id: "native-sqlite-driver:ch03:t03"
flow_id: native-sqlite-driver
chapter_id: ch03-worker-pool-arrow-and-pyo3-clients
title: "PyO3 SyncSQLiteClient, AsyncSQLiteClient, and SQLite BrokerEngineCapsule"
description: "Expose SyncSQLiteClient, AsyncSQLiteClient, and broker_engine_capsule() (tag 3) in vertebra._sqlite and sync _sqlite.pyi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch03:t02"
files:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/rs/vertebra-sqlite/src/lib.rs
  - src/py/vertebra/_sqlite.pyi
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
read_set:
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_sqlite.pyi
estimated_loc: 245
tags:
  - sqlite
  - pyo3
  - capsule
  - async
sources:
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "AsyncCompletionQueue and Notifier PyO3 bridge"
  - id: duckdb-pyi
    resource: src/py/vertebra/_duckdb.pyi
    title: "Standalone extension stub precedent"
---

# Task `t03`: PyO3 SyncSQLiteClient, AsyncSQLiteClient, and SQLite BrokerEngineCapsule

## Objective & Scope Boundary

Implement `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`) in `vertebra-sqlite` and synchronize `src/py/vertebra/_sqlite.pyi` per [`../spec.md` §2](../spec.md) [^py-async] [^duckdb-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/py_async.rs` — `SyncCompletionQueue`, `AsyncCompletionQueue`, and `Notifier`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_sqlite.pyi` — Type stubs for `vertebra._sqlite`.

## Contracts & Edge Cases

- Release the GIL (`py.detach`) on all `SyncSQLiteClient` waits and drive `AsyncSQLiteClient` completions via `AsyncCompletionQueue` + `Notifier` + `cancel_operation(op_id)`.
- Export `broker_engine_capsule()` implementing `BrokerPoolBackend` (`DIALECT_TAG_SQLITE = 3`) from `broker-pool-sharing`.
- **Edge Case**: Dropping or closing a client shuts down worker threads deterministically without leaking `Notifier` file descriptors.

## Test Matrix

- `mypy.stubtest vertebra._sqlite` verifies 100% signature parity across `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule`.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._sqlite --concise && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite
```

## Acceptance Criteria

- [ ] `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` pass `stubtest` and Rust client tests.
- [ ] Zero regressions in the chapter fast verification gate.

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^duckdb-pyi]: `src/py/vertebra/_duckdb.pyi` — Standalone extension stub precedent.
