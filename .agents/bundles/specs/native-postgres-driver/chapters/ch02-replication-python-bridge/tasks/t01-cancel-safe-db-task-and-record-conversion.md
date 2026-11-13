---
type: Task
id: "native-postgres-driver:ch02-replication-python-bridge:t01"
flow_id: native-postgres-driver
chapter_id: ch02-replication-python-bridge
title: "Add spawn_rust_db_task_with_attr_and_future and lossless Python record conversion"
description: "Extend vertebra-db py_async for cancel-safe lease restoration and convert SupervisedDelivery records to PostgresReplicationRecord on the event loop."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch01:t02"
files: 
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests: 
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
read_set:
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/notifications.rs
  - src/py/vertebra/db/adapters/postgres/types.py
estimated_loc: 195
tags:
  - rust
  - pyo3
  - postgres
sources:
  - id: py-async
    resource: "src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr"
    title: "AsyncCompletionQueue drainer"
---

# Task `t01`: Add spawn_rust_db_task_with_attr_and_future and lossless Python record conversion

## Objective & Scope Boundary

Implement cancel-safe `spawn_rust_db_task_with_attr_and_future` (`-> PyResult<RustDbTaskAwaitable>`) and `replication_delivery_to_python` per `../spec.md` §2 [^py-async].

## Bounded Read Manifest

- `py_async.rs` — completion queue drainer.
- `notifications.rs` — `PendingDeliveryGuard` precedent.
- `postgres/types.py` — `PG_UNCHANGED_TOAST` and `PostgresReplicationRecord`.

## Contracts & Edge Cases

- Preserve `PG_UNCHANGED_TOAST` singleton identity and non-UTF-8 `bytes` payloads without lossy UTF-8 coercion.
- If the awaiting Python task is cancelled before `map_ok` runs on the loop thread, drop the delivery guard so the record returns to the head of the supervisor queue.

## Test Matrix

- Rust unit tests for cancelled-future payload drop restoration and `PgUnchangedToast`/binary column projection.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --features gcp replication::
```

## Acceptance Criteria

- [ ] Cancelled Python futures never consume or acknowledge an undelivered WAL record.
- [ ] `replication_delivery_to_python` preserves `PG_UNCHANGED_TOAST` and raw bytes.
- [ ] Zero regressions in `ch02-replication-python-bridge` fast verification gate.

[^py-async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — AsyncCompletionQueue drainer.
