---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch02-replication-python-bridge
title: "Chapter 02: PyO3 Replication Bridge, Stubs, and FFI OKF Sync"
description: "Expose cancel-safe AsyncPostgresReplication, AsyncPostgresClient.open_replication/replication_identity, 14-class _postgres.pyi, and PyO3 FFI OKF updates."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-13T00:25:00Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
tags:
  - rust
  - pyo3
  - postgres
  - replication
sources:
  - id: py-async
    resource: "src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr"
    title: "Shared event-loop completion drainer and cancel hook"
  - id: pg-async-client
    resource: src/rs/vertebra-postgres/src/async_client.rs
    title: "AsyncPostgresClient PyO3 methods"
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: "Typed stub surface for vertebra._postgres"
  - id: ffi-okf
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: "PyO3 0.30.1 FFI and GIL discipline chapter"
---

# Chapter 02: PyO3 Replication Bridge, Stubs, and FFI OKF Sync

<!-- truth: start -->
- `spawn_rust_db_task_with_attr_and_future(...) -> PyResult<RustDbTaskAwaitable>` attaches a `PendingDeliveryGuard` so Python `Future.cancel()` restores held deliveries without losing WAL records [^py-async].
- Frozen `AsyncPostgresReplication` (`next`, `acknowledge`, `aclose`, `closed`, `__aenter__`/`__aexit__`) and `AsyncPostgresClient.{replication_identity, open_replication}` complete the 14-class `_postgres.pyi` surface [^pg-async-client] [^pg-pyi].
- FFI OKF chapters (`ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, `pyo3-ffi-and-batch-dispatch.md`) stay `<= 200` lines each [^ffi-okf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Bridges `ReplicationSupervisor` (`ch01`) into `vertebra._postgres` with cancellation-safe event-loop delivery and synchronizes FFI OKF chapters.

## 2. Requirements, Concrete Contracts & Architecture

- `vertebra_db::py_async::spawn_rust_db_task_with_attr_and_future`: returns `PyResult<RustDbTaskAwaitable>` (matching `spawn_rust_db_task_with_attr`), running `map_ok` only when the Python consumer has not cancelled the awaitable; otherwise drops the Rust payload guard to restore the lease.
- `AsyncPostgresReplication`: `@final` frozen `#[pyclass(frozen)]` with `__aiter__`/`__anext__`, `next()`, `acknowledge(lsn: int)`, `aclose()`, `closed: bool`, and async context manager protocol.
- `AsyncPostgresClient`: `replication_identity(slot_name, publication_names)` and `open_replication(...)` rejecting `PoolerMode::{Transaction, Statement}` (`NotSupportedError`) and `queue_capacity` outside `1..=1024` (`ValueError`).

## 3. Documentation & OKF Knowledge Plan

Task `t04` updates `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, and `pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` Cancel-safe future drainer & record conversion | `t01` | `py_async.rs`, `replication/mod.rs` | `cargo test -p vertebra-db -p vertebra-postgres` |
| `R1` `AsyncPostgresReplication` pyclass | `t02` | `replication/mod.rs` | `pytest test_postgres_native_replication.py` |
| `R1` `AsyncPostgresClient` methods & `_postgres.pyi` | `t03` | `async_client.rs`, `_postgres.pyi` | `mypy.stubtest vertebra._postgres` |
| `R5` PyO3 FFI OKF sync (`<= 200L`) | `t04` | 3 OKF chapters | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-cancel-safe-db-task-and-record-conversion.md](tasks/t01-cancel-safe-db-task-and-record-conversion.md) — Extend vertebra-db py_async for cancel-safe lease restoration and convert ReplicationEventDelivery records to PostgresReplicationRecord on the event loop.
- [ ] [t02-async-postgres-replication-pyclass.md](tasks/t02-async-postgres-replication-pyclass.md) — Expose AsyncPostgresReplication (#[pyclass(frozen)]) with cancel-safe next, fenced acknowledge, aclose, closed, and async context management.
- [ ] [t03-client-open-replication-and-pyi-stubs.md](tasks/t03-client-open-replication-and-pyi-stubs.md) — Add replication_identity and open_replication on AsyncPostgresClient and finalize _postgres.pyi for stubtest parity.
- [ ] [t04-pyo3-ffi-okf-sync.md](tasks/t04-pyo3-ffi-okf-sync.md) — Update the 3 PyO3 FFI OKF chapters (each <= 200 lines) for vertebra._postgres, ShmCapsule, and PendingDeliveryGuard.

## 6. Current State & Remaining Plan Snapshot

Planned (`4` tasks). Fast `< 60s` local `maturin` + `stubtest` + unit test gate.

```bash
UV_NO_SYNC=1 PYO3_PYTHON=.venv/bin/python make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_replication.py -q && make review-check
```

[^py-async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared event-loop completion drainer and cancel hook.
[^pg-async-client]: `src/rs/vertebra-postgres/src/async_client.rs` — AsyncPostgresClient PyO3 methods.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Typed stub surface for vertebra._postgres.
[^ffi-okf]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 0.30.1 FFI and GIL discipline chapter.
