---
type: Task
id: "spanner-queue-and-rw-transactions:ch01-interactive-rw-transactions:t02"
flow_id: spanner-queue-and-rw-transactions
chapter_id: ch01-interactive-rw-transactions
title: "Expose Sync/Async Spanner ReadWrite Transaction Handles in lib.rs and _spanner.pyi"
description: "Bind SyncSpannerReadWriteTransaction and AsyncSpannerReadWriteTransaction in vertebra-spanner/src/lib.rs and update _spanner.pyi stubs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/rs/vertebra-spanner/src/lib.rs
  - src/py/vertebra/_spanner.pyi
tests:
  - src/py/tests/unit/db/test_spanner.py
read_set:
  - src/rs/vertebra-spanner/src/lib.rs
  - src/rs/vertebra-spanner/src/client.rs
  - src/py/vertebra/_spanner.pyi
estimated_loc: 210
tags:
  - spanner
  - pyo3
  - transactions
sources:
  - id: spn-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: "PyO3 bindings for SyncSpannerClient and AsyncSpannerClient"
  - id: spn-pyi
    resource: src/py/vertebra/_spanner.pyi
    title: "Type stubs verified by mypy.stubtest for vertebra._spanner"
---

# Task `t02`: Expose Sync/Async Spanner ReadWrite Transaction Handles in lib.rs and _spanner.pyi

## Objective & Scope Boundary

Expose `@final SyncSpannerReadWriteTransaction` and `@final AsyncSpannerReadWriteTransaction` plus `begin_read_write_transaction` on `SyncSpannerClient` and `AsyncSpannerClient` in `src/rs/vertebra-spanner/src/lib.rs` and `src/py/vertebra/_spanner.pyi` per [`../spec.md`](../spec.md) [^spn-lib-rs] [^spn-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-spanner/src/lib.rs` — `block_on_db` sync execution and `spawn_rust_db_task_with_attr` Tokio completion bridge.
- `src/py/vertebra/_spanner.pyi` — `@final` stub declarations and `__all__`.

## Contracts & Edge Cases

- Expose `begin_read_write_transaction(transaction_tag=None, previous_transaction_id=None, exclude_txn_from_change_streams=False, max_commit_delay_ms=None)` on `SyncSpannerClient` and `AsyncSpannerClient`.
- Expose `query`, `execute_dml`, `execute_batch_dml`, `commit(mutations_json=None)`, `rollback()`, and `transaction_id: bytes` on `SyncSpannerReadWriteTransaction` and `AsyncSpannerReadWriteTransaction`, detaching the GIL across every RPC and supporting 3.14t/3.15t (`gil_used = false`).

## Test Matrix

- Python unit tests + `uv run --no-sync python -m mypy.stubtest vertebra._spanner` verifying exact stub parity and GIL-detached execution.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && uv run --no-sync ruff check src/py/vertebra/_spanner.pyi src/py/vertebra/db/adapters/spanner && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py
```

## Acceptance Criteria

- [ ] `SyncSpannerReadWriteTransaction` and `AsyncSpannerReadWriteTransaction` pass `mypy.stubtest vertebra._spanner` and unit binding checks.
- [ ] Zero regressions in the chapter fast verification gate.

[^spn-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — PyO3 bindings for SyncSpannerClient and AsyncSpannerClient.
[^spn-pyi]: `src/py/vertebra/_spanner.pyi` — Type stubs verified by mypy.stubtest for vertebra._spanner.
