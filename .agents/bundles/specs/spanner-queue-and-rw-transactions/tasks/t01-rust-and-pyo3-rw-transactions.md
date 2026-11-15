---
type: Task
id: spanner-queue-and-rw-transactions:t01
flow_id: spanner-queue-and-rw-transactions
title: Implement Rust Interactive Spanner ReadWrite Engine and PyO3 Bindings
description: Build stateful ReadWrite transaction handles in vertebra-spanner/src/{client,errors,lib}.rs and expose Sync/AsyncSpannerReadWriteTransaction in _spanner.pyi.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on: []
files:
  - src/rs/vertebra-spanner/src/client.rs
  - src/rs/vertebra-spanner/src/errors.rs
  - src/rs/vertebra-spanner/src/lib.rs
  - src/py/vertebra/_spanner.pyi
tests:
  - src/py/tests/unit/db/test_spanner.py
  - src/rs/vertebra-spanner/tests/spanner_wire_tests.rs
read_set:
  - src/rs/vertebra-spanner/src/client.rs
  - src/rs/vertebra-spanner/src/codec.rs
  - src/rs/vertebra-spanner/src/errors.rs
  - src/rs/vertebra-spanner/src/lib.rs
  - src/py/vertebra/_spanner.pyi
estimated_loc: 235
tags:
  - spanner
  - rust
  - transactions
  - pyo3
sources:
  - id: spn-client-rs
    resource: src/rs/vertebra-spanner/src/client.rs
    title: Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner
  - id: spn-errors-rs
    resource: src/rs/vertebra-spanner/src/errors.rs
    title: SpannerError and SpannerAbortedError retry_delay_ms extraction
  - id: spn-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: PyO3 bindings for SyncSpannerClient and AsyncSpannerClient
  - id: spn-pyi
    resource: src/py/vertebra/_spanner.pyi
    title: Type stubs verified by mypy.stubtest for vertebra._spanner
verification_strategy: regression_tdd
---

# Task `t01`: Implement Rust Interactive Spanner ReadWrite Engine and PyO3 Bindings

## Objective & Scope Boundary

Implement interactive multi-statement `ReadWrite` transaction session handles in `src/rs/vertebra-spanner/src/{client,errors}.rs` and expose `@final SyncSpannerReadWriteTransaction` and `@final AsyncSpannerReadWriteTransaction` plus `begin_read_write_transaction` on `SyncSpannerClient` and `AsyncSpannerClient` in `src/rs/vertebra-spanner/src/lib.rs` and `src/py/vertebra/_spanner.pyi` per [`../spec.md`](../spec.md) [^spn-client-rs] [^spn-errors-rs] [^spn-lib-rs] [^spn-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-spanner/src/client.rs` — `SpannerClient` multiplexed session lifecycle and `read_write_transaction()` usage.
- `src/rs/vertebra-spanner/src/codec.rs` — query/DML parameter serialization and row/mutation JSON codecs.
- `src/rs/vertebra-spanner/src/errors.rs` — `SpannerError` -> `SpannerAbortedError` (gRPC `ABORTED`, `retry_delay_ms`) exception mapping.
- `src/rs/vertebra-spanner/src/lib.rs` — `block_on_db` sync execution and `spawn_rust_db_task_with_attr` Tokio completion bridge.
- `src/py/vertebra/_spanner.pyi` — `@final` stub declarations and `__all__`.

## Contracts & Edge Cases

- Implement `SpannerReadWriteTx` acquiring an interactive `ReadWrite` transaction (`BeginTransaction` / inline begin with optional `multiplexed_session_previous_transaction_id` for wound-wait lock priority and `transaction_tag`), routing `query`, `execute_dml`, and `execute_batch_dml` over the pinned `TransactionSelector.id` with monotonic 1-based `seqno`, `x-goog-spanner-route-to-leader: true`, `ExecuteBatchDmlResponse.status` inspection, and highest-`seq_num` `MultiplexedSessionPrecommitToken` tracking.
- Implement `commit(mutations)` (attaching latest `precommit_token`, `max_commit_delay_ms`, `exclude_txn_from_change_streams`, and replaying `CommitResponse.MultiplexedSessionRetry` with empty mutations) and `rollback()`, surfacing `SpannerAbortedError` with server `retry_delay_ms` through existing `src/rs/vertebra-spanner/src/errors.rs::spanner_error_to_py`.
- Expose `begin_read_write_transaction(transaction_tag=None, previous_transaction_id=None, exclude_txn_from_change_streams=False, max_commit_delay_ms=None)` on `SyncSpannerClient` and `AsyncSpannerClient`, and expose `query`, `execute_dml`, `execute_batch_dml`, `commit(mutations_json=None)`, `rollback()`, and `transaction_id: bytes` on `SyncSpannerReadWriteTransaction` and `AsyncSpannerReadWriteTransaction` (detaching the GIL across every RPC with `gil_used = false`).

## Test Matrix

- Rust stub/unit tests (stub-backed `Spanner` client via `from_stub`) verifying `BeginTransaction` -> `ExecuteStreamingSql` (`SELECT ... FOR UPDATE`) -> `ExecuteSql` (`UPDATE`) -> `Commit` carries one `transaction_id`, increments `seqno` `1, 2`, forwards highest `precommit_token`, replays `MultiplexedSessionRetry`, and extracts `ABORTED` `retry_delay_ms`, alongside Python unit tests and `mypy.stubtest vertebra._spanner`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && uv run --no-sync maturin develop --uv --manifest-path src/rs/vertebra-spanner/Cargo.toml && uv run --no-sync ruff check src/py/vertebra/_spanner.pyi src/py/vertebra/db/adapters/spanner && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner && uv run --no-sync python -m mypy.stubtest vertebra._spanner && uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py
```

## Acceptance Criteria

- [ ] `cargo test -p vertebra-spanner` passes interactive multi-statement `ReadWrite` commit, rollback, precommit-token, and `ABORTED` retry-info tests.
- [ ] `SyncSpannerReadWriteTransaction` and `AsyncSpannerReadWriteTransaction` pass `mypy.stubtest vertebra._spanner` and unit binding checks.
- [ ] Zero regressions in the fast verification gate.

[^spn-client-rs]: `src/rs/vertebra-spanner/src/client.rs` — Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner.
[^spn-errors-rs]: `src/rs/vertebra-spanner/src/errors.rs` — SpannerError and SpannerAbortedError retry_delay_ms extraction.
[^spn-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — PyO3 bindings for SyncSpannerClient and AsyncSpannerClient.
[^spn-pyi]: `src/py/vertebra/_spanner.pyi` — Type stubs verified by mypy.stubtest for vertebra._spanner.
