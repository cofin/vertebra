---
type: Spec
flow_id: spanner-queue-and-rw-transactions
title: Cloud Spanner Interactive ReadWrite Transactions and Native Queue Store
description: Implement genuine multi-statement interactive ReadWrite transactions in vertebra-spanner and Python Spanner drivers, build SyncSpannerQueueStore and AsyncSpannerQueueStore across GoogleSQL and Spangres, and remediate SpannerQueueBackend claim, lease, and memory-leak defects.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
current_task: spanner-queue-and-rw-transactions:t01
parent_prd: integration-remediation
depends_on_flows:
- queues-tasks-and-channels
plan_revision: 1
plan_commit: 1b191388
created_at: '2026-11-12T23:58:00Z'
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- spanner
- spangres
- transactions
- queues
- rust
sources:
- id: spn-client-rs
  resource: src/rs/vertebra-spanner/src/client.rs
  title: Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner
- id: spn-lib-rs
  resource: src/rs/vertebra-spanner/src/lib.rs
  title: PyO3 bindings for SyncSpannerClient and AsyncSpannerClient
- id: spn-pyi
  resource: src/py/vertebra/_spanner.pyi
  title: Type stubs for vertebra._spanner
- id: spn-driver
  resource: src/py/vertebra/db/adapters/spanner/transactions.py::SyncSpannerTransaction
  title: SyncSpannerTransaction and AsyncSpannerTransaction delegating to non-atomic single-shot RPCs
- id: spn-sync
  resource: src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver
  title: SyncSpannerDriver run_in_transaction and transaction context manager
- id: queue-spanner
  resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
  title: SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL
- id: okf-gcp
  resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
  title: BigQuery and Cloud Spanner drivers OKF chapter
- id: okf-queues
  resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  title: Distributed queues and email OKF chapter
---

# Specification: Cloud Spanner Interactive ReadWrite Transactions and Native Queue Store

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncSpannerClient` and `AsyncSpannerClient` expose only single-shot RPCs, so `driver.run_in_transaction(fn)` executes `tx.select()` as an independent `ReadOnly` query, `tx.execute()` as a self-committing DML RPC, and `tx.mutations` as a separate `apply_mutations()` RPC [^spn-client-rs] [^spn-pyi] [^spn-driver].
- **Queue Baseline Defects (`18baadcd`)**: `framework/queues/spanner.py` passes 6 positional args to 3-param `CLAIM_SPANNER_JOBS_SQL`, never executes `UPDATE_CLAIM_SPANNER_JOBS_SQL`, hardcodes GoogleSQL `@param` syntax on Spangres, steals active leases via unconditional `INSERT OR UPDATE`, and leaks timed-out tasks in `_receipt_index` [^queue-spanner].
- **Target Interactive `ReadWrite` Contract**: `vertebra-spanner` implements `BeginTransaction` -> `ExecuteSql` / `ExecuteStreamingSql` / `ExecuteBatchDml` carrying `TransactionSelector::Id` + monotonic `seqno` + `precommit_token` tracking -> `Commit` / `Rollback`, retrying `run_in_transaction(fn)` on `SpannerAbortedError` with server `retry_delay_ms` and `previous_transaction_id` [^spn-lib-rs] [^spn-sync] [^okf-gcp].
- **Target Spanner Queue Contract**: `SyncSpannerQueueStore` and `AsyncSpannerQueueStore` execute select-then-update claims atomically inside `run_in_transaction` across GoogleSQL (`@param`) and Spangres (`$1`), auto-wired into `SpannerQueueBackend` [^okf-queues].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Atomic Cloud Spanner queue claiming requires `SELECT ... FOR UPDATE` followed by `UPDATE vertebra_queue_jobs SET status = 'running' ... THEN RETURN *` inside a single `ReadWrite` transaction. At `main @ 18baadcd`, `src/rs/vertebra-spanner` only exposes self-committing single-statement DML and mutation helpers, so `SyncSpannerTransaction` and `AsyncSpannerTransaction` cannot hold row locks across statements [^spn-client-rs] [^spn-driver]. Simultaneously, `src/py/vertebra/framework/queues/spanner.py` contains concrete runtime bugs: mismatched parameter counts in `dequeue()`, missing claim `UPDATE` execution, GoogleSQL-only syntax breaking Spangres, unconditional `acquire_lease` stealing live cron leases, and an unbounded `_receipt_index` dictionary [^queue-spanner].

**Non-Goals**: PostgreSQL, Oracle, Redis, and Valkey queue stores and `SyncQueueStore` / `AsyncQueueStore` protocol definitions (delivered in upstream `queues-tasks-and-channels`), and SQLSpec Spanner stores (`sqlspec-adapter-parity`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Rust Interactive `ReadWrite` Session Handle)**: Implement `SpannerReadWriteTx` in `src/rs/vertebra-spanner/src/{client,errors}.rs` managing `BeginTransaction` (with optional `multiplexed_session_previous_transaction_id` and `transaction_tag`), `ExecuteStreamingSql` / `ExecuteSql` / `ExecuteBatchDml` carrying `TransactionSelector::Id` and monotonic 1-based `seqno`, leader routing headers, highest-`seq_num` `MultiplexedSessionPrecommitToken` tracking, `Commit` (with buffered mutations and `MultiplexedSessionRetry` replay), `Rollback`, and `SpannerAbortedError.retry_delay_ms`.
- **R2 (PyO3 Bindings & `_spanner.pyi` Stubs)**: Expose `@final SyncSpannerReadWriteTransaction` and `@final AsyncSpannerReadWriteTransaction` plus `begin_read_write_transaction` on `SyncSpannerClient` and `AsyncSpannerClient` (`gil_used = false`, verified by `mypy.stubtest`).
- **R3 (Python `run_in_transaction` & `transaction()`)**: Bind `SyncSpannerTransaction` and `AsyncSpannerTransaction` to one interactive `ReadWrite` handle; commit on closure completion, roll back on exception, and retry `run_in_transaction(fn)` on `SpannerAbortedError` honoring server `retry_delay_ms` and `previous_transaction_id`; provide single-attempt `with driver.transaction():` / `async with driver.transaction():`.
- **R4 (`SyncSpannerQueueStore` & `AsyncSpannerQueueStore`)**: Implement `SyncQueueStore` and `AsyncQueueStore` in `src/py/vertebra/db/adapters/spanner/queue.py` executing atomic `SELECT ... FOR UPDATE` -> `UPDATE ... THEN RETURN *` (`RETURNING *` on Spangres) inside `run_in_transaction` across GoogleSQL (`@param`, `UNNEST(@lanes)`, `INSERT OR UPDATE`) and Spangres (`$1`, `ANY($1)`, `ON CONFLICT DO UPDATE`), with attempt fencing, DLQ transitions, and conditional `expires_at <= now OR owner_id = effective_owner` lease acquisition.
- **R5–R6 (`SpannerQueueBackend` Auto-Wiring & Docs/OKF)**: Auto-construct `SyncSpannerQueueStore` / `AsyncSpannerQueueStore` in `SpannerQueueBackend` and `QueueService`, eliminate `_receipt_index` memory leaks, and synchronize `gcp-bigquery-and-spanner-drivers.md`, `distributed-queues-and-email.md`, and `docs/usage/queues-and-realtime.md`.

```python
@final
class SyncSpannerReadWriteTransaction:
    @property
    def transaction_id(self) -> bytes: ...
    def query(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> dict[str, Any]: ...
    def execute_dml(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> int: ...
    def execute_batch_dml(self, statements_json: str, request_tag: str | None = None) -> list[int]: ...
    def commit(self, mutations_json: str | None = None) -> dict[str, Any]: ...
    def rollback(self) -> None: ...

class SyncSpannerQueueStore: ...
class AsyncSpannerQueueStore: ...
```

## 3. Documentation & OKF Knowledge Plan

- Public documentation (`t04`): `docs/usage/queues-and-realtime.md`.
- OKF knowledge chapters (`t04`, each `<= 200` lines): [gcp-bigquery-and-spanner-drivers.md](../../knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md) and [distributed-queues-and-email.md](../../knowledge/domains/framework/distributed-queues-and-email.md).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Rust Interactive Spanner ReadWrite Engine and PyO3 Bindings | `src/rs/vertebra-spanner/src/client.rs`, `src/rs/vertebra-spanner/src/errors.rs`, `src/rs/vertebra-spanner/src/lib.rs` | `src/py/tests/unit/db/test_spanner.py` |
| `t02` | Wire SyncSpannerDriver / AsyncSpannerDriver Interactive Transactions & Aborted Closure Retries | `src/py/vertebra/db/adapters/spanner/transactions.py`, `src/py/vertebra/db/adapters/spanner/_sync.py`, `src/py/vertebra/db/adapters/spanner/_async.py` (+1 more) | `src/py/tests/unit/db/test_spanner.py` |
| `t03` | Implement Sync/AsyncSpannerQueueStore and Remediate SpannerQueueBackend | `src/py/vertebra/db/adapters/spanner/queue.py`, `src/py/vertebra/db/adapters/spanner/__init__.py`, `src/py/vertebra/framework/queues/spanner.py` | `src/py/tests/unit/db/test_spanner_queue_and_cdc.py`, `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t04` | Synchronize Spanner ReadWrite Transactions & Queue Store Docs and OKF Chapters | `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md`, `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`, `docs/usage/queues-and-realtime.md` | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py` |

## 5. Implementation Plan

- [ ] [t01-rust-and-pyo3-rw-transactions.md](tasks/t01-rust-and-pyo3-rw-transactions.md) — Build stateful ReadWrite transaction handles in vertebra-spanner/src/{client,errors,lib}.rs and expose Sync/AsyncSpannerReadWriteTransaction in _spanner.pyi.
- [ ] [t02-python-driver-rw-transactions-and-retries.md](tasks/t02-python-driver-rw-transactions-and-retries.md) — Route SyncSpannerTransaction, AsyncSpannerTransaction, run_in_transaction, and transaction() through interactive ReadWrite handles with SpannerAbortedError retry backoff.
- [ ] [t03-spanner-queue-store-and-backend-wiring.md](tasks/t03-spanner-queue-store-and-backend-wiring.md) — Build SyncSpannerQueueStore and AsyncSpannerQueueStore across GoogleSQL and Spangres, fix SpannerQueueBackend claim/lease/leak defects, and auto-wire QueueService.
- [ ] [t04-spanner-rw-and-queue-docs-okf-sync.md](tasks/t04-spanner-rw-and-queue-docs-okf-sync.md) — Update gcp-bigquery-and-spanner-drivers.md, distributed-queues-and-email.md, and docs/usage/queues-and-realtime.md.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `spanner-queue-and-rw-transactions:t01` (`0/4` tasks completed).
- **Upstream Flow Gates**: `t03`–`t04` require `queues-tasks-and-channels` (`SyncQueueStore` / `AsyncQueueStore` protocols and `QueueBackendSettings`); `t01`–`t02` touch only `vertebra-spanner` and `db/adapters/spanner`.
- **Full `/flow:finish` Gate**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && uv run --no-sync maturin develop --uv --manifest-path src/rs/vertebra-spanner/Cargo.toml && uv run --no-sync python -m mypy.stubtest vertebra._spanner && make lint-all && make py-test && make docs`.

[^spn-client-rs]: `src/rs/vertebra-spanner/src/client.rs` — Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner.
[^spn-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — PyO3 bindings for SyncSpannerClient and AsyncSpannerClient.
[^spn-pyi]: `src/py/vertebra/_spanner.pyi` — Type stubs for vertebra._spanner.
[^spn-driver]: `src/py/vertebra/db/adapters/spanner/transactions.py::SyncSpannerTransaction` — SyncSpannerTransaction and AsyncSpannerTransaction delegating to non-atomic single-shot RPCs.
[^spn-sync]: `src/py/vertebra/db/adapters/spanner/_sync.py::SyncSpannerDriver` — SyncSpannerDriver run_in_transaction and transaction context manager.
[^queue-spanner]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL.
[^okf-gcp]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — BigQuery and Cloud Spanner drivers OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues and email OKF chapter.
