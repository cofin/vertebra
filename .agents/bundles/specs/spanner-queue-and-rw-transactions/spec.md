---
type: Spec
flow_id: spanner-queue-and-rw-transactions
title: "Cloud Spanner Interactive ReadWrite Transactions and Native Queue Store"
description: "Implement genuine multi-statement interactive ReadWrite transactions in vertebra-spanner and Python Spanner drivers, build SyncSpannerQueueStore and AsyncSpannerQueueStore across GoogleSQL and Spangres, and remediate SpannerQueueBackend claim, lease, and memory-leak defects."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-interactive-rw-transactions
  - ch02-spanner-queue-store-and-docs
current_chapter: ch01-interactive-rw-transactions
current_task: "spanner-queue-and-rw-transactions:ch01:t01"
parent_prd: integration-remediation
depends_on_flows:
  - queues-tasks-and-channels
plan_revision: 1
plan_commit: 18baadcd
created_at: "2026-11-12T23:58:00Z"
updated_at: "2026-11-12T23:58:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:58:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:58:00Z"
    commit: 18baadcd
tags:
  - spanner
  - spangres
  - transactions
  - queues
  - rust
sources:
  - id: spn-client-rs
    resource: src/rs/vertebra-spanner/src/client.rs
    title: "Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner"
  - id: spn-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: "PyO3 bindings for SyncSpannerClient and AsyncSpannerClient"
  - id: spn-pyi
    resource: src/py/vertebra/_spanner.pyi
    title: "Type stubs for vertebra._spanner"
  - id: spn-driver
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerTransaction
    title: "SyncSpannerTransaction and AsyncSpannerTransaction delegating to non-atomic single-shot RPCs"
  - id: queue-spanner
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: "SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL"
  - id: okf-gcp
    resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
    title: "BigQuery and Cloud Spanner drivers OKF chapter"
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: "Distributed queues and email OKF chapter"
---

# Specification: Cloud Spanner Interactive ReadWrite Transactions and Native Queue Store

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncSpannerClient` and `AsyncSpannerClient` expose only single-shot RPCs, so `driver.run_in_transaction(fn)` executes `tx.select()` as an independent `ReadOnly` query, `tx.execute()` as a self-committing DML RPC, and `tx.mutations` as a separate `apply_mutations()` RPC [^spn-client-rs] [^spn-pyi] [^spn-driver].
- **Queue Baseline Defects (`18baadcd`)**: `framework/queues/spanner.py` passes 6 positional args to 3-param `CLAIM_SPANNER_JOBS_SQL`, never executes `UPDATE_CLAIM_SPANNER_JOBS_SQL`, hardcodes GoogleSQL `@param` syntax on Spangres, steals active leases via unconditional `INSERT OR UPDATE`, and leaks timed-out tasks in `_receipt_index` [^queue-spanner].
- **Target Interactive `ReadWrite` Contract**: `vertebra-spanner` implements `BeginTransaction` -> `ExecuteSql` / `ExecuteStreamingSql` / `ExecuteBatchDml` carrying `TransactionSelector::Id` + monotonic `seqno` + `precommit_token` tracking -> `Commit` / `Rollback`, retrying `run_in_transaction(fn)` on `SpannerAbortedError` with server `retry_delay_ms` [^spn-lib-rs] [^okf-gcp].
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

- Public documentation (`ch02:t03`): `docs/usage/queues-and-realtime.md`.
- OKF knowledge chapters (`ch02:t03`, each `<= 200` lines): [gcp-bigquery-and-spanner-drivers.md](../../knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md) and [distributed-queues-and-email.md](../../knowledge/domains/framework/distributed-queues-and-email.md).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R3` Interactive `ReadWrite` transactions & retries | `ch01` (`t01`–`t03`) | `src/rs/vertebra-spanner/src/{client,errors,lib}.rs`, `_spanner.pyi`, `db/adapters/spanner/driver.py` | `cargo test -p vertebra-spanner && pytest test_spanner.py` |
| `R4`–`R5` `Sync/AsyncSpannerQueueStore` & backend fixes | `ch02` (`t01`–`t02`) | `db/adapters/spanner/queue.py`, `framework/queues/{spanner,base}.py` | `pytest test_spanner_queue_and_cdc.py test_distributed_queues.py` |
| `R6` Docs & OKF sync | `ch02` (`t03`) | `gcp-bigquery-and-spanner-drivers.md`, `distributed-queues-and-email.md`, `queues-and-realtime.md` | `review_setup.py validate` |

## 5. Implementation Plan

- [ ] [ch01-interactive-rw-transactions](chapters/ch01-interactive-rw-transactions/spec.md) — Implement interactive multi-statement `ReadWrite` transactions (`TransactionSelector::Id` + `seqno` + `precommit_token` + `SpannerAbortedError` retries) in `vertebra-spanner` and Python Spanner drivers.
- [ ] [ch02-spanner-queue-store-and-docs](chapters/ch02-spanner-queue-store-and-docs/spec.md) — Implement `SyncSpannerQueueStore`/`AsyncSpannerQueueStore` across GoogleSQL and Spangres, fix `SpannerQueueBackend` claim/lease/leak defects, and sync docs/OKF.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `ch01-interactive-rw-transactions` (`spanner-queue-and-rw-transactions:ch01:t01`, `0/6` tasks completed across `2` chapters).
- **Upstream Flow Gates**: `ch02` requires `queues-tasks-and-channels` (`SyncQueueStore` / `AsyncQueueStore` protocols and `QueueBackendSettings`); `ch01` touches only `vertebra-spanner` and `db/adapters/spanner`.
- **Full `/flow:finish` Gate**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && make lint-all && make py-test && make docs`.

[^spn-client-rs]: `src/rs/vertebra-spanner/src/client.rs` — Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner.
[^spn-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — PyO3 bindings for SyncSpannerClient and AsyncSpannerClient.
[^spn-pyi]: `src/py/vertebra/_spanner.pyi` — Type stubs for vertebra._spanner.
[^spn-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerTransaction` — SyncSpannerTransaction and AsyncSpannerTransaction delegating to non-atomic single-shot RPCs.
[^queue-spanner]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — SpannerQueueBackend passing 6 args to 3-param CLAIM_SPANNER_JOBS_SQL and skipping UPDATE_CLAIM_SPANNER_JOBS_SQL.
[^okf-gcp]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — BigQuery and Cloud Spanner drivers OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues and email OKF chapter.
