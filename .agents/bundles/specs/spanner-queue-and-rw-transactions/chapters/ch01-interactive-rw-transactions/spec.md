---
type: Spec
flow_id: spanner-queue-and-rw-transactions
chapter_id: ch01-interactive-rw-transactions
title: "Chapter 01: Interactive Multi-Statement Spanner ReadWrite Transactions"
description: "Implement genuine multi-statement Spanner ReadWrite transactions (BeginTransaction -> TransactionSelector::Id + seqno + precommit_token -> Commit/Rollback with SpannerAbortedError retries) in vertebra-spanner and Python drivers."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
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
  - rust
  - pyo3
  - transactions
sources:
  - id: spn-client-rs
    resource: src/rs/vertebra-spanner/src/client.rs
    title: "Rust SpannerClient single-shot DML helpers"
  - id: spn-pyi
    resource: src/py/vertebra/_spanner.pyi
    title: "vertebra._spanner type stubs"
  - id: spn-driver
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerTransaction
    title: "Python SyncSpannerTransaction and AsyncSpannerTransaction"
---

# Chapter 01: Interactive Multi-Statement Spanner ReadWrite Transactions

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncSpannerClient` and `AsyncSpannerClient` expose only single-shot RPCs; inside `driver.run_in_transaction(fn)`, `tx.select()` runs an independent `ReadOnly` query, `tx.execute()` self-commits in its own transaction, and `tx.mutations` commits separately [^spn-client-rs] [^spn-pyi] [^spn-driver].
- **Target Architecture**: `vertebra-spanner` exposes `SyncSpannerReadWriteTransaction` / `AsyncSpannerReadWriteTransaction` carrying `TransactionSelector::Id`, monotonic `seqno`, and `MultiplexedSessionPrecommitToken` into `Commit` / `Rollback`, while `run_in_transaction(fn)` retries the closure on `SpannerAbortedError` honoring server `retry_delay_ms` and `previous_transaction_id`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter resolves the transactional atomicity gap in `vertebra-spanner` and `vertebra.db.adapters.spanner` so multi-statement `SELECT ... FOR UPDATE` + `UPDATE ... THEN RETURN *` workloads execute inside a single ACID Cloud Spanner `ReadWrite` transaction [^spn-client-rs] [^spn-driver].

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (Rust Interactive `ReadWrite` Session Handle)**: Track `transaction_id`, 1-based `seqno`, highest-`seq_num` `MultiplexedSessionPrecommitToken`, `MultiplexedSessionRetry` commit replay, `Rollback`, and `SpannerAbortedError.retry_delay_ms`.
- **R2 (PyO3 Bindings & Stubs)**: Expose `@final SyncSpannerReadWriteTransaction` and `@final AsyncSpannerReadWriteTransaction` (`gil_used = false`, `mypy.stubtest` clean).
- **R3 (Python `run_in_transaction` & `transaction()`)**: Execute reads, DML, batch DML, and buffered mutations in one `ReadWrite` transaction, rolling back on error and retrying `run_in_transaction(fn)` on `SpannerAbortedError` with server retry delay and `previous_transaction_id`.

```python
@final
class AsyncSpannerReadWriteTransaction:
    @property
    def transaction_id(self) -> bytes: ...
    def query(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> Awaitable[dict[str, Any]]: ...
    def execute_dml(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> Awaitable[int]: ...
    def execute_batch_dml(self, statements_json: str, request_tag: str | None = None) -> Awaitable[list[int]]: ...
    def commit(self, mutations_json: str | None = None) -> Awaitable[dict[str, Any]]: ...
    def rollback(self) -> Awaitable[None]: ...

class AsyncSpannerDriver(AsyncDatabaseDriver):
    async def run_in_transaction(
        self,
        fn: Callable[[AsyncSpannerTransaction], Awaitable[T]],
        *,
        max_attempts: int = 5,
        transaction_tag: str | None = None,
    ) -> T: ...
```

## 3. Documentation & OKF Knowledge Plan

Interactive `ReadWrite` transaction semantics are documented in `ch02-spanner-queue-store-and-docs` (`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` Rust `SpannerReadWriteTx` | `t01` | `src/rs/vertebra-spanner/src/{client,errors}.rs` | `cargo test -p vertebra-spanner` |
| `R2` PyO3 bindings & `_spanner.pyi` | `t02` | `src/rs/vertebra-spanner/src/lib.rs`, `src/py/vertebra/_spanner.pyi` | `mypy.stubtest vertebra._spanner` |
| `R3` Driver `run_in_transaction` & retry | `t03` | `src/py/vertebra/db/adapters/spanner/{driver,__init__}.py` | `pytest test_spanner.py` |

## 5. Implementation Plan

- [ ] [t01-rust-interactive-rw-session-engine.md](tasks/t01-rust-interactive-rw-session-engine.md) — Implement Rust `SpannerReadWriteTx` (`BeginTransaction` -> `TransactionSelector::Id` + `seqno` + `precommit_token` -> `Commit`/`Rollback`).
- [ ] [t02-pyo3-rw-transaction-bindings-and-stubs.md](tasks/t02-pyo3-rw-transaction-bindings-and-stubs.md) — Expose `SyncSpannerReadWriteTransaction` and `AsyncSpannerReadWriteTransaction` in `lib.rs` and `_spanner.pyi`.
- [ ] [t03-python-driver-rw-transactions-and-retries.md](tasks/t03-python-driver-rw-transactions-and-retries.md) — Wire `SyncSpannerDriver` / `AsyncSpannerDriver` `run_in_transaction` and `transaction()` onto interactive `ReadWrite` handles.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-interactive-rw-transactions` is `planned` behind [`../../spec.md`](../../spec.md) prerequisite `queues-tasks-and-channels`.

Fast Chapter Gate (`< 60s` offline):

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && uv run --no-sync ruff check src/py/vertebra/_spanner.pyi src/py/vertebra/db/adapters/spanner && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py
```

[^spn-client-rs]: `src/rs/vertebra-spanner/src/client.rs` — Rust SpannerClient single-shot DML helpers.
[^spn-pyi]: `src/py/vertebra/_spanner.pyi` — vertebra._spanner type stubs.
[^spn-driver]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerTransaction` — Python SyncSpannerTransaction and AsyncSpannerTransaction.
