---
type: Spec
parent_flow: db-pool-broker-perf
chapter_id: ch01-sqlite-sqlalchemy-fastpath
title: SQLite SQLAlchemy Fastpath and Direct WorkerSlot Execution
description: Skip implicit BEGIN IMMEDIATE on read-only SQLite lazy_begin queries, default max_readers=0 for SQLAlchemy SQLite dialects, and eliminate dead mpsc::channel allocations in SqlitePool.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
planning_status: complete
depends_on_chapters: []
replan_trigger: null
plan_revision: 2
plan_commit: "16ccb56e"
current_task: null
last_verified_checkpoint: null
last_reviewed_commit: null
generated:
  by: flow/0.26.0
  at: "2026-11-27T18:30:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T18:48:00Z"
    commit: "16ccb56e"
sources:
  - id: src-sqlite-py-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs::AsyncSQLiteCursor
    title: SyncSQLiteCursor and AsyncSQLiteCursor lazy_begin logic
  - id: src-sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqlitePool
    title: SqlitePool WorkerSlot execution and statement_facts cache
  - id: src-sa-sqlite-dialect
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::_SQLiteDialectMixin
    title: SQLAlchemy SQLite +vtb and +vtb_async dialect configuration
  - id: src-sqlite-tests
    resource: src/py/tests/unit/db/test_sqlite_driver.py::test_async_cursor_lazy_begin_opens_transactions
    title: SQLite cursor lazy_begin and transaction unit tests
tags:
  - sqlite
  - sqlalchemy
  - performance
  - rust
---

# Chapter 01: SQLite SQLAlchemy Fastpath and Direct WorkerSlot Execution

<!-- truth: start -->
- `SyncSQLiteCursor::execute` and `AsyncSQLiteCursor::execute` skip implicit `BEGIN IMMEDIATE` when `lazy_begin=True` and `pool.statement_facts(sql).read_only` is `True`, matching Python stdlib `sqlite3` / `aiosqlite` semantics while preserving `BEGIN IMMEDIATE` on DML/DDL and `executemany`.
- `_SQLiteDialectMixin.create_connect_args` defaults `max_readers=0` for SQLAlchemy-pooled SQLite connections unless explicitly provided via URL query or `connect_args`.
- `SqlitePool` executes all `WorkerSlot` operations directly under `self.conn.lock()` without per-statement `mpsc::channel()` allocations.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Under 4-worker `c=128` SQLAlchemy concurrency, `vtb-data-aa-native-4w` (`sqlite+vtb_async`) achieves `2,258 RPS` (`/db`), `120 RPS` (`queries_20`), and `242 RPS` (`updates_5`) versus `granian-asgi-data-aa` (`sqlite+aiosqlite`) at `4,550` / `406` / `409 RPS`. Every `SELECT` executed on a `lazy_begin=True` cursor currently acquires an exclusive SQLite write lock via `BEGIN IMMEDIATE` [^src-sqlite-py-client], every SQLAlchemy pool slot opens 9 SQLite connections (`1` writer + `8` readers) [^src-sa-sqlite-dialect], and `SqlitePool` allocates a synchronous `mpsc::channel()` on every call [^src-sqlite-worker].

### 1.2 Upstream Execution Baseline

- Parent spec: `../../spec.md` (`plan_revision: 2`, `plan_commit: "16ccb56e"`).
- Existing `lazy_begin` unit tests in `test_sqlite_driver.py` assert `in_transaction is True` on `INSERT` and non-empty `executemany` [^src-sqlite-tests].

### 1.3 Code Analysis & Interface Summary

| Area | Files Examined (`path::Symbol`) | Public Interface & Dependency Category | Affected Docs / OKF Chapters |
| :--- | :--- | :--- | :--- |
| SQLite PyO3 cursors | `src/rs/vertebra-sqlite/src/py_client.rs::{SyncSQLiteCursor,AsyncSQLiteCursor}` | In-process PyO3 cursor (`execute`, `executemany`) | `domains/extensions/sqlspec-alchemy-and-dialects.md` |
| SQLite Rust pool | `src/rs/vertebra-sqlite/src/worker.rs::{WorkerSlot,SqlitePool}` | In-process Rust crate internal (`SqlitePool::*_with_op_id`) | None |
| SQLAlchemy SQLite dialect | `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::_SQLiteDialectMixin` | In-process SQLAlchemy dialect (`create_connect_args`) | `domains/extensions/sqlspec-alchemy-and-dialects.md` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge chapters: `domains/extensions/sqlspec-alchemy-and-dialects.md`, `domains/data/sqlite-limitations.md`.
- Skills: `pyo3-bridge`, `python-framework`, `review-performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R1**: Replace naive substring matching in `is_read_only_sql` (`src/rs/vertebra-sqlite/src/worker.rs`) with comment/quote-aware whole-token lexical scanning (skipping leading whitespace and `--`/`/* */` comments, ignoring quoted string/identifier literals, requiring leading `SELECT`/`EXPLAIN`/`VALUES`/`WITH`, and rejecting write keywords `INSERT`/`UPDATE`/`DELETE`/`REPLACE`/`DROP`/`ALTER`/`CREATE`/`ATTACH`/`DETACH`/`VACUUM`/`REINDEX`/`PRAGMA` only as standalone tokens). In `SyncSQLiteCursor::execute` and `AsyncSQLiteCursor::execute`, compute `let begin = this.lazy_begin && !this.pool.statement_facts(sql).read_only;`. Read-only queries do not emit `BEGIN IMMEDIATE` when not already in a transaction; DML/DDL statements and non-empty `executemany` continue emitting `BEGIN IMMEDIATE`.
- **R2**: In `_SQLiteDialectMixin.create_connect_args`, extract any case-insensitive `max_readers` entry from `url.query`, strip it via `url.difference_update_query([...])` before delegating to `super().create_connect_args(clean_url)`, convert the extracted value to `int` in `kwargs["max_readers"]`, and otherwise inject `kwargs["max_readers"] = 0` (SQLAlchemy merges caller `connect_args` afterward).
- **R3**: Replace `WorkerTask` + `mpsc::channel()` in `SqlitePool` methods (`execute_with_op_id`, `execute_many_with_op_id`, `execute_script_with_op_id`, `fetch_all_with_op_id`, `execute_pipeline_with_op_id`, `select_to_arrow_with_op_id`, `load_from_arrow_with_op_id`, `backup_to_with_op_id`, `restore_from_with_op_id`, `begin_with_op_id`, `commit_with_op_id`, `rollback_with_op_id`) with a generic `WorkerSlot::with_conn` helper executing directly under `self.conn.lock()` + `run_flagged_op`.

### 2.2 Shared Concrete Contracts

```rust
// src/rs/vertebra-sqlite/src/worker.rs
impl WorkerSlot {
    fn with_conn<T>(
        &self,
        op_id: u64,
        f: impl FnOnce(&mut Connection, Option<&Arc<AtomicI64>>) -> Result<T, SqliteClientError>,
    ) -> Result<T, SqliteClientError>;
}
```

```python
# src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
class _SQLiteDialectMixin:
    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        max_readers_keys = [k for k in url.query if k.lower() == "max_readers"]
        raw_max_readers = url.query[max_readers_keys[-1]] if max_readers_keys else None
        clean_url = url.difference_update_query(max_readers_keys) if max_readers_keys else url
        args, kwargs = super().create_connect_args(clean_url)
        if raw_max_readers is not None:
            resolved = raw_max_readers[-1] if isinstance(raw_max_readers, tuple) else raw_max_readers
            kwargs["max_readers"] = int(resolved)
        elif "max_readers" not in kwargs:
            kwargs["max_readers"] = 0
        return args, kwargs
```

### 2.3 Caller -> Interface -> Collaborator Map

- SQLAlchemy `create_async_engine("sqlite+vtb_async:///...")` -> `AsyncSQLiteDialect.create_connect_args` -> `SQLAlchemyAsyncDBAPI.connect` -> `AsyncSQLiteDriver` (`max_readers=0`) -> `AsyncSQLiteClient` (`1` writer connection, `0` reader connections).
- `session.get(...)` -> `AsyncAdapt_vertebra_cursor._execute_async` -> `AsyncSQLiteCursor::execute` (`read_only == true` -> `begin = false`) -> `SqlitePool::run_with_op_id` -> `WorkerSlot::run_statement`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| `SELECT created_at, updated_at, deleted_at FROM items` | Whole-token lexical scan classifies as `read_only == true` (substrings inside column names do not trigger write classification); `lazy_begin` skips `BEGIN IMMEDIATE`. |
| `/* trace */ SELECT 1` or `-- note\nSELECT 1` | Leading comments are skipped before identifying leading `SELECT`; classified as `read_only == true`. |
| `WITH cte AS (SELECT 1) REPLACE INTO items VALUES (1)` | Standalone `REPLACE` token outside literals classifies as `read_only == false`; `lazy_begin` emits `BEGIN IMMEDIATE`. |
| `SELECT ...` followed by `UPDATE ...` on same `lazy_begin=True` cursor | `SELECT` runs outside transaction; `UPDATE` triggers `BEGIN IMMEDIATE` and respects `busy_timeout`; `rollback()` reverts the `UPDATE`. |
| `driver.begin()` followed by `SELECT` on `lazy_begin=True` cursor | `pool.in_transaction()` is `true`; `SELECT` stays inside the open transaction on `pool.writer`. |
| Explicit `sqlite+vtb_async:///db.sqlite?max_readers=4` or `connect_args={"max_readers": 4}` | Query param is extracted as `kwargs["max_readers"] = 4` and stripped from the rendered URL so it does not leak into SQLite `file:` URIs; `connect_args` overrides kwargs afterward. |
| Cancelled `op_id` during direct `WorkerSlot::with_conn` execution | `run_flagged_op` checks `InterruptState::begin_op` and returns `SqliteClientError::Cancelled`, identical to prior `WorkerTask` handling. |

### 2.5 Acceptance Scenarios

1. 40 concurrent `AsyncSQLiteCursor(lazy_begin=True).execute("SELECT ...")` calls across separate `AsyncSQLiteClient` handles on a WAL file complete concurrently without `in_transaction` flipping to `True` or taking write locks.
2. Concurrent read-then-write SQLAlchemy sessions on `sqlite+vtb_async` commit cleanly without `SQLITE_BUSY` snapshot-upgrade failures.
3. `AsyncSQLiteDialect` creates SQLite clients with `max_readers == 0` by default while honoring `?max_readers=3` URL overrides and keeping standalone `AsyncSQLiteDriver` at `max_readers == 8`.

### 2.6 Alternatives Considered

- **`BEGIN DEFERRED` on read-only `lazy_begin`**: Rejected because WAL read-to-write upgrades fail immediately with `SQLITE_BUSY`, bypassing `busy_timeout`.

## 3. Documentation & OKF Knowledge Plan

- Deferred to `ch03:t09` (`.agents/bundles/knowledge/domains/data/sqlite-limitations.md` and `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Verification Strategy |
| :--- | :--- | :--- |
| R1, R3 | `t01` | `behavior_tdd`, `regression_tdd` |
| R2 | `t02` | `behavior_tdd` |
| R1, R2, R3 | `t03` | `regression_tdd` |

## 5. Implementation Plan

- [ ] t01 — Skip `BEGIN IMMEDIATE` on read-only `lazy_begin` SQL and replace `mpsc::channel()` in `SqlitePool` (`tasks/t01-sqlite-lazy-begin-and-direct-slot.md`)
- [ ] t02 — Default `max_readers=0` in SQLAlchemy SQLite `+vtb` / `+vtb_async` dialects (`tasks/t02-sqlalchemy-sqlite-max-readers.md`)
- [ ] t03 — Add Rust and Python concurrency and transaction regression tests (`tasks/t03-sqlite-fastpath-tests.md`)

### Chapter / Phase Recovery Checkpoint

```bash
make dev-ext EXT=sqlite && cargo test -p vertebra-sqlite && uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `planned` (`0/3` tasks complete).
- **Next Task**: `t01`.

[^src-sqlite-py-client]: `src/rs/vertebra-sqlite/src/py_client.rs::AsyncSQLiteCursor` — SyncSQLiteCursor and AsyncSQLiteCursor lazy_begin logic.
[^src-sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs::SqlitePool` — SqlitePool WorkerSlot execution and statement_facts cache.
[^src-sa-sqlite-dialect]: `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::_SQLiteDialectMixin` — SQLAlchemy SQLite +vtb and +vtb_async dialect configuration.
[^src-sqlite-tests]: `src/py/tests/unit/db/test_sqlite_driver.py::test_async_cursor_lazy_begin_opens_transactions` — SQLite cursor lazy_begin and transaction unit tests.
