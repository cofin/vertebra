---
type: Task
id: t01
chapter_id: ch01-sqlite-sqlalchemy-fastpath
title: Skip BEGIN IMMEDIATE on read-only lazy_begin and remove dead SqlitePool channels
description: Update SyncSQLiteCursor and AsyncSQLiteCursor to skip implicit BEGIN IMMEDIATE for read-only statements and replace WorkerSlot mpsc channels with direct locked execution.
status: stable
state: completed
priority: high
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/rs/vertebra-sqlite/src/worker.rs
files:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/rs/vertebra-sqlite/src/worker.rs
tests:
  - src/rs/vertebra-sqlite/src/worker.rs
estimated_loc: 180
plan_revision: 2
plan_commit: "16ccb56e"
commit: "b14aa2c3"
sources:
  - id: src-sqlite-py-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs::AsyncSQLiteCursor
    title: SyncSQLiteCursor and AsyncSQLiteCursor execute implementation
  - id: src-sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqlitePool
    title: SqlitePool WorkerSlot execution
tags:
  - sqlite
  - rust
  - performance
---

# t01: Skip BEGIN IMMEDIATE on read-only lazy_begin and remove dead SqlitePool channels

## Objective & Scope Boundary

Eliminate write-lock serialization on read-only SQLite `lazy_begin` queries and remove dead per-statement `mpsc::channel()` allocations in `SqlitePool`.

In scope:
- `src/rs/vertebra-sqlite/src/worker.rs`:
  - Replace naive substring checks in `is_read_only_sql` with comment/quote-aware whole-token lexical scanning: skip leading whitespace and `--`/`/* */` comments, ignore single-quoted string literals and `"..."`/`[...]`/`` `...` `` quoted identifiers, require leading `SELECT`/`EXPLAIN`/`VALUES`/`WITH`, and reject `INSERT`/`UPDATE`/`DELETE`/`REPLACE`/`DROP`/`ALTER`/`CREATE`/`ATTACH`/`DETACH`/`VACUUM`/`REINDEX`/`PRAGMA` only when matched as standalone identifier tokens.
  - Replace `WorkerTask` + `mpsc::channel()` with direct `WorkerSlot::with_conn` execution under `self.conn.lock()` and `run_flagged_op`.
- `src/rs/vertebra-sqlite/src/py_client.rs`: `SyncSQLiteCursor::execute` and `AsyncSQLiteCursor::execute` compute `let begin = this.lazy_begin && !this.pool.statement_facts(sql).read_only;`.

Out of scope:
- Explicit `driver.begin()` / `client.begin()` transaction modes.
- `executemany` (continues `begin = this.lazy_begin && !params_seq.is_empty()`).

## Bounded Read Manifest

- `src/rs/vertebra-sqlite/src/py_client.rs` (lines 790-915, 1020-1170) [^src-sqlite-py-client]
- `src/rs/vertebra-sqlite/src/worker.rs` (lines 100-380, 730-1250) [^src-sqlite-worker]

## Contracts & Edge Cases

- Implement `WorkerSlot::with_conn` per `../spec.md` Section 2.2.
- Preserve `InterruptState::begin_op` cancellation checks, `last_rowid` updates on writer slots, and `pool.in_transaction()` routing (when already in a transaction, read-only `SELECT` continues routing to `pool.writer`).
- Whole-token lexical scan must treat `SELECT created_at, updated_at, deleted_at FROM items` and `/* c */ SELECT 1` as `read_only == true` and `WITH c AS (SELECT 1) REPLACE INTO items VALUES (1)` as `read_only == false`.

## Test Matrix

- Rust unit tests in `src/rs/vertebra-sqlite/src/worker.rs`: `SELECT 1`, `SELECT created_at, updated_at, deleted_at FROM items`, `/* c */ SELECT 1`, `-- c\nSELECT 'UPDATE'`, `WITH c AS (SELECT 1) INSERT ...`, `WITH c AS (SELECT 1) REPLACE INTO items VALUES (1)`, direct `with_conn` cancellation returning `SqliteClientError::Cancelled`, and `last_insert_rowid` updates on `INSERT`.

## Verification

```bash
cargo test -p vertebra-sqlite && make dev-ext EXT=sqlite
```

## Acceptance Criteria

- Read-only `SELECT` on `lazy_begin=True` cursor leaves `in_transaction == false`.
- Write `INSERT`/`UPDATE`/`DELETE` on `lazy_begin=True` cursor opens `BEGIN IMMEDIATE` and sets `in_transaction == true`.
- `WorkerTask` and `std::sync::mpsc` are removed from `src/rs/vertebra-sqlite/src/worker.rs` with zero clippy warnings.

[^src-sqlite-py-client]: `src/rs/vertebra-sqlite/src/py_client.rs::AsyncSQLiteCursor` — SyncSQLiteCursor and AsyncSQLiteCursor execute implementation.
[^src-sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs::SqlitePool` — SqlitePool WorkerSlot execution.
