---
type: Task
id: t03
chapter_id: ch01-sqlite-sqlalchemy-fastpath
title: Add SQLite lazy_begin read-concurrency and SQLAlchemy pool regression tests
description: Add Python regression tests verifying read-only lazy_begin concurrency without write locks, read-then-write SQLAlchemy sessions without SQLITE_BUSY, and max_readers=0 pool slot isolation.
status: stable
state: completed
priority: high
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - t01
  - t02
read_set:
  - src/py/tests/unit/db/test_sqlite_driver.py
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
files:
  - src/py/tests/unit/db/test_sqlite_driver.py
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
estimated_loc: 120
plan_revision: 2
plan_commit: "16ccb56e"
commit: "0fa384d3"
sources:
  - id: src-sqlite-tests
    resource: src/py/tests/unit/db/test_sqlite_driver.py
    title: SQLite native driver and cursor unit tests
  - id: src-sa-tests
    resource: src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
    title: SQLAlchemy +vtb dialect and DBAPI unit tests
tags:
  - sqlite
  - sqlalchemy
  - testing
---

# t03: Add SQLite lazy_begin read-concurrency and SQLAlchemy pool regression tests

## Objective & Scope Boundary

Lock in the `ch01` SQLite read-only `lazy_begin` and `max_readers=0` behavioral contracts with observable concurrency and transaction tests.

In scope:
- `src/py/tests/unit/db/test_sqlite_driver.py`: tests for sync/async `cursor(lazy_begin=True).execute("SELECT ...")` leaving `in_transaction is False`, read-then-write `SELECT` -> `INSERT`/`UPDATE` opening `in_transaction is True` and rolling back cleanly, and explicit `begin()` + `SELECT` staying inside the active transaction.
- `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py`: tests for `create_connect_args` `max_readers=0` default + explicit override, concurrent `sqlite+vtb_async` read sessions (20 concurrent coroutines against a WAL file while another connection holds a `BEGIN IMMEDIATE` write transaction — proving reads do not block on writer lock), and concurrent read-then-write update sessions committing cleanly without `SQLITE_BUSY`.

## Bounded Read Manifest

- `src/py/tests/unit/db/test_sqlite_driver.py` (lines 375-420, 760-800, 1065-1105) [^src-sqlite-tests]
- `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py` (SQLite test section) [^src-sa-tests]

## Contracts & Edge Cases

- Prove read concurrency deterministically: hold an active `BEGIN IMMEDIATE` write lock on connection A while running `SELECT` on SQLAlchemy `sqlite+vtb_async` connection B; before `t01` this blocks until `busy_timeout`, after `t01` it returns immediately in `< 100 ms`.
- Zero inline `#` comments in Python test code.

## Test Matrix

- `test_sync_and_async_cursor_lazy_begin_skips_read_only_select` (including `SELECT created_at, updated_at, deleted_at FROM ...` and `/* c */ SELECT ...`)
- `test_lazy_begin_read_then_write_opens_rollbackable_tx` (including `WITH c AS (SELECT 1) REPLACE INTO ...`)
- `test_sqlite_dialect_defaults_max_readers_zero_and_respects_override` (verifying `max_readers=0` default, `?max_readers=3` URL extraction + stripping, and `connect_args` override)
- `test_async_sqlite_sqlalchemy_reads_succeed_while_writer_holds_lock`
- `test_async_sqlite_sqlalchemy_concurrent_read_modify_write_sessions`

## Verification

```bash
make dev-ext EXT=sqlite && cargo test -p vertebra-sqlite && uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
```

## Acceptance Criteria

- All new and existing SQLite Rust and Python tests pass with zero warnings.
- `ruff`, `mypy`, and `pyright` pass on touched test files.

[^src-sqlite-tests]: `src/py/tests/unit/db/test_sqlite_driver.py` — SQLite native driver and cursor unit tests.
[^src-sa-tests]: `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py` — SQLAlchemy +vtb dialect and DBAPI unit tests.
