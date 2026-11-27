---
type: Task
id: t02
chapter_id: ch01-sqlite-sqlalchemy-fastpath
title: Default max_readers=0 in SQLAlchemy SQLite dialects
description: Override create_connect_args on _SQLiteDialectMixin so SQLAlchemy-pooled SQLite +vtb and +vtb_async connections open one physical connection per pool slot unless max_readers is explicitly configured.
status: stable
state: completed
priority: high
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
  - src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
  - src/py/vertebra/db/adapters/sqlite/config.py
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
estimated_loc: 35
plan_revision: 2
plan_commit: "16ccb56e"
commit: "66a8acf6"
sources:
  - id: src-sa-sqlite-dialect
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::_SQLiteDialectMixin
    title: SQLAlchemy SQLite +vtb and +vtb_async dialect mixin
  - id: src-sa-base-dialect
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/_base.py::_CommonDialectMixin
    title: Shared SQLAlchemy dialect create_connect_args implementation
tags:
  - sqlite
  - sqlalchemy
  - python
---

# t02: Default max_readers=0 in SQLAlchemy SQLite dialects

## Objective & Scope Boundary

Eliminate the 9× connection fan-out (1 writer + 8 readers per SQLAlchemy pool slot) on `sqlite+vtb` and `sqlite+vtb_async` while keeping standalone `SyncSQLiteDriver` / `AsyncSQLiteDriver` defaults (`max_readers=8`) unchanged.

In scope:
- `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py`: add `create_connect_args` on `_SQLiteDialectMixin` (typed cleanly for mypy/pyright with MRO calling `super().create_connect_args(url)`).

Out of scope:
- Standalone `SQLiteConfig.max_readers` default (`8`) used by direct native and SQLSpec drivers.

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py` (full file) [^src-sa-sqlite-dialect]
- `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` (lines 164-200) [^src-sa-base-dialect]

## Contracts & Edge Cases

- Implement `_SQLiteDialectMixin.create_connect_args` per `../spec.md` Section 2.2.
- Extract any case-insensitive `max_readers` entry from `url.query`, strip it via `url.difference_update_query(max_readers_keys)` before calling `super().create_connect_args(clean_url)` (so it never leaks into SQLite `file:` URIs), convert to `int` in `kwargs["max_readers"]`, and otherwise inject `kwargs["max_readers"] = 0`.
- SQLAlchemy merges `connect_args` from `create_engine(..., connect_args={...})` *after* `create_connect_args`, so caller `connect_args={"max_readers": 4}` overrides the `0` default naturally.
- Follow Python style rules: PEP 585/604 types, zero inline `#` comments.

## Test Matrix

- Unit test verifying `SyncSQLiteDialect().create_connect_args(make_url("sqlite+vtb:///tmp.db"))` returns `max_readers=0` in kwargs.
- Unit test verifying `AsyncSQLiteDialect().create_connect_args(make_url("sqlite+vtb_async:///tmp.db?max_readers=3"))` strips `max_readers` from `kwargs["url"]` and sets `kwargs["max_readers"] == 3`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py -k sqlite && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
```

## Acceptance Criteria

- `SyncSQLiteDialect` and `AsyncSQLiteDialect` default `max_readers=0` when unspecified.
- Explicit URL query or `connect_args` `max_readers` overrides are preserved.
- `ruff`, `mypy`, and `pyright` pass cleanly.

[^src-sa-sqlite-dialect]: `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::_SQLiteDialectMixin` — SQLAlchemy SQLite +vtb and +vtb_async dialect mixin.
[^src-sa-base-dialect]: `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py::_CommonDialectMixin` — Shared SQLAlchemy dialect create_connect_args implementation.
