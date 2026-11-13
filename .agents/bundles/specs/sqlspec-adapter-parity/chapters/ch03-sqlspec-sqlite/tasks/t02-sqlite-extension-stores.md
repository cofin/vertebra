---
type: Task
id: "sqlspec-adapter-parity:ch03-sqlspec-sqlite:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-sqlite
title: "Implement SQLSpec SQLite ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/sqlite/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
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
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 245
tags:
  - sqlspec
  - sqlite
  - adk
  - events
  - litestar
sources:
  - id: sqlite-drv
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: "Native SQLite sync and async drivers"
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing SQLite SQLSpec adapter"
---

# Task `t02`: Implement SQLSpec SQLite ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^sqlite-drv] [^storage-art] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py` — SQLite SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Implement `SQLiteSyncADKStore`/`SQLiteAsyncADKStore`, `FTS5`-backed `SQLiteSyncADKMemoryStore`/`SQLiteAsyncADKMemoryStore`, `SQLiteSyncADKArtifactStore`/`SQLiteAsyncADKArtifactStore` (`ArtifactReference`), `SQLiteEventQueueStore`, and `SQLiteLitestarStore` (`ON CONFLICT(key) DO UPDATE`) executing exclusively via `provide_session()` over native `SyncSQLiteDriver`/`AsyncSQLiteDriver`.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `SQLiteSyncADKStore`/`SQLiteAsyncADKStore`, memory search, `ArtifactReference` metadata, `SQLiteEventQueueStore`, and `SQLiteLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Native SQLite sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing SQLite SQLSpec adapter.
