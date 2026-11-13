---
type: Task
id: "sqlspec-adapter-parity:ch06-sqlspec-mssql:t02"
flow_id: sqlspec-adapter-parity
chapter_id: ch06-sqlspec-mssql
title: "Implement SQLSpec MSSQL ADK, Events & Litestar Extension Stores"
description: "Implement SDK-free src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py subclassing upstream SQLSpec store bases."
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
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/litestar.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 245
tags:
  - sqlspec
  - mssql
  - adk
  - events
  - litestar
sources:
  - id: mssql-drv
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: "Native SQL Server (MSSQL) sync and async drivers"
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: "Storage-neutral ArtifactReference value struct"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing MSSQL SQLSpec adapter"
---

# Task `t02`: Implement SQLSpec MSSQL ADK, Events & Litestar Extension Stores

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^mssql-drv] [^storage-art] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py` — SQL Server (MSSQL) SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

## Contracts & Edge Cases

- Implement SDK-free `MSSQLSyncADKStore`/`MSSQLAsyncADKStore` (`MERGE ... WITH (HOLDLOCK)`, `NVARCHAR(MAX)` JSON, `DATETIMEOFFSET(6)`), `MSSQLSyncADKMemoryStore`/`MSSQLAsyncADKMemoryStore`, `MSSQLSyncADKArtifactStore`/`MSSQLAsyncADKArtifactStore`, `MSSQLEventQueueStore` (`IF OBJECT_ID(...) IS NULL` table/index DDL guards), and `MSSQLLitestarStore` (`VARBINARY(MAX)`).
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

## Test Matrix

- Parametrized unit/contract tests for `MSSQLSyncADKStore`/`MSSQLAsyncADKStore`, memory search, `ArtifactReference` metadata, `MSSQLEventQueueStore`, and `MSSQLLitestarStore` TTL/upsert/delete.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^mssql-drv]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native SQL Server (MSSQL) sync and async drivers.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing MSSQL SQLSpec adapter.
