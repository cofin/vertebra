---
type: Task
id: "sqlalchemy-and-django-integration:ch02-sqlalchemy-core-relational-dialects:t03"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-sqlalchemy-core-relational-dialects
title: "Implement SyncOracleDialect / AsyncOracleDialect and SyncMSSQLDialect / AsyncMSSQLDialect"
description: "Subclass SQLAlchemy built-in OracleDialect and MSDialect in dialects/oracle.py and dialects/mssql.py over vertebra.db.dbapi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-dbapi-hardening-and-dialect-base:t03"
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py
  - src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
  - src/py/vertebra/extensions/sqlalchemy/types.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/mssql/driver.py
estimated_loc: 230
tags:
  - sqlalchemy
  - oracle
  - mssql
  - dialects
sources:
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver"
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: "Native vertebra._mssql SyncMSSQLDriver and AsyncMSSQLDriver"
---

# Task `t03`: Implement SyncOracleDialect / AsyncOracleDialect and SyncMSSQLDialect / AsyncMSSQLDialect

## Objective & Scope Boundary

Implement `SyncOracleDialect` / `AsyncOracleDialect` (`OracleDialect`) and `SyncMSSQLDialect` / `AsyncMSSQLDialect` (`MSDialect`) per [`../spec.md`](../spec.md) [^oracle-driver] [^mssql-driver]. Out of scope: analytical/cloud dialects (`ch03`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — shared dialect mixins.
- `src/py/vertebra/extensions/sqlalchemy/types.py` — `OracleVector` type registration.
- `src/py/vertebra/db/adapters/oracle/driver.py` — `:name` named binds and 23ai `VECTOR` / `BOOLEAN` / `OSON` capabilities.
- `src/py/vertebra/db/adapters/mssql/driver.py` — `qmark` `?` -> `@P1` placeholder rewriting and `OUTPUT INSERTED` handling.

## Contracts & Edge Cases

- `SyncOracleDialect(_SyncDialectMixin, OracleDialect)` and `AsyncOracleDialect(_AsyncDialectMixin, OracleDialect)` use `paramstyle = 'named'` (`:name`), handle `RETURNING ... INTO` outbinds, and register 23ai `VECTOR` (`OracleVector`), `BOOLEAN`, and `JSON` reflection.
- `SyncMSSQLDialect(_SyncDialectMixin, MSDialect)` and `AsyncMSSQLDialect(_AsyncDialectMixin, MSDialect)` use `paramstyle = 'qmark'` (rewritten to `@P1..@Pn` by `MSSQLCore`), `OUTPUT INSERTED`, and `NVARCHAR` / `UNIQUEIDENTIFIER` reflection.

## Test Matrix

- Parametrized compilation, parameter bind style, `RETURNING`/`OUTPUT`, and vector/UUID type reflection tests across `SyncOracleDialect`, `AsyncOracleDialect`, `SyncMSSQLDialect`, and `AsyncMSSQLDialect`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `dialects/oracle.py` and `dialects/mssql.py` subclass built-in `OracleDialect` and `MSDialect` with leading `Sync*`/`Async*` names.
- [ ] Zero regressions in the chapter fast verification gate.

[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native vertebra._mssql SyncMSSQLDriver and AsyncMSSQLDriver.
