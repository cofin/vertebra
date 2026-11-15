---
type: Task
id: sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t04
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-foundation-and-relational-dialects
title: Implement Native MySQL, Oracle, and SQL Server Sync/Async SQLAlchemy Dialects
description: Build dialects/{mysql,oracle,mssql}.py with qmark/named bind compilation, Oracle RETURNING out-binds, and T-SQL OUTPUT reflection.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t03
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects/mysql.py
  - src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py
  - src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlalchemy/types.py
  - src/py/vertebra/db/adapters/mysql/_sync.py
  - src/py/vertebra/db/adapters/mysql/_async.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/oracle/_sync.py
  - src/py/vertebra/db/adapters/oracle/_async.py
  - src/py/vertebra/db/adapters/mssql/_sync.py
  - src/py/vertebra/db/adapters/mssql/_async.py
estimated_loc: 230
tags:
  - sqlalchemy
  - mysql
  - dialects
  - oracle
  - mssql
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: Current MySQL dialect definitions
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/_sync.py
    title: Native vertebra._mysql SyncMySQLDriver and AsyncMySQLDriver
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/_sync.py
    title: Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/_sync.py
    title: Native vertebra._mssql SyncMSSQLDriver and AsyncMSSQLDriver
verification_strategy: regression_tdd
---

# Task `t04`: Implement Native MySQL, Oracle, and SQL Server Sync/Async SQLAlchemy Dialects

## Objective & Scope Boundary

Implement `SyncMySQLDialect`/`AsyncMySQLDialect` (`MySQLDialect`), `SyncOracleDialect`/`AsyncOracleDialect` (`OracleDialect`), and `SyncMSSQLDialect`/`AsyncMSSQLDialect` (`MSDialect`) in `dialects/{mysql,oracle,mssql}.py` per [`../spec.md` §2](../spec.md) [^sa-dialects] [^mysql-driver] [^oracle-driver] [^mssql-driver]. Out of scope: SQLSpec adapters (`sqlspec-adapter-parity`) and analytical/cloud dialects (`ch02`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — MySQL dialect definitions (plus shared mixins from `t02`).
- `src/py/vertebra/extensions/sqlalchemy/types.py` — `OracleVector` type registration.
- `src/py/vertebra/db/adapters/mysql/_sync.py` and `src/py/vertebra/db/adapters/mysql/_async.py` — `vertebra._mysql` binary `?` prepared statement execution and `lastrowid` metadata.
- `src/py/vertebra/db/adapters/mysql/core.py` — MySQL / MariaDB version and charset helpers.
- `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — `:name` named binds and 23ai `VECTOR` / `BOOLEAN` / `OSON` capabilities.
- `src/py/vertebra/db/adapters/mssql/_sync.py` and `src/py/vertebra/db/adapters/mssql/_async.py` — `qmark` `?` -> `@P1` placeholder rewriting and `OUTPUT INSERTED` handling.

## Contracts & Edge Cases

- `SyncMySQLDialect(_SyncDialectMixin, MySQLDialect)` and `AsyncMySQLDialect(_AsyncDialectMixin, MySQLDialect)` set `paramstyle = 'qmark'` matching `vertebra._mysql` binary prepared statements, parse MySQL 8/9 vs MariaDB server versions for `RETURNING`, and propagate `lastrowid`.
- `SyncOracleDialect(_SyncDialectMixin, OracleDialect)` and `AsyncOracleDialect(_AsyncDialectMixin, OracleDialect)` use `paramstyle = 'named'` (`:name`), handle `RETURNING ... INTO` outbinds, and register 23ai `VECTOR` (`OracleVector`), `BOOLEAN`, and `JSON` reflection.
- `SyncMSSQLDialect(_SyncDialectMixin, MSDialect)` and `AsyncMSSQLDialect(_AsyncDialectMixin, MSDialect)` use `paramstyle = 'qmark'` (rewritten to `@P1..@Pn` by `MSSQLCore`), `OUTPUT INSERTED`, and `NVARCHAR` / `UNIQUEIDENTIFIER` reflection.

## Test Matrix

- Parametrized compilation (`?` qmark vs `:name` binds, backtick vs identifier quoting, MariaDB vs MySQL 8 `RETURNING`, `RETURNING INTO` out-binds, `OUTPUT INSERTED`), `create_connect_args`, and `OracleVector`/UUID reflection tests across all three dialect pairs.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
```

## Acceptance Criteria

- [ ] `dialects/{mysql,oracle,mssql}.py` subclass built-in `MySQLDialect`, `OracleDialect`, and `MSDialect` with leading `Sync*`/`Async*` names and pass Ruff/Mypy/pytest checks.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current MySQL dialect definitions.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/_sync.py` and `src/py/vertebra/db/adapters/mysql/_async.py` — Native vertebra._mysql SyncMySQLDriver and AsyncMySQLDriver.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/_sync.py` and `src/py/vertebra/db/adapters/mssql/_async.py` — Native vertebra._mssql SyncMSSQLDriver and AsyncMSSQLDriver.
