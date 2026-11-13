---
type: Task
id: "sqlalchemy-and-django-integration:ch02-sqlalchemy-core-relational-dialects:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-sqlalchemy-core-relational-dialects
title: "Implement SyncMySQLDialect and AsyncMySQLDialect"
description: "Subclass SQLAlchemy built-in MySQLDialect in dialects/mysql.py with qmark prepared binds over vertebra.db.dbapi."
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
  - src/py/vertebra/extensions/sqlalchemy/dialects/mysql.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/mysql/core.py
estimated_loc: 165
tags:
  - sqlalchemy
  - mysql
  - dialects
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Current MySQL dialect definitions"
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: "Native vertebra._mysql SyncMySQLDriver and AsyncMySQLDriver"
---

# Task `t02`: Implement SyncMySQLDialect and AsyncMySQLDialect

## Objective & Scope Boundary

Implement `SyncMySQLDialect` and `AsyncMySQLDialect` subclassing `sqlalchemy.dialects.mysql.base.MySQLDialect` per [`../spec.md`](../spec.md) [^sa-dialects] [^mysql-driver]. Out of scope: SQLSpec MySQL adapter (`sqlspec-adapter-parity`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — shared dialect mixins.
- `src/py/vertebra/db/adapters/mysql/driver.py` — `vertebra._mysql` binary `?` prepared statement execution and `lastrowid` metadata.
- `src/py/vertebra/db/adapters/mysql/core.py` — MySQL / MariaDB version and charset helpers.

## Contracts & Edge Cases

- `SyncMySQLDialect(_SyncDialectMixin, MySQLDialect)` and `AsyncMySQLDialect(_AsyncDialectMixin, MySQLDialect)` set `paramstyle = 'qmark'` matching `vertebra._mysql` binary prepared statements, parse MySQL 8/9 vs MariaDB server versions for `RETURNING`, and propagate `lastrowid`.

## Test Matrix

- Parametrized compilation (`?` qmark binds, backtick quoting, MariaDB vs MySQL 8 `RETURNING`) and `create_connect_args` tests for `SyncMySQLDialect` and `AsyncMySQLDialect`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `SyncMySQLDialect` and `AsyncMySQLDialect` compile `?` placeholders and pass strict Mypy/Ruff checks.
- [ ] Zero regressions in the chapter fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current MySQL dialect definitions.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py` — Native vertebra._mysql SyncMySQLDriver and AsyncMySQLDriver.
