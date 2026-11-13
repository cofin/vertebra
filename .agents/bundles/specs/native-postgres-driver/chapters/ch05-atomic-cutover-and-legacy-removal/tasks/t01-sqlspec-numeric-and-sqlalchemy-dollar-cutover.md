---
type: Task
id: "native-postgres-driver:ch05-atomic-cutover-and-legacy-removal:t01"
flow_id: native-postgres-driver
chapter_id: ch05-atomic-cutover-and-legacy-removal
title: "Switch SQLSpec and SQLAlchemy PostgreSQL adapters directly to $N numeric binds"
description: "Cut PostgreSQLSyncDriver/PostgreSQLAsyncDriver to ParameterStyle.NUMERIC and SQLAlchemy PostgreSQL dialects to numeric_dollar."
status: stable
state: completed
commit: cffa8feb
last_verified_commit: cffa8feb
last_verified_at: "2026-11-13T15:43:00Z"
verification_evidence: "pytest test_postgres_compiler_transport.py test_sqlalchemy_and_sqlspec_vtb.py + ruff + mypy clean"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch04:t03"
files: 
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
tests: 
  - src/py/tests/unit/extensions/test_postgres_compiler_transport.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
read_set:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 235
tags:
  - python
  - postgres
  - sqlspec
  - sqlalchemy
sources:
  - id: sqlspec-adp
    resource: "src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver"
    title: "SQLSpec statement compilation"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "SQLAlchemy PostgreSQL dialects"
---

# Task `t01`: Switch SQLSpec and SQLAlchemy PostgreSQL adapters directly to $N numeric binds

## Objective & Scope Boundary

Switch SQLSpec and SQLAlchemy PostgreSQL parameter compilation directly to `$N` numeric binds per `../spec.md` §2 [^sqlspec-adp] [^sa-dialects].

## Bounded Read Manifest

- `sqlspec/adapters.py`, `sqlalchemy/dialects.py`, and existing extension tests.

## Contracts & Edge Cases

- Preserve `statement_transformers`, single-pass `StatementFilter`s, and `TypedParameter` unwrapping in `_compile_postgres_numeric_transport`.
- Set `default_paramstyle = paramstyle = 'numeric_dollar'` across sync and async PostgreSQL SQLAlchemy dialects with zero legacy staging branches.

## Test Matrix

- Parameterized compiler transport, RLS `$N` bind, and SQLAlchemy dialect compilation tests.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
```

## Acceptance Criteria

- [x] Both sync and async PostgreSQL SQLSpec and SQLAlchemy paths emit `$1..$N` binds cleanly.
- [x] Zero regressions in `ch05-atomic-cutover-and-legacy-removal` fast verification gate.

[^sqlspec-adp]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec statement compilation.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy PostgreSQL dialects.
