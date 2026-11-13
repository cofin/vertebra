---
type: Task
id: "sqlspec-adapter-parity:ch06-sqlspec-mssql:t03"
flow_id: sqlspec-adapter-parity
chapter_id: ch06-sqlspec-mssql
title: "Implement MSSQLSyncConfig / MSSQLAsyncConfig and PEP 562 Lazy Barrel"
description: "Wire src/py/vertebra/extensions/sqlspec/adapters/mssql/{config,__init__}.py with adapter_name = 'vertebra_mssql' and explicit store class attributes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t02
files:
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/litestar.py
estimated_loc: 145
tags:
  - sqlspec
  - mssql
  - config
sources:
  - id: mssql-drv
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: "Native SQL Server (MSSQL) sync and async drivers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing MSSQL SQLSpec adapter"
---

# Task `t03`: Implement MSSQLSyncConfig / MSSQLAsyncConfig and PEP 562 Lazy Barrel

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{config,__init__}.py` declaring `adapter_name = 'vertebra_mssql'` and explicit store class attributes per [`../spec.md`](../spec.md) [^mssql-drv] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Define `MSSQLSyncConfig` and `MSSQLAsyncConfig` declaring `adapter_name = 'vertebra_mssql'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_mssql'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `MSSQLSyncConfig` and `MSSQLAsyncConfig` resolve `adapter_name = 'vertebra_mssql'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the chapter fast verification gate.

[^mssql-drv]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native SQL Server (MSSQL) sync and async drivers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing MSSQL SQLSpec adapter.
