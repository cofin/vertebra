---
type: Task
id: "sqlspec-adapter-parity:ch02-sqlspec-postgres:t03"
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-postgres
title: "Implement PostgresSyncConfig / PostgresAsyncConfig and PEP 562 Lazy Barrel"
description: "Wire src/py/vertebra/extensions/sqlspec/adapters/postgres/{config,__init__}.py with adapter_name = 'vertebra_postgres' and explicit store class attributes."
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
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/litestar.py
estimated_loc: 145
tags:
  - sqlspec
  - postgres
  - config
sources:
  - id: postgres-drv
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Native PostgreSQL sync and async drivers"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification source"
---

# Task `t03`: Implement PostgresSyncConfig / PostgresAsyncConfig and PEP 562 Lazy Barrel

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{config,__init__}.py` declaring `adapter_name = 'vertebra_postgres'` and explicit store class attributes per [`../spec.md`](../spec.md) [^postgres-drv] [^pg-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/postgres/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Define `PostgresSyncConfig` and `PostgresAsyncConfig` declaring `adapter_name = 'vertebra_postgres'`, `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class`, using PEP 562 `__getattr__` in `__init__.py` so `google.adk` is loaded lazily and guarded via `MissingDependencyError`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_postgres'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `PostgresSyncConfig` and `PostgresAsyncConfig` resolve `adapter_name = 'vertebra_postgres'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the chapter fast verification gate.

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL sync and async drivers.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
