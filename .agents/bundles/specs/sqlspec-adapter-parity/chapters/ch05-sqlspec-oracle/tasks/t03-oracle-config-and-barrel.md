---
type: Task
id: "sqlspec-adapter-parity:ch05-sqlspec-oracle:t03"
flow_id: sqlspec-adapter-parity
chapter_id: ch05-sqlspec-oracle
title: "Implement OracleSyncConfig / OracleAsyncConfig and PEP 562 Lazy Barrel"
description: "Wire src/py/vertebra/extensions/sqlspec/adapters/oracle/{config,__init__}.py with adapter_name = 'vertebra_oracle' and explicit store class attributes."
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
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/litestar.py
estimated_loc: 145
tags:
  - sqlspec
  - oracle
  - config
sources:
  - id: oracle-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Native Oracle sync and async drivers"
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: "Oracle TxEventQ notification source"
---

# Task `t03`: Implement OracleSyncConfig / OracleAsyncConfig and PEP 562 Lazy Barrel

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{config,__init__}.py` declaring `adapter_name = 'vertebra_oracle'` and explicit store class attributes per [`../spec.md`](../spec.md) [^oracle-drv] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/oracle/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Define `OracleSyncConfig` and `OracleAsyncConfig` declaring `adapter_name = 'vertebra_oracle'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_oracle'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `OracleSyncConfig` and `OracleAsyncConfig` resolve `adapter_name = 'vertebra_oracle'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the chapter fast verification gate.

[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle sync and async drivers.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
