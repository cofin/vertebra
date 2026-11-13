---
type: Task
id: "sqlspec-adapter-parity:ch09-sqlspec-spanner:t03"
flow_id: sqlspec-adapter-parity
chapter_id: ch09-sqlspec-spanner
title: "Implement SpannerSyncConfig / SpannerAsyncConfig and PEP 562 Lazy Barrel"
description: "Wire src/py/vertebra/extensions/sqlspec/adapters/spanner/{config,__init__}.py with adapter_name = 'vertebra_spanner' and explicit store class attributes."
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
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/litestar.py
estimated_loc: 145
tags:
  - sqlspec
  - spanner
  - config
sources:
  - id: spanner-drv
    resource: src/py/vertebra/db/adapters/spanner/driver.py
    title: "Native Google Cloud Spanner & Spangres sync and async drivers"
  - id: sqlspec-spn
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import"
---

# Task `t03`: Implement SpannerSyncConfig / SpannerAsyncConfig and PEP 562 Lazy Barrel

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{config,__init__}.py` declaring `adapter_name = 'vertebra_spanner'` and explicit store class attributes per [`../spec.md`](../spec.md) [^spanner-drv] [^sqlspec-spn].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Define `SpannerSyncConfig`/`SpannerAsyncConfig` and `SpangresSyncConfig`/`SpangresAsyncConfig` declaring `adapter_name = 'vertebra_spanner'`, isolated per-session transaction state on `provide_session`, and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_spanner'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py
```

## Acceptance Criteria

- [ ] `SpannerSyncConfig` and `SpannerAsyncConfig` resolve `adapter_name = 'vertebra_spanner'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the chapter fast verification gate.

[^spanner-drv]: `src/py/vertebra/db/adapters/spanner/driver.py` — Native Google Cloud Spanner & Spangres sync and async drivers.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
