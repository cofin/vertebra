---
type: Task
id: "broker-pool-sharing:ch06:t02"
flow_id: broker-pool-sharing
chapter_id: ch06-sqlspec-seams-and-multiprocess
title: "Re-Parent ProxySyncDriver and ProxyAsyncDriver onto SQLSpec Adapter Bases"
description: "Subclass SyncDriverAdapterBase and AsyncDriverAdapterBase in proxy.py while preserving wrapped statement_config."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch06:t01"
files:
  - src/py/vertebra/extensions/sqlspec/proxy.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/proxy.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
estimated_loc: 225
tags:
  - sqlspec
  - proxy
  - python
sources:
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
    title: "Worker-side SQLSpec proxy drivers"
  - id: sqlspec-plugin
    resource: src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig
    title: "ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs"
---

# Task `t02`: Re-Parent ProxySyncDriver and ProxyAsyncDriver onto SQLSpec Adapter Bases

## Objective & Scope Boundary

Subclass `SyncDriverAdapterBase` and `AsyncDriverAdapterBase` on `ProxySyncDriver` and `ProxyAsyncDriver` and remove silent rollback/exit suppression per [`../spec.md` §2](../spec.md) [^sqlspec-proxy] [^sqlspec-plugin].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/proxy.py` — `ProxySyncDriver` and `ProxyAsyncDriver`.
- `src/py/vertebra/extensions/sqlspec/plugin.py` — `ProxySyncConfig` and `ProxyAsyncConfig`.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — Adapter base patterns in Vertebra SQLSpec integration.
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Proxy driver tests.

## Contracts & Edge Cases

- Ensure `isinstance(driver, SyncDriverAdapterBase)` and `isinstance(driver, AsyncDriverAdapterBase)` hold and `driver.statement_config` matches the wrapped upstream config.
- Remove `contextlib.suppress(Exception)` around transaction rollback and session exit in `proxy.py`.
- **Edge Case**: Wrapped configs with numeric (`$1`), named (`:name`), or positional (`?`) parameter styles preserve exact SQL compilation.

## Test Matrix

- `@pytest.mark.parametrize` across wrapped sync and async SQLSpec configs verifies `isinstance` checks, `statement_config` parity, and fail-loud rollback error propagation.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec
```

## Acceptance Criteria

- [ ] `ProxySyncDriver` and `ProxyAsyncDriver` inherit SQLSpec adapter bases with zero suppressed errors.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
