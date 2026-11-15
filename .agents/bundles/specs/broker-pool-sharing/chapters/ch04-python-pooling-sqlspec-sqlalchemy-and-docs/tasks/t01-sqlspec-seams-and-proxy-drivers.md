---
type: Task
id: broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t01
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: Expose Typed SQLSpec Provider Seams and Refactor ProxySyncDriver / ProxyAsyncDriver
description: Add public borrow_lease/execute_leased/stream_arrow seams on SQLSpec providers and rebase ProxySyncDriver/ProxyAsyncDriver onto base drivers with VTBARROW ArrowCursor.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch03-lease-safety-and-uds-attach:t04
files:
- src/py/vertebra/extensions/sqlspec/provider.py
- src/py/vertebra/extensions/sqlspec/proxy.py
- src/py/vertebra/extensions/sqlspec/plugin.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
- src/py/vertebra/extensions/sqlspec/provider.py
- src/py/vertebra/extensions/broker/transport.py
- src/py/vertebra/extensions/broker/errors.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/vertebra/extensions/sqlspec/proxy.py
- src/py/vertebra/extensions/sqlspec/plugin.py
estimated_loc: 225
tags:
- sqlspec
- quarantine
- python
- proxy
sources:
- id: sqlspec-provider
  resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
  title: Broker-side SQLSpec provider session and lease management
- id: sqlspec-proxy
  resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
  title: Worker-side SQLSpec proxy drivers
- id: sqlspec-plugin
  resource: src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig
  title: ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs
---

# Task `t01`: Expose Typed SQLSpec Provider Seams and Refactor ProxySyncDriver / ProxyAsyncDriver

## Objective & Scope Boundary

Replace signature reflection and suppressed release errors across sync and async paths in `provider.py` with `SyncBrokerLeaseSource`, `AsyncBrokerLeaseSource`, `SyncLeasedSession`, and `AsyncLeasedSession` per [`../spec.md` §2](../spec.md) ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].

Subclass `SyncDriverAdapterBase` and `AsyncDriverAdapterBase` on `ProxySyncDriver` and `ProxyAsyncDriver` and remove silent rollback/exit suppression per [`../spec.md` §2](../spec.md) [^sqlspec-proxy] [^sqlspec-plugin].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/provider.py` — `_call_broker_acquire`, `_call_with_params`, and `_release_*` methods.
- `src/py/vertebra/extensions/broker/transport.py` — `SyncConnectionLease` and `AsyncConnectionLease`.
- `src/py/vertebra/extensions/broker/errors.py` — `LeaseQuarantinedError`.
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — SQLSpec provider unit tests.

- `src/py/vertebra/extensions/sqlspec/proxy.py` — `ProxySyncDriver` and `ProxyAsyncDriver`.
- `src/py/vertebra/extensions/sqlspec/plugin.py` — `ProxySyncConfig` and `ProxyAsyncConfig`.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — Adapter base patterns in Vertebra SQLSpec integration.
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Proxy driver tests.

## Contracts & Edge Cases

- Delete `_call_broker_acquire`, `_call_with_params`, and every `contextlib.suppress(Exception)` wrapper in `_release_session_sync`, `_release_session_async`, `_release_broker_lease`, and `_release_broker_lease_async`.
- On rollback or exit exception, invoke `broker.quarantine(lease)` and `raise LeaseQuarantinedError(...) from exc` without calling `lease.release()`.
- **Edge Case**: Parameter dispatch handles `None`, `()`, `Mapping`, and `Sequence` deterministically without signature inspection.
- **Edge Case**: If `quarantine` itself raises, propagate with the rollback error preserved on `__context__`.

- Ensure `isinstance(driver, SyncDriverAdapterBase)` and `isinstance(driver, AsyncDriverAdapterBase)` hold and `driver.statement_config` matches the wrapped upstream config.
- Remove `contextlib.suppress(Exception)` around transaction rollback and session exit in `proxy.py`.
- **Edge Case**: Wrapped configs with numeric (`$1`), named (`:name`), or positional (`?`) parameter styles preserve exact SQL compilation.

## Test Matrix

- `@pytest.mark.parametrize` over sync/async rollback failures and `__exit__`/`__aexit__` failures asserts `quarantine` count == 1, `release` count == 0, and chained `LeaseQuarantinedError`.
- Confirm zero `inspect.signature` calls (`PY-10`).

- `@pytest.mark.parametrize` across wrapped sync and async SQLSpec configs verifies `isinstance` checks, `statement_config` parity, and fail-loud rollback error propagation.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec
```

## Acceptance Criteria

- [ ] Zero `inspect.signature` or `contextlib.suppress(Exception)` remain in `provider.py`.
- [ ] Uncertain sync and async session releases always quarantine and raise `LeaseQuarantinedError`.
- [ ] `ProxySyncDriver` and `ProxyAsyncDriver` inherit SQLSpec adapter bases with zero suppressed errors.
- [ ] Zero regressions in the fast verification gate.

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
