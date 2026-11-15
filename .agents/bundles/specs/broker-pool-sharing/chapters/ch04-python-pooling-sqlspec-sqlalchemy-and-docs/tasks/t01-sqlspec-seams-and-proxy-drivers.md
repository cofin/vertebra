---
type: Task
id: broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t01
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: Expose Typed SQLSpec Provider Seams and Refactor ProxySyncDriver / ProxyAsyncDriver
description: Replace inspect.signature and suppressed release errors in SQLSpec providers with typed Protocols and quarantine, drop invalid gc=False on BrokerRpc structs, and rebase ProxySyncDriver/ProxyAsyncDriver onto SQLSpec driver base classes.
status: stable
state: completed
commit: 1b3820b7
last_verified_commit: 1b3820b7
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
- src/py/vertebra/extensions/sqlspec/proxy.py
- src/py/vertebra/extensions/sqlspec/plugin.py
- src/py/vertebra/extensions/broker/transport.py
- src/py/vertebra/extensions/broker/errors.py
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
estimated_loc: 230
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

Replace signature reflection and suppressed release errors across sync and async paths in `provider.py` with `@runtime_checkable` `SyncBrokerLeaseSource`, `AsyncBrokerLeaseSource`, `SyncLeasedSession`, and `AsyncLeasedSession` Protocols, fix invalid `gc=False` container structs on `BrokerRpcRequest`/`BrokerRpcResponse`, subclass `SyncDriverAdapterBase` and `AsyncDriverAdapterBase` on `ProxySyncDriver` / `ProxyAsyncDriver` with clean rollback propagation, and remove stale `SyncInMemoryBrokerTransport` / `AsyncInMemoryBrokerTransport` type unions from `plugin.py` per [`../spec.md` §2](../spec.md) ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider] [^sqlspec-proxy] [^sqlspec-plugin].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/provider.py` — `_call_broker_acquire`, `_call_with_params`, `_release_*`, and `BrokerRpcRequest`/`BrokerRpcResponse`.
- `src/py/vertebra/extensions/sqlspec/proxy.py` — `ProxySyncDriver` and `ProxyAsyncDriver`.
- `src/py/vertebra/extensions/sqlspec/plugin.py` — `ProxySyncConfig`, `ProxyAsyncConfig`, and stale `InMemoryBrokerTransport` imports.
- `src/py/vertebra/extensions/broker/transport.py` — `SyncConnectionLease` and `AsyncConnectionLease`.
- `src/py/vertebra/extensions/broker/errors.py` — `LeaseQuarantinedError`.
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — SQLSpec provider and proxy driver unit tests.

## Contracts & Edge Cases

- Delete `_call_broker_acquire`, `_call_with_params`, and every `contextlib.suppress(Exception)` wrapper in `_release_session_sync`, `_release_session_async`, `_release_broker_lease`, and `_release_broker_lease_async`.
- Remove `gc=False` from `BrokerRpcRequest` and `BrokerRpcResponse` (`frozen=True` only) so `list`/`dict`/`Any` container fields never bypass cycle collection (`PY-04`).
- On rollback or exit exception, invoke `broker.quarantine(lease)` and `raise LeaseQuarantinedError(...) from exc` without calling `lease.release()`.
- Ensure `isinstance(driver, SyncDriverAdapterBase)` and `isinstance(driver, AsyncDriverAdapterBase)` hold on proxy drivers, `driver.statement_config` matches the wrapped upstream config, and `contextlib.suppress(Exception)` around transaction rollback/exit in `proxy.py` is removed.
- Remove `SyncInMemoryBrokerTransport` and `AsyncInMemoryBrokerTransport` imports and unions from `src/py/vertebra/extensions/sqlspec/plugin.py` (`PY-07`).
- **Edge Case**: Parameter dispatch handles `None`, `()`, `Mapping`, and `Sequence` deterministically without `inspect.signature` (`PY-10`).
- **Edge Case**: If `quarantine` itself raises, propagate with the rollback error preserved on `__context__`.
- **Edge Case**: Wrapped configs with numeric (`$1`), named (`:name`), or positional (`?`) parameter styles preserve exact SQL compilation.

## Test Matrix

- `@pytest.mark.parametrize` over sync/async rollback failures and `__exit__`/`__aexit__` failures asserts `quarantine` count == 1, `release` count == 0, and chained `LeaseQuarantinedError`.
- Assert zero `inspect.signature` calls (`PY-10`) and verify `gc.isenabled` tracking / non-`gc=False` struct safety on `BrokerRpcRequest` and `BrokerRpcResponse`.
- `@pytest.mark.parametrize` across wrapped sync and async SQLSpec configs verifies `isinstance` base checks, `statement_config` parity, and fail-loud rollback error propagation.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec
```

## Acceptance Criteria

- [x] Zero `inspect.signature` or `contextlib.suppress(Exception)` remain in `provider.py` or `proxy.py`.
- [x] Uncertain sync and async session releases always quarantine and raise `LeaseQuarantinedError`.
- [x] `BrokerRpcRequest`/`BrokerRpcResponse` drop `gc=False`; `ProxySyncDriver` and `ProxyAsyncDriver` inherit SQLSpec adapter bases; stale in-memory transport unions are removed from `plugin.py`.
- [x] Zero regressions in the fast verification gate.

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
