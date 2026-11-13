---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch06-sqlspec-seams-and-multiprocess
title: "Chapter 06: Typed SQLSpec Provider Seams, Adapter-Base Proxies, and Multi-Process Acceptance"
description: "Replace inspect.signature and suppressed release errors in SQLSpec providers with typed Protocols and quarantine, rebase ProxySyncDriver/ProxyAsyncDriver on upstream SQLSpec adapter bases, and prove multi-process pool sharing."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlspec
  - proxy
  - quarantine
  - multiprocess
sources:
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
    title: "Broker-side SQLSpec provider session and lease management"
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
    title: "Worker-side SQLSpec proxy drivers"
  - id: sqlspec-plugin
    resource: src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig
    title: "ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs"
  - id: otel-metrics
    resource: src/py/vertebra/telemetry/otel.py
    title: "OpenTelemetry and ZPages database pool metric samples"
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: "Containerized PostgreSQL and MySQL integration fixtures"
---

# Chapter 06: Typed SQLSpec Provider Seams, Adapter-Base Proxies, and Multi-Process Acceptance

<!-- truth: start -->
- `_BaseSQLSpecProvider` uses `@runtime_checkable` `SyncBrokerLeaseSource`, `AsyncBrokerLeaseSource`, `SyncLeasedSession`, and `AsyncLeasedSession` Protocols with zero `inspect.signature` calls or `contextlib.suppress(Exception)` blocks ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].
- Rollback or context-exit failure during session release quarantines the lease and raises `LeaseQuarantinedError` chained from the underlying error [^sqlspec-provider].
- `ProxySyncDriver` and `ProxyAsyncDriver` subclass `sqlspec.driver.SyncDriverAdapterBase` and `AsyncDriverAdapterBase`, preserving `ProxySyncConfig` / `ProxyAsyncConfig` upstream `statement_config` and parameter styles [^sqlspec-proxy] [^sqlspec-plugin].
- ZPages / OpenTelemetry export per-pool quarantine metrics and 4-worker multi-process acceptance verifies shared pools and `SIGKILL` reclamation [^otel-metrics] [^int-conftest].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/extensions/sqlspec/provider.py` introspects `inspect.signature` per acquire/execute call and wraps rollback, context exit, and lease release in `contextlib.suppress(Exception)`, returning dirty connections to the pool ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider]. `ProxySyncDriver` and `ProxyAsyncDriver` do not inherit `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` [^sqlspec-proxy].

**Non-Goals:** PEP 249 extraction and SQLAlchemy `BrokerPool` (`ch07`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SQLSpec provider | `src/py/vertebra/extensions/sqlspec/provider.py` [^sqlspec-provider] | Typed lease/session Protocols + fail-loud quarantine |
| SQLSpec proxy | `src/py/vertebra/extensions/sqlspec/{proxy,plugin}.py` [^sqlspec-proxy] [^sqlspec-plugin] | `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` inheritance |
| Telemetry & E2E | `src/py/vertebra/telemetry/otel.py`, `tests/integration/extensions/` [^otel-metrics] [^int-conftest] | ZPages `/debug/broker` gauges + 4-worker pool-sharing suite |

**Knowledge Anchors:** [SQLSpec, Alchemy, and Dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R15**: Replace all `inspect.signature` duck-typing and `contextlib.suppress(Exception)` release blocks across sync and async paths in `provider.py` with typed Protocols and quarantine (`t01`).
- **R16**: Re-parent `ProxySyncDriver` and `ProxyAsyncDriver` onto `SyncDriverAdapterBase` and `AsyncDriverAdapterBase` without suppressing rollback/exit exceptions (`t02`).
- **R20 / R21**: Export quarantine/reclaim counters in `telemetry/otel.py` and add multi-process worker pool-sharing, crash `SIGKILL` reclamation, and UDS rejection acceptance tests (`t03`).

```python
@runtime_checkable
class SyncBrokerLeaseSource(Protocol):
    def acquire(self, *, pool_name: str, worker_pid: int) -> SyncConnectionLease: ...
    def quarantine(self, lease: SyncConnectionLease) -> None: ...

@runtime_checkable
class AsyncBrokerLeaseSource(Protocol):
    async def acquire(self, *, pool_name: str, worker_pid: int) -> AsyncConnectionLease: ...
    async def quarantine(self, lease: AsyncConnectionLease) -> None: ...

class ProxySyncDriver(SyncDriverAdapterBase): ...
class ProxyAsyncDriver(AsyncDriverAdapterBase): ...
```

**Integration Flow:**
- Provider session exit -> `session.rollback()` -> context `__exit__` -> `lease.release()`; any exception calls `broker.quarantine(lease)` and raises `LeaseQuarantinedError(...) from exc`.
- `ProxySyncConfig` / `ProxyAsyncConfig` forward wrapped upstream `statement_config` to `ProxySyncDriver` / `ProxyAsyncDriver`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Rollback raises | `session.rollback()` raises `OSError` | `quarantine(lease)` called once; `release()` skipped; `LeaseQuarantinedError` raised | Zero dirty connection reuse |
| Context exit raises | `__aexit__` raises after rollback | Lease quarantined and `LeaseQuarantinedError` raised | Broken socket evicted |
| Worker `SIGKILL` | Child killed mid-`BEGIN` | `BrokerHost` runs server `ROLLBACK` and reclaims slot | No orphaned server tx |

## 3. Documentation & OKF Knowledge Plan

`ProxySyncConfig`, `ProxyAsyncConfig`, and quarantine semantics are documented in `ch08`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R15` | `t01` | `src/py/vertebra/extensions/sqlspec/provider.py` | `pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `R16` | `t02` | `src/py/vertebra/extensions/sqlspec/{proxy,plugin}.py` | `pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `R20`, `R21` | `t03` | `src/py/vertebra/telemetry/otel.py` | `pytest src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py` |

## 5. Implementation Plan

- [ ] [t01-typed-sqlspec-provider-seams.md](tasks/t01-typed-sqlspec-provider-seams.md) — Remove inspect.signature and contextlib.suppress(Exception) in provider.py; quarantine leases on failed release.
- [ ] [t02-adapter-base-proxy-drivers.md](tasks/t02-adapter-base-proxy-drivers.md) — Subclass SyncDriverAdapterBase and AsyncDriverAdapterBase in proxy.py while preserving wrapped statement_config.
- [ ] [t03-multiprocess-pool-sharing-acceptance.md](tasks/t03-multiprocess-pool-sharing-acceptance.md) — Propagate quarantine/reclaim metrics in telemetry/otel.py and verify 4-worker pool sharing, crash reclamation, and UDS auth.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: Zero `inspect.signature` or `contextlib.suppress(Exception)` calls exist in `src/py/vertebra/extensions/sqlspec/`.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_shm_broker.py`

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
[^otel-metrics]: `src/py/vertebra/telemetry/otel.py` — OpenTelemetry and ZPages database pool metric samples.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Containerized PostgreSQL and MySQL integration fixtures.
