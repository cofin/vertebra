---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: 'Chapter 04: Proxy Drivers, Multi-Process SQLSpec & SQLAlchemy Pool Sharing, and Docs/OKF'
description: Refactor SQLSpec provider seams and ProxySyncDriver/ProxyAsyncDriver, prove 4-worker single-pool sharing, extract standalone vertebra.db.dbapi and wire SQLAlchemy/Advanced Alchemy broker pooling, and sync docs/OKF.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- sqlspec
- proxy
- quarantine
- multiprocess
- dbapi
- sqlalchemy
- advanced-alchemy
- pool
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
- id: otel-metrics
  resource: src/py/vertebra/telemetry/otel.py
  title: OpenTelemetry and ZPages database pool metric samples
- id: int-conftest
  resource: src/py/tests/integration/conftest.py::vtb_postgres_config
  title: Containerized PostgreSQL and MySQL integration fixtures
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
  title: SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes
- id: db-config
  resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
  title: BaseDatabaseConfig with dialect ClassVar and pool_key
- id: py-db-registry
  resource: src/py/vertebra/db/registry.py::DatabaseRegistry
  title: DatabaseRegistry URL parsing and driver resolution
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
- id: docs-ext
  resource: docs/usage/extensions.md
  title: Extensions usage documentation
- id: docs-db
  resource: docs/usage/databases.md
  title: Databases usage documentation
- id: docs-changelog
  resource: docs/changelog/index.md
  title: Release changelog
- id: broker-okf
  resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  title: Broker and SHM leases OKF chapter
- id: domains-idx
  resource: .agents/bundles/knowledge/domains/index.md
  title: OKF domains catalog index
---

# Chapter 04: Proxy Drivers, Multi-Process SQLSpec & SQLAlchemy Pool Sharing, and Docs/OKF

<!-- truth: start -->
- `_BaseSQLSpecProvider` uses `@runtime_checkable` `SyncBrokerLeaseSource`, `AsyncBrokerLeaseSource`, `SyncLeasedSession`, and `AsyncLeasedSession` Protocols with zero `inspect.signature` calls or `contextlib.suppress(Exception)` blocks ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].
- Rollback or context-exit failure during session release quarantines the lease and raises `LeaseQuarantinedError` chained from the underlying error [^sqlspec-provider].
- `ProxySyncDriver` and `ProxyAsyncDriver` subclass `sqlspec.driver.SyncDriverAdapterBase` and `AsyncDriverAdapterBase`, preserving `ProxySyncConfig` / `ProxyAsyncConfig` upstream `statement_config` and parameter styles [^sqlspec-proxy] [^sqlspec-plugin].
- ZPages / OpenTelemetry export per-pool quarantine metrics and 4-worker multi-process acceptance verifies shared pools and `SIGKILL` reclamation [^otel-metrics] [^int-conftest].
- `vertebra.db.dbapi` exports PEP 249 errors, `is_row_returning_sql`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, and `AsyncDBAPIModule` with zero imports from `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` [^sa-dialects] [^db-config].
- `vertebra.extensions.sqlalchemy.async_adapter` wraps `vertebra.db.dbapi.AsyncDBAPIConnection` in `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection` [^sa-dialects].
- `extensions/sqlalchemy/dialects.py` deletes embedded DB-API duplicates with zero compatibility re-exports and checks `type(config).dialect` / `capabilities.supports_returning` instead of adapter `isinstance` [^sa-dialects] [^db-config].
- `extensions/sqlalchemy/pool.py` provides `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` so SQLAlchemy and Advanced Alchemy share broker-leased pools across workers [^broker-transport].
- `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/changelog/index.md` document `ProxySyncConfig`/`ProxyAsyncConfig`, `BrokerPool`/`AsyncBrokerPool`, `vtb broker serve`, UDS attach, quarantine, `vertebra.db.dbapi`, and the capsule registration matrix [^docs-ext] [^docs-db] [^docs-changelog].
- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` and `.agents/bundles/knowledge/domains/data/shm-broker-limitations.md` stay `<= 200` lines each and are indexed in `.agents/bundles/knowledge/domains/index.md` [^broker-okf] [^domains-idx] [^py-db-registry].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/extensions/sqlspec/provider.py` introspects `inspect.signature` per acquire/execute call and wraps rollback, context exit, and lease release in `contextlib.suppress(Exception)`, returning dirty connections to the pool ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider]. `ProxySyncDriver` and `ProxyAsyncDriver` do not inherit `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` [^sqlspec-proxy].

**Non-Goals:** PEP 249 extraction and SQLAlchemy `BrokerPool` (`ch04:t03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SQLSpec provider | `src/py/vertebra/extensions/sqlspec/provider.py` [^sqlspec-provider] | Typed lease/session Protocols + fail-loud quarantine |
| SQLSpec proxy | `src/py/vertebra/extensions/sqlspec/{proxy,plugin}.py` [^sqlspec-proxy] [^sqlspec-plugin] | `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` inheritance |
| Telemetry & E2E | `src/py/vertebra/telemetry/otel.py`, `tests/integration/extensions/` [^otel-metrics] [^int-conftest] | ZPages `/debug/broker` gauges + 4-worker pool-sharing suite |

**Knowledge Anchors:** [SQLSpec, Alchemy, and Dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

At `18baadcd`, `src/py/vertebra/extensions/sqlalchemy/dialects.py` embeds PEP 249 exceptions, `SyncDBAPI*`, and `AsyncDBAPI*` directly alongside SQLAlchemy dialects, importing `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection` and concrete `BigQueryConfig` / `SpannerConfig` / `SQLiteConfig` classes [^sa-dialects]. Extracting `vertebra.db.dbapi` establishes a clean database-layer PEP 249 seam for SQLAlchemy, Advanced Alchemy, and downstream Django backends while adding `BrokerPool`.

**Non-Goals:** Django backend rewrites (owned by `sqlalchemy-and-django-integration`); Relocating `vertebra.db.dbapi` in later flows.

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Neutral DB-API | `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py` [^db-config] [^py-db-registry] | PEP 249 sync + async DB-API with strict import boundary |
| SQLAlchemy adapter | `src/py/vertebra/extensions/sqlalchemy/{async_adapter,dialects,alembic}.py` [^sa-dialects] | `AsyncAdapt_dbapi_connection` bridge + `+vertebra`/`+vtb` dialects |
| Broker pool | `src/py/vertebra/extensions/sqlalchemy/{pool,__init__}.py` [^broker-transport] | `BrokerPool`, `AsyncBrokerPool`, `broker_creator` |

**Knowledge Anchors:** [SQLSpec, Alchemy, and Dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md).

Public documentation and OKF chapters must reflect the shipped SHM ownership model, UDS attach boundary, cross-wheel `BrokerEngineCapsule` registry, fail-loud quarantine, `vertebra.db.dbapi`, and explicit operational caveats while removing deleted `SyncInMemoryBrokerTransport` / `use_shm=False` references [^docs-ext] [^broker-okf].

**Non-Goals:** SQLite or Redis driver documentation (owned by `native-sqlite-driver` and `native-redis-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Public docs | `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md` [^docs-ext] [^docs-db] [^docs-changelog] | Zensical user docs and `Unreleased` changelog |
| OKF knowledge | `.agents/bundles/knowledge/domains/data/{broker-and-shm-leases,shm-broker-limitations}.md` [^broker-okf] [^domains-idx] | Present-tense OKF architecture and limitations chapters (`<= 200` lines) |

**Knowledge Anchors:** [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Project Workflow](../../../../knowledge/workflow.md).

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

- **R17a**: Create `vertebra.db.dbapi` exporting PEP 249 errors, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, `AsyncDBAPIModule`, `is_row_returning_sql`, and `connect` with zero upward or engine-specific imports (`t01`).
- **R17b / R17c**: Isolate `AsyncAdapt_dbapi_connection` in `extensions/sqlalchemy/async_adapter.py`, delete embedded DB-API classes from `dialects.py`, replace concrete config `isinstance` checks with `dialect` / capability metadata, and rewire import sites (`t02`).
- **R18**: Implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` in `extensions/sqlalchemy/pool.py` with rollback-on-return and quarantine-on-failure (`t03`).

```python
# src/py/vertebra/db/dbapi/__init__.py
apilevel: Final = "2.0"
threadsafety: Final = 2
paramstyle: Final = "qmark"
class SyncDBAPICursor: ...
class SyncDBAPIConnection: ...
class SyncDBAPIModule: ...
class AsyncDBAPICursor: ...
class AsyncDBAPIConnection: ...
class AsyncDBAPIModule: ...
def is_row_returning_sql(operation: str) -> bool: ...
def connect(url: str, **kwargs: Any) -> SyncDBAPIConnection: ...

# src/py/vertebra/extensions/sqlalchemy/pool.py
class BrokerPool(Pool): ...
class AsyncBrokerPool(Pool): ...
def broker_creator(url: str, *, broker: SyncBrokerLeaseSource) -> Callable[[], SyncDBAPIConnection]: ...
```

**Integration Flow:**
- `Sync*Dialect.import_dbapi()` -> `vertebra.db.dbapi.SyncDBAPIModule`.
- `Async*Dialect.import_dbapi()` -> `extensions.sqlalchemy.async_adapter.SQLAlchemyAsyncDBAPI` wrapping `vertebra.db.dbapi.AsyncDBAPIConnection`.
- `create_engine(url, poolclass=BrokerPool, creator=broker_creator(url, broker=transport))` -> shared SHM partition checkout + quarantine on failed return rollback.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Import boundary | Fresh subprocess `import vertebra.db.dbapi` | Zero `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters` in `sys.modules` | Clean layer separation |
| Statement autocommit | `type(config).dialect in {"bigquery", "spanner"}` with `autocommit=False` | Raises PEP 249 `NotSupportedError` | No adapter `isinstance` coupling |
| Pool return failure | Rollback raises in `BrokerPool._do_return_conn` | Quarantines lease and raises `LeaseQuarantinedError` | No dirty pooled session |

- **R22a**: Update `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/changelog/index.md` with tabbed install snippets, conceptual 4–6 node Mermaid overview, and exact public class/function names (`t01`).
- **R22b**: Update `broker-and-shm-leases.md`, create `shm-broker-limitations.md` (`<= 200` lines each), and link from `domains/index.md` (`t02`).

```text
Documented public symbols:
- vertebra.extensions.broker: BrokerConfig, BrokerPlugin, BrokerHost, BrokerEngineDiscovery,
  BrokerAttachError, BrokerSegmentInUseError, BrokerAuthenticationError, LeaseQuarantinedError, PoolMetrics
- vertebra._vertebra: LeaseBroker.open_or_create_named, BrokerAttachServer, BrokerAttachClient,
  BrokerEngineRegistry, stable_hash64
- vertebra.extensions.sqlspec: ProxySyncConfig, ProxyAsyncConfig, ProxySyncDriver, ProxyAsyncDriver
- vertebra.extensions.sqlalchemy: BrokerPool, AsyncBrokerPool, broker_creator
- vertebra.db.dbapi: SyncDBAPIConnection, AsyncDBAPIConnection, SyncDBAPIModule, AsyncDBAPIModule
```

**Integration Flow:**
- `make docs` validates Zensical rendering, docstring scans, and artifact budgets.
- `make review-check` validates OKF v0.2 frontmatter, truth blocks, relative links, and privacy hygiene.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Stale transport names | Mention of `SyncInMemoryBrokerTransport` or `use_shm=False` | Removed across `docs/` and `knowledge/` | `DOC-02` / `DOC-09` parity |
| OKF line cap | Knowledge chapter `> 200` lines | Split cleanly between `broker-and-shm-leases.md` and `shm-broker-limitations.md` | `<= 200` lines per chapter |

## 3. Documentation & OKF Knowledge Plan

`ProxySyncConfig`, `ProxyAsyncConfig`, and quarantine semantics are documented in `ch04:t04`.

`vertebra.db.dbapi` and `BrokerPool` usage are documented in `ch04:t04`.

This chapter completes `[6/6]` documentation and OKF deliverables for `broker-pool-sharing`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Expose Typed SQLSpec Provider Seams and Refactor ProxySyncDriver / ProxyAsyncDriver | `src/py/vertebra/extensions/sqlspec/provider.py`, `src/py/vertebra/extensions/sqlspec/proxy.py`, `src/py/vertebra/extensions/sqlspec/plugin.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t02` | Multi-Process Broker Pool Sharing and Worker Crash Reclamation Acceptance Suite | `src/py/vertebra/telemetry/otel.py` | `src/py/tests/unit/extensions/test_shm_broker.py`, `src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py` |
| `t03` | Extract Standalone vertebra.db.dbapi and Wire SQLAlchemy & Advanced Alchemy Broker Pooling | `src/py/vertebra/db/dbapi/__init__.py`, `src/py/vertebra/db/dbapi/_sync.py`, `src/py/vertebra/db/dbapi/_async.py` (+5 more) | `src/py/tests/unit/db/test_dbapi_standalone.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t04` | Synchronize Broker Pool Sharing Documentation, Changelog, and OKF Chapters | `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/changelog/index.md` (+3 more) | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-sqlspec-seams-and-proxy-drivers.md](tasks/t01-sqlspec-seams-and-proxy-drivers.md) — Add public borrow_lease/execute_leased/stream_arrow seams on SQLSpec providers and rebase ProxySyncDriver/ProxyAsyncDriver onto base drivers with VTBARROW ArrowCursor.
- [ ] [t02-multiprocess-pool-sharing-acceptance.md](tasks/t02-multiprocess-pool-sharing-acceptance.md) — Verify 4 OS worker processes share a single physical server pool and reclaim crashed worker leases cleanly.
- [ ] [t03-standalone-dbapi-and-sqlalchemy-pooling.md](tasks/t03-standalone-dbapi-and-sqlalchemy-pooling.md) — Extract PEP 249 dbapi.py into vertebra.db, rewire extensions/sqlalchemy/{adapter,dialects}.py, and support broker_client across SQLAlchemy and Advanced Alchemy.
- [ ] [t04-broker-docs-changelog-and-okf-sync.md](tasks/t04-broker-docs-changelog-and-okf-sync.md) — Update databases/extensions/dbapi docs, changelog, and OKF broker, driver, SQLSpec/SQLAlchemy, and broker-pool-limitations chapters.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Every OKF chapter stays `<= 200` lines in present tense with zero private paths or historical prose.
- **Fast Chapter Gate (`< 60s`)**: `make review-check && uv run --no-sync python -m tools.docs.scan_public_docstrings`

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
[^otel-metrics]: `src/py/vertebra/telemetry/otel.py` — OpenTelemetry and ZPages database pool metric samples.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Containerized PostgreSQL and MySQL integration fixtures.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^docs-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^docs-db]: `docs/usage/databases.md` — Databases usage documentation.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
