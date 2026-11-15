---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: 'Chapter 04: Proxy Drivers, Multi-Process SQLSpec & SQLAlchemy Pool Sharing, and Docs/OKF'
description: Refactor SQLSpec provider seams and ProxySyncDriver/ProxyAsyncDriver, prove 4-worker single-pool sharing, extract standalone vertebra.db.dbapi and wire SQLAlchemy/Advanced Alchemy broker pooling, and sync docs/OKF.
status: stable
state: in_progress
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
- ch03-lease-safety-and-uds-attach
plan_commit: 1b191388
updated_at: '2026-11-14T23:08:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:08:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T23:08:00Z'
  commit: 1b191388
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
  resource: src/py/vertebra/telemetry/otel.py::DbPoolMetricSample
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
- id: alchemy-plugin
  resource: src/py/vertebra/extensions/alchemy/plugin.py::create_sync_alchemy_config
  title: Advanced Alchemy config factory and broker connect_args wiring
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
- `BrokerRpcRequest` and `BrokerRpcResponse` drop `gc=False` because they hold container (`list`/`dict`/`Any`) fields (`PY-04`), and `ProxySyncDriver` / `ProxyAsyncDriver` subclass `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` preserving upstream `statement_config` and removing stale `InMemoryBrokerTransport` unions [^sqlspec-provider] [^sqlspec-proxy] [^sqlspec-plugin].
- ZPages / OpenTelemetry export per-pool quarantine metrics and 4-worker multi-process acceptance verifies shared pools and `SIGKILL` reclamation [^otel-metrics] [^int-conftest].
- `vertebra.db.dbapi` exports PEP 249 errors, `is_row_returning_sql`, `connect`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, and `AsyncDBAPIModule` with zero imports from `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` [^sa-dialects] [^db-config] [^py-db-registry].
- `extensions/sqlalchemy/async_adapter.py` isolates `AsyncAdapt_dbapi_connection`, `dialects.py` removes embedded DB-API duplicates without shims, and `extensions/sqlalchemy/pool.py` exports `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` wired into Advanced Alchemy [^sa-dialects] [^alchemy-plugin] [^broker-transport].
- `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, and `shm-broker-limitations.md` (`<= 200` lines each) synchronize public APIs and caveats with zero stale in-memory transport references [^docs-ext] [^docs-db] [^docs-changelog] [^broker-okf] [^domains-idx].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/extensions/sqlspec/provider.py` introspects `inspect.signature` per acquire/execute call, wraps rollback/exit/release in `contextlib.suppress(Exception)` ([#98](https://github.com/cofin/vertebra/issues/98)), and marks container-holding `BrokerRpcRequest` / `BrokerRpcResponse` with `gc=False` [^sqlspec-provider]. `ProxySyncDriver` and `ProxyAsyncDriver` do not inherit `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` [^sqlspec-proxy], while `extensions/sqlspec/plugin.py` and `extensions/alchemy/__init__.py` retain `SyncInMemoryBrokerTransport` / `AsyncInMemoryBrokerTransport` references removed in `ch01`. Meanwhile, `extensions/sqlalchemy/dialects.py` embeds PEP 249 `SyncDBAPI*` and `AsyncDBAPI*` classes alongside SQLAlchemy dialect hooks and concrete config `isinstance` checks [^sa-dialects].

**Non-Goals:** Django database backend rewrites (owned by `sqlalchemy-and-django-integration`, which builds on `vertebra.db.dbapi` and `BrokerPool`); SQLite or Redis driver docs (owned by `native-sqlite-driver` and `native-redis-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SQLSpec provider & proxy | `extensions/sqlspec/{provider,proxy,plugin}.py` [^sqlspec-provider] [^sqlspec-proxy] [^sqlspec-plugin] | Typed Protocols, fail-loud quarantine, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` |
| Telemetry & E2E | `telemetry/otel.py`, `tests/integration/extensions/` [^otel-metrics] [^int-conftest] | `DbPoolMetricSample` quarantine gauges + 4-worker sharing & `SIGKILL` suite |
| Neutral DB-API | `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py` [^db-config] [^py-db-registry] | PEP 249 sync + async DB-API with strict upward import boundary |
| SQLAlchemy & Alchemy pool | `extensions/sqlalchemy/{async_adapter,dialects,alembic,pool,__init__}.py`, `extensions/alchemy/{__init__,plugin}.py` [^sa-dialects] [^alchemy-plugin] | `AsyncAdapt_dbapi_connection`, `+vtb` dialects, `BrokerPool`, `AsyncBrokerPool`, `broker_creator` |
| Public docs & OKF | `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `knowledge/domains/data/{broker-and-shm-leases,shm-broker-limitations}.md`, `domains/index.md` [^docs-ext] [^docs-db] [^docs-changelog] [^broker-okf] [^domains-idx] | Tabbed snippets, conceptual Mermaid, changelog, present-tense OKF chapters (`<= 200` lines) |

**Knowledge Anchors:** [SQLSpec, Alchemy, and Dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R15 / R16**: Replace all `inspect.signature` duck-typing and `contextlib.suppress(Exception)` release blocks in `provider.py` with typed Protocols and fail-loud quarantine; drop `gc=False` on `BrokerRpcRequest`/`BrokerRpcResponse`; re-parent `ProxySyncDriver`/`ProxyAsyncDriver` onto `SyncDriverAdapterBase`/`AsyncDriverAdapterBase`; clean `InMemoryBrokerTransport` refs from `sqlspec/plugin.py` (`t01`).
- **R20 / R21**: Expose `quarantined`, `total_quarantined`, `total_disposed`, and `total_reclaimed` on `DbPoolMetricSample` in `telemetry/otel.py` and prove 4-worker single-pool sharing, `SIGKILL` crash `ROLLBACK`, quarantine disposal, and UDS rejection (`t02`).
- **R17a / R17b / R17c / R18**: Extract standalone `vertebra.db.dbapi` (`__init__.py` PEP 562 lazy exports, `_sync.py`, `_async.py`), isolate `AsyncAdapt_dbapi_connection` in `extensions/sqlalchemy/async_adapter.py`, remove embedded DB-API classes and `isinstance` config branching from `dialects.py`, rewire `alembic.py`, remove any remaining stale broker/cache exports from `extensions/alchemy/__init__.py`, and implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` across SQLAlchemy and Advanced Alchemy (`t03`).
- **R22a / R22b**: Update `docs/usage/{extensions,databases}.md` and `docs/changelog/index.md` (`Unreleased`), update `broker-and-shm-leases.md`, create `shm-broker-limitations.md` (`<= 200` lines each), and index it in `domains/index.md` (`t04`).

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

class BrokerPool(Pool): ...
class AsyncBrokerPool(Pool): ...
def broker_creator(url: str, *, broker: SyncBrokerLeaseSource) -> Callable[[], SyncDBAPIConnection]: ...
```

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
- Provider session exit -> `session.rollback()` -> context exit -> `lease.release()`; any exception invokes `broker.quarantine(lease)` and raises `LeaseQuarantinedError(...) from exc`.
- `Sync*Dialect.import_dbapi()` -> `vertebra.db.dbapi.SyncDBAPIModule`; `Async*Dialect.import_dbapi()` -> `extensions.sqlalchemy.async_adapter.SQLAlchemyAsyncDBAPI`.
- `create_engine(url, poolclass=BrokerPool, creator=broker_creator(url, broker=transport))` -> partitioned SHM checkout (`_do_get`) + rollback on return (`_do_return_conn`) + quarantine on failure.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Rollback / exit raises | `session.rollback()` or `__aexit__` raises | `quarantine(lease)` called once, `release()` skipped, `LeaseQuarantinedError` raised | Zero dirty connection reuse |
| Worker `SIGKILL` | Child killed mid-`BEGIN` | `BrokerHost` runs server `ROLLBACK` and reclaims slot | Zero orphaned server tx |
| DB-API import boundary | Subprocess `import vertebra.db.dbapi` | Zero `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters` in `sys.modules` | Strict layer separation |
| Statement autocommit | `type(config).dialect in {"bigquery", "spanner"}` with `autocommit=False` | Raises PEP 249 `NotSupportedError` | No adapter `isinstance` coupling |
| Unconfigured `BrokerPool` | Constructing `BrokerPool` without broker | Raises `ImproperConfigurationError` immediately | Fail-loud pool config |
| Pool return failure | Rollback raises in `BrokerPool._do_return_conn` | Quarantines lease and raises `LeaseQuarantinedError` | No dirty pooled session |
| Stale doc transport names | References to `SyncInMemoryBrokerTransport` or `use_shm=False` | Removed across `docs/` and `knowledge/` | `DOC-02` / `DOC-09` parity |
| OKF line cap | Knowledge chapter `> 200` lines | Split cleanly between `broker-and-shm-leases.md` and `shm-broker-limitations.md` | `<= 200` lines per chapter |

## 3. Documentation & OKF Knowledge Plan

Task `t04` completes `[6/6]` documentation and OKF deliverables for `broker-pool-sharing`: `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/changelog/index.md` (`Unreleased`), `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`, `.agents/bundles/knowledge/domains/data/shm-broker-limitations.md`, and `.agents/bundles/knowledge/domains/index.md`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Typed SQLSpec Provider Seams and Proxy Driver Refactor | `src/py/vertebra/extensions/sqlspec/{provider,proxy,plugin}.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t02` | Multi-Process Broker Pool Sharing and Worker Crash Reclamation Acceptance Suite | `src/py/vertebra/telemetry/otel.py` | `src/py/tests/unit/extensions/test_shm_broker.py`, `src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py` |
| `t03` | Extract Standalone vertebra.db.dbapi and Wire SQLAlchemy & Advanced Alchemy Broker Pooling | `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py`, `src/py/vertebra/extensions/sqlalchemy/{async_adapter,dialects,alembic,pool,__init__}.py`, `src/py/vertebra/extensions/alchemy/{__init__,plugin}.py` | `src/py/tests/unit/db/test_dbapi_standalone.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `t04` | Synchronize Broker Pool Sharing Documentation, Changelog, and OKF Chapters | `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/{broker-and-shm-leases,shm-broker-limitations}.md`, `.agents/bundles/knowledge/domains/index.md` | `python -m tools.docs.scan_public_docstrings`, `make review-check` |

## 5. Implementation Plan

- [ ] [t01-sqlspec-seams-and-proxy-drivers.md](tasks/t01-sqlspec-seams-and-proxy-drivers.md) — Replace `inspect.signature` and suppressed release blocks on SQLSpec providers with typed Protocols and quarantine, drop invalid `gc=False` on `BrokerRpc*`, rebase `ProxySyncDriver`/`ProxyAsyncDriver` onto adapter base classes, and remove stale in-memory transport unions in `plugin.py`.
- [ ] [t02-multiprocess-pool-sharing-acceptance.md](tasks/t02-multiprocess-pool-sharing-acceptance.md) — Export quarantine counters on `DbPoolMetricSample` and verify 4 OS worker processes share one server pool, recover cleanly from `SIGKILL`, and enforce UDS rejection.
- [ ] [t03-standalone-dbapi-and-sqlalchemy-pooling.md](tasks/t03-standalone-dbapi-and-sqlalchemy-pooling.md) — Extract standalone PEP 249 `vertebra.db.dbapi`, isolate `async_adapter.py`, remove embedded DB-API copies and adapter `isinstance` branching from `dialects.py`, clean stale `alchemy/__init__.py` exports, and wire `BrokerPool`/`AsyncBrokerPool`/`broker_creator`.
- [ ] [t04-broker-docs-changelog-and-okf-sync.md](tasks/t04-broker-docs-changelog-and-okf-sync.md) — Update `docs/usage/{databases,extensions}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, `shm-broker-limitations.md`, and `domains/index.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero `inspect.signature` or `contextlib.suppress(Exception)` in `extensions/sqlspec`; `vertebra.db.dbapi` loads zero upward packages; OKF chapters stay `<= 200` lines with zero private paths.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_shm_broker.py -q && uv run --no-sync ruff check src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy src/py/vertebra/telemetry && uv run --no-sync mypy src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy src/py/vertebra/telemetry && uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check`

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
[^otel-metrics]: `src/py/vertebra/telemetry/otel.py::DbPoolMetricSample` — OpenTelemetry and ZPages database pool metric samples.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Containerized PostgreSQL and MySQL integration fixtures.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^alchemy-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py::create_sync_alchemy_config` — Advanced Alchemy config factory and broker connect_args wiring.
[^docs-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^docs-db]: `docs/usage/databases.md` — Databases usage documentation.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
