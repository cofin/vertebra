---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: 'Chapter 04: Proxy Drivers, Multi-Process SQLSpec & SQLAlchemy Pool Sharing, and Docs/OKF'
description: Refactor SQLSpec provider seams and ProxySyncDriver/ProxyAsyncDriver, prove 4-worker single-pool sharing, extract standalone vertebra.db.dbapi and wire SQLAlchemy/Advanced Alchemy broker pooling, and sync docs/OKF.
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
- ch03-lease-safety-and-uds-attach
plan_commit: 1b191388
updated_at: '2026-11-15T00:50:00Z'
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
  resource: src/py/vertebra/db/dbapi/__init__.py::SyncDBAPIModule
  title: Standalone PEP 249 DB-API module, connection, cursor, and exports
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
- `_BaseSQLSpecProvider` uses `@runtime_checkable` `SyncBrokerLeaseSource` and `AsyncBrokerLeaseSource` Protocols with zero `inspect.signature` calls or `contextlib.suppress(Exception)` blocks ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].
- Rollback or context-exit failure during session release quarantines the lease and raises `LeaseQuarantinedError` chained from the underlying error [^sqlspec-provider].
- `BrokerRpcRequest` and `BrokerRpcResponse` keep GC enabled for container fields, and `ProxySyncDriver` / `ProxyAsyncDriver` subclass `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` preserving upstream `statement_config` without stale in-memory transport unions [^sqlspec-provider] [^sqlspec-proxy] [^sqlspec-plugin].
- ZPages / OpenTelemetry export per-pool quarantine metrics and 4-worker multi-process acceptance verifies shared pools and `SIGKILL` reclamation [^otel-metrics] [^int-conftest].
- `vertebra.db.dbapi` exports PEP 249 errors, `is_row_returning_sql`, `connect`, and sync/async cursor, connection, and module classes with zero imports from `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` [^sa-dialects] [^db-config] [^py-db-registry].
- `extensions/sqlalchemy/async_adapter.py` isolates `AsyncAdapt_dbapi_connection`, `dialects.py` drops embedded DB-API duplicates without shims, and `extensions/sqlalchemy/pool.py` exports `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` wired into Advanced Alchemy [^sa-dialects] [^alchemy-plugin] [^broker-transport].
- `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, and `shm-broker-limitations.md` (`<= 200` lines each) synchronize public APIs and caveats with zero stale in-memory transport references [^docs-ext] [^docs-db] [^docs-changelog] [^broker-okf] [^domains-idx].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`extensions/sqlspec` eliminates `inspect.signature` calls, `contextlib.suppress(Exception)` release handlers ([#98](https://github.com/cofin/vertebra/issues/98)), invalid `gc=False` container structs, and deleted in-memory transport references while separating standalone PEP 249 `vertebra.db.dbapi` from `extensions/sqlalchemy/dialects.py` [^sqlspec-provider] [^sa-dialects]. **Non-Goals**: Django backend rewrites (`sqlalchemy-and-django-integration`); SQLite/Redis docs (`native-sqlite-driver`, `native-redis-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SQLSpec proxy | `extensions/sqlspec/{provider,proxy,plugin}.py` [^sqlspec-provider] [^sqlspec-proxy] [^sqlspec-plugin] | Typed Protocols, fail-loud quarantine, `SyncDriverAdapterBase`/`AsyncDriverAdapterBase` |
| Telemetry & E2E | `telemetry/otel.py`, `tests/integration/extensions/` [^otel-metrics] [^int-conftest] | `DbPoolMetricSample` quarantine gauges + 4-worker sharing & `SIGKILL` suite |
| Neutral DB-API | `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py` [^db-config] [^py-db-registry] | PEP 249 sync + async DB-API with strict upward import boundary |
| SQLAlchemy & Alchemy | `extensions/sqlalchemy/*`, `extensions/alchemy/{__init__,plugin}.py` [^sa-dialects] [^alchemy-plugin] | `AsyncAdapt_dbapi_connection`, `+vtb` dialects, `BrokerPool`, `AsyncBrokerPool` |
| Public docs & OKF | `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `domains/data/` [^docs-ext] [^docs-db] [^docs-changelog] [^broker-okf] [^domains-idx] | Tabbed snippets, conceptual Mermaid, changelog, OKF chapters (`<= 200` lines) |

**Knowledge Anchors:** [SQLSpec, Alchemy, and Dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R15 / R16**: Typed SQLSpec Protocols, fail-loud quarantine, container GC safety, and adapter base proxy drivers (`t01`).
- **R20 / R21**: `DbPoolMetricSample` quarantine gauges and 4-worker single-pool sharing + `SIGKILL` + UDS checks (`t02`).
- **R17 / R18**: Standalone `vertebra.db.dbapi`, `async_adapter.py`, clean `dialects.py`/`alchemy/__init__.py`, and `BrokerPool`/`AsyncBrokerPool` (`t03`).
- **R22**: Updated `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, `shm-broker-limitations.md`, and `domains/index.md` (`t04`).

```python
@runtime_checkable
class SyncBrokerLeaseSource(Protocol):
    def acquire(
        self, *, bind_key: str | None = "default", worker_id: int | None = None
    ) -> Any: ...
    def quarantine(self, lease: Any) -> None: ...

@runtime_checkable
class AsyncBrokerLeaseSource(Protocol):
    async def acquire(
        self, *, bind_key: str | None = "default", worker_id: int | None = None
    ) -> Any: ...
    async def quarantine(self, lease: Any) -> Any: ...

class ProxySyncDriver(SyncDriverAdapterBase): ...
class ProxyAsyncDriver(AsyncDriverAdapterBase): ...
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
def broker_creator(
    url: str, *, broker: SyncBrokerLeaseSource
) -> Callable[[], SyncDBAPIConnection]: ...
```

| Boundary | Trigger | Expected Contract |
| --- | --- | --- |
| Rollback / exit raises | `session.rollback()` or `__aexit__` raises | `quarantine(lease)` once, `LeaseQuarantinedError` raised |
| Worker `SIGKILL` | Child killed mid-`BEGIN` | `BrokerHost` runs server `ROLLBACK` and reclaims slot |
| DB-API import boundary | Subprocess `import vertebra.db.dbapi` | Zero upward `sqlalchemy`/`extensions`/`adapters` imports |
| Statement autocommit | BigQuery/Spanner with `autocommit=False` | Raises PEP 249 `NotSupportedError` |
| Unconfigured `BrokerPool` | Constructing `BrokerPool` without broker | Raises `ImproperConfigurationError` immediately |
| Pool return failure | Rollback raises in `BrokerPool._do_return_conn` | Quarantines lease and raises `LeaseQuarantinedError` |
| Stale doc names | `SyncInMemoryBrokerTransport` / `use_shm=False` | Removed across `docs/` and `knowledge/` (`<= 200` lines) |

## 3. Documentation & Traceability

- [x] [1b3820b7] [t01-sqlspec-seams-and-proxy-drivers.md](tasks/t01-sqlspec-seams-and-proxy-drivers.md) — Replace `inspect.signature` and suppressed release blocks on SQLSpec providers with typed Protocols and quarantine, drop invalid `gc=False` on `BrokerRpc*`, rebase `ProxySyncDriver`/`ProxyAsyncDriver` onto adapter base classes, and remove stale in-memory transport unions in `plugin.py`.
- [x] [1b3820b7] [t02-multiprocess-pool-sharing-acceptance.md](tasks/t02-multiprocess-pool-sharing-acceptance.md) — Export quarantine counters on `DbPoolMetricSample` and verify 4 OS worker processes share one server pool, recover cleanly from `SIGKILL`, and enforce UDS rejection.
- [x] [1b3820b7] [t03-standalone-dbapi-and-sqlalchemy-pooling.md](tasks/t03-standalone-dbapi-and-sqlalchemy-pooling.md) — Extract standalone PEP 249 `vertebra.db.dbapi`, isolate `async_adapter.py`, remove embedded DB-API copies and adapter `isinstance` branching from `dialects.py`, clean stale `alchemy/__init__.py` exports, and wire `BrokerPool`/`AsyncBrokerPool`/`broker_creator`.
- [x] [1b3820b7] [t04-broker-docs-changelog-and-okf-sync.md](tasks/t04-broker-docs-changelog-and-okf-sync.md) — Update `docs/usage/{databases,extensions}.md`, `docs/changelog/index.md`, `broker-and-shm-leases.md`, `shm-broker-limitations.md`, and `domains/index.md`.

```bash
uv run --no-sync pytest \
  src/py/tests/unit/db/test_dbapi_standalone.py \
  src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py \
  src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py \
  src/py/tests/unit/extensions/test_shm_broker.py -q && \
  uv run --no-sync ruff check \
    src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlspec \
    src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy \
    src/py/vertebra/telemetry && \
  uv run --no-sync mypy \
    src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlspec \
    src/py/vertebra/extensions/sqlalchemy src/py/vertebra/extensions/alchemy \
    src/py/vertebra/telemetry && \
  uv run --no-sync python -m tools.docs.scan_public_docstrings && \
  make review-check
```

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side SQLSpec proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — ProxySyncConfig and ProxyAsyncConfig wrappers for upstream SQLSpec configs.
[^otel-metrics]: `src/py/vertebra/telemetry/otel.py::DbPoolMetricSample` — OpenTelemetry and ZPages database pool metric samples.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Containerized PostgreSQL and MySQL integration fixtures.
[^sa-dialects]: `src/py/vertebra/db/dbapi/__init__.py::SyncDBAPIModule` — Standalone PEP 249 DB-API module, connection, cursor, and exports.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^alchemy-plugin]: `src/py/vertebra/extensions/alchemy/plugin.py::create_sync_alchemy_config` — Advanced Alchemy config factory and broker connect_args wiring.
[^docs-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^docs-db]: `docs/usage/databases.md` — Databases usage documentation.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
