---
type: Spec
flow_id: broker-pool-sharing
title: "Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config"
description: "Route any upstream SQLSpec config or Advanced Alchemy/SQLAlchemy config through the shared-memory ring broker so worker processes share one broker-owned connection pool per pool_key with session affinity, transactions, VTBARROW streaming, and crash reclamation."
status: draft
state: draft
has_chapters: false
parent_prd: native-data-platform
depends_on_flows:
  - native-postgres-driver
research:
  - ../native-data-platform/research/gap-analysis_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-07T18:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: Shared-memory ring broker lease table and pool partitioning
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py
    title: SyncSHMBrokerTransport and AsyncSHMBrokerTransport worker transports
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py
    title: ProxySyncDriver and ProxyAsyncDriver upstream SQLSpec wrappers
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud baseline removing unused admission-only *Pool structs from vertebra-db
tags:
  - broker
  - shm
  - pool
  - sqlspec
  - advanced-alchemy
  - sqlalchemy
---

# Flow Charter: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config

<!-- truth: start -->
- **Shared Broker Pool Ownership**: Every worker process leases connections over POSIX SHM rings from a single broker-owned pool per `BaseDatabaseConfig.pool_key()`, dynamically backed by installed `vertebra-<engine>` crates [^core-broker] [^broker-transport].
- **Framework-Neutral DB-API Boundary**: `vertebra.db.dbapi` (`SyncDBAPI*` / `AsyncDBAPI*`) extracts directly into `src/py/vertebra/db/dbapi/` with zero upward SQLAlchemy, Django, or engine-specific imports, while SQLAlchemy async adaptation stays in `extensions/sqlalchemy/async_adapter.py`.
- **Upstream Config Wrapping & Crash Reclamation**: `ProxySyncConfig` / `ProxyAsyncConfig` and `BrokerPool` preserve upstream SQL compilers and session affinity (`VTBARROW` result streaming) and roll back crashed worker leases server-side [^sqlspec-proxy].
<!-- truth: end -->

## Charter

Users keep existing configurations such as `sqlspec.adapters.asyncpg.AsyncpgConfig`, `sqlspec.adapters.oracledb.OracleAsyncConfig`, or `advanced_alchemy.config.SQLAlchemyAsyncConfig`, wrap them with Vertebra, and share one broker-owned connection pool across all worker processes over POSIX shared-memory rings. Where a native Vertebra driver crate (`vertebra-<engine>`) is installed for the engine, the broker dynamically registers that engine's native pool handler and executes I/O directly in Rust without forcing `vertebra-core` to depend on optional database crates.

## Scope

1. **Broker-owned pools & `dbrpc` transport.**
   - Create `vertebra-core/src/dbrpc/` defining versioned `msgspec` `array_like=True` RPC frames (`execute`, `execute_many`, `begin`, `commit`, `rollback`, `savepoint`, `fetch_chunk`, `cancel`, `close`) over `vertebra-core/src/transport/ring_pair.rs` plus a dynamic `BrokerEngineBackend` registration trait in `vertebra-db` so installed `vertebra-<engine>` wheels register native Rust pools at startup.
   - Partition `vertebra-core/src/broker.rs::ShmLeaseTable` per `BaseDatabaseConfig.pool_key()` using deterministic cross-process hashing (`xxh3`/FNV, never seeded Python `hash()`), lease heartbeats instead of blind 30s TTL reaping, and standalone broker daemon startup for external `uvicorn`/`granian` workers alongside `vertebra-http` supervisor spawning.
   - Delete `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin(use_shm=False)`, and `extensions/alchemy/broker.py::StatementCache` (`SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport` over `SyncSHMSessionBroker` / `AsyncSHMSessionBroker` and `SyncConnectionLease` / `AsyncConnectionLease` run locally on Linux and macOS via `ShmRegion`/`memfd` without external services). `driver-fail-loud` deletes the unused admission-only `*Pool` structs in `vertebra-db`; native Rust pools register through `BrokerEngineBackend` on that baseline [^fail-loud-baseline].
2. **Data path & session affinity.**
   - Stream result sets back to workers as `VTBARROW` SHM frames (`ArrowCursor`) or compact row frames, supporting chunked cursor iteration and `cancel()` propagation across `SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport`.
   - Pin leased connections (`SyncConnectionLease` / `AsyncConnectionLease`) across transactions, temporary tables, prepared statements, and session GUCs (`SET LOCAL` / RLS context).
3. **Upstream SQLSpec proxy.**
   - `extensions/sqlspec/plugin.py::{ProxySyncConfig,ProxyAsyncConfig}` and `extensions/sqlspec/proxy.py::{ProxySyncDriver,ProxyAsyncDriver}` subclass `SyncDriverAdapterBase` / `AsyncDriverAdapterBase`, preserving the wrapped upstream adapter's SQL compiler, parameter style, and stores while routing connection execution through `SyncConnectionLease` / `AsyncConnectionLease` without `inspect.signature` duck-typing or suppressed rollback errors.
4. **Framework-neutral DB-API extraction & SQLAlchemy broker pool.**
   - Extract `SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, and `AsyncDBAPICursor` from `extensions/sqlalchemy/dialects.py` directly into `db/dbapi/{__init__,_sync,_async,_errors}.py`. This flow owns the first extraction into its final package. It creates no intermediate `extensions/sqlalchemy/dbapi.py` or compatibility re-export. Downstream `sqlalchemy-and-django-integration` hardens and consumes this package without relocating it again.
   - The shared DB-API depends on generic `DatabaseRegistry`, `BaseDatabaseConfig`, driver, and lease contracts. It imports no SQLAlchemy, Django, framework, agent, extension, or engine adapter modules. The registry retains its declarative config registration boundary. Engine-specific URL parsing and transaction behavior remain on their owning native configs/drivers; the shared package contains no concrete-engine type checks, vendor/URL dispatch, SQL snippets for engine-specific behavior, fallback SQLite database, or extra Python `sqlite3` execution path.
   - `db/dbapi/_sync.py` and `_async.py` own their respective generic connections, cursors, and module facades; `_errors.py` adapts the canonical database exception hierarchy. Preserve the established native execution contracts: `execute` and `execute_many` return affected counts; `execute_tuples` returns tuple rows; `execute_with_description` returns `(tuple_rows, column_names)`; `select_to_arrow` returns `ArrowCursor`. Preserve required DB-API tuple fetching, `description`, `rowcount`, `lastrowid`, parameters, transaction/cancel/error behavior, and `execute_arrow_shm`/ArrowCursor behavior where exposed. Broker transport adapts these contracts without adding a new public result API.
   - Move SQLAlchemy `AsyncAdapt_dbapi_connection`, await/greenlet adaptation, and SQLAlchemy-specific cursor/connection wrapping into `extensions/sqlalchemy/async_adapter.py`. SQLAlchemy async dialects and pool creators use that adapter; their SQLAlchemy-facing connect contract stays intact. The neutral async DB-API exposes the async driver/connection behavior without SQLAlchemy wrapping.
   - Add `extensions/sqlalchemy/pool.py` (`BrokerPool` + `creator` helper) so SQLAlchemy and Advanced Alchemy sync/async configs share broker-owned pools. Pool and SQLAlchemy adaptation depend downward on `db/dbapi/` and generic lease contracts; the shared package never imports them. Reuse existing native contract/config metadata for observed transaction requirements; resolve the contract research below before adding the minimum engine-neutral metadata needed, without policy registries, class hierarchies, or speculative capability flags.
5. **Crash reclamation & observability.**
   - Reclaim crashed worker leases (`reclaim_worker`), issue server-side `ROLLBACK` before returning connections to the pool, enforce idle/abandoned lease timeouts, and expose per-pool metrics via `BrokerPlugin.snapshot_metrics` (`zpages`) and accurate documentation in `docs/usage/extensions.md` and `docs/architecture/pyo3-and-workers.md`.

## Public Seams

`ProxySyncConfig`, `ProxyAsyncConfig`, `BrokerPool`, `SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, `SyncConnectionLease`, `AsyncConnectionLease`, `vertebra.db.dbapi`, `BrokerPlugin`, and `BrokerConfig`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`), `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`), the completed [`driver-fail-loud` baseline](../../knowledge/domains/data/database-drivers-and-dialers.md) (deletes the admission-only `*Pool` structs in `vertebra-db`; this flow starts from that baseline), and `native-postgres-driver` (first native Rust pool hosted by the broker).

## Acceptance Boundary

- DB-API contract tests exercise tuple results, empty query descriptions, affected counts, `lastrowid` where supported, iteration/fetch positions, bound parameters, native errors, transaction/cancel handling, and Arrow/lease release. Import checks prove `db/dbapi/` has no upward or engine-specific dependencies; invalid URLs fail without creating SQLite state. SQLAlchemy async tests separately verify its connect/await adaptation.
- Integration tests verify an unmodified upstream `AsyncpgConfig` and an Advanced Alchemy `SQLAlchemyAsyncConfig` running across 4 worker processes open a single shared pool in `pg_stat_activity`, isolate concurrent transactions per lease, and roll back + release leases when a worker process `SIGKILL`s mid-transaction.
- `make lint-all`, `make rs-test`, `make py-test`, and `make docs` pass.

## Contract Research Before Task Refinement

- Map each existing consumer's native and DB-API execution needs before refining t05. The current generic `execute_with_description` provides rows and names, while DB-API also needs statement-kind handling, affected counts, and optional generated IDs. Confirm how native clients and broker frames provide these values for empty SELECTs, DDL/DML, `RETURNING`, and multi-result statements. Do not infer columns from the first row, fabricate generated IDs, or introduce a new result object to avoid this analysis.
- Verify the current sync/async SQL classifier (`_is_row_returning_sql`) against supported statements and upstream dialect expectations. Determine the existing driver or transport metadata that identifies result-bearing statements, including CTEs, comments, and `RETURNING`; keep any engine-specific resolution on the owning adapter. Record any genuine shared-contract gap explicitly before implementation.
- Reconcile transaction/autocommit requirements across native BigQuery/Spanner drivers, relational drivers, generic DB-API, and SQLAlchemy's adapter. Locate existing contract/config metadata and supported transaction methods; record the precise missing datum if one exists. Do not design a generic policy registry in this charter.
- Trace broker lease access and transport dependencies to keep the neutral package free of `vertebra.extensions.broker` imports. Confirm the existing native/generic lease boundary used by the shared DB-API; record any extraction prerequisite rather than adding a speculative transport abstraction.
- Trace `AsyncDBAPIModule.connect` and the currently embedded `AsyncAdapt_dbapi_connection` wrapper against installed SQLAlchemy call sites. Specify the neutral connect signature and extension wrapper adaptation needed to preserve SQLAlchemy behavior without making the neutral module depend upward.
- Confirm DB-API `description` type-code/nullability requirements and dialect-specific module attributes used by supported SQLAlchemy and Django versions. Put framework expectations on their owning adapters; preserve real metadata where the underlying driver already supplies it.

These are planning research gaps, not runtime acceptance evidence. Resolve them into executable worksheets before implementing t05; source location changes alone do not close them.

## Draft Task Outline

- t01 Per-`pool_key` `ShmLeaseTable` partitions, deterministic hashing, lease heartbeats, and dead `SyncInMemoryBrokerTransport`/`AsyncInMemoryBrokerTransport`/`StatementCache` cleanup.
- t02 `vertebra-core/src/dbrpc/` ring RPC protocol + `BrokerEngineBackend` dynamic pool registration + `VTBARROW` streaming + cancellation.
- t03 Session/transaction affinity (`SyncConnectionLease` / `AsyncConnectionLease`) and standalone + supervisor broker host lifecycle.
- t04 `ProxySyncConfig`/`ProxyAsyncConfig` and `ProxySyncDriver`/`ProxyAsyncDriver` on `SyncDriverAdapterBase`/`AsyncDriverAdapterBase`.
- t05 Extract the shared `SyncDBAPI*` / `AsyncDBAPI*` directly into `db/dbapi/{__init__,_sync,_async,_errors}.py`, preserve verified execution/cursor/Arrow contracts, isolate SQLAlchemy wrapping in `extensions/sqlalchemy/async_adapter.py`, and implement `extensions/sqlalchemy/pool.py::BrokerPool` for SQLAlchemy & Advanced Alchemy.
- t06 Worker crash reclamation (`reclaim_worker` + server-side `ROLLBACK`) and multi-process integration tests.
- t07 ZPages pool metrics, `docs/`, and OKF `broker-and-shm-leases.md` update.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs` — Shared-memory ring broker lease table and pool partitioning.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py` — SyncSHMBrokerTransport and AsyncSHMBrokerTransport worker transports.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py` — ProxySyncDriver and ProxyAsyncDriver upstream SQLSpec wrappers.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud baseline removing unused admission-only `*Pool` structs from `vertebra-db`.
