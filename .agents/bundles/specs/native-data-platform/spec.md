---
type: Spec
flow_id: native-data-platform
title: Native Data Platform (Shared Broker Pools, Rust Drivers, Storage)
description: Master PRD for shared connection pools across worker processes through the shared-memory ring broker and feature-complete native Rust database and object-storage drivers packaged as per-driver PyO3 wheels across every supported engine and framework integration.
status: draft
state: planned
has_chapters: false
parent_prd: null
research:
  - research/gap-analysis_20261103.md
  - research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 11
plan_commit: null
state_revision: 7
current_task: null
last_verified_checkpoint: c8722c76
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified: []
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: Shared-memory ring broker pool admission and worker lease primitives
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
    title: VTBARROW shared-memory IPC result allocator
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: Polymorphic DatabaseRegistry and per-engine adapter resolution
  - id: fail-loud
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud driver, codec, and install-hint baseline
  - id: postgres
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Standalone vertebra._postgres PyO3 module on main ahead of the Python driver cutover
  - id: postgres-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python PostgreSQL driver still executing through psycopg_pool until the native cutover
  - id: notification-source
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource capability protocol split into SyncNotificationSource and AsyncNotificationSource by queues-tasks-and-channels
  - id: datafusion
    resource: src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine
    title: Embedded DataFusion federation engine loading the datafusion Python package
  - id: mysql
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: Standalone vertebra._mysql PyO3 module
  - id: mssql
    resource: src/rs/vertebra-mssql/src/lib.rs
    title: Standalone vertebra._mssql PyO3 module
  - id: oracle
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: Standalone vertebra._oracle PyO3 module
tags:
  - prd
  - drivers
  - rust
  - broker
  - storage
  - postgres
  - oracle
  - mysql
---

# Master PRD: Native Data Platform

<!-- truth: start -->
- **Per-Engine Optional PyO3 Wheels**: Each database engine compiles as an isolated optional PyO3 extension (`src/rs/vertebra-<engine>` -> `vertebra._<engine>`, `vertebra-core[<engine>]`) sharing pure-Rust `vertebra-db` (`rlib`) codecs, keeping `vertebra-core` (`vertebra._vertebra`) and `vertebra-http` free of engine SDKs [^db-registry].
- **Merged Engine Wheels on `main @ c8722c76`**: DuckDB (`14aa83b8`, [#105](https://github.com/cofin/vertebra/pull/105)), MySQL (`e86d8daf`, [#106](https://github.com/cofin/vertebra/pull/106)), SQL Server (`134546d4`, [#107](https://github.com/cofin/vertebra/pull/107)), and Oracle (`47da6820`, [#103](https://github.com/cofin/vertebra/pull/103)) execute through their standalone wheels; BigQuery and Spanner run on standalone wheels from `db-layout-and-contract` [^mysql] [^mssql] [^oracle].
- **PostgreSQL Wheel Landed, Cutover Pending**: `vertebra-postgres` (`vertebra._postgres`, [#104](https://github.com/cofin/vertebra/pull/104) at `c8722c76`) is on `main`, while `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` still execute through `psycopg_pool` until `native-postgres-driver` Chapter 27 cuts them over [^postgres] [^postgres-py].
- **Shared Broker Pools & `VTBARROW` Plane (target state)**: The broker process owns server connections partitioned by `BaseDatabaseConfig.pool_key()`, and workers lease sessions over SHM rings with zero-copy `VTBARROW` result transfer; `broker-pool-sharing` builds this [^core-broker] [^arrow-shm].
<!-- truth: end -->

Baseline: `main @ c8722c76`. Merged squash commits: `364c7cbe` `critical-correctness-fixes`, `984ce732` `gcp-wire-protocol-correctness`, `abd33ab6` `gcp-driver-resilience` ([#73](https://github.com/cofin/vertebra/pull/73)), `f7417015` `Sync*` / `Async*` split ([#75](https://github.com/cofin/vertebra/pull/75)), `21c99f95` `db-layout-and-contract` ([#76](https://github.com/cofin/vertebra/pull/76)), `80ff8a65` `db-engine-primitives` ([#77](https://github.com/cofin/vertebra/pull/77)), `2b737240` post-merge remediation ([#80](https://github.com/cofin/vertebra/pull/80)), `d2fa5072` `runtime-structure-and-throughput` ([#92](https://github.com/cofin/vertebra/pull/92)), `077a3fe9` `mcp-webmcp-core` ([#94](https://github.com/cofin/vertebra/pull/94)), `067fa901` `storage-completeness` and `storage-remediation` ([#101](https://github.com/cofin/vertebra/pull/101)), `af1b3d30` `driver-fail-loud` ([#102](https://github.com/cofin/vertebra/pull/102)), `1b8449fe` build packaging ([#108](https://github.com/cofin/vertebra/pull/108)), `14aa83b8` `native-duckdb-driver` ([#105](https://github.com/cofin/vertebra/pull/105)), `e86d8daf` `native-mysql-driver` ([#106](https://github.com/cofin/vertebra/pull/106)), `134546d4` `native-mssql-driver` ([#107](https://github.com/cofin/vertebra/pull/107)), `47da6820` `native-oracle-driver` ([#103](https://github.com/cofin/vertebra/pull/103)), and `c8722c76` the `vertebra-postgres` wheel ([#104](https://github.com/cofin/vertebra/pull/104)). Sibling PRD: [integration-remediation](../integration-remediation/spec.md), which consumes this platform.

## 1. North Star Goal & Problem Framing

### Objective
Two product commitments, both feature complete:

1. **Shared pools through the ring broker** [^core-broker]. Any existing Advanced Alchemy / SQLAlchemy config or SQLSpec config, including upstream configs such as `sqlspec.adapters.asyncpg.AsyncpgConfig` and Advanced Alchemy `SQLAlchemyAsyncConfig`, routes through Vertebra's shared-memory ring broker so every worker process shares one connection pool.
2. **Vertebra's own high-performance Rust drivers packaged as modular per-engine wheels** [^db-registry]. Rust drivers back every engine: PostgreSQL, MySQL, Oracle, MSSQL, SQLite, DuckDB, BigQuery, Spanner, and Redis/Valkey. Each database engine compiles as its own optional PyO3 extension crate (`src/rs/vertebra-<engine>` -> `vertebra._<engine>`) and install extra (`vertebra-core[<engine>]`), keeping `vertebra-core` (`vertebra._vertebra`), shared `vertebra-db` (`rlib`), and `vertebra-http` free of engine wire, gRPC, or C++ client trees. Object storage (`vertebra-storage` / `vertebra-core[storage]`) and the Arrow/SHM data plane are equally complete [^arrow-shm], and every integration (SQLSpec adapters, Advanced Alchemy / SQLAlchemy dialects, Django backends, queues, channels, stores) runs on these drivers.

### Current state and limits
- **PostgreSQL.** The standalone `vertebra-postgres` wheel (`src/rs/vertebra-postgres`, `vertebra._postgres`, `[postgres]` extra installing `vertebra-postgres==0.3.0`) is on `main` with transport, SCRAM and channel binding, Cloud SQL / AlloyDB TLS and IAM, typed binary codecs, physical pools, single-`Sync` pipelines, cancellation and deadlines, binary `COPY`, native Arrow batches over `VTBARROW`, live `LISTEN`/`NOTIFY` listeners, and `CopyBoth` / `pgoutput` relation decoding [^postgres]. `PostgresNotificationSource` publishes and listens through the native `AsyncPostgresListener`. `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` still execute queries through `psycopg_pool`, `framework/queues/postgres.py::PostgreSQLQueueBackend` probes `psycopg`, and `vertebra-db/src/postgres/mod.rs` keeps its frame encoders until the cutover [^postgres-py]. `native-postgres-driver` (Chapter 22 active, `80/118` tasks) finishes `pgoutput` envelopes, replication feedback, change sources, the permanent native drivers, and the atomic cutover that deletes `psycopg` / `asyncpg`.
- **MySQL, SQL Server, Oracle, and DuckDB.** These engines execute exclusively through their merged standalone wheels (`vertebra._mysql`, `vertebra._mssql`, `vertebra._oracle`, `vertebra._duckdb`) and raise `MissingDependencyError` naming `vertebra-core[<engine>]` when the wheel is absent [^mysql] [^mssql] [^oracle] [^fail-loud]. `driver-conformance-ci` adds the live `pytest-databases` CI lanes and engine-specific edge suites that prove them against real servers.
- **SQLite and DataFusion.** SQLite executes through Python `sqlite3` until `native-sqlite-driver`. `DataFusionEngine` loads the `datafusion` Python package for execution and propagates missing-dependency and query failures; DataFusion is an embedded Arrow federation engine rather than a database client (see Open Decisions) [^datafusion].
- **BigQuery and Spanner.** `vertebra._bigquery` and `vertebra._spanner` provide standalone client bindings with official Google Cloud Rust SDKs, Arrow SHM transfer, panic/cancel-safe completion, and canonical `GoogleCloudError` / `DatabaseError` exceptions. The [database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md) defines the shared contract, and [ADR-004](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) records the wheel and ownership boundary.
- **Redis/Valkey.** The Python drivers execute over Python sockets and the `vertebra_db::resp3` bindings on `vertebra._vertebra`, and reject `memory://` URLs with `ImproperConfigurationError`. `framework/queues/redis.py::RedisQueueBackend` and `ValkeyQueueBackend` build clients through `redis.asyncio`. `native-redis-driver` replaces both.
- **Broker.** `vertebra-core/src/broker.rs`, `vertebra-py/src/broker.rs`, `extensions/broker/transport.py::{SyncSHMBrokerTransport, AsyncSHMBrokerTransport}`, and `extensions/sqlspec/plugin.py::{ProxySyncConfig, ProxyAsyncConfig}` perform slot admission without owning server connections or serving `BrokerRpcRequest` frames across workers (addressed in Chapter 3 `broker-pool-sharing`).

### What is new and why it succeeds
- Each engine gets a complete Rust client in its own PyO3 crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, sharing `vertebra-db` `rlib` primitives for `connection.rs`, `dialer.rs`, `arrow_shm.rs`, `gcp`, and `py_async.rs`) behind one unified Python `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract.
- Separate `Sync*` and `Async*` PyO3 and Python classes (`Sync<Engine>Client` / `Async<Engine>Client`, `Sync<Engine>Driver` / `Async<Engine>Driver`, `SyncCompletionQueue` / `AsyncCompletionQueue`, `SyncObjectStore` / `AsyncObjectStore`) expose plain un-suffixed method names in each execution mode without mixed `*_async` methods or runtime awaitable inspection.
- The broker process owns the real connections per `BaseDatabaseConfig.pool_key()`, and workers lease sessions over shared memory with `VTBARROW` zero-copy result transfer.
- Upstream configs are wrapped rather than reimplemented, so users keep existing Advanced Alchemy / SQLSpec configurations and gain cross-worker pool sharing and Rust I/O.

### Success metrics
- No production driver imports a third-party Python database client or opens SQLite for a non-SQLite engine. The embedded `DataFusionEngine` and its `datafusion` package are exempt because DataFusion is an in-process Arrow query engine, not a database client (recommended option, pending confirmation in Open Decisions).
- `vertebra-core` (`vertebra._vertebra`), `vertebra-db` (`default = []`), and `vertebra-http` carry zero database client or gRPC SDK dependencies; installing `vertebra-core[<engine>]` installs `vertebra-<engine>` (`vertebra._<engine>`).
- Each engine passes `src/py/tests/integration/db/test_driver_conformance.py` against real servers (`pytest-databases`) in CI with zero skips (`driver-conformance-ci` for merged wheels; each remaining driver flow for its own engine).
- An unmodified upstream `AsyncpgConfig`, `OracleAsyncConfig`, or Advanced Alchemy `SQLAlchemyAsyncConfig` wrapped by Vertebra runs across N workers with one broker-owned pool, verified by server-side connection counts.
- `driver.pipeline()` sends N statements in one round trip on PostgreSQL; on Oracle the same API executes operations sequentially until `oracledb` supports 26ai pipelining.
- `make bench-check` confirms Rust drivers meet or exceed the Python clients they replace.

### Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Oracle TTC thin client (`oracledb =26.0.0-beta.5`) is synchronous and lacks 26ai pipelining | Connections run on dedicated Rust worker threads bridged through `SyncCompletionQueue` (`SyncOracleClient`) and `AsyncCompletionQueue` + `Notifier` (`AsyncOracleClient`), with sequential `SyncOraclePipeline` / `AsyncOraclePipeline` behind `SyncPipelineCapable` / `AsyncPipelineCapable` and an upstream-support tracking test |
| PyO3 `cdylib` symbol isolation across per-driver `.so` extensions | Share pure-Rust helpers via `vertebra-db` (`rlib`, `pyo3` feature), dynamically import `DatabaseError` / `GoogleCloudError` / `TaskPanicError` classes registered once on `vertebra._vertebra`, key each extension's completion drainer attribute uniquely (`_vertebra_<engine>_completion_drainer`), and cross wheel boundaries through C-ABI capsules (`GcpTokenCacheCapsule` precedent) rather than per-`cdylib` process globals |
| Behavior differences versus mature Python clients | Shared cross-engine conformance suite (`test_driver_conformance.py`) plus engine-specific edge suites (`NULL`s, `Decimal` precision, time zones, LOBs, Unicode) running live in CI (`driver-conformance-ci`) |
| PostgreSQL cutover touches every consumer at once | `native-postgres-driver` Chapter 27 performs one atomic cutover with a repository-wide `psycopg` / `asyncpg` absence gate, followed by PG14–18 live conformance |
| Broker single point of failure | Supervisor restart plus worker crash lease reclamation (`reclaim_worker`) with server-side transaction rollback tests |

## 2. Locked Architectural Seams

1. **Layout & per-driver wheels** [^db-registry].
   - Python engine code lives in `vertebra/db/adapters/<engine>/` (`config.py`, `core.py`, `driver.py`, plus engine-local primitives `pipeline.py`, `notify.py`, `queue.py`, `change_stream.py`, `embeddings.py`), loading `vertebra._<engine>` lazily via `vertebra.utils.module_loader.ensure_module("vertebra._<engine>", package="vertebra-core[<engine>]", extra="<engine>")`.
   - Engine-agnostic Python code stays directly under `vertebra/db/` (`driver/`, `registry.py`, `arrow.py`, `cdc.py`, `rls.py`, `dialer/`, and the framework-neutral `dbapi/` that `broker-pool-sharing` extracts from `extensions/sqlalchemy/dialects.py`).
   - Shared Rust primitives and pure-CPU wire/type codecs live in `vertebra-db` (`rlib`: `<engine>/mod.rs`, `arrow_shm.rs`, `connection.rs`, `dialer.rs`, optional `gcp.rs`, optional `py_async.rs`), while live engine I/O and PyO3 classes (`Sync<Engine>Client`, `Async<Engine>Client`, plus `#[pymodule(gil_used = false)]`) live in dedicated workspace crates `src/rs/vertebra-<engine>` (`vertebra._<engine>`, `src/py/vertebra/_<engine>.pyi`) paired with `[project.optional-dependencies].<engine>` (`valkey` installing `vertebra-redis`, `storage` installing `vertebra-storage`).
2. **Driver contract & `Sync*` / `Async*` split**.
   - Every dual-mode primitive splits into explicit `Sync*` and `Async*` types (`SyncDatabaseDriver` / `AsyncDatabaseDriver`, `SyncCompletionQueue` / `AsyncCompletionQueue`, `Sync<Engine>Client` / `Async<Engine>Client`, `Sync<Engine>Transaction` / `Async<Engine>Transaction`, `Sync<Engine>Pipeline` / `Async<Engine>Pipeline`, `SyncPipelineCapable` / `AsyncPipelineCapable`, `SyncChangeSource` / `AsyncChangeSource`, `SyncCloudSQLDialer` / `AsyncCloudSQLDialer`, `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`, `SyncObjectStore` / `AsyncObjectStore`) with plain method names and zero compatibility aliases.
   - Notification sources follow the same split: `vertebra/db/driver/capabilities.py` defines `SyncNotificationSource` (blocking `publish`, `subscribe`, `unsubscribe`, `listen() -> Iterator[NotificationEvent]`) and `AsyncNotificationSource` (the current `async` methods with `listen() -> AsyncIterator[NotificationEvent]`), and engine implementations ship as `SyncPostgresNotificationSource` / `AsyncPostgresNotificationSource`, `SyncOracleNotificationSource` / `AsyncOracleNotificationSource`, and `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` [^notification-source]. Synchronous consumers (Django, Flask, synchronous queue workers, and CLI listeners) use the `Sync*` types; `ChannelBus`, Litestar and Starlette channels, and async queue wakeups use the `Async*` types. `queues-tasks-and-channels` owns the rename of the existing unprefixed `NotificationSource`, `PostgresNotificationSource`, and `OracleNotificationSource` with no compatibility aliases. `QueueStore` keeps a single unprefixed protocol because every method is `async` and no synchronous consumer exists.
   - Methods on `SyncDatabaseDriver` and `AsyncDatabaseDriver` in `vertebra/db/driver/`:
      - `execute` (`-> int`), `execute_many` (`-> int`), `execute_tuples`, `execute_with_description` (`-> (rows, col_names)`), `select`, `select_one` (raises `NotFoundError`), `select_one_or_none`, `select_value` (raises `NotFoundError`), `select_to_arrow` (`-> ArrowCursor`), `stream_arrow_batches`, `load_from_arrow`
      - `transaction()` context manager, `begin()`, `commit()`, `rollback()`
      - optional `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`)
      - `cancel()` and `close()`
      - typed `DatabaseError` hierarchy registered on `vertebra._vertebra` carrying `sqlstate`, `vendor_code`, and `is_transient`
3. **Broker ownership** [^core-broker] [^arrow-shm].
   - The broker process owns real Rust connections partitioned per `BaseDatabaseConfig.pool_key()` (loading engine pools from installed `vertebra-<engine>` crates on demand).
   - Workers lease connections over SHM rings (`SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport`) for session/transaction scopes, streaming result sets zero-copy via `VTBARROW`.
   - Upstream SQLSpec configs wrap via `ProxySyncConfig`/`ProxyAsyncConfig`; Advanced Alchemy / SQLAlchemy configs use `BrokerPool` + `creator` over `vertebra.db.dbapi` (`SyncDBAPIModule` / `AsyncDBAPIModule`), extracted directly from the existing dialect module in Chapter 3 `broker-pool-sharing`. SQLAlchemy-specific async wrapping lives in `extensions/sqlalchemy/async_adapter.py`; Django later consumes the neutral package without a SQLAlchemy dependency. The shared DB-API consumes generic registry/driver/lease contracts and contains no engine dispatch, upward imports, or SQLite fallback. Existing native execution, DB-API cursor, and Arrow/lease contracts remain required; unresolved metadata and async-connect details are researched before Chapter 3 task refinement rather than replaced with a new result API or policy registry.
4. **Pipelining.** PostgreSQL implements `SyncPipelineCapable.pipeline()` / `AsyncPipelineCapable.pipeline()` natively as an extended-protocol batch with a single `Sync`. Oracle exposes `SyncOraclePipeline` / `AsyncOraclePipeline` executing sequentially until `oracledb` ships 26ai pipelining.
5. **Storage.** `vertebra-storage` (`vertebra-core[storage]`) `SyncObjectStore` and `AsyncObjectStore` (`gs`, `s3`, `r2`, `oci`, `az`, `file`) are the single implementation behind SQLSpec `ObjectStoreProtocol`, Advanced Alchemy `StorageBackend`, Django `Storage`, Litestar static/file responses, and Arrow Parquet import/export, sharing `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`.

## 3. Master Roadmap (Ordered Child Flows)

| Ch | Flow ID | Title | Key Deliverables | Depends on | State |
| --- | --- | --- | --- | --- | --- |
| 1 | `db-layout-and-contract` | Adapter package layout, GCP wheel extraction, driver contract, and conformance suite | Package move (`db/adapters/<engine>`, `vertebra-db/src/<engine>`), `connection.rs` + `py_async.rs`, `DatabaseError` hierarchy on `_vertebra`, standalone `vertebra-bigquery` & `vertebra-spanner` wheels + `[bigquery]`/`[spanner]`/`[storage]` extras, unified `Sync*`/`Async*` driver contract & capability protocols, polymorphic `DatabaseRegistry` + `provide_driver`, import gate, conformance suite | integration-remediation `critical-correctness-fixes`, `gcp-wire-protocol-correctness`, `gcp-driver-resilience` | completed (`21c99f95`) |
| 2 | `native-postgres-driver` | Rust PostgreSQL client (`vertebra-postgres`) | `src/rs/vertebra-postgres` (`vertebra._postgres`, `[postgres]` extra) on `main` at `c8722c76`; remaining: `pgoutput` envelopes, replication feedback and change sources, permanent native Python drivers, atomic cutover deleting `psycopg`/`asyncpg`, PG14–18 conformance, wheel CI and benchmarks, docs and OKF | `driver-fail-loud`, `db-layout-and-contract`; integration-remediation `db-engine-primitives` | active (`ch22`, `80/118` tasks) |
| 3 | `broker-pool-sharing` | Shared pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config | Broker-owned pools, `dbrpc` SHM frames, session/transaction affinity, `VTBARROW` streaming, `ProxySync/AsyncConfig` for upstream configs, `db/dbapi/` (`SyncDBAPI*`/`AsyncDBAPI*`) with SQLAlchemy-owned `extensions/sqlalchemy/{async_adapter,pool}.py`, crash reclamation | `native-postgres-driver` | draft |
| 4 | `native-oracle-driver` | Rust Oracle thin client (`vertebra-oracle`) | `src/rs/vertebra-oracle` (`vertebra._oracle`, `[oracle]` extra): `oracledb =26.0.0-beta.5` worker threads, TNS/wallets/TLS, LOBs, OSON/VECTOR, DRCP, sequential `pipeline()`, `VTBARROW`, TxEventQ notifications | `driver-fail-loud` | completed (`47da6820`) |
| 5 | `native-mysql-driver` | Rust MySQL and MariaDB client (`vertebra-mysql`) | `src/rs/vertebra-mysql` (`vertebra._mysql`, `[mysql]` extra, `mysql_async` rustls + `ring`, binary protocol, Cloud SQL IAM, `KILL QUERY` cancel, `VTBARROW`) | `driver-fail-loud` | completed (`e86d8daf`) |
| 6 | `native-mssql-driver` | Rust SQL Server client (`vertebra-mssql`) | `src/rs/vertebra-mssql` (`vertebra._mssql`, `[mssql]` extra, `tiberius` rustls, TDS 7.4/8.0, `sp_executesql` RPC, `BulkLoad`, `ATTENTION` cancel, `VTBARROW`) | `driver-fail-loud` | completed (`134546d4`) |
| 7 | `native-sqlite-driver` | Rust SQLite client (`vertebra-sqlite`) | `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `[sqlite]` extra, bundled `rusqlite`, worker threads, WAL/mmap pragmas, `sqlite3_interrupt` cancel, `VTBARROW`); stdlib `sqlite3` removed from the SQLite adapter | `driver-fail-loud`, `db-layout-and-contract` | draft |
| 8 | `native-duckdb-driver` | Rust DuckDB client (`vertebra-duckdb`) | `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `[duckdb]` extra, bundled `duckdb`, `Appender`, `DuckDBSecretConfig`) | `driver-fail-loud` | completed (`14aa83b8`) |
| 9 | `native-redis-driver` | Rust Redis and Valkey client (`vertebra-redis`) | `src/rs/vertebra-redis` (`vertebra._redis`, `[redis]`/`[valkey]` extras, `redis` 1.8 RESP3 client with cluster, Sentinel, pub/sub, client tracking); Python sockets, `vertebra_db::resp3` bindings, and `redis.asyncio` queue clients removed | `driver-fail-loud`, `db-layout-and-contract` | draft |
| 10 | `storage-completeness` | Feature-complete storage (`vertebra-storage`) | `SyncObjectStore`/`AsyncObjectStore` streaming/pagination/signing behind SQLSpec, Advanced Alchemy, Django, and Litestar contracts | `db-layout-and-contract` | completed (`067fa901`) |
| 10a | `storage-remediation` | Native GCS data-plane, cross-extension token capsule, zero-copy FFI, and storage throughput hardening | `GcpTokenCacheCapsule`, GCS conditional writes and emulator endpoints, zero-copy `PyBackedBytes`, streaming OCI, bulk transfers, `glob`, `compose`, `open_reader` / `open_writer` | `storage-completeness` | completed (`067fa901`) |
| 11 | `driver-fail-loud` | Fail-loud driver baseline | `memory://` fakes removed, truthful install hints with an extras gate, explicit conformance skip/fail outcomes, collaborator-required queue backends | `db-layout-and-contract` | completed (`af1b3d30`) |
| 12 | `driver-conformance-ci` | Live driver conformance CI | Live `pytest-databases` CI lanes for the merged DuckDB, MySQL, MariaDB, SQL Server, and Oracle wheels, engine-specific live edge suites, zero-skip enforcement, and DuckDB `httpfs` GCS ADC bearer-token secret rotation ([spec.md](../driver-conformance-ci/spec.md)) | `native-duckdb-driver`, `native-mysql-driver`, `native-mssql-driver`, `native-oracle-driver` (all merged) | planned |
| 13 | `storage-integrity-and-streaming` | Storage integrity and streaming | Object-storage integrity and end-to-end streaming follow-ups to the merged storage plane across `vertebra-storage` and its framework consumers ([spec.md](../storage-integrity-and-streaming/spec.md)) | `storage-completeness`, `storage-remediation` (merged) | planned |

### Cross-PRD execution order
1. **Merged on `main @ c8722c76`**: `critical-correctness-fixes`, `gcp-wire-protocol-correctness`, `gcp-driver-resilience`, the `Sync*` / `Async*` split, `db-layout-and-contract`, `db-engine-primitives`, post-merge remediation, `runtime-structure-and-throughput`, `mcp-webmcp-core`, `storage-completeness`, `storage-remediation`, `driver-fail-loud`, build packaging, `native-duckdb-driver`, `native-mysql-driver`, `native-mssql-driver`, `native-oracle-driver`, and the `vertebra-postgres` wheel.
2. **Active**: `native-postgres-driver` (Ch2) through the Python cutover and release gates.
3. **Independent of the PostgreSQL cutover**: `driver-conformance-ci` (Ch12) and `storage-integrity-and-streaming` (Ch13) start from merged code.
4. **After the PostgreSQL cutover**: `broker-pool-sharing` (Ch3).
5. **Per-engine driver stack (one PR per engine)**: `native-sqlite-driver` (Ch7) and `native-redis-driver` (Ch9), followed by integration-remediation `sqlalchemy-and-django-integration`, `sqlspec-adapter-parity`, and `queues-tasks-and-channels`.

## 4. Global Constraints

The global constraints from [integration-remediation](../integration-remediation/spec.md) apply. In addition:

- No third-party Python database client or SQLite stand-in in production driver paths once an engine's chapter completes.
- Every Rust driver crate (`src/rs/vertebra-<engine>`) exposes `#[pymodule(gil_used = false)]`, ships a complete `.pyi` stub verified by `mypy.stubtest vertebra._<engine>`, registers `[project.optional-dependencies].<engine>`, `[tool.uv.sources]`, and `[[tool.bumpversion.files]]`, and passes golden wire/stub tests plus `pytest-databases` integration coverage in `test_driver_conformance.py`.
- `unsafe` blocks carry `// SAFETY:` comments explaining pointer validity, alignment, and lifetime invariants.
- Async Rust never calls `Python::attach` on Tokio or driver worker threads; completion delivery always routes through `AsyncCompletionQueue` + `Notifier` (`vertebra-db/src/py_async.rs`).
- Plan checkpoints cite squash commits on `main`; feature-branch SHAs orphaned by squash merges are never used as checkpoints.

## 5. Backlog (Deferred)

- **Adapter organization follow-up**: `/flow:plan native-adapter-organization` reconciles concrete consumers before separating BigQuery Object Table operations from connection settings, Spanner mutation values from connection configuration, and redundant DataFusion vendor registration wrappers. Preserve existing public engine values, native errors, Arrow ownership, optional dependencies, and wire behavior. This is bounded planning scope scheduled independently of the PostgreSQL and broker cutovers.
- AWS RDS/Aurora DSQL IAM ([#67](https://github.com/cofin/vertebra/issues/67)) and Azure Entra ID ([#68](https://github.com/cofin/vertebra/issues/68)) auth in the native clients.
- CockroachDB- and AlloyDB-specific protocol extensions beyond PostgreSQL wire compatibility.

## Open Decisions

- **DataFusion and the no-third-party-client metric.** Options: (a) exempt `DataFusionEngine` and its `datafusion` package from the metric; (b) replace it with a native Rust DataFusion wheel. Recommended: (a). DataFusion is an embedded Arrow federation engine that is absent from the per-engine driver list in the objective, holds no server connections, and already fails loud when missing; a native rewrite is scheduled only if a concrete consumer needs it.

## Open Questions

- **Resolved Q5** *(user, 2026-11-04)*: Build each native driver on the official or best-in-class Rust client for its engine ([crate research](research/rust-driver-crates_20261103.md)) and package **every** database engine as its own standalone PyO3 extension crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, matching `vertebra-bundler` and `vertebra-storage`) paired with `[project.optional-dependencies].<engine>` (`vertebra-core[<engine>]`), keeping `vertebra-core` (`vertebra._vertebra`), shared `vertebra-db` (`rlib`), and `vertebra-http` lightweight and free of database client SDKs:

  | Engine | Crate & Optional Wheel Packaging |
  | --- | --- |
  | BigQuery | `google-cloud-bigquery =0.19.0` (`rustls` + `ring`) + Vertebra BigQuery REST in `src/rs/vertebra-bigquery` (`vertebra._bigquery`) + `[bigquery]` extra |
  | Spanner | `google-cloud-spanner =0.34.8-preview` (`rustls` + `ring`) in `src/rs/vertebra-spanner` (`vertebra._spanner`) + `[spanner]` extra |
  | PostgreSQL | `postgres-protocol` 0.6.12 + Vertebra connection in `src/rs/vertebra-postgres` (`vertebra._postgres`) + `[postgres]` extra (`tokio-postgres` as dev test oracle) |
  | Oracle | `oracledb =26.0.0-beta.5` (`arrow`) in `src/rs/vertebra-oracle` (`vertebra._oracle`) + `[oracle]` extra |
  | MySQL | `mysql_async` 0.37.1 (`default-rustls-ring`) in `src/rs/vertebra-mysql` (`vertebra._mysql`) + `[mysql]` extra |
  | MSSQL | `tiberius` 0.13.0 (`rustls`) in `src/rs/vertebra-mssql` (`vertebra._mssql`) + `[mssql]` extra |
  | SQLite | `rusqlite` 0.41.1 (`bundled`) in `src/rs/vertebra-sqlite` (`vertebra._sqlite`) + `[sqlite]` extra |
  | DuckDB | `duckdb` 1.20000.1 (`bundled`, `appender-arrow`) in `src/rs/vertebra-duckdb` (`vertebra._duckdb`) + `[duckdb]` extra |
  | Redis/Valkey | `redis` 1.8.1 (`tokio-rustls-comp`, `cluster-async`, `cache-aio`) in `src/rs/vertebra-redis` (`vertebra._redis`) + `[redis]` and `[valkey]` extras |
  | Object Storage | `object_store` in `src/rs/vertebra-storage` (`vertebra._storage`) + `[storage]` extra |

- **Resolved Q6** *(user, 2026-11-03)*: Oracle ships on the official `oracledb` crate with each connection on a dedicated Rust worker thread (`SyncCompletionQueue` for `SyncOracleClient`, `AsyncCompletionQueue` + `Notifier` for `AsyncOracleClient`); `pipeline()` executes sequentially until `oracledb` adds 26ai pipelining.
- **Resolved notification source shape** *(user, 2026-11-12)*: Notification sources split into `SyncNotificationSource` / `AsyncNotificationSource`, with `Sync<Engine>NotificationSource` / `Async<Engine>NotificationSource` implementations for PostgreSQL, Oracle, and Redis, because Django, Flask, synchronous workers, and CLI listeners consume notifications synchronously. `queues-tasks-and-channels` owns the rename.
- **Resolved Q7** *(user, 2026-11-04)*: All dual-mode Rust and Python primitives use leading `Sync*` and `Async*` prefixes (`SyncCompletionQueue` / `AsyncCompletionQueue`, `Sync<Engine>Client` / `Async<Engine>Client`, `SyncObjectStore` / `AsyncObjectStore`, `Sync<Engine>Dialect` / `Async<Engine>Dialect`) and split into separate sync and async classes with plain method names and zero backward-compatibility aliases (preserving upstream `sqlspec` `<Engine>SyncConfig` / `<Engine>AsyncConfig` infix naming in `vertebra.extensions.sqlspec`).

## Implementation Plan

- [x] [21c99f95] db-layout-and-contract — Adapter layout, standalone GCP wheels, unified contract, and conformance suite
- [x] [af1b3d30] driver-fail-loud — Fail-loud driver baseline ([drivers](../../knowledge/domains/data/database-drivers-and-dialers.md), [queues](../../knowledge/domains/framework/distributed-queues-and-email.md))
- [~] native-postgres-driver — Rust PostgreSQL client; wheel on `main` at `c8722c76`, Chapter 22 active, Python cutover pending ([spec.md](../native-postgres-driver/spec.md))
- [ ] broker-pool-sharing — Shared pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config ([spec.md](../broker-pool-sharing/spec.md))
- [x] [47da6820] native-oracle-driver — Rust Oracle thin client ([drivers](../../knowledge/domains/data/database-drivers-and-dialers.md), [tenancy](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md))
- [x] [e86d8daf] native-mysql-driver — Rust MySQL and MariaDB client ([drivers](../../knowledge/domains/data/database-drivers-and-dialers.md))
- [x] [134546d4] native-mssql-driver — Rust SQL Server client ([drivers](../../knowledge/domains/data/database-drivers-and-dialers.md))
- [ ] native-sqlite-driver — Rust SQLite client ([spec.md](../native-sqlite-driver/spec.md))
- [x] [14aa83b8] native-duckdb-driver — Rust DuckDB client ([drivers](../../knowledge/domains/data/database-drivers-and-dialers.md), [pipelines](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md))
- [ ] native-redis-driver — Rust Redis and Valkey client ([spec.md](../native-redis-driver/spec.md))
- [x] [067fa901] storage-completeness — Feature-complete storage ([knowledge](../../knowledge/domains/storage/object-storage.md))
- [x] [067fa901] storage-remediation — Native GCS data-plane, cross-extension token capsule, zero-copy FFI, and storage throughput hardening ([knowledge](../../knowledge/domains/storage/object-storage.md), [limitations](../../knowledge/domains/storage/object-storage-limitations.md))
- [ ] driver-conformance-ci — Live driver conformance CI and DuckDB GCS ADC secret rotation ([spec.md](../driver-conformance-ci/spec.md))
- [ ] storage-integrity-and-streaming — Storage integrity and streaming ([spec.md](../storage-integrity-and-streaming/spec.md))

## Continuity Snapshot

- **Flow ID:** `native-data-platform` (`state: planned`, `plan_revision: 11`, `plan_commit: null`, `last_verified_checkpoint: c8722c76`)
- **Active Focus:** `native-postgres-driver` is active at `ch22:t03` with `80/118` tasks complete; its wheel is on `main @ c8722c76` and the Python driver cutover (Chapter 27) is pending. `driver-conformance-ci` and `storage-integrity-and-streaming` are planned and independent of the cutover. `broker-pool-sharing`, `native-sqlite-driver`, and `native-redis-driver` are draft charters.
- **Key decisions** *(user, 2026-11-03 – 2026-11-04)*:
  - Shared broker pools for Advanced Alchemy / SQLSpec configs and native Rust drivers across all engines remain required acceptance targets for their owning child flows.
  - Layout: `vertebra/db/adapters/<engine>/` for Python engine code, `vertebra/db/*` for engine-agnostic code, shared `vertebra-db` (`rlib`) for Rust primitives/codecs, and standalone PyO3 extension crates `src/rs/vertebra-<engine>` (`vertebra._<engine>`) + `[project.optional-dependencies].<engine>` per database engine.
  - Leading `Sync*` / `Async*` class split across Rust and Python with zero compatibility aliases.
- **Research:** [gap-analysis_20261103.md](research/gap-analysis_20261103.md) and [rust-driver-crates_20261103.md](research/rust-driver-crates_20261103.md).

[^core-broker]: `src/rs/vertebra-core/src/broker.rs` — Shared-memory ring broker pool admission and worker leases.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named shared-memory allocator for `VTBARROW` IPC frames.
[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — Polymorphic database registry and driver loader.
[^fail-loud]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud driver, codec, and install-hint baseline.
[^postgres]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `vertebra._postgres` PyO3 module on `main` ahead of the Python driver cutover.
[^postgres-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver still executing through `psycopg_pool`.
[^notification-source]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — Current unprefixed asynchronous notification protocol that `queues-tasks-and-channels` splits into `SyncNotificationSource` and `AsyncNotificationSource`.
[^datafusion]: `src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine` — Embedded DataFusion federation engine loading the `datafusion` package.
[^mysql]: `src/rs/vertebra-mysql/src/lib.rs` — Standalone `vertebra._mysql` PyO3 module backing `SyncMySQLDriver` and `AsyncMySQLDriver`.
[^mssql]: `src/rs/vertebra-mssql/src/lib.rs` — Standalone `vertebra._mssql` PyO3 module backing `SyncMSSQLDriver` and `AsyncMSSQLDriver`.
[^oracle]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` PyO3 module backing `SyncOracleDriver` and `AsyncOracleDriver`.
